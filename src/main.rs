use axum::{
    extract::{ConnectInfo, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env, fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    process::Command,
    sync::{oneshot, Mutex, Notify},
    time,
};
use uuid::Uuid;

const CONTROL_ADDR: &str = "127.0.0.1:7372";
const START_PORT: u16 = 7373;
const END_PORT: u16 = 7399;
const PROTOCOL_VERSION: u32 = 1;
const VERSION: &str = env!("CARGO_PKG_VERSION");
type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
    http: reqwest::Client,
    connectors: Arc<Vec<PathBuf>>,
}

struct Inner {
    advertise: String,
    advertise_locked: bool,
    machine: String,
    local_agents: HashMap<String, LocalAgent>,
    remote_agents: HashMap<String, RemoteAgent>,
    peers: HashSet<String>,
    queues: HashMap<String, VecDeque<MeshMessage>>,
    waiters: HashMap<String, oneshot::Sender<Value>>,
}

struct LocalAgent {
    alias: String,
    last_seen: Instant,
}

#[derive(Clone)]
struct RemoteAgent {
    alias: String,
    addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MeshMessage {
    from: String,
    to: String,
    id: String,
    kind: String,
    body: Value,
}

#[derive(Debug, Serialize)]
struct Health {
    ok: bool,
    version: &'static str,
    protocol: u32,
    machine: String,
    advertise: String,
    local_agents: Vec<AgentInfo>,
    remote_agents: Vec<AgentInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInfo {
    id: String,
    alias: String,
    addr: String,
}

#[derive(Debug, Deserialize)]
struct RegisterReq {
    id: String,
    alias: String,
}

#[derive(Debug, Deserialize)]
struct PeerReq {
    addr: String,
}

#[derive(Debug, Deserialize)]
struct LocalSendReq {
    from: String,
    to: String,
    body: Value,
}

#[derive(Debug, Deserialize)]
struct LocalRequestReq {
    from: String,
    to: String,
    body: Value,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Deserialize)]
struct ReplyReq {
    id: String,
    body: Value,
}

#[derive(Debug, Deserialize)]
struct NextQuery {
    agent: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AnnounceReq {
    #[serde(default)]
    protocol: u32,
    addr: String,
    agents: Vec<AgentInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AnnounceResp {
    protocol: u32,
    addr: String,
    peers: Vec<String>,
    agents: Vec<AgentInfo>,
}

#[derive(Debug, Deserialize)]
struct ConnectorEvent {
    #[serde(rename = "type")]
    kind: String,
    addr: Option<String>,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let args: Vec<String> = env::args().collect();
    if args.get(1).is_some_and(|x| x == "self-check") {
        self_check();
        return Ok(());
    }

    let peers = cli_peers(&args);
    run_daemon(peers).await?;
    Ok(())
}

async fn run_daemon(mut peers: Vec<String>) -> AnyResult<()> {
    let machine = machine_name();
    let control_addr: SocketAddr = env::var("PI_MESH_CONTROL_ADDR")
        .unwrap_or_else(|_| CONTROL_ADDR.to_string())
        .parse()?;

    let (network_listener, port) = bind_network().await?;
    let env_advertise = env::var("PI_MESH_ADVERTISE").ok();
    let advertise_locked = env_advertise.is_some();
    let advertise = env_advertise.unwrap_or_else(|| format!("{}:{}", machine, port));
    peers.extend(read_peer_file());
    peers.sort();
    peers.dedup();

    let connectors = Arc::new(find_connectors());

    let state = AppState {
        inner: Arc::new(Mutex::new(Inner {
            advertise: advertise.clone(),
            advertise_locked,
            machine: machine.clone(),
            local_agents: HashMap::new(),
            remote_agents: HashMap::new(),
            peers: peers.into_iter().collect(),
            queues: HashMap::new(),
            waiters: HashMap::new(),
        })),
        notify: Arc::new(Notify::new()),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(35))
            .build()?,
        connectors,
    };

    eprintln!("pi-mesh: control http://{control_addr}");
    eprintln!("pi-mesh: listen  http://0.0.0.0:{port}");
    eprintln!("pi-mesh: addr    {advertise}");

    tokio::spawn(peer_loop(state.clone()));
    tokio::spawn(connector_loop(state.clone(), port));

    let control = Router::new()
        .route("/health", get(health))
        .route("/local/register", post(register))
        .route("/local/unregister", post(unregister))
        .route("/local/list", get(list))
        .route("/local/peer", post(local_peer))
        .route("/local/send", post(local_send))
        .route("/local/request", post(local_request))
        .route("/local/reply", post(local_reply))
        .route("/local/next", get(local_next))
        .route("/local/shutdown", post(local_shutdown))
        .with_state(state.clone());

    let network = Router::new()
        .route("/hello", get(hello))
        .route("/announce", post(announce))
        .route("/msg", post(network_msg))
        .with_state(state);

    let control_listener = TcpListener::bind(control_addr).await?;
    let control_srv = axum::serve(control_listener, control);
    let network_srv = axum::serve(
        network_listener,
        network.into_make_service_with_connect_info::<SocketAddr>(),
    );

    tokio::try_join!(control_srv, network_srv)?;
    Ok(())
}

async fn bind_network() -> std::io::Result<(TcpListener, u16)> {
    let host = env::var("PI_MESH_LISTEN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let start = env::var("PI_MESH_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(START_PORT);

    let end = END_PORT.max(start);
    for port in start..=end {
        if let Ok(listener) = TcpListener::bind((host.as_str(), port)).await {
            return Ok((listener, port));
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "no free pi-mesh port",
    ))
}

async fn health(AxumState(state): AxumState<AppState>) -> Json<Health> {
    Json(health_payload(&state).await)
}

async fn hello(AxumState(state): AxumState<AppState>) -> Json<Health> {
    Json(health_payload(&state).await)
}

async fn health_payload(state: &AppState) -> Health {
    let inner = state.inner.lock().await;
    Health {
        ok: true,
        version: VERSION,
        protocol: PROTOCOL_VERSION,
        machine: inner.machine.clone(),
        advertise: inner.advertise.clone(),
        local_agents: inner
            .local_agents
            .iter()
            .map(|(id, agent)| AgentInfo {
                id: id.clone(),
                alias: agent.alias.clone(),
                addr: inner.advertise.clone(),
            })
            .collect(),
        remote_agents: inner
            .remote_agents
            .iter()
            .map(|(id, agent)| AgentInfo {
                id: id.clone(),
                alias: agent.alias.clone(),
                addr: agent.addr.clone(),
            })
            .collect(),
    }
}

async fn local_shutdown() -> impl IntoResponse {
    tokio::spawn(async {
        time::sleep(Duration::from_millis(50)).await;
        std::process::exit(0);
    });
    (StatusCode::OK, Json(json!({"ok": true})))
}

fn alias_taken(inner: &Inner, id: &str, alias: &str) -> bool {
    inner
        .local_agents
        .iter()
        .any(|(other_id, agent)| other_id != id && agent.alias == alias)
        || inner
            .remote_agents
            .iter()
            .any(|(other_id, agent)| other_id != id && agent.alias == alias)
}

async fn register(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<RegisterReq>,
) -> impl IntoResponse {
    let mut inner = state.inner.lock().await;
    if alias_taken(&inner, &req.id, &req.alias) {
        return (StatusCode::CONFLICT, format!("alias in use: {}", req.alias)).into_response();
    }
    inner.remote_agents.remove(&req.id);
    inner.local_agents.insert(
        req.id.clone(),
        LocalAgent {
            alias: req.alias.clone(),
            last_seen: Instant::now(),
        },
    );
    inner.queues.entry(req.id).or_default();
    state.notify.notify_waiters();
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

async fn unregister(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<RegisterReq>,
) -> impl IntoResponse {
    let mut inner = state.inner.lock().await;
    inner.local_agents.remove(&req.id);
    inner.queues.remove(&req.id);
    state.notify.notify_waiters();
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

async fn list(AxumState(state): AxumState<AppState>) -> Json<Value> {
    let inner = state.inner.lock().await;
    let local: Vec<_> = inner
        .local_agents
        .iter()
        .map(|(id, agent)| json!({"id": id, "alias": agent.alias, "addr": inner.advertise}))
        .collect();
    let remote: Vec<_> = inner
        .remote_agents
        .iter()
        .map(|(id, agent)| json!({"id": id, "alias": agent.alias, "addr": agent.addr}))
        .collect();
    let peers: Vec<_> = inner.peers.iter().cloned().collect();
    Json(json!({"local": local, "remote": remote, "peers": peers, "self": inner.advertise}))
}

async fn local_peer(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<PeerReq>,
) -> impl IntoResponse {
    {
        let mut inner = state.inner.lock().await;
        inner.peers.insert(req.addr.clone());
    }
    let _ = announce_to_peer(&state, &req.addr).await;
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

async fn local_send(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<LocalSendReq>,
) -> impl IntoResponse {
    let msg = MeshMessage {
        from: req.from,
        to: req.to,
        id: Uuid::new_v4().to_string(),
        kind: "send".into(),
        body: req.body,
    };
    match route_message(state, msg, Duration::from_secs(1)).await {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e).into_response(),
    }
}

async fn local_request(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<LocalRequestReq>,
) -> impl IntoResponse {
    let msg = MeshMessage {
        from: req.from,
        to: req.to,
        id: Uuid::new_v4().to_string(),
        kind: "request".into(),
        body: req.body,
    };
    match route_message(state, msg, Duration::from_millis(req.timeout_ms)).await {
        Ok(body) => (StatusCode::OK, Json(json!({"ok": true, "body": body}))).into_response(),
        Err(e) => (StatusCode::GATEWAY_TIMEOUT, e).into_response(),
    }
}

async fn local_reply(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<ReplyReq>,
) -> impl IntoResponse {
    let sender = {
        let mut inner = state.inner.lock().await;
        inner.waiters.remove(&req.id)
    };
    if let Some(sender) = sender {
        let _ = sender.send(req.body);
        (StatusCode::OK, Json(json!({"ok": true}))).into_response()
    } else {
        (StatusCode::NOT_FOUND, "reply id not found").into_response()
    }
}

async fn local_next(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<NextQuery>,
) -> impl IntoResponse {
    let deadline = time::sleep(Duration::from_secs(60));
    tokio::pin!(deadline);

    loop {
        if let Some(msg) = pop_next(&state, &query.agent).await {
            return (StatusCode::OK, Json(json!(msg))).into_response();
        }

        tokio::select! {
            _ = state.notify.notified() => {},
            _ = &mut deadline => return (StatusCode::NO_CONTENT, "").into_response(),
        }
    }
}

async fn pop_next(state: &AppState, agent: &str) -> Option<MeshMessage> {
    let mut inner = state.inner.lock().await;
    inner.queues.get_mut(agent).and_then(|q| q.pop_front())
}

fn reachable_addr(advertised: &str, remote: SocketAddr) -> String {
    let port = advertised
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or(START_PORT);
    SocketAddr::new(remote.ip(), port).to_string()
}

async fn announce(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    AxumState(state): AxumState<AppState>,
    Json(req): Json<AnnounceReq>,
) -> impl IntoResponse {
    if !authorize(&state, addr.ip()).await {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    if req.protocol != PROTOCOL_VERSION {
        return (StatusCode::UPGRADE_REQUIRED, "protocol mismatch").into_response();
    }
    let mut inner = state.inner.lock().await;
    if req.addr != inner.advertise {
        let peer_addr = reachable_addr(&req.addr, addr);
        let mut live = HashSet::new();
        inner.peers.insert(peer_addr.clone());
        for mut agent in req.agents {
            if agent.addr == req.addr {
                agent.addr = peer_addr.clone();
                live.insert(agent.id.clone());
            }
            if !inner.local_agents.contains_key(&agent.id) {
                inner.remote_agents.insert(
                    agent.id.clone(),
                    RemoteAgent {
                        alias: agent.alias,
                        addr: agent.addr,
                    },
                );
            }
        }
        inner
            .remote_agents
            .retain(|id, agent| agent.addr != peer_addr || live.contains(id));
    }
    announce_response(&inner).into_response()
}

async fn network_msg(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    AxumState(state): AxumState<AppState>,
    Json(msg): Json<MeshMessage>,
) -> impl IntoResponse {
    if !authorize(&state, addr.ip()).await {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    let timeout = if msg.kind == "request" {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(1)
    };
    match route_message(state, msg, timeout).await {
        Ok(body) => (StatusCode::OK, Json(json!({"ok": true, "body": body}))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e).into_response(),
    }
}

async fn authorize(state: &AppState, ip: IpAddr) -> bool {
    if ip.is_loopback() || env::var("PI_MESH_INSECURE").ok().as_deref() == Some("1") {
        return true;
    }

    for connector in state.connectors.iter() {
        let Ok(output) = Command::new(connector)
            .arg("auth")
            .arg("--remote-ip")
            .arg(ip.to_string())
            .output()
            .await
        else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let Ok(v) = serde_json::from_slice::<Value>(&output.stdout) else {
            continue;
        };
        if v.get("allow").and_then(Value::as_bool) == Some(true) {
            return true;
        }
    }

    false
}

async fn route_message(
    state: AppState,
    mut msg: MeshMessage,
    timeout: Duration,
) -> Result<Value, String> {
    if let Some(local_id) = resolve_local(&state, &msg.to).await {
        msg.to = local_id;
        return route_local(state, msg, timeout).await;
    }

    let target = resolve_remote(&state, &msg.to).await;

    let Some(agent) = target else {
        return Err(format!("unknown agent: {}", msg.to));
    };

    let url = format!("http://{}/msg", agent.addr);
    let res = state
        .http
        .post(url)
        .json(&msg)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!("peer returned {}", res.status()));
    }
    if msg.kind == "request" {
        let v: Value = res.json().await.map_err(|e| e.to_string())?;
        Ok(v.get("body").cloned().unwrap_or(Value::Null))
    } else {
        Ok(Value::Bool(true))
    }
}

async fn route_local(
    state: AppState,
    msg: MeshMessage,
    timeout: Duration,
) -> Result<Value, String> {
    if msg.kind == "request" {
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = state.inner.lock().await;
            inner.waiters.insert(msg.id.clone(), tx);
            inner
                .queues
                .entry(msg.to.clone())
                .or_default()
                .push_back(msg);
        }
        state.notify.notify_waiters();
        match time::timeout(timeout, rx).await {
            Ok(Ok(body)) => Ok(body),
            _ => Err("request timed out".into()),
        }
    } else {
        {
            let mut inner = state.inner.lock().await;
            inner
                .queues
                .entry(msg.to.clone())
                .or_default()
                .push_back(msg);
        }
        state.notify.notify_waiters();
        Ok(Value::Bool(true))
    }
}

async fn resolve_local(state: &AppState, to: &str) -> Option<String> {
    let inner = state.inner.lock().await;
    if inner.local_agents.contains_key(to) {
        return Some(to.to_string());
    }
    inner
        .local_agents
        .iter()
        .find(|(_, agent)| agent.alias == to)
        .map(|(id, _)| id.clone())
}

async fn resolve_remote(state: &AppState, to: &str) -> Option<RemoteAgent> {
    let inner = state.inner.lock().await;
    if let Some(agent) = inner.remote_agents.get(to) {
        return Some(agent.clone());
    }
    inner
        .remote_agents
        .iter()
        .find(|(_, agent)| agent.alias == to)
        .map(|(_, agent)| agent.clone())
}

fn announce_response(inner: &Inner) -> Json<AnnounceResp> {
    let mut agents: Vec<AgentInfo> = inner
        .local_agents
        .iter()
        .map(|(id, agent)| AgentInfo {
            id: id.clone(),
            alias: agent.alias.clone(),
            addr: inner.advertise.clone(),
        })
        .collect();
    agents.extend(inner.remote_agents.iter().map(|(id, agent)| AgentInfo {
        id: id.clone(),
        alias: agent.alias.clone(),
        addr: agent.addr.clone(),
    }));
    Json(AnnounceResp {
        protocol: PROTOCOL_VERSION,
        addr: inner.advertise.clone(),
        peers: inner.peers.iter().cloned().collect(),
        agents,
    })
}

async fn peer_loop(state: AppState) {
    loop {
        prune_stale_local_agents(&state).await;
        let peers = {
            let inner = state.inner.lock().await;
            let mut peers: Vec<_> = inner.peers.iter().cloned().collect();
            peers.sort();
            peers.dedup();
            peers
        };
        for peer in peers {
            let state = state.clone();
            tokio::spawn(async move {
                if announce_to_peer(&state, &peer).await.is_err() {
                    prune_peer(&state, &peer).await;
                }
            });
        }
        time::sleep(Duration::from_secs(15)).await;
    }
}

async fn prune_stale_local_agents(state: &AppState) {
    let mut inner = state.inner.lock().await;
    let stale: Vec<String> = inner
        .local_agents
        .iter()
        .filter(|(_, agent)| agent.last_seen.elapsed() > Duration::from_secs(45))
        .map(|(name, _)| name.clone())
        .collect();
    for name in stale {
        inner.local_agents.remove(&name);
        inner.queues.remove(&name);
        inner.remote_agents.remove(&name);
    }
}

async fn prune_peer(state: &AppState, peer: &str) {
    let mut inner = state.inner.lock().await;
    inner.peers.remove(peer);
    inner.remote_agents.retain(|_, agent| agent.addr != peer);
}

async fn announce_to_peer(state: &AppState, peer: &str) -> Result<(), String> {
    let req = {
        let inner = state.inner.lock().await;
        if peer == inner.advertise {
            return Ok(());
        }
        AnnounceReq {
            protocol: PROTOCOL_VERSION,
            addr: inner.advertise.clone(),
            agents: inner
                .local_agents
                .iter()
                .map(|(id, agent)| AgentInfo {
                    id: id.clone(),
                    alias: agent.alias.clone(),
                    addr: inner.advertise.clone(),
                })
                .collect(),
        }
    };

    let url = format!("http://{peer}/announce");
    let resp = state
        .http
        .post(url)
        .json(&req)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("announce failed: {}", resp.status()));
    }
    let data: AnnounceResp = resp.json().await.map_err(|e| e.to_string())?;
    if data.protocol != PROTOCOL_VERSION {
        return Err(format!("protocol mismatch: {}", data.protocol));
    }
    let mut inner = state.inner.lock().await;
    if data.addr != inner.advertise {
        inner.peers.insert(peer.to_string());
    }
    for peer in data.peers {
        if peer != inner.advertise {
            inner.peers.insert(peer);
        }
    }
    let mut live = HashSet::new();
    for mut agent in data.agents {
        if agent.addr == data.addr {
            agent.addr = peer.to_string();
            live.insert(agent.id.clone());
        }
        if !inner.local_agents.contains_key(&agent.id) && agent.addr != inner.advertise {
            inner.remote_agents.insert(
                agent.id.clone(),
                RemoteAgent {
                    alias: agent.alias,
                    addr: agent.addr,
                },
            );
        }
    }
    inner
        .remote_agents
        .retain(|id, agent| agent.addr != peer || live.contains(id));
    Ok(())
}

async fn connector_loop(state: AppState, port: u16) {
    loop {
        for connector in state.connectors.iter().cloned() {
            let state = state.clone();
            tokio::spawn(async move {
                run_connector(state, connector, port).await;
            });
        }
        time::sleep(Duration::from_secs(15)).await;
    }
}

async fn run_connector(state: AppState, connector: PathBuf, port: u16) {
    let Ok(output) = Command::new(&connector)
        .arg("run")
        .arg("--port")
        .arg(port.to_string())
        .output()
        .await
    else {
        return;
    };

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Ok(event) = serde_json::from_str::<ConnectorEvent>(line) else {
            continue;
        };
        let Some(addr) = event.addr else {
            continue;
        };
        if event.kind == "self" {
            let mut inner = state.inner.lock().await;
            if !inner.advertise_locked {
                inner.advertise = addr;
            }
        } else if event.kind == "peer" {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = announce_to_peer(&state, &addr).await;
            });
        }
    }
}

fn find_connectors() -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Some(paths) = env::var_os("PATH") else {
        return out;
    };
    for dir in env::split_paths(&paths) {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|x| x.to_str()) else {
                continue;
            };
            if name.starts_with("pi-mesh-") {
                out.push(path);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn machine_name() -> String {
    hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .filter(|s| !s.is_empty())
        .or_else(|| env::var("HOSTNAME").ok())
        .or_else(|| env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "machine".into())
}

fn cli_peers(args: &[String]) -> Vec<String> {
    let mut peers = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--peer" || args[i] == "peer" {
            if let Some(peer) = args.get(i + 1) {
                peers.push(peer.clone());
                i += 1;
            }
        }
        i += 1;
    }
    if let Ok(env_peers) = env::var("PI_MESH_PEERS") {
        peers.extend(
            env_peers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        );
    }
    peers
}

fn read_peer_file() -> Vec<String> {
    let Some(home) = env::var_os("HOME") else {
        return vec![];
    };
    let path = PathBuf::from(home).join(".pi/mesh/peers");
    let Ok(text) = fs::read_to_string(path) else {
        return vec![];
    };
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToString::to_string)
        .collect()
}

fn self_check() {
    let line = r#"{"type":"peer","addr":"127.0.0.1:7373"}"#;
    let event: ConnectorEvent = serde_json::from_str(line).unwrap();
    assert_eq!(event.kind, "peer");
    assert_eq!(event.addr.unwrap(), "127.0.0.1:7373");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connector_peer_event() {
        self_check();
    }

    #[test]
    fn parses_cli_peers() {
        let args = vec![
            "pi-mesh".to_string(),
            "daemon".to_string(),
            "--peer".to_string(),
            "one:7373".to_string(),
            "peer".to_string(),
            "two:7373".to_string(),
        ];
        assert_eq!(cli_peers(&args), vec!["one:7373", "two:7373"]);
    }
}
