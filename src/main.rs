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
    env, fs, io,
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
const PROTOCOL_VERSION: u32 = 3;
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
    title: Option<String>,
    cwd: String,
    runtime: Option<Value>,
    last_seen: Instant,
}

#[derive(Clone)]
struct RemoteAgent {
    alias: String,
    title: Option<String>,
    cwd: String,
    runtime: Option<Value>,
    addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MeshMessage {
    from: String,
    to: String,
    id: String,
    kind: String,
    body: Value,
    from_agent: AgentInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInfo {
    id: String,
    alias: String,
    title: Option<String>,
    cwd: String,
    runtime: Option<Value>,
    addr: String,
}

#[derive(Debug, Deserialize)]
struct RegisterReq {
    id: String,
    alias: String,
    title: Option<String>,
    cwd: String,
    runtime: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct UnregisterReq {
    id: String,
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

struct Cli {
    json: bool,
    args: Vec<String>,
}

#[derive(Debug, PartialEq)]
struct MessageArgs {
    from: String,
    to: String,
    message: String,
    timeout_seconds: Option<u64>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = try_main().await {
        eprintln!("pi-mesh: {error}");
        std::process::exit(1);
    }
}

async fn try_main() -> AnyResult<()> {
    let raw_args: Vec<String> = env::args().skip(1).collect();
    let cli = parse_cli(&raw_args);
    let rest = if cli.args.len() > 1 {
        &cli.args[1..]
    } else {
        &[]
    };

    match cli.args.first().map(String::as_str) {
        None | Some("start") => command_start(optional_peer(rest)?, cli.json).await?,
        Some("daemon") => run_daemon(optional_peer(rest)?.into_iter().collect()).await?,
        Some("stop") => command_stop(cli.json).await?,
        Some("status") => command_status(cli.json).await?,
        Some("list") => command_list(cli.json).await?,
        Some("connectors") => command_connectors(cli.json).await?,
        Some("peer") => command_peer(required_arg(rest, "peer <addr>")?, cli.json).await?,
        Some("send") => command_send(parse_message_args(rest, false)?, cli.json).await?,
        Some("request") => command_request(parse_message_args(rest, true)?, cli.json).await?,
        Some("version") => command_version(),
        Some("self-check") => self_check(),
        Some("help" | "-h" | "--help") => print_usage(),
        Some(cmd) => {
            print_usage();
            return Err(boxed_error(format!("unknown command: {cmd}")));
        }
    }
    Ok(())
}

async fn command_start(peer: Option<String>, json_output: bool) -> AnyResult<()> {
    if control_get("/health").await.is_ok() {
        if let Some(peer) = peer.as_deref() {
            add_peer(peer).await?;
        }
        if json_output {
            print_json(json!({"ok": true, "running": true, "already_running": true, "peer": peer}));
        } else {
            println!("pi-mesh: already running");
        }
        return Ok(());
    }

    let peers: Vec<_> = peer.iter().cloned().collect();
    if json_output {
        print_json(json!({"ok": true, "running": true, "foreground": true, "peer": peer}));
    }
    run_daemon(peers).await
}

async fn command_stop(json_output: bool) -> AnyResult<()> {
    ensure_loopback_control_url()?;
    control_post("/local/shutdown", json!({})).await?;
    if json_output {
        print_json(json!({"ok": true, "stopped": true}));
    } else {
        println!("pi-mesh: stopped");
    }
    Ok(())
}

async fn command_status(json_output: bool) -> AnyResult<()> {
    let health = control_get("/health").await?;
    if json_output {
        print_json(health);
        return Ok(());
    }
    println!("pi-mesh: running");
    println!(
        "version: {}",
        value_str(&health, "version").unwrap_or("unknown")
    );
    println!(
        "protocol: {}",
        health.get("protocol").unwrap_or(&Value::Null)
    );
    println!(
        "machine: {}",
        value_str(&health, "machine").unwrap_or("unknown")
    );
    println!(
        "addr: {}",
        value_str(&health, "advertise").unwrap_or("unknown")
    );
    Ok(())
}

async fn command_list(json_output: bool) -> AnyResult<()> {
    let list = control_get("/local/list").await?;
    if json_output {
        print_json(list);
    } else {
        println!("{}", format_list(&list));
    }
    Ok(())
}

async fn command_connectors(json_output: bool) -> AnyResult<()> {
    let mut connectors = Vec::new();
    for path in find_connectors() {
        let name = connector_name(&path);
        let metadata = connector_metadata(&path).await;
        connectors.push((name, path, metadata));
    }

    if json_output {
        print_json(json!({
            "connectors": connectors.iter().map(|(name, path, metadata)| {
                json!({"name": name, "path": path.to_string_lossy(), "metadata": metadata})
            }).collect::<Vec<_>>()
        }));
    } else if connectors.is_empty() {
        println!("none");
    } else {
        for (name, _, metadata) in connectors {
            match metadata {
                Some(metadata) => println!("{name} {metadata}"),
                None => println!("{name} found"),
            }
        }
    }
    Ok(())
}

async fn command_peer(peer: &str, json_output: bool) -> AnyResult<()> {
    add_peer(peer).await?;
    if json_output {
        print_json(json!({"ok": true, "peer": peer}));
    } else {
        println!("pi-mesh: peer added {peer}");
    }
    Ok(())
}

async fn command_send(args: MessageArgs, json_output: bool) -> AnyResult<()> {
    let res = control_post(
        "/local/send",
        json!({"from": args.from, "to": args.to, "body": args.message}),
    )
    .await?;
    if json_output {
        print_json(res);
    } else {
        println!("sent to {}", args.to);
    }
    Ok(())
}

async fn command_request(args: MessageArgs, json_output: bool) -> AnyResult<()> {
    let timeout_seconds = args.timeout_seconds.unwrap_or(30).max(1);
    let res = control_post_with_timeout(
        "/local/request",
        json!({
            "from": args.from,
            "to": args.to,
            "body": args.message,
            "timeout_ms": timeout_seconds.saturating_mul(1000)
        }),
        Duration::from_secs(timeout_seconds.saturating_add(5)),
    )
    .await?;
    if json_output {
        print_json(res);
    } else {
        print_body(res.get("body").unwrap_or(&Value::Null));
    }
    Ok(())
}

fn command_version() {
    println!("pi-mesh {VERSION}");
}

async fn add_peer(peer: &str) -> AnyResult<Value> {
    control_post("/local/peer", json!({"addr": peer})).await
}

async fn run_daemon(peers: Vec<String>) -> AnyResult<()> {
    let machine = machine_name();
    let control_addr: SocketAddr = env::var("PI_MESH_CONTROL_ADDR")
        .unwrap_or_else(|_| CONTROL_ADDR.to_string())
        .parse()?;

    let (network_listener, port) = bind_network().await?;
    let env_advertise = env::var("PI_MESH_ADVERTISE").ok();
    let advertise_locked = env_advertise.is_some();
    let advertise = env_advertise.unwrap_or_else(|| format!("{}:{}", machine, port));
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

fn control_base() -> String {
    env::var("PI_MESH_CONTROL_URL").unwrap_or_else(|_| format!("http://{CONTROL_ADDR}"))
}

fn control_url(path: &str) -> String {
    format!("{}{}", control_base().trim_end_matches('/'), path)
}

fn print_json(value: Value) {
    println!("{value}");
}

fn print_body(value: &Value) {
    if let Some(text) = value.as_str() {
        println!("{text}");
    } else {
        println!("{value}");
    }
}

async fn control_get(path: &str) -> AnyResult<Value> {
    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(control_url(path))
        .send()
        .await
        .map_err(control_send_error)?;
    if !resp.status().is_success() {
        return Err(boxed_error(format!(
            "control request failed: {}",
            resp.status()
        )));
    }
    Ok(resp.json().await?)
}

async fn control_post(path: &str, body: Value) -> AnyResult<Value> {
    control_post_with_timeout(path, body, Duration::from_secs(5)).await
}

async fn control_post_with_timeout(path: &str, body: Value, timeout: Duration) -> AnyResult<Value> {
    let resp = reqwest::Client::builder()
        .timeout(timeout)
        .build()?
        .post(control_url(path))
        .json(&body)
        .send()
        .await
        .map_err(control_send_error)?;
    if !resp.status().is_success() {
        return Err(boxed_error(format!(
            "control request failed: {}",
            resp.text().await.unwrap_or_else(|_| "unknown error".into())
        )));
    }
    Ok(resp.json().await?)
}

fn control_send_error(error: reqwest::Error) -> Box<dyn std::error::Error + Send + Sync> {
    if error.is_connect() {
        return boxed_error(
            "service is not running; run `pi-mesh start`, `pi --mesh-on`, or `/mesh on` in Pi",
        );
    }
    Box::new(error)
}

fn ensure_loopback_control_url() -> AnyResult<()> {
    let url = reqwest::Url::parse(&control_base())?;
    let Some(host) = url.host_str() else {
        return Err(boxed_error("control URL has no host"));
    };
    if host == "localhost" {
        return Ok(());
    }
    if host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()) {
        return Ok(());
    }
    Err(boxed_error(format!(
        "refusing to stop non-loopback control URL: {url}"
    )))
}

fn value_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn display_cwd() -> String {
    let cwd = env::current_dir()
        .ok()
        .map(|p| p.display().to_string().replace('\\', "/"))
        .unwrap_or_else(|| ".".into());
    let home = env::var("HOME").unwrap_or_default().replace('\\', "/");
    if !home.is_empty() && cwd == home {
        "~".into()
    } else if !home.is_empty() && cwd.starts_with(&format!("{home}/")) {
        format!("~/{}", &cwd[home.len() + 1..])
    } else {
        cwd
    }
}

fn runtime_label(agent: &Value) -> String {
    let Some(runtime) = agent.get("runtime") else {
        return String::new();
    };
    let Some(model) = value_str(runtime, "model") else {
        return String::new();
    };
    let provider = value_str(runtime, "provider")
        .map(|provider| format!("@{provider}"))
        .unwrap_or_default();
    let free = runtime
        .get("context")
        .and_then(|ctx| ctx.get("free"))
        .and_then(Value::as_u64)
        .map(|free| format!(", {free} ctx free"))
        .unwrap_or_default();
    format!(" [{model}{provider}{free}]")
}

fn format_list(list: &Value) -> String {
    let local = format_agents(list, "local");
    let remote = format_agents(list, "remote");
    let peers = list
        .get("peers")
        .and_then(Value::as_array)
        .map(|xs| {
            xs.iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n  ")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "none".into());
    format!(
        "service: {}\nlocal:\n  {local}\nremote:\n  {remote}\npeers:\n  {peers}",
        value_str(list, "self").unwrap_or("unknown")
    )
}

fn format_agents(list: &Value, key: &str) -> String {
    list.get(key)
        .and_then(Value::as_array)
        .map(|agents| {
            agents
                .iter()
                .map(|agent| {
                    let title = value_str(agent, "title")
                        .map(|title| format!(" - {title}"))
                        .unwrap_or_default();
                    let cwd = value_str(agent, "cwd")
                        .map(|cwd| format!(" {cwd}"))
                        .unwrap_or_default();
                    format!(
                        "{}{}{}{} ({}) {}",
                        value_str(agent, "alias").unwrap_or("unknown"),
                        title,
                        cwd,
                        runtime_label(agent),
                        value_str(agent, "id").unwrap_or("unknown"),
                        value_str(agent, "addr").unwrap_or("unknown")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n  ")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "none".into())
}

async fn health(AxumState(state): AxumState<AppState>) -> Json<Value> {
    Json(health_payload(&state).await)
}

async fn hello(AxumState(state): AxumState<AppState>) -> Json<Value> {
    Json(health_payload(&state).await)
}

async fn health_payload(state: &AppState) -> Value {
    let inner = state.inner.lock().await;
    json!({
        "ok": true,
        "version": VERSION,
        "protocol": PROTOCOL_VERSION,
        "machine": inner.machine,
        "advertise": inner.advertise,
        "local_agents": inner.local_agents.iter().map(|(id, agent)| AgentInfo {
            id: id.clone(),
            alias: agent.alias.clone(),
            title: agent.title.clone(),
            cwd: agent.cwd.clone(),
            runtime: agent.runtime.clone(),
            addr: inner.advertise.clone(),
        }).collect::<Vec<_>>(),
        "remote_agents": inner.remote_agents.iter().map(|(id, agent)| AgentInfo {
            id: id.clone(),
            alias: agent.alias.clone(),
            title: agent.title.clone(),
            cwd: agent.cwd.clone(),
            runtime: agent.runtime.clone(),
            addr: agent.addr.clone(),
        }).collect::<Vec<_>>(),
    })
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
            title: req.title.clone(),
            cwd: req.cwd.clone(),
            runtime: req.runtime.clone(),
            last_seen: Instant::now(),
        },
    );
    inner.queues.entry(req.id).or_default();
    state.notify.notify_waiters();
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

async fn unregister(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<UnregisterReq>,
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
        .map(|(id, agent)| json!({"id": id, "alias": agent.alias, "title": agent.title, "cwd": agent.cwd, "runtime": agent.runtime, "addr": inner.advertise}))
        .collect();
    let remote: Vec<_> = inner
        .remote_agents
        .iter()
        .map(|(id, agent)| json!({"id": id, "alias": agent.alias, "title": agent.title, "cwd": agent.cwd, "runtime": agent.runtime, "addr": agent.addr}))
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
    let from_agent = resolve_agent_info(&state, &req.from).await;
    let msg = MeshMessage {
        from: req.from,
        to: req.to,
        id: Uuid::new_v4().to_string(),
        kind: "send".into(),
        body: req.body,
        from_agent,
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
    let from_agent = resolve_agent_info(&state, &req.from).await;
    let msg = MeshMessage {
        from: req.from,
        to: req.to,
        id: Uuid::new_v4().to_string(),
        kind: "request".into(),
        body: req.body,
        from_agent,
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
                        title: agent.title,
                        cwd: agent.cwd,
                        runtime: agent.runtime,
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

async fn resolve_agent_info(state: &AppState, from: &str) -> AgentInfo {
    let inner = state.inner.lock().await;
    if let Some((id, agent)) = inner.local_agents.get_key_value(from) {
        return agent_info(
            id.as_str(),
            agent.alias.as_str(),
            agent.title.clone(),
            agent.cwd.clone(),
            agent.runtime.clone(),
            inner.advertise.as_str(),
        );
    }
    if let Some((id, agent)) = inner.remote_agents.get_key_value(from) {
        return agent_info(
            id.as_str(),
            agent.alias.as_str(),
            agent.title.clone(),
            agent.cwd.clone(),
            agent.runtime.clone(),
            agent.addr.as_str(),
        );
    }
    if let Some((id, agent)) = inner
        .local_agents
        .iter()
        .find(|(_, agent)| agent.alias == from)
    {
        return agent_info(
            id.as_str(),
            agent.alias.as_str(),
            agent.title.clone(),
            agent.cwd.clone(),
            agent.runtime.clone(),
            inner.advertise.as_str(),
        );
    }
    if let Some((id, agent)) = inner
        .remote_agents
        .iter()
        .find(|(_, agent)| agent.alias == from)
    {
        return agent_info(
            id.as_str(),
            agent.alias.as_str(),
            agent.title.clone(),
            agent.cwd.clone(),
            agent.runtime.clone(),
            agent.addr.as_str(),
        );
    }
    placeholder_agent_info(from)
}

fn agent_info(
    id: impl Into<String>,
    alias: impl Into<String>,
    title: Option<String>,
    cwd: String,
    runtime: Option<Value>,
    addr: impl Into<String>,
) -> AgentInfo {
    AgentInfo {
        id: id.into(),
        alias: alias.into(),
        title,
        cwd,
        runtime,
        addr: addr.into(),
    }
}

fn placeholder_agent_info(id: &str) -> AgentInfo {
    agent_info(id, id, None, display_cwd(), None, "")
}

fn announce_response(inner: &Inner) -> Json<AnnounceResp> {
    let mut agents: Vec<AgentInfo> = inner
        .local_agents
        .iter()
        .map(|(id, agent)| AgentInfo {
            id: id.clone(),
            alias: agent.alias.clone(),
            title: agent.title.clone(),
            cwd: agent.cwd.clone(),
            runtime: agent.runtime.clone(),
            addr: inner.advertise.clone(),
        })
        .collect();
    agents.extend(inner.remote_agents.iter().map(|(id, agent)| AgentInfo {
        id: id.clone(),
        alias: agent.alias.clone(),
        title: agent.title.clone(),
        cwd: agent.cwd.clone(),
        runtime: agent.runtime.clone(),
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
        time::sleep(Duration::from_secs(5)).await;
    }
}

async fn prune_stale_local_agents(state: &AppState) {
    let mut inner = state.inner.lock().await;
    let stale: Vec<String> = inner
        .local_agents
        .iter()
        .filter(|(_, agent)| agent.last_seen.elapsed() > Duration::from_secs(3))
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
                    title: agent.title.clone(),
                    cwd: agent.cwd.clone(),
                    runtime: agent.runtime.clone(),
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
                    title: agent.title,
                    cwd: agent.cwd,
                    runtime: agent.runtime,
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

async fn connector_metadata(path: &PathBuf) -> Option<Value> {
    let output = time::timeout(
        Duration::from_secs(2),
        Command::new(path).arg("status").arg("--json").output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
}

fn connector_name(path: &std::path::Path) -> String {
    path.file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("unknown")
        .trim_start_matches("pi-mesh-")
        .trim_end_matches(".exe")
        .trim_end_matches(".js")
        .to_string()
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
            if !name.starts_with("pi-mesh-") || !path.is_file() {
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if fs::metadata(&path)
                    .map(|m| m.permissions().mode() & 0o111 == 0)
                    .unwrap_or(true)
                {
                    continue;
                }
            }
            out.push(path);
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

fn parse_cli(args: &[String]) -> Cli {
    let mut json = false;
    let mut version = false;
    let mut out = Vec::new();

    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            "--version" | "-V" => version = true,
            _ => out.push(arg.clone()),
        }
    }

    if version {
        out.clear();
        out.push("version".into());
    }

    Cli { json, args: out }
}

fn parse_message_args(args: &[String], allow_timeout: bool) -> AnyResult<MessageArgs> {
    let mut from =
        env::var("PI_MESH_CLI_NAME").unwrap_or_else(|_| format!("cli@{}", machine_name()));
    let mut timeout_seconds = None;
    let mut positionals = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--from" => {
                let Some(value) = args.get(i + 1) else {
                    return Err(boxed_error("--from requires a name"));
                };
                from = value.clone();
                i += 2;
            }
            "--timeout" => {
                if !allow_timeout {
                    return Err(boxed_error("--timeout is only supported for request"));
                }
                let Some(value) = args.get(i + 1) else {
                    return Err(boxed_error("--timeout requires seconds"));
                };
                timeout_seconds = Some(value.parse()?);
                i += 2;
            }
            value => {
                positionals.push(value.to_string());
                i += 1;
            }
        }
    }

    if positionals.len() < 2 {
        let command = if allow_timeout { "request" } else { "send" };
        return Err(boxed_error(format!(
            "usage: pi-mesh {command} <to> <message>"
        )));
    }

    Ok(MessageArgs {
        from,
        to: positionals[0].clone(),
        message: positionals[1..].join(" "),
        timeout_seconds,
    })
}

fn optional_peer(args: &[String]) -> AnyResult<Option<String>> {
    match args {
        [] => Ok(None),
        [peer] => Ok(Some(peer.clone())),
        _ => Err(boxed_error("usage: pi-mesh start [peer]")),
    }
}

fn required_arg<'a>(args: &'a [String], usage: &str) -> AnyResult<&'a str> {
    match args {
        [value] => Ok(value),
        _ => Err(boxed_error(format!("usage: pi-mesh {usage}"))),
    }
}

fn print_usage() {
    println!(
        "Usage:
  pi-mesh start [peer]
  pi-mesh stop
  pi-mesh status
  pi-mesh list
  pi-mesh connectors
  pi-mesh peer <addr>
  pi-mesh send <to> <message>
  pi-mesh request <to> <message>
  pi-mesh version

Options:
  --json
  --version
  --from <name>
  --timeout <seconds>"
    );
}

fn boxed_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
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
    fn parses_agent_without_title() {
        let agent: AgentInfo =
            serde_json::from_str(r#"{"id":"a","alias":"b","cwd":"~","addr":"c"}"#).unwrap();
        assert_eq!(agent.title, None);
    }

    #[test]
    fn parses_unregister_without_metadata() {
        let req: UnregisterReq = serde_json::from_str(r#"{"id":"a"}"#).unwrap();
        assert_eq!(req.id, "a");
    }

    #[test]
    fn names_connector() {
        assert_eq!(
            connector_name(std::path::Path::new("/bin/pi-mesh-tailscale")),
            "tailscale"
        );
        assert_eq!(
            connector_name(std::path::Path::new("pi-mesh-tailscale.js")),
            "tailscale"
        );
    }

    #[test]
    fn parses_optional_peer() {
        assert_eq!(optional_peer(&[]).unwrap(), None);
        assert_eq!(
            optional_peer(&["one:7373".to_string()]).unwrap(),
            Some("one:7373".into())
        );
        assert!(optional_peer(&["one:7373".to_string(), "two:7373".to_string()]).is_err());
    }

    #[test]
    fn parses_global_flags() {
        let cli = parse_cli(&[
            "--json".to_string(),
            "status".to_string(),
            "--version".to_string(),
        ]);
        assert!(cli.json);
        assert_eq!(cli.args, vec!["version"]);

        let cli = parse_cli(&["list".to_string(), "--json".to_string()]);
        assert!(cli.json);
        assert_eq!(cli.args, vec!["list"]);
    }

    #[test]
    fn parses_message_args() {
        let args = vec![
            "--from".to_string(),
            "ops@machine".to_string(),
            "target@machine".to_string(),
            "hello".to_string(),
            "there".to_string(),
        ];
        assert_eq!(
            parse_message_args(&args, false).unwrap(),
            MessageArgs {
                from: "ops@machine".into(),
                to: "target@machine".into(),
                message: "hello there".into(),
                timeout_seconds: None,
            }
        );

        let args = vec![
            "target@machine".to_string(),
            "status?".to_string(),
            "--timeout".to_string(),
            "60".to_string(),
        ];
        let parsed = parse_message_args(&args, true).unwrap();
        assert_eq!(parsed.to, "target@machine");
        assert_eq!(parsed.message, "status?");
        assert_eq!(parsed.timeout_seconds, Some(60));
    }
}
