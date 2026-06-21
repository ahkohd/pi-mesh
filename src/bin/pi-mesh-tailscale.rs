use serde_json::{json, Value};
use std::{env, io::Write, process::Command};

fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("auth") => auth(&args),
        Some("run") => run(&args),
        Some("status") => status(),
        _ => eprintln!(
            "usage: pi-mesh-tailscale run --port 7373 | auth --remote-ip 100.x.y.z | status --json"
        ),
    }
}

fn status() {
    let Some(v) = tailscale_status() else {
        println!("{}", json!({"ok": false}));
        return;
    };
    let peers = v
        .get("Peer")
        .and_then(Value::as_object)
        .map(|peers| {
            peers
                .values()
                .filter(|peer| peer.get("Online").and_then(Value::as_bool) != Some(false))
                .count()
        })
        .unwrap_or(0);
    println!(
        "{}",
        json!({
            "ok": true,
            "self": v.get("Self").and_then(node_ip),
            "peers": peers
        })
    );
}

fn auth(args: &[String]) {
    let Some(ip) = arg(args, "--remote-ip") else {
        println!("{}", json!({"allow": false}));
        return;
    };
    let ok = Command::new("tailscale")
        .arg("whois")
        .arg(ip)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    println!("{}", json!({"allow": ok, "source": "tailscale"}));
}

fn run(args: &[String]) {
    discover(arg(args, "--port").unwrap_or("7373"));
}

fn discover(port: &str) {
    let Some(v) = tailscale_status() else {
        return;
    };
    if let Some(addr) = v.get("Self").and_then(|node| node_addr(node, port)) {
        emit("self", &addr);
    }

    let Some(peers) = v.get("Peer").and_then(Value::as_object) else {
        return;
    };

    for peer in peers.values() {
        if peer.get("Online").and_then(Value::as_bool) == Some(false) {
            continue;
        }
        if let Some(addr) = node_addr(peer, port) {
            emit("peer", &addr);
        }
    }
}

fn tailscale_status() -> Option<Value> {
    let output = Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .ok()?;
    output.status.success().then_some(())?;
    serde_json::from_slice::<Value>(&output.stdout).ok()
}

fn node_ip(node: &Value) -> Option<&str> {
    node.get("TailscaleIPs")
        .and_then(Value::as_array)
        .and_then(|ips| ips.first())
        .and_then(Value::as_str)
}

fn node_addr(node: &Value, port: &str) -> Option<String> {
    if let Some(ip) = node_ip(node) {
        return Some(format!("{ip}:{port}"));
    }

    let dns = node.get("DNSName")?.as_str()?.trim_end_matches('.');
    (!dns.is_empty()).then(|| format!("{dns}:{port}"))
}

fn emit(kind: &str, addr: &str) {
    println!("{}", json!({"type":kind,"addr":addr,"source":"tailscale"}));
    let _ = std::io::stdout().flush();
}

fn arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].as_str())
}
