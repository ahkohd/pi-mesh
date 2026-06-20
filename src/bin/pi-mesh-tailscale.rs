use serde_json::{json, Value};
use std::{env, io::Write, process::Command, thread, time::Duration};

fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("auth") => auth(&args),
        Some("run") => run(&args),
        _ => eprintln!("usage: pi-mesh-tailscale run --port 7373 | auth --remote-ip 100.x.y.z"),
    }
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
    let port = arg(args, "--port").unwrap_or("7373").to_string();
    loop {
        discover(&port);
        thread::sleep(Duration::from_secs(15));
    }
}

fn discover(port: &str) {
    let Ok(output) = Command::new("tailscale")
        .args(["status", "--json"])
        .output()
    else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let Ok(v) = serde_json::from_slice::<Value>(&output.stdout) else {
        return;
    };
    let Some(peers) = v.get("Peer").and_then(Value::as_object) else {
        return;
    };

    for peer in peers.values() {
        if peer.get("Online").and_then(Value::as_bool) == Some(false) {
            continue;
        }
        if let Some(ip) = peer
            .get("TailscaleIPs")
            .and_then(Value::as_array)
            .and_then(|ips| ips.first())
            .and_then(Value::as_str)
        {
            emit(&format!("{ip}:{port}"));
        }
        if let Some(dns) = peer.get("DNSName").and_then(Value::as_str) {
            let dns = dns.trim_end_matches('.');
            if !dns.is_empty() {
                emit(&format!("{dns}:{port}"));
            }
        }
    }
}

fn emit(addr: &str) {
    println!(
        "{}",
        json!({"type":"peer","addr":addr,"source":"tailscale"})
    );
    let _ = std::io::stdout().flush();
}

fn arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].as_str())
}
