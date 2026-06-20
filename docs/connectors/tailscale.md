# Tailscale connector

`pi-mesh-tailscale` lets pi-mesh discover and authorize peers over Tailscale.

Put `pi-mesh-tailscale` on `PATH` to enable it. The service discovers connector executables by scanning `PATH` for names that start with `pi-mesh-`.

For local development:

```bash
cargo build --bins
export PATH="$PWD/target/debug:$PATH"
```

## Discovery

The service starts the connector like this:

```bash
pi-mesh-tailscale run --port 7373
```

Each run calls:

```bash
tailscale status --json
```

It emits newline-delimited JSON:

```json
{"type":"self","addr":"100.64.0.7:7373","source":"tailscale"}
{"type":"peer","addr":"100.64.0.8:7373","source":"tailscale"}
```

The service uses `self` as its advertised address unless `PI_MESH_ADVERTISE` is set. It emits the first Tailscale IP, falling back to DNS when needed.

## Authorization

For inbound mesh requests, the service asks the connector:

```bash
pi-mesh-tailscale auth --remote-ip 100.64.0.8
```

The connector runs:

```bash
tailscale whois 100.64.0.8
```

If `tailscale whois` succeeds, the connector replies:

```json
{"allow":true,"source":"tailscale"}
```

Otherwise it replies with `allow: false`.

## Notes

- Tailscale must be installed, running, and authenticated.
- Peer machines must be online in the same tailnet.
- The remote pi-mesh service must be reachable on its mesh port, usually `7373`.
- Set `PI_MESH_ADVERTISE` only if you want to override the Tailscale address.
