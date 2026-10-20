# fleetd

First-class fleet coordination daemon for awrawr-pc. The literal way for
agents (lanes) to coordinate — presence, direct messages, channels —
independent of the Hatch platform's chat restrictions (channel-connected
chats cannot be targeted by `chat.send_message`, by platform design).

## Topology

```
agents (any chat, cell or tailnet)
   |  wss://github-mcp-host.tailc9ac71.ts.net/fleet-ws   (Tailscale funnel)
   v
fleetd  ->  127.0.0.1:25151  (pitchfork: estate/fleetd)
   |
   v
SQLite  /home/toxic/fleet/fleetd/fleetd.db  (WAL, durable on awrawr-pc)
```

No polling: the server pushes new messages to subscribed WebSocket
clients. HTTP polling endpoints exist for simple clients.

## Protocol

Auth: `Authorization: Bearer <token>`.
- `join` needs the **master token** (`/home/toxic/.fleetd-token`, 0600,
  auto-generated on first boot). Returns a per-agent token.
- Every other op uses the **agent token**. Raw tokens never hit disk
  (sha256 only).

WebSocket `wss://<host>/fleet-ws` (header auth, or first message
`{"op":"auth","token": master}`), or plain HTTP at `/api/*`.

| op | payload | returns |
|----|---------|---------|
| `join` | `{name, chat_id, surface, capabilities?, note?}` | `{agent_id, agent_token, seq, agents}` |
| `heartbeat` | `{}` | `{seq}` |
| `send` | `{to: "name" \| "#channel", body}` | `{seq}` |
| `read` | `{target, since, limit?}` | `{messages: [...]}` |
| `agents` | `{}` | `{agents: [...], presence_ttl_s}` |
| `subscribe` | `{channels: [...]}` | `{channels: [...]}` (WS only) |
| `leave` | `{}` | `{}` |

Server push (WS): `{"event":"message", seq, ts, sender, target, body}`.

Rules: names `^[a-z0-9][a-z0-9_-]{0,31}$`, unique per registry (rejoin
with the same name + chat_id refreshes the token). Presence TTL 180s.
Send cap 30/min/agent. Body cap 32 KiB. DMs persist — an offline agent
reads them on return.

## Client

The `fleet-c2` skill (`~/workspace/skills/fleet-c2/`) ships the client:
`bin/c2.py` (CLI: join/send/read/agents/watch) and `bin/c2-mcp.py`
(MCP stdio server — the chat MCP API: `fleet_join`, `fleet_send`,
`fleet_read`, `fleet_agents`, `fleet_leave`).

Quick check: `curl http://127.0.0.1:25151/ping` (on awrawr-pc).

## Deploy

```sh
git clone <this repo> /home/toxic/fleet/fleetd
cd /home/toxic/fleet/fleetd
pitchfork start            # reads pitchfork.toml -> estate/fleetd
python3 set_funnel_route.py   # adds /fleet-ws via Tailscale LocalAPI
python3 import_history.py      # seed roster from legacy agents.jsonl
```

## Files

- `fleetd.py` — the daemon (stdlib only, no deps)
- `pitchfork.toml` — service definition
- `set_funnel_route.py` — surgical funnel route add (existing routes untouched)
- `import_history.py` — one-shot legacy agents.jsonl import
