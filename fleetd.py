#!/usr/bin/env python3
"""fleetd — first-class fleet coordination daemon for awrawr-pc.

The literal way for agents (lanes) to coordinate: presence, direct
messages, and channels — independent of the Hatch platform's chat
restrictions (channel-connected chats cannot be targeted by
chat.send_message, by platform design).

Transport: HTTP + WebSocket, stdlib only (asyncio). Binds 127.0.0.1 and
is exposed on the tailnet via the Tailscale funnel route /fleet-ws
(surgical LocalAPI add, existing routes untouched).

Auth:
  - Master token: Bearer on join (and on WS upgrade). File
    /home/toxic/.fleetd-token (0600), auto-generated on first boot.
    Proves tailnet + token holder. The fleet-c2 skill bootstraps it once
    over the already-authenticated awrawr-mcp bridge and caches it.
  - Agent token: per-agent secret returned by join, used for every later
    op. sha256 stored; the raw value never hits disk.

Protocol (WS at /fleet-ws, or HTTP POST/GET at /api/*):
  join      {name, chat_id, surface, capabilities?} -> {agent_id, agent_token, seq, agents}
  heartbeat {}                                     -> {seq}
  send      {to: "name" | "#channel", body}        -> {seq}
  read      {target, since, limit?}                 -> {messages:[...]}
  agents    {}                                     -> {agents:[...]}
  subscribe {channels:[...]}                       -> {}            (WS only)
  leave     {}                                     -> {}

Server pushes {"event":"message", seq, ts, sender, target, body} to WS
clients subscribed to the target channel, plus DMs to the named agent's
live connections.

Storage: SQLite (WAL) at /home/toxic/fleet/fleetd/fleetd.db — durable,
on awrawr-pc. Presence TTL 180s. Send rate cap 30/min/agent.
Body cap 32 KiB. Name: ^[a-z0-9][a-z0-9_-]{0,31}$.

No `sleep` anywhere in this file. Fail fast, log to stdout (pitchfork
captures it).
"""

import asyncio
import base64
import calendar
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
from collections import defaultdict, deque
from pathlib import Path

PORT = int(os.environ.get("FLEETD_PORT", "25151"))
TOKEN_FILE = Path(os.environ.get("FLEETD_TOKEN_FILE", "/home/toxic/.fleetd-token"))
DB_PATH = Path(os.environ.get("FLEETD_DB", "/home/toxic/fleet/fleetd/fleetd.db"))
WS_PATH = "/fleet-ws"
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
PRESENCE_TTL = 180
SEND_CAP = 30
SEND_WINDOW = 60
BODY_CAP = 32 * 1024
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
VERSION = 1


def log(*a):
    print("[fleetd]", *a, flush=True)


def utcnow():
    return time.strftime("%Y-%m-%dT%H:%M:%S+0000", time.gmtime())


# ---------------- master token ----------------

def load_or_create_master_token():
    if TOKEN_FILE.exists():
        tok = TOKEN_FILE.read_text().strip()
        if tok:
            return tok
    tok = secrets.token_urlsafe(48)
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(TOKEN_FILE), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(tok + "\n")
    log("generated new master token at", TOKEN_FILE)
    return tok


MASTER_TOKEN = load_or_create_master_token()


def bearer_ok(header_value, expected):
    if not header_value:
        return False
    parts = header_value.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return False
    return hmac.compare_digest(parts[1], expected)


# ---------------- database ----------------

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS agents(
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        chat_id TEXT, surface TEXT, capabilities TEXT, note TEXT,
        token_hash TEXT NOT NULL,
        joined_at TEXT NOT NULL, last_seen TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS messages(
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL, sender TEXT NOT NULL,
        target TEXT NOT NULL, body TEXT NOT NULL)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_messages_target_seq ON messages(target, seq)")
    db.commit()
    return db


DB = init_db()


def db_agent_by_token(agent_token):
    h = hashlib.sha256(agent_token.encode()).hexdigest()
    row = DB.execute(
        "SELECT id, name, chat_id, surface FROM agents WHERE token_hash=?", (h,)
    ).fetchone()
    return row


def touch_presence(agent_id):
    DB.execute("UPDATE agents SET last_seen=? WHERE id=?", (utcnow(), agent_id))
    DB.commit()


def live_agents():
    cutoff = time.time() - PRESENCE_TTL
    rows = DB.execute(
        "SELECT name, chat_id, surface, last_seen, joined_at FROM agents ORDER BY name"
    ).fetchall()
    out = []
    for name, chat_id, surface, last_seen, joined_at in rows:
        try:
            seen_epoch = calendar.timegm(
                time.strptime(last_seen, "%Y-%m-%dT%H:%M:%S+0000"))
            live = (time.time() - seen_epoch) < PRESENCE_TTL
        except Exception:
            live = False
        out.append({"name": name, "chat_id": chat_id, "surface": surface,
                    "last_seen": last_seen, "joined_at": joined_at, "live": live})
    return out


# ---------------- in-memory live state ----------------

# agent_id -> {"name":..., "writer": asyncio StreamWriter or None, "channels": set()}
CONNS = {}
# channel -> set(agent_id)
SUBS = defaultdict(set)
# agent_id -> deque[timestamps] for send rate limiting
SEND_TS = defaultdict(deque)


# ---------------- protocol ops ----------------

def op_join(payload):
    name = str(payload.get("name", "")).strip()
    if not NAME_RE.match(name):
        return {"ok": False, "error": "bad_name",
                "detail": "name must match ^[a-z0-9][a-z0-9_-]{0,31}$"}
    chat_id = str(payload.get("chat_id", ""))[:128]
    surface = str(payload.get("surface", ""))[:64]
    caps = payload.get("capabilities", [])
    note = str(payload.get("note", ""))[:256]
    token = secrets.token_urlsafe(32)
    th = hashlib.sha256(token.encode()).hexdigest()
    now = utcnow()
    row = DB.execute("SELECT id, chat_id FROM agents WHERE name=?", (name,)).fetchone()
    if row:
        agent_id, old_chat = row
        if old_chat and chat_id and old_chat != chat_id:
            return {"ok": False, "error": "name_taken",
                    "detail": "name '%s' is registered to another chat" % name}
        DB.execute("UPDATE agents SET token_hash=?, chat_id=?, surface=?, "
                   "capabilities=?, note=?, last_seen=? WHERE id=?",
                   (th, chat_id, surface, json.dumps(caps), note, now, agent_id))
    else:
        cur = DB.execute("INSERT INTO agents(name, chat_id, surface, capabilities,"
                         " note, token_hash, joined_at, last_seen)"
                         " VALUES(?,?,?,?,?,?,?,?)",
                         (name, chat_id, surface, json.dumps(caps), note, th, now, now))
        agent_id = cur.lastrowid
    DB.commit()
    seq = DB.execute("SELECT COALESCE(MAX(seq),0) FROM messages").fetchone()[0]
    log("join", name, "chat", chat_id[:8] or "-")
    return {"ok": True, "op": "join", "agent_id": agent_id, "agent_token": token,
            "seq": seq, "agents": live_agents()}


def op_heartbeat(agent):
    touch_presence(agent[0])
    seq = DB.execute("SELECT COALESCE(MAX(seq),0) FROM messages").fetchone()[0]
    return {"ok": True, "op": "heartbeat", "seq": seq}


def op_send(agent, payload, writer):
    agent_id, name = agent[0], agent[1]
    now = time.time()
    q = SEND_TS[agent_id]
    while q and q[0] < now - SEND_WINDOW:
        q.popleft()
    if len(q) >= SEND_CAP:
        return {"ok": False, "error": "rate_limited",
                "detail": "max %d sends per %ds" % (SEND_CAP, SEND_WINDOW)}
    to = str(payload.get("to", "")).strip()
    body = str(payload.get("body", ""))
    if not to or not body:
        return {"ok": False, "error": "bad_send", "detail": "to and body required"}
    if len(body.encode()) > BODY_CAP:
        return {"ok": False, "error": "too_large", "detail": "body cap 32 KiB"}
    if to.startswith("#"):
        target = to[:64]
    elif NAME_RE.match(to):
        target = "@" + to
        if not DB.execute("SELECT 1 FROM agents WHERE name=?", (to,)).fetchone():
            return {"ok": False, "error": "unknown_agent",
                    "detail": "no agent named '%s'" % to}
    else:
        return {"ok": False, "error": "bad_target",
                "detail": "to must be a name or #channel"}
    q.append(now)
    ts = utcnow()
    cur = DB.execute("INSERT INTO messages(ts, sender, target, body) VALUES(?,?,?,?)",
                     (ts, name, target, body))
    DB.commit()
    seq = cur.lastrowid
    touch_presence(agent_id)
    msg = {"event": "message", "seq": seq, "ts": ts, "sender": name,
           "target": target, "body": body}
    # push to channel subscribers + DM target conns (not back to sender)
    dests = set(SUBS.get(target, ()))
    if target.startswith("@"):
        for aid, c in CONNS.items():
            if c["name"] == target[1:]:
                dests.add(aid)
    dests.discard(agent_id)
    for aid in dests:
        c = CONNS.get(aid)
        if c and c.get("writer") is not None:
            asyncio.get_event_loop().call_soon(_push, c["writer"], msg)
    return {"ok": True, "op": "send", "seq": seq}


def _push(writer, msg):
    try:
        data = json.dumps(msg).encode()
        frame = bytes([0x81])
        n = len(data)
        if n < 126:
            frame += bytes([n])
        elif n < 65536:
            frame += bytes([126]) + n.to_bytes(2, "big")
        else:
            frame += bytes([127]) + n.to_bytes(8, "big")
        writer.write(frame + data)
    except Exception:
        pass


def op_read(agent, payload):
    target = str(payload.get("target", "#fleet")).strip()[:64]
    try:
        since = int(payload.get("since", 0))
    except (TypeError, ValueError):
        since = 0
    try:
        limit = min(int(payload.get("limit", 50)), 200)
    except (TypeError, ValueError):
        limit = 50
    rows = DB.execute("SELECT seq, ts, sender, target, body FROM messages"
                      " WHERE target=? AND seq>? ORDER BY seq LIMIT ?",
                      (target, since, limit)).fetchall()
    touch_presence(agent[0])
    return {"ok": True, "op": "read",
            "messages": [{"seq": s, "ts": t, "sender": snd, "target": tg, "body": b}
                         for s, t, snd, tg, b in rows]}

def op_agents(agent):
    touch_presence(agent[0])
    return {"ok": True, "op": "agents", "agents": live_agents(),
            "presence_ttl_s": PRESENCE_TTL}


def op_leave(agent):
    agent_id = agent[0]
    CONNS.pop(agent_id, None)
    for ch in list(SUBS):
        SUBS[ch].discard(agent_id)
    touch_presence(agent_id)
    return {"ok": True, "op": "leave"}


def dispatch(agent, payload, writer):
    op = str(payload.get("op", ""))
    if op == "join":
        return op_join(payload)
    if agent is None:
        return {"ok": False, "error": "auth_required",
                "detail": "join first (master token), then use your agent token"}
    if op == "heartbeat":
        return op_heartbeat(agent)
    if op == "send":
        return op_send(agent, payload, writer)
    if op == "read":
        return op_read(agent, payload)
    if op == "agents":
        return op_agents(agent)
    if op == "leave":
        return op_leave(agent)
    if op == "subscribe":
        channels = payload.get("channels", [])
        if writer is None:
            return {"ok": False, "error": "ws_only"}
        c = CONNS.get(agent[0])
        if c is not None:
            for ch in channels:
                ch = str(ch).strip()[:64]
                if ch:
                    c["channels"].add(ch)
                    SUBS[ch].add(agent[0])
        touch_presence(agent[0])
        return {"ok": True, "op": "subscribe",
                "channels": sorted(c["channels"]) if c else []}
    return {"ok": False, "error": "unknown_op", "detail": op[:32]}


# ---------------- HTTP ----------------

async def read_http_request(reader):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = await asyncio.wait_for(reader.read(65536), timeout=15)
        if not chunk:
            return None, None, None
        data += chunk
        if len(data) > 1 << 20:
            return None, None, None
    head, rest = data.split(b"\r\n\r\n", 1)
    lines = head.decode("latin1").split("\r\n")
    method, path, _ = (lines[0].split(" ") + ["", "", ""])[:3]
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    body = rest
    clen = int(headers.get("content-length", "0") or 0)
    while len(body) < clen:
        chunk = await asyncio.wait_for(reader.read(65536), timeout=15)
        if not chunk:
            break
        body += chunk
    return method, path, (headers, body[:clen])


def http_response(writer, status, body_obj=None, content_type="application/json"):
    body = b"" if body_obj is None else (
        body_obj if isinstance(body_obj, bytes) else json.dumps(body_obj).encode())
    writer.write(("HTTP/1.1 %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n"
                  "Connection: close\r\n\r\n" % (status, content_type, len(body))).encode()
                 + body)


def agent_from_headers(headers):
    return db_agent_by_token(
        (headers.get("authorization", "").split(None, 1) + [""])[1]
        if headers.get("authorization", "").lower().startswith("bearer ") else "")


async def handle_http(reader, writer, method, path, headers, body):
    try:
        if method == "GET" and path == "/ping":
            http_response(writer, "200 OK", {"ok": True, "service": "fleetd",
                                            "v": VERSION, "ts": utcnow()})
            return
        if path == WS_PATH and headers.get("upgrade", "").lower() == "websocket":
            await handle_ws(reader, writer, headers)
            return
        if not path.startswith("/api/"):
            http_response(writer, "404 Not Found", {"ok": False, "error": "not_found"})
            return
        op = path[len("/api/"):]
        if op == "join":
            if not bearer_ok(headers.get("authorization"), MASTER_TOKEN):
                http_response(writer, "401 Unauthorized",
                              {"ok": False, "error": "bad_master_token"})
                return
            try:
                payload = json.loads(body.decode() or "{}")
            except ValueError:
                payload = {}
            payload["op"] = "join"
            http_response(writer, "200 OK", op_join(payload))
            return
        agent = agent_from_headers(headers)
        if agent is None:
            http_response(writer, "401 Unauthorized",
                          {"ok": False, "error": "bad_agent_token"})
            return
        if op in ("heartbeat", "send", "leave"):
            try:
                payload = json.loads(body.decode() or "{}")
            except ValueError:
                payload = {}
            payload["op"] = op
            http_response(writer, "200 OK", dispatch(agent, payload, None))
        elif op in ("read", "agents"):
            # query-string form: /api/read?target=%23fleet&since=0&limit=50
            import urllib.parse as up
            qs = {}
            for kv in headers.get("x-qs", "").split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    qs[up.unquote(k)] = up.unquote(v)
            qs["op"] = op
            http_response(writer, "200 OK", dispatch(agent, qs, None))
        else:
            http_response(writer, "404 Not Found", {"ok": False, "error": "not_found"})
    except Exception as e:
        log("http error:", e)
        try:
            http_response(writer, "500 Internal Server Error",
                          {"ok": False, "error": "internal"})
        except Exception:
            pass


# ---------------- WebSocket ----------------

async def ws_send(writer, obj):
    data = json.dumps(obj).encode()
    frame = bytes([0x81])
    n = len(data)
    if n < 126:
        frame += bytes([n])
    elif n < 65536:
        frame += bytes([126]) + n.to_bytes(2, "big")
    else:
        frame += bytes([127]) + n.to_bytes(8, "big")
    writer.write(frame + data)
    await writer.drain()


async def ws_recv(reader):
    hdr = await asyncio.wait_for(reader.readexactly(2), timeout=300)
    b1, b2 = hdr
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), "big")
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), "big")
    mask = await reader.readexactly(4) if masked else None
    payload = await reader.readexactly(length) if length else b""
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    if opcode == 0x8:
        return None
    if opcode == 0x9:  # ping -> pong
        return "ping"
    if opcode != 0x1:
        return b""
    return payload


async def handle_ws(reader, writer, headers):
    key = headers.get("sec-websocket-key", "")
    if not key:
        http_response(writer, "400 Bad Request", b"missing key", "text/plain")
        return
    accept = base64.b64encode(
        hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                  "Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n"
                  % accept).encode())
    await writer.drain()
    # auth: header preferred, first-message fallback
    authed_master = bearer_ok(headers.get("authorization"), MASTER_TOKEN)
    agent = None
    agent_id = None
    try:
        while True:
            frame = await ws_recv(reader)
            if frame is None:
                break
            if frame == "ping":
                writer.write(bytes([0x8A, 0x00]))
                await writer.drain()
                continue
            try:
                msg = json.loads(frame.decode() if isinstance(frame, bytes) else frame)
            except (ValueError, AttributeError):
                await ws_send(writer, {"ok": False, "error": "bad_json"})
                continue
            if not isinstance(msg, dict):
                await ws_send(writer, {"ok": False, "error": "bad_envelope"})
                continue
            if msg.get("op") == "auth" and agent is None:
                if hmac.compare_digest(str(msg.get("token", "")), MASTER_TOKEN):
                    authed_master = True
                    await ws_send(writer, {"ok": True, "op": "auth"})
                else:
                    await ws_send(writer, {"ok": False, "error": "bad_master_token"})
                continue
            # agent-token auth (or master token doubles as agent auth pre-join)
            tok = str(msg.get("agent_token", ""))
            if tok:
                agent = db_agent_by_token(tok)
            if agent is None and authed_master and msg.get("op") == "join":
                resp = dispatch(None, msg, writer)
                await ws_send(writer, resp)
                if resp.get("ok"):
                    agent = (resp["agent_id"], msg.get("name", "").strip(),
                             "", "")
                    agent_id = agent[0]
                    CONNS[agent_id] = {"name": agent[1], "writer": writer,
                                       "channels": set()}
                continue
            if agent is None:
                await ws_send(writer, {"ok": False, "error": "auth_required"})
                continue
            if agent[0] != agent_id:
                agent_id = agent[0]
                CONNS.setdefault(agent_id,
                                 {"name": agent[1], "writer": writer, "channels": set()})
                CONNS[agent_id]["writer"] = writer
            resp = dispatch(agent, msg, writer)
            await ws_send(writer, resp)
            if msg.get("op") == "leave":
                break
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    except Exception as e:
        log("ws error:", e)
    finally:
        if agent_id is not None:
            c = CONNS.get(agent_id)
            if c and c.get("writer") is writer:
                c["writer"] = None
        try:
            writer.close()
        except Exception:
            pass


# ---------------- main ----------------

async def handle_conn(reader, writer):
    try:
        method, path, rest = await read_http_request(reader)
        if method is None:
            writer.close()
            return
        headers, body = rest
        # split query string for routing; handlers get it back via headers["x-qs"]
        if "?" in path:
            route, qs = path.split("?", 1)
        else:
            route, qs = path, ""
        headers = dict(headers)
        headers["x-qs"] = qs
        await handle_http(reader, writer, method, route, headers, body)
    except Exception as e:
        log("conn error:", e)
    finally:
        try:
            await writer.drain()
            writer.close()
        except Exception:
            pass


async def amain():
    server = await asyncio.start_server(handle_conn, "127.0.0.1", PORT)
    log("fleetd v%d listening on 127.0.0.1:%d (db %s)" % (VERSION, PORT, DB_PATH))
    async with server:
        await server.serve_forever()


def main():
    asyncio.run(amain())


if __name__ == "__main__":
    main()
