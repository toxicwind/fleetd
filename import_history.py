#!/usr/bin/env python3
"""One-shot import of the legacy cell-side fleet event log into fleetd.

Reads ~/workspace/c2/agents.jsonl (append-only JSONL written by agent.py
and lane join events) and seeds the fleetd agents table so the new
coordination plane continues the old record instead of rupturing it.

Imported agents get token_hash='imported' (no valid token — they must
re-join) and last_seen set to the event timestamp, so they show in the
roster as stale history, honestly marked.
"""

import json
import sqlite3
import sys
from pathlib import Path

SRC = Path(sys.argv[1] if len(sys.argv) > 1 else str(Path.home() / "workspace/c2/agents.jsonl"))
DB = Path(sys.argv[2] if len(sys.argv) > 2 else "/home/toxic/fleet/fleetd/fleetd.db")

db = sqlite3.connect(str(DB))
db.execute("""CREATE TABLE IF NOT EXISTS agents(
    id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL,
    chat_id TEXT, surface TEXT, capabilities TEXT, note TEXT,
    token_hash TEXT NOT NULL, joined_at TEXT NOT NULL, last_seen TEXT NOT NULL)""")

n_agents = n_skip = 0
for line in SRC.read_text().splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        ev = json.loads(line)
    except ValueError:
        n_skip += 1
        continue
    if ev.get("event") != "join":
        continue
    name = str(ev.get("name", ""))[:64]
    if not name:
        n_skip += 1
        continue
    ts = str(ev.get("ts", ""))[:32]
    try:
        db.execute(
            "INSERT INTO agents(name, chat_id, surface, capabilities, note,"
            " token_hash, joined_at, last_seen) VALUES(?,?,?,?,?,?,?,?)",
            (name, str(ev.get("chat", ""))[:128], str(ev.get("surface", ""))[:64],
             "[]", "imported from agents.jsonl; re-join for a live token",
             "imported", ts, ts))
        n_agents += 1
    except sqlite3.IntegrityError:
        n_skip += 1
db.commit()
print("imported %d agents, skipped %d" % (n_agents, n_skip))
