#!/usr/bin/env python3
"""CC Bridge mailbox — sqlite-backed agent-to-agent message channel.

No Ray. Pure server-side state. A named recipient has an inbox; peers post to it;
the recipient pulls new messages by id cursor (optionally long-polling).

Hard rule: this cannot wake an idle Claude — the recipient must actively call
inbox(). To wake a session that is not polling, drive its TTY via the bridge
tools (cc_send) instead.
"""
import os
import re
import sqlite3
import threading
import time

DB_PATH = os.path.expanduser(os.environ.get("CC_MCP_MAILBOX_DB", "~/.cc-bridge/mailbox.db"))
_NAME_RE = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")
_init_lock = threading.Lock()
_initialized = False


def _valid_name(name: str) -> str:
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise ValueError(f"invalid name {name!r} (alphanumeric . _ - only, max 64)")
    return name


def _connect() -> sqlite3.Connection:
    global _initialized
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    with _init_lock:  # re-check inside the lock, not outside it
        if not _initialized:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    name TEXT PRIMARY KEY,
                    registered_at TEXT,
                    last_seen TEXT
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipient TEXT NOT NULL,
                    sender TEXT,
                    body TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_msg_recipient ON messages(recipient, id);
                """
            )
            conn.commit()
            _initialized = True
    return conn


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def register(name: str) -> dict:
    """Create/claim an inbox for `name`. Idempotent."""
    name = _valid_name(name)
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO agents(name, registered_at, last_seen) VALUES(?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET last_seen=excluded.last_seen",
            (name, _now(), _now()),
        )
        conn.commit()
        pending = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE recipient=? AND read=0", (name,)
        ).fetchone()[0]
        return {"ok": True, "name": name, "pending": pending}
    finally:
        conn.close()


def post(to: str, body: str, sender: str = None) -> dict:
    """Deliver `body` to recipient `to`'s inbox."""
    to = _valid_name(to)
    if sender is not None:
        sender = _valid_name(sender)
    if not isinstance(body, str) or not body:
        return {"ok": False, "error": "empty body"}
    conn = _connect()
    try:
        cur = conn.execute(
            "INSERT INTO messages(recipient, sender, body, ts) VALUES(?,?,?,?)",
            (to, sender, body, _now()),
        )
        conn.commit()
        return {"ok": True, "id": cur.lastrowid, "to": to}
    finally:
        conn.close()


def _fetch(me: str, since_id: int, conn: sqlite3.Connection = None) -> list:
    own = conn is None
    conn = conn or _connect()
    try:
        rows = conn.execute(
            "SELECT id, sender, body, ts FROM messages WHERE recipient=? AND id>? ORDER BY id",
            (me, since_id),
        ).fetchall()
        if rows:
            ids = [r["id"] for r in rows]
            conn.executemany("UPDATE messages SET read=1 WHERE id=?", [(i,) for i in ids])
        conn.execute("UPDATE agents SET last_seen=? WHERE name=?", (_now(), me))
        conn.commit()
        return [dict(r) for r in rows]
    finally:
        if own:
            conn.close()


# A long-poll occupies one server worker thread for its whole duration, so an unbounded
# timeout (an LLM can pass timeout=100000) would let a few calls exhaust the pool and
# stall the server. Cap it; callers just poll again.
MAX_WAIT_S = int(os.environ.get("CC_MCP_INBOX_MAX_WAIT", "120"))


def inbox(me: str, since_id: int = 0, wait: bool = False, timeout: int = 30) -> dict:
    """Return messages with id > since_id. If wait, long-poll until some arrive or timeout.

    Response includes `cursor` = max id seen, to pass back as since_id next call.
    `timeout` is clamped to CC_MCP_INBOX_MAX_WAIT (default 120s).
    """
    me = _valid_name(me)
    since_id = int(since_id or 0)
    timeout = max(0, min(int(timeout or 0), MAX_WAIT_S))
    if not wait:
        msgs = _fetch(me, since_id)
        return {"ok": True, "me": me, "messages": msgs,
                "cursor": msgs[-1]["id"] if msgs else since_id}

    # Hold ONE connection for the whole poll instead of reopening every second.
    deadline = time.time() + timeout
    conn = _connect()
    try:
        while True:
            msgs = _fetch(me, since_id, conn)
            if msgs or time.time() >= deadline:
                return {"ok": True, "me": me, "messages": msgs,
                        "cursor": msgs[-1]["id"] if msgs else since_id,
                        "waited": round(timeout - max(0.0, deadline - time.time()), 1)}
            time.sleep(1.0)
    finally:
        conn.close()
