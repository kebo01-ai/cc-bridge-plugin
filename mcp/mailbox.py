#!/usr/bin/env python3
"""Durable mailbox, stable aliases, receiver leases and explicit delivery receipts.

Mailbox storage does not run models. CLI-owned hooks watch with cc_peek; informational
acknowledgements are retained for explicit inbox reads without waking a conversation.
"""
import contextlib
import hashlib
import json
import logging
import logging.handlers
import os
import re
import sqlite3
import threading
import time

DB_PATH = os.path.expanduser(os.environ.get('CC_MCP_MAILBOX_DB', '~/.cc-bridge/mailbox.db'))
_NAME_RE = re.compile(r'^[a-zA-Z0-9_.-]{1,64}$')
_init_lock = threading.Lock()
_initialized = False
_conditions = [threading.Condition() for _ in range(64)]
MAX_WAIT_S = int(os.environ.get('CC_MCP_INBOX_MAX_WAIT', '120'))
MAX_BODY_BYTES = 256 * 1024


def _valid_name(name):
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise ValueError('invalid mailbox name (alphanumeric . _ - only, max 64)')
    return name


def _now():
    return time.strftime('%Y-%m-%dT%H:%M:%S')


def _connect():
    global _initialized
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=5000')
    with _init_lock:
        if not _initialized:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS agents(name TEXT PRIMARY KEY, registered_at TEXT, last_seen TEXT);
                CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipient TEXT NOT NULL, sender TEXT, body TEXT NOT NULL, ts TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS idx_msg_recipient ON messages(recipient,id);
                CREATE TABLE IF NOT EXISTS aliases(alias TEXT PRIMARY KEY, agent TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_alias_agent ON aliases(agent);
                CREATE TABLE IF NOT EXISTS message_events(id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER, at REAL NOT NULL, stage TEXT NOT NULL, actor TEXT, via TEXT);
                CREATE INDEX IF NOT EXISTS idx_events_message ON message_events(message_id,id);
            ''')
            columns = {
                'agents': {'node': "TEXT NOT NULL DEFAULT ''", 'session': "TEXT NOT NULL DEFAULT ''",
                           'instance_id': "TEXT NOT NULL DEFAULT ''", 'lease_until': 'REAL NOT NULL DEFAULT 0',
                           'receiver_state': "TEXT NOT NULL DEFAULT 'unknown'", 'last_error': "TEXT NOT NULL DEFAULT ''"},
                'messages': {'topic_id': 'TEXT', 'reply_to': 'INTEGER', 'kind': "TEXT NOT NULL DEFAULT 'message'",
                             'requires_reply': 'INTEGER', 'wake': 'INTEGER NOT NULL DEFAULT 1',
                             'idempotency_key': 'TEXT', 'created_at': 'REAL', 'delivered_at': 'REAL',
                             'fetched_at': 'REAL', 'acknowledged_at': 'REAL', 'completed_at': 'REAL'},
            }
            for table, fields in columns.items():
                present = {r[1] for r in conn.execute('PRAGMA table_info(' + table + ')')}
                for key, declaration in fields.items():
                    if key not in present:
                        conn.execute(f'ALTER TABLE {table} ADD COLUMN {key} {declaration}')
            conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_msg_idempotent '
                         "ON messages(recipient,COALESCE(sender,''),idempotency_key) WHERE idempotency_key IS NOT NULL")
            conn.commit()
            _initialized = True
    return conn


@contextlib.contextmanager
def _db():
    conn = _connect()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def _resolve(conn, name):
    _valid_name(name)
    row = conn.execute('SELECT agent FROM aliases WHERE alias=?', (name,)).fetchone()
    if row:
        return row[0]
    if conn.execute('SELECT 1 FROM agents WHERE name=?', (name,)).fetchone():
        return name
    rows = conn.execute('SELECT name FROM agents WHERE session=? AND name NOT IN (SELECT alias FROM aliases)',
                        (name,)).fetchall()
    if len(rows) > 1:
        raise ValueError('ambiguous session alias; use cc_directory and the full mailbox name')
    return rows[0][0] if rows else name


def _addresses(conn, name):
    return [name] + [r[0] for r in conn.execute('SELECT alias FROM aliases WHERE agent=?', (name,))]


def _condition(name):
    return _conditions[int(hashlib.sha256(name.encode()).hexdigest()[:8], 16) % len(_conditions)]


def _notify(name):
    with _condition(name):
        _condition(name).notify_all()


def _event(conn, message_id, stage, actor, via=''):
    at = time.time()
    conn.execute('INSERT INTO message_events(message_id,at,stage,actor,via) VALUES(?,?,?,?,?)',
                 (message_id, at, stage, actor, via))
    # Structured, bounded metadata only: never bodies, credentials or request arguments.
    logging.getLogger('cc.mailbox').info(json.dumps(dict(at=at, message_id=message_id, stage=stage,
                                                         actor=actor, via=via), separators=(',', ':')))


def configure_logging():
    logger = logging.getLogger('cc.mailbox')
    if logger.handlers:
        return
    path = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), 'mailbox-events.jsonl')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(path, maxBytes=5 * 1024 * 1024, backupCount=3)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def register(name: str, node: str = '', session: str = '', instance_id: str = '',
             aliases: list[str] | None = None) -> dict:
    """Register a logical mailbox; aliases survive CLI restarts. Does not assert receiver health."""
    _valid_name(name)
    if len(node) > 128 or len(session) > 128 or len(instance_id) > 160:
        raise ValueError('identity field too long')
    aliases = list(dict.fromkeys(aliases or []))
    if len(aliases) > 32:
        raise ValueError('too many aliases')
    with _db() as c:
        c.execute('BEGIN IMMEDIATE')
        name = _resolve(c, name)
        for alias in aliases:
            _valid_name(alias)
            existing = c.execute('SELECT agent FROM aliases WHERE alias=?', (alias,)).fetchone()
            if existing and existing[0] != name:
                raise ValueError('alias already belongs to another mailbox')
            row = c.execute('SELECT instance_id,lease_until FROM agents WHERE name=?', (alias,)).fetchone()
            if alias != name and row and row['lease_until'] > time.time() and row['instance_id'] != instance_id:
                raise ValueError('alias has a live receiver; handover required')
        old = c.execute('SELECT instance_id FROM agents WHERE name=?', (name,)).fetchone()
        c.execute('INSERT INTO agents(name,registered_at,last_seen,node,session,instance_id) VALUES(?,?,?,?,?,?) '
                  "ON CONFLICT(name) DO UPDATE SET last_seen=excluded.last_seen, "
                  "node=CASE WHEN excluded.node!='' THEN excluded.node ELSE agents.node END, "
                  "session=CASE WHEN excluded.session!='' THEN excluded.session ELSE agents.session END",
                  (name, _now(), _now(), node, session, instance_id))
        if instance_id and (not old or old[0] != instance_id):
            c.execute("UPDATE agents SET instance_id=?,lease_until=0,receiver_state='unknown' WHERE name=?",
                      (instance_id, name))
        for alias in aliases:
            if alias != name:
                c.execute('INSERT OR IGNORE INTO aliases(alias,agent) VALUES(?,?)', (alias, name))
                c.execute('UPDATE aliases SET agent=? WHERE agent=?', (name, alias))
        addresses = _addresses(c, name)
        unread = c.execute('SELECT COUNT(*) FROM messages WHERE recipient IN (' + ','.join('?' * len(addresses))
                            + ') AND read=0', addresses).fetchone()[0]
        pending = c.execute('SELECT COUNT(*) FROM messages WHERE recipient IN (' + ','.join('?' * len(addresses))
                            + ') AND read=0 AND wake=1 AND delivered_at IS NULL AND acknowledged_at IS NULL '
                            'AND completed_at IS NULL', addresses).fetchone()[0]
    _notify(name)
    return {'ok': True, 'name': name, 'pending': pending, 'unread': unread, 'protocol': 2}


def heartbeat(name: str, instance_id: str, lease_seconds: int = 90,
              state: str = 'watching', error: str = '') -> dict:
    """Renew the current receiver lease, independently of mailbox reads. Old instances are fenced."""
    if state not in ('watching', 'error', 'offline') or not re.fullmatch(r'[A-Za-z0-9_]{0,64}', error):
        raise ValueError('invalid receiver state/error code')
    with _db() as c:
        name = _resolve(c, name)
        row = c.execute('SELECT * FROM agents WHERE name=?', (name,)).fetchone()
        if not row or not instance_id or row['instance_id'] != instance_id:
            return {'ok': False, 'error': 'stale_instance'}
        until = time.time() + max(5, min(120, lease_seconds)) if state != 'offline' else 0
        c.execute('UPDATE agents SET lease_until=?,receiver_state=?,last_error=?,last_seen=? WHERE name=?',
                  (until, state, error, _now(), name))
        if state != row['receiver_state'] or error != row['last_error']:
            _event(c, None, 'receiver_' + state, name, error)
    return {'ok': True, 'lease_until': until}


def directory(query: str = '', active_only: bool = False) -> dict:
    """Resolve human session names to mailboxes and inspect receiver leases (not merely recent polls)."""
    with _db() as c:
        rows = c.execute('SELECT * FROM agents WHERE name NOT IN (SELECT alias FROM aliases) ORDER BY name').fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d['aliases'] = _addresses(c, d['name'])[1:]
            d['online'] = d['lease_until'] > time.time() and d['receiver_state'] == 'watching'
            if query and not any(query in str(d[k]) for k in ('name', 'node', 'session', 'aliases')):
                continue
            if active_only and not d['online']:
                continue
            result.append(d)
    return {'ok': True, 'agents': result}


def post(to: str, body: str, sender: str = None, topic_id: str = None, reply_to: int = None,
         kind: str = 'message', requires_reply: bool = None, wake: bool = None,
         idempotency_key: str = None) -> dict:
    """Post a request/update/result/ack; ack defaults to no wake and no reply. Retry with the same idempotency key."""
    if kind not in ('message', 'request', 'update', 'result', 'ack'):
        raise ValueError('invalid message kind')
    if not isinstance(body, str) or not body:
        return {'ok': False, 'error': 'empty body'}
    if len(body.encode()) > MAX_BODY_BYTES:
        raise ValueError('body exceeds 256 KiB; send a summary and artifact reference')
    for value in (topic_id, idempotency_key):
        if value is not None and (not isinstance(value, str) or not 1 <= len(value) <= 128):
            raise ValueError('invalid topic/idempotency key')
    with _db() as c:
        c.execute('BEGIN IMMEDIATE')
        to = _resolve(c, to)
        sender = _resolve(c, sender) if sender else None
        if reply_to is not None:
            parent = c.execute('SELECT topic_id FROM messages WHERE id=?', (reply_to,)).fetchone()
            if not parent:
                raise ValueError('reply_to message does not exist')
            topic_id = topic_id or parent[0] or 'message-' + str(reply_to)
        if requires_reply is None:
            requires_reply = True if kind == 'request' else False if kind == 'ack' else None
        wake = kind != 'ack' if wake is None else bool(wake)
        if idempotency_key:
            row = c.execute("SELECT * FROM messages WHERE recipient=? AND COALESCE(sender,'')=? AND idempotency_key=?",
                            (to, sender or '', idempotency_key)).fetchone()
            if row:
                if (row['body'], row['topic_id'], row['reply_to'], row['kind'], row['requires_reply'], bool(row['wake'])) != (
                        body, topic_id, reply_to, kind, requires_reply, wake):
                    raise ValueError('idempotency key reused with different message')
                return {'ok': True, 'id': row['id'], 'to': to, 'duplicate': True}
        cur = c.execute('INSERT INTO messages(recipient,sender,body,ts,topic_id,reply_to,kind,requires_reply,wake,'
                        'idempotency_key,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                        (to, sender, body, _now(), topic_id, reply_to, kind, requires_reply, int(wake),
                         idempotency_key, time.time()))
        ident = cur.lastrowid
        _event(c, ident, 'queued', sender)
        agent = c.execute('SELECT lease_until,receiver_state FROM agents WHERE name=?', (to,)).fetchone()
        status = 'online' if agent and agent[0] > time.time() and agent[1] == 'watching' else 'offline' if agent else 'unregistered'
    if wake:
        _notify(to)
    return {'ok': True, 'id': ident, 'to': to, 'recipient_status': status, 'wake': wake,
            'requires_reply': requires_reply, 'topic_id': topic_id}


def receipt(me: str, ids: list[int] | None = None, stage: str = 'handed_off', via: str = 'client',
            instance_id: str = '', through_id: int = 0, source_address: str = '') -> dict:
    """Record handoff, explicit acknowledgement or completion; handoff never claims model processing."""
    fields = {'handed_off': 'delivered_at', 'acknowledged': 'acknowledged_at', 'completed': 'completed_at'}
    if stage not in fields or not re.fullmatch(r'[a-zA-Z0-9_-]{1,40}', via):
        raise ValueError('invalid receipt stage/via')
    ids = list(dict.fromkeys(ids or []))
    if len(ids) > 500 or any(type(i) is not int or i < 1 for i in ids):
        raise ValueError('invalid receipt ids')
    if through_id and (stage != 'handed_off' or via != 'legacy_cursor'):
        raise ValueError('through_id is only for importing a legacy delivery cursor')
    with _db() as c:
        me = _resolve(c, me)
        if instance_id:
            row = c.execute('SELECT instance_id FROM agents WHERE name=?', (me,)).fetchone()
            if not row or row[0] != instance_id:
                return {'ok': False, 'error': 'stale_instance'}
        addresses = _addresses(c, me)
        if source_address:
            if source_address not in addresses or not through_id:
                raise ValueError('legacy source must belong to this mailbox')
            addresses = [source_address]
        q = 'SELECT id,created_at FROM messages WHERE recipient IN (' + ','.join('?' * len(addresses)) + ')'
        args = list(addresses)
        if through_id:
            q += ' AND id<=?'; args.append(through_id)
        elif ids:
            q += ' AND id IN (' + ','.join('?' * len(ids)) + ')'; args.extend(ids)
        else:
            return {'ok': True, 'updated': 0}
        q += ' AND ' + fields[stage] + ' IS NULL'
        rows = c.execute(q, args).fetchall()
        for r in rows:
            c.execute('UPDATE messages SET ' + fields[stage] + '=? WHERE id=?', (time.time(), r['id']))
            _event(c, r['id'], stage, me, via)
    return {'ok': True, 'updated': len(rows)}


def delivery_status(me: str = '', message_id: int = 0, limit: int = 50) -> dict:
    """Inspect metadata/receipt timestamps without fetching or consuming message bodies."""
    with _db() as c:
        where, args = [], []
        if me:
            addresses = _addresses(c, _resolve(c, me))
            where.append('recipient IN (' + ','.join('?' * len(addresses)) + ')'); args.extend(addresses)
        if message_id:
            where.append('id=?'); args.append(message_id)
        sql = 'SELECT id,recipient,sender,ts,topic_id,reply_to,kind,requires_reply,wake,read,created_at,delivered_at,' \
              'fetched_at,acknowledged_at,completed_at FROM messages'
        rows = [dict(r) for r in c.execute(sql + (' WHERE ' + ' AND '.join(where) if where else '') +
                                           ' ORDER BY id DESC LIMIT ?', [*args, max(1, min(200, limit))])]
        for r in rows:
            r['stage'] = next((name for name, field in [('completed', 'completed_at'), ('acknowledged', 'acknowledged_at'),
                              ('fetched', 'fetched_at'), ('handed_off', 'delivered_at')] if r[field] is not None),
                             'legacy_fetched' if r['read'] else 'queued')
            r['handoff_ms'] = round((r['delivered_at'] - r['created_at']) * 1000) if r['delivered_at'] and r['created_at'] else None
        events = [dict(r) for r in c.execute('SELECT * FROM message_events WHERE message_id=? ORDER BY id',
                                            (message_id,))] if message_id else []
    return {'ok': True, 'messages': rows, 'events': events}


def _fetch(me, since_id, conn=None, mark_read=True, pending_only=False, auto_only=False, limit=100):
    own = conn is None
    c = conn or _connect()
    try:
        canonical = _resolve(c, me)
        addresses = _addresses(c, canonical)
        q = 'SELECT * FROM messages WHERE recipient IN (' + ','.join('?' * len(addresses)) + ')'
        args = list(addresses)
        if pending_only:
            q += ' AND delivered_at IS NULL AND read=0 AND acknowledged_at IS NULL AND completed_at IS NULL'
        else:
            q += ' AND id>?'; args.append(since_id)
        if auto_only:
            q += ' AND wake=1'
        rows = c.execute(q + ' ORDER BY id LIMIT ?', [*args, max(1, min(200, limit))]).fetchall()
        messages, size = [], 0
        for row in rows:
            d = dict(row); size += len(d['body'].encode())
            if messages and size > 64 * 1024:
                break
            messages.append(d)
        if messages and mark_read:
            for d in messages:
                if not d['read']:
                    c.execute('UPDATE messages SET read=1,fetched_at=? WHERE id=?', (time.time(), d['id']))
                    _event(c, d['id'], 'fetched', canonical, 'inbox')
            c.execute('UPDATE agents SET last_seen=? WHERE name=?', (_now(), canonical))
            c.commit()
        return messages
    finally:
        if own:
            c.close()


def inbox(me: str, since_id: int = 0, wait: bool = False, timeout: int = 30, limit: int = 100) -> dict:
    """Explicit history read, marks fetched, never completed. Prefer the watcher for ordinary waiting."""
    return _poll(me, since_id, wait, timeout, True, limit=limit)


def peek(me: str, since_id: int = 0, wait: bool = False, timeout: int = 30,
         pending_only: bool = False, auto_only: bool = False, limit: int = 100, instance_id: str = '') -> dict:
    """Observe without marking fetched. Receivers use pending_only+auto_only and report handoff receipts."""
    return _poll(me, since_id, wait, timeout, False, pending_only, auto_only, limit, instance_id)


def _poll(me, since_id, wait, timeout, mark_read, pending_only=False, auto_only=False, limit=100, instance_id=''):
    _valid_name(me)
    since_id = int(since_id or 0)
    timeout = max(0, min(int(timeout or 0), MAX_WAIT_S))
    started = time.monotonic(); deadline = started + timeout
    with _db() as c:
        # A condition closes the check/sleep race with same-process post. A bounded
        # fallback also observes writes made by another service process during upgrades.
        canonical = _resolve(c, me)
        condition = _condition(canonical)
        with condition:
            while True:
                if instance_id:
                    row = c.execute('SELECT instance_id FROM agents WHERE name=?', (canonical,)).fetchone()
                    if not row or row[0] != instance_id:
                        raise ValueError('stale receiver instance')
                msgs = _fetch(me, since_id, c, mark_read, pending_only, auto_only, limit)
                if msgs or not wait or time.monotonic() >= deadline:
                    return {'ok': True, 'me': canonical, 'messages': msgs,
                            'cursor': max(since_id, msgs[-1]['id']) if msgs else since_id,
                            'waited': round(time.monotonic() - started, 3), 'protocol': 2}
                condition.wait(min(5, max(0, deadline - time.monotonic())))
