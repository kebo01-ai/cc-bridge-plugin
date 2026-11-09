#!/usr/bin/env python3
"""CC Bridge — manage Claude Code sessions on worker nodes via tmux.

Architecture:  你(如流) -> kebo02(透传) -> Ray remote -> worker tmux -> claude

Usage (from Head, via loop .venv):
    .venv/bin/python3 .claude/skills/cc-bridge/cc_bridge.py start <node_ip> [--cwd /path] [--name session]
    .venv/bin/python3 .claude/skills/cc-bridge/cc_bridge.py send  <node_ip> <session> "<message>" [--wait] [--timeout 120]
    .venv/bin/python3 .claude/skills/cc-bridge/cc_bridge.py read  <node_ip> <session> [--lines 80]
    .venv/bin/python3 .claude/skills/cc-bridge/cc_bridge.py list  [node_ip]   # omit to list all known nodes
    .venv/bin/python3 .claude/skills/cc-bridge/cc_bridge.py stop  <node_ip> <session>
    .venv/bin/python3 .claude/skills/cc-bridge/cc_bridge.py keys  <node_ip> <session> <key>   # send raw key: Enter/Escape/Up/etc
"""

import argparse
import contextlib
import fcntl
import json
import os
import re
import shlex
import sys
import threading
import time

import ray

TMUX_PREFIX = os.environ.get("CC_BRIDGE_PREFIX", "cc-")
# CC busy markers: session is busy when any of these appear in pane output.
# This list should be updated if CC changes its UI strings.
BUSY_MARKERS = (
    "esc to interrupt",
    "Topsy-turvying",
    "Thinking",
    "esc to cancel",
    "Generating",
)
# CC readiness markers: session is ready when any of these appear after start.
# Kept as a tuple for easy extension when CC versions change.
READY_MARKERS = (
    "bypass permissions",
    "Welcome back",
    "for getting started",
    "What can I help",
    ">",  # prompt char (generic fallback)
)
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z]|\x1b\][^\x07]*\x07|\x1b[()][AB012]")
# Deterministic JSON output delimiter — printed before JSON so parser doesn't
# need to hunt for '{' in arbitrary Ray/python output.
JSON_DELIMITER = "__CC_BRIDGE_JSON__"

# --- Input validation ---
IP_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
SESSION_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

# --- Session registry ---
REGISTRY_DIR = os.path.expanduser("~/.cc-bridge")
REGISTRY_FILE = os.path.join(REGISTRY_DIR, "sessions.json")
_registry_thread_lock = threading.Lock()


def _validate_ip(ip: str) -> str:
    """Validate IP address format."""
    if not IP_RE.match(ip):
        raise ValueError(f"Invalid IP address format: {ip!r}")
    parts = ip.split(".")
    if not all(0 <= int(p) <= 255 for p in parts):
        raise ValueError(f"IP octets out of range: {ip!r}")
    return ip


def _validate_session_name(name: str) -> str:
    """Validate session name (alphanumeric, hyphens, underscores only)."""
    if not SESSION_NAME_RE.match(name):
        raise ValueError(f"Invalid session name: {name!r} (use alphanumeric, hyphens, underscores, max 64 chars)")
    return name


def _load_registry() -> dict:
    """Load session registry from disk. Corrupt file is quarantined, not discarded."""
    if not os.path.exists(REGISTRY_FILE):
        return {}
    try:
        with open(REGISTRY_FILE) as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                data = json.load(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        # Don't silently start from {} — that would make the next save wipe the record.
        bad = REGISTRY_FILE + ".corrupt." + time.strftime("%Y%m%d-%H%M%S")
        try:
            os.replace(REGISTRY_FILE, bad)
            print(f"[cc_bridge] registry was corrupt; moved to {bad}", file=sys.stderr, flush=True)
        except OSError:
            pass
        return {}
    except OSError:
        return {}


def _save_registry(registry: dict):
    """Atomically persist the registry (temp file + os.replace), so a crash mid-write
    can never leave a truncated/corrupt sessions.json."""
    os.makedirs(REGISTRY_DIR, exist_ok=True)
    tmp = f"{REGISTRY_FILE}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(registry, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, REGISTRY_FILE)


@contextlib.contextmanager
def _registry_txn():
    """Serialize read-modify-write on the registry across processes and threads.

    Several writers share one file: the MCP server (threadpool), the workbench's
    read/write agent children, and the CLI. Without this, two concurrent
    load→mutate→save cycles lose one of the two edits (last writer wins).
    """
    os.makedirs(REGISTRY_DIR, exist_ok=True)
    lock_path = REGISTRY_FILE + ".lock"
    with _registry_thread_lock:
        with open(lock_path, "a+") as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)


def _register_session(name: str, node_ip: str, cwd: str, role: str = ""):
    """Register a session in the registry."""
    with _registry_txn():
        registry = _load_registry()
        registry[name] = {
            "node_ip": node_ip,
            "cwd": cwd,
            "role": role,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        _save_registry(registry)


def _unregister_session(name: str):
    """Remove a session from the registry."""
    with _registry_txn():
        registry = _load_registry()
        registry.pop(name, None)
        _save_registry(registry)


def _get_registered_nodes() -> list[str]:
    """Get unique node IPs from registry."""
    registry = _load_registry()
    return list({v["node_ip"] for v in registry.values()})


_ray_ready = False
_ray_lock = threading.Lock()


def _ensure_ray():
    """Connect to Ray once, on first use. Library callers (workbench probe child, CLI,
    MCP) need not pre-init. The internal cluster's GCS connection breaks through an HTTP
    proxy, so strip proxy env first (mirrors the RSI bridge)."""
    global _ray_ready
    if _ray_ready and ray.is_initialized():
        return
    with _ray_lock:
        if ray.is_initialized():
            _ray_ready = True
            return
        for var in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            os.environ.pop(var, None)
        os.environ.setdefault("NO_PROXY", "10.0.0.0/8,127.0.0.1,localhost,*.baidu-int.com,*.baidu.com")
        os.environ.setdefault("no_proxy", os.environ["NO_PROXY"])
        ray.init(address=os.environ.get("RAY_ADDRESS", "auto"),
                 ignore_reinit_error=True, logging_level="ERROR")
        _ray_ready = True


def _run_fn(node_ip: str):
    node_ip = _validate_ip(node_ip)
    _ensure_ray()

    @ray.remote(num_cpus=0, resources={f"node:{node_ip}": 0.001})
    def run_on_node(cmd: str, timeout: int = 30):
        import subprocess

        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return {"stdout": r.stdout, "stderr": r.stderr, "returncode": r.returncode}

    return run_on_node


def _norm_name(name: str) -> str:
    raw = name if name.startswith(TMUX_PREFIX) else TMUX_PREFIX + name
    _validate_session_name(raw)
    return raw


def _clean(text: str) -> str:
    """Strip ANSI escapes and trailing blank lines."""
    text = ANSI_RE.sub("", text)
    lines = [ln.rstrip() for ln in text.splitlines()]
    # drop trailing empties
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def _has_session(run, name: str) -> bool:
    r = ray.get(
        run.remote(f"tmux has-session -t {name} 2>/dev/null && echo YES || echo NO"),
        timeout=15,
    )
    return "YES" in r["stdout"]


def _capture(run, name: str, lines: int = 120) -> str:
    r = ray.get(run.remote(f"tmux capture-pane -t {name} -p -S -{lines}"), timeout=15)
    return r["stdout"]


def _is_busy(pane: str) -> bool:
    return any(m in pane for m in BUSY_MARKERS)


# tmux key names only (Enter, Escape, C-c, M-x, PageUp, F1, BSpace…). `key` is
# interpolated into a remote tmux command, so anything outside this shape is a
# caller trying to append a second command.
TMUX_KEY_RE = re.compile(r"^[A-Za-z0-9-]{1,16}$")


def validate_key(key: str) -> str:
    if not TMUX_KEY_RE.match(key or ""):
        raise ValueError(f"invalid tmux key: {key!r} (letters, digits and - only, <=16 chars)")
    return key


def quote_path(p: str) -> str:
    """Quote a path for a remote shell while keeping a leading ``~`` expandable.

    `shlex.quote("~")` → `'~'` which no shell expands (default cwd would break).
    `~/x` → `"$HOME"'/x'`: $HOME still expands, the rest is literal, and nothing a
    caller supplies can end the quoting early (closes the cwd shell-injection hole).
    """
    if p == "~":
        return '"$HOME"'
    if p.startswith("~/"):
        return '"$HOME"' + shlex.quote("/" + p[2:])
    return shlex.quote(p)


_session_locks: dict[str, threading.Lock] = {}
_session_locks_guard = threading.Lock()


def session_lock(node_ip: str, name: str) -> threading.RLock:
    """Per-session REENTRANT lock: writes serialize per pane so concurrent sends cannot
    interleave keystrokes. Reentrant because an outer caller (ccops' write agent) also
    takes it around the same verb — a plain Lock would self-deadlock."""
    key = f"{node_ip}/{name}"
    with _session_locks_guard:
        lock = _session_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _session_locks[key] = lock
        return lock


def _send_keys_unlocked(run, name: str, literal: str = None, key: str = None):
    """Raw send — caller must already hold session_lock(node_ip, name)."""
    if key:
        ray.get(run.remote(f"tmux send-keys -t {name} {validate_key(key)}"), timeout=15)
    else:
        escaped = literal.replace("'", "'\\''")
        ray.get(run.remote(f"tmux send-keys -t {name} '{escaped}'"), timeout=15)


def _send_keys(run, name: str, literal: str = None, key: str = None):
    """Backwards-compatible alias; prefer taking session_lock in the verb."""
    _send_keys_unlocked(run, name, literal=literal, key=key)


def cc_start(node_ip: str, name: str = None, cwd: str = "~", role: str = "",
             claude_path: str = None) -> dict:
    """Start a CC session in a fresh tmux on `node_ip`.

    Positional order is (node_ip, name, cwd, role) — ccops.py calls it positionally
    that way; do not reorder without fixing that caller.
    """
    run = _run_fn(node_ip)
    if name is None:
        name = f"cc-{int(time.time()) % 100000}"
    else:
        name = _norm_name(name)

    with session_lock(node_ip, name):
        if _has_session(run, name):
            return {"ok": False, "error": f"Session '{name}' already exists", "name": name}

        # Resolve cwd to absolute path (expand ~)
        resolve_cwd = cwd or "~"
        if resolve_cwd != "~" and not resolve_cwd.startswith("/") and not resolve_cwd.startswith("~/"):
            resolve_cwd = f"~/{resolve_cwd}"

        # Claude binary: use configured path or default to 'claude' from PATH
        claude_bin = claude_path or "claude"

        # start tmux + cd to cwd + start claude. cwd/claude_path come from a caller
        # (LLM/MCP/workbench) and land in a remote shell → quote them, never interpolate raw.
        # IS_SANDBOX=1 bypasses root+skip-permissions security block in CC 2.1+
        q_cwd = quote_path(resolve_cwd)
        inner = f"cd {q_cwd} && IS_SANDBOX=1 {shlex.quote(claude_bin)} --dangerously-skip-permissions"
        cmd = (
            f"tmux new-session -d -s {name} -c {q_cwd} && "
            f"tmux send-keys -t {name} {shlex.quote(inner)} Enter && "
            f"echo OK"
        )
        r = ray.get(run.remote(cmd, timeout=20), timeout=25)
        if "OK" not in r["stdout"]:
            return {"ok": False, "error": r["stderr"] or r["stdout"], "name": name}

        # Wait for CC to boot; auto-pass "trust this folder" prompt if it appears
        for _ in range(20):
            time.sleep(1)
            pane = _capture(run, name, 40)
            if "trust this folder" in pane or "Yes, I trust" in pane:
                _send_keys_unlocked(run, name, key="Enter")
                time.sleep(2)
                continue
            # CC ready when the interactive prompt box is present
            if any(m in pane for m in READY_MARKERS):
                _register_session(name, node_ip, cwd, role)
                return {"ok": True, "name": name, "node": node_ip, "node_ip": node_ip,
                        "cwd": cwd, "role": role, "state": "ready"}

        # started but readiness unconfirmed — still register
        _register_session(name, node_ip, cwd, role)
        return {"ok": True, "name": name, "node": node_ip, "node_ip": node_ip,
                "cwd": cwd, "role": role, "state": "started_unconfirmed"}


def cc_send(node_ip: str, name: str, message: str, wait: bool = False,
            timeout: int = 120) -> dict:
    run = _run_fn(node_ip)
    name = _norm_name(name)
    if not _has_session(run, name):
        return {"ok": False, "error": f"Session '{name}' not found"}

    # Serialize the keystroke delivery per pane: two concurrent sends would otherwise
    # interleave characters into the same prompt. Only the typing is held — the
    # wait-for-idle poll below is a read and must not block another session's writes.
    with session_lock(node_ip, name):
        # type the message, then Enter as a separate send so multi-word text lands intact
        if message:
            _send_keys_unlocked(run, name, literal=message)
            time.sleep(0.3)
        _send_keys_unlocked(run, name, key="Enter")

    if not wait:
        time.sleep(3)
        return {"ok": True, "name": name, "output": _clean(_capture(run, name, 120))}

    # wait mode: poll until CC goes idle (no busy markers) for 2 consecutive checks
    deadline = time.time() + timeout
    idle_streak = 0
    last_pane = ""
    time.sleep(2)  # let CC register the input first
    while time.time() < deadline:
        pane = _capture(run, name, 200)
        if _is_busy(pane):
            idle_streak = 0
        else:
            idle_streak += 1
            if idle_streak >= 2:
                return {
                    "ok": True,
                    "name": name,
                    "state": "idle",
                    "output": _clean(pane),
                }
        last_pane = pane
        time.sleep(2)

    return {
        "ok": True,
        "name": name,
        "state": "timeout",
        "note": f"still busy after {timeout}s; output is partial",
        "output": _clean(last_pane),
    }


def cc_read(node_ip: str, name: str, lines: int = 80) -> dict:
    run = _run_fn(node_ip)
    name = _norm_name(name)
    if not _has_session(run, name):
        return {"ok": False, "error": f"Session '{name}' not found"}
    pane = _capture(run, name, lines)
    return {"ok": True, "name": name, "busy": _is_busy(pane), "output": _clean(pane)}


def cc_keys(node_ip: str, name: str, key: str) -> dict:
    """Send a raw tmux key (Enter/Escape/Up/Down/C-c/'2' etc) — for menus/interrupts."""
    run = _run_fn(node_ip)
    name = _norm_name(name)
    if not _has_session(run, name):
        return {"ok": False, "error": f"Session '{name}' not found"}
    with session_lock(node_ip, name):
        _send_keys_unlocked(run, name, key=key)
    time.sleep(1.5)
    return {"ok": True, "name": name, "sent_key": key, "output": _clean(_capture(run, name, 60))}


def cc_status(node_ip: str, name: str) -> dict:
    """Check health of a CC session: tmux alive + claude process alive + busy state."""
    run = _run_fn(node_ip)
    name = _norm_name(name)

    # 1. tmux session exists?
    if not _has_session(run, name):
        return {"ok": True, "name": name, "tmux": False, "claude": False, "state": "dead"}

    # 2. claude process alive inside that tmux?
    r = ray.get(
        run.remote(
            f"tmux list-panes -t {name} -F '#{{pane_pid}}' | head -1 | "
            f"xargs -I{{}} sh -c 'pgrep -P {{}} -a 2>/dev/null | grep -q claude && echo ALIVE || echo GONE'"
        ),
        timeout=15,
    )
    claude_alive = "ALIVE" in r["stdout"]

    # 3. Capture pane for busy state
    pane = _capture(run, name, 40)
    busy = _is_busy(pane)

    if not claude_alive:
        state = "tmux_only"  # tmux alive but claude exited/crashed
    elif busy:
        state = "busy"
    else:
        state = "idle"

    return {
        "ok": True,
        "name": name,
        "tmux": True,
        "claude": claude_alive,
        "state": state,
        "busy": busy,
    }


def _load_owner_aliases() -> tuple[dict, dict]:
    """owners.yaml → (job_id→alias, ip→alias). Pure file read, no cluster round trip."""
    owners_file = os.environ.get(
        "LOOP_OWNERS_YAML", "/root/.openclaw/workspace/cauldron/data/owners.yaml")
    job_id_to_alias, ip_to_alias = {}, {}
    try:
        import yaml

        with open(owners_file) as f:
            owners = yaml.safe_load(f) or {}
        for jid, meta in (owners.get("by_job_id") or {}).items():
            exp, owner = meta.get("exp", ""), meta.get("owner", "")
            if exp:
                job_id_to_alias[jid] = f"{owner}/{exp}" if owner else exp
        for ip, meta in (owners.get("by_ip") or {}).items():
            exp, owner = meta.get("exp", ""), meta.get("owner", "")
            if exp:
                ip_to_alias[ip] = f"{owner}/{exp}" if owner else exp
    except Exception:
        pass
    return job_id_to_alias, ip_to_alias


# job facts come from loop-ray's registry actor, which is itself a cache the `loop-ray
# status` CLI also reads — so reading the actor is exactly as fresh as shelling out, minus
# ~4s of process start + working-dir packaging per call. Memoized because one roster
# refresh asks for this once per node it lists. Pass force=True (the board's manual
# refresh does) to bypass.
JOB_MAP_TTL = float(os.environ.get("CC_JOB_MAP_TTL", "60"))
_job_map_cache: tuple[float, dict] | None = None
_job_map_lock = threading.Lock()

LOOP_REGISTRY_NAME = "loop-registry"
LOOP_NAMESPACE = "loop"


def _get_job_name_map(force: bool = False) -> dict:
    """ip -> {job_name, job_id, alias, user, gpus, nnode, status}.

    Reads the loop-ray registry actor in-process. Degrades to bare `ray.nodes()` facts
    (node present, job_name empty) when the actor is unreachable — never to a subprocess,
    so a registry hiccup can't stall every caller for seconds.
    """
    global _job_map_cache
    with _job_map_lock:
        hit = _job_map_cache
        if not force and hit and time.time() - hit[0] < JOB_MAP_TTL:
            return hit[1]

    job_id_to_alias, ip_to_alias = _load_owner_aliases()
    out: dict = {}
    records = None
    try:
        _ensure_ray()
        reg = ray.get_actor(LOOP_REGISTRY_NAME, namespace=LOOP_NAMESPACE)
        records = ray.get(reg.list_all.remote(), timeout=20)
    except Exception as e:
        print(f"[cc_bridge] job_map: registry actor unavailable ({e!r}); "
              f"falling back to bare ray.nodes()", file=sys.stderr, flush=True)

    if isinstance(records, list):
        for r in records:
            if not isinstance(r, dict):
                continue
            ip = r.get("node_ip") or ""
            if not IP_RE.match(ip):
                continue
            job_id = r.get("job_id") or ""
            out[ip] = {
                "job_name": r.get("job_name") or r.get("agent_name") or "",
                "job_id": job_id,
                "alias": job_id_to_alias.get(job_id) or ip_to_alias.get(ip, ""),
                "user": r.get("user_name") or "",
                "gpus": r.get("gpus") or 0,
                "nnode": r.get("nnode") or 0,
                "status": r.get("status") or "",
            }

    # Union with live Ray facts: a node that just joined may not be in the registry yet,
    # and it should still be listable (blank job_name) rather than invisible.
    try:
        _ensure_ray()
        for n in ray.nodes():
            ip = n.get("NodeManagerAddress") or ""
            if not n.get("Alive") or not IP_RE.match(ip):
                continue
            entry = out.setdefault(ip, {"job_name": "", "job_id": "",
                                        "alias": ip_to_alias.get(ip, ""),
                                        "user": "", "status": ""})
            res = n.get("Resources") or {}
            entry.setdefault("gpus", int(res.get("GPU", 0)))
            entry["cpus"] = int(res.get("CPU", 0))
    except Exception:
        pass

    with _job_map_lock:
        _job_map_cache = (time.time(), out)
    return out


def _get_session_summary(run, name: str) -> str:
    """Get a meaningful summary from the session — find the last substantive CC output."""
    try:
        pane = _capture(run, name, 80)
        cleaned = _clean(pane)
        lines = cleaned.splitlines()

        # Skip lines that are just UI chrome (prompt, borders, status)
        chrome_markers = (
            "───", "───", "━━━", "bypass permissions",
            "new task?", "/clear to save", "shift+tab",
            "← for agents", "⏵⏵",
        )

        # Walk backwards to find substantive content
        substantive = []
        for line in reversed(lines):
            stripped = line.strip()
            if not stripped:
                continue
            # Skip chrome
            if any(m in stripped for m in chrome_markers):
                continue
            # Skip bare prompt chars
            if stripped in ("❯", ">", "$"):
                continue
            substantive.append(stripped)
            if len(substantive) >= 3:
                break

        substantive.reverse()
        return "\n".join(substantive) if substantive else "(empty session)"
    except Exception:
        return ""


def _live_node_ips() -> set:
    """IPs of Ray nodes currently alive. Raises on failure — callers must NOT treat an
    unreadable cluster as 'everything is dead' (that once wiped the whole registry)."""
    _ensure_ray()
    return {n.get("NodeManagerAddress") for n in ray.nodes() if n.get("Alive")}


def _live_node_ips_or_none() -> set | None:
    """Same, but None when liveness is unknowable (Ray blip / GCS stall)."""
    try:
        return _live_node_ips()
    except Exception as e:
        print(f"[cc_bridge] ray.nodes() unavailable: {e!r}", file=sys.stderr, flush=True)
        return None


MAX_OUTPUT_CHARS = int(os.environ.get("CC_MAX_OUTPUT_CHARS", "20000"))
CHROME_MARKERS = ("───", "━━━", "bypass permissions", "new task?", "/clear to save",
                  "shift+tab", "← for agents", "⏵⏵")


def _capture_ip(node_ip: str, name: str, lines: int = 120) -> str:
    """Capture a pane by IP (via sh), for the roster/list path."""
    return sh(node_ip, f"tmux capture-pane -t {name} -p -S -{lines}", 20)["stdout"]


def _summarize(node_ip: str, name: str) -> str:
    """Last few substantive lines (skip CC UI chrome) — the roster's 'what is it doing'."""
    try:
        picked = []
        for line in reversed(_clean(_capture_ip(node_ip, name, 80)).splitlines()):
            s = line.strip()
            if not s or s in ("❯", ">", "$") or any(m in s for m in CHROME_MARKERS):
                continue
            picked.append(s)
            if len(picked) >= 3:
                break
        picked.reverse()
        return "\n".join(picked) if picked else "(empty session)"
    except Exception:
        return ""


def cc_list(node_ip: str = None, with_summary: bool = True, force: bool = False) -> dict:
    """List cc sessions on one node, or (node_ip=None) across the live-registry nodes.

    Returns {ok, session_count, sessions:[{name,node_ip,job_name,alias,role,cwd,state,
    summary}], unreachable_nodes?}. For the full scan, dead registry nodes (absent from
    ray.nodes()) are pruned and skipped so one recycled node can't stall or hang the sweep.
    """
    jm = job_map(force=force)
    reg = _load_registry()
    pruned: list[str] = []
    if node_ip:
        targets = [_validate_ip(node_ip)]
    else:
        live = _live_node_ips_or_none()
        reg_ips = {v.get("node_ip") for v in reg.values() if v.get("node_ip")}
        if live is None:
            # Liveness unknown (Ray blip). Scan what the registry claims and DO NOT
            # prune: treating "can't ask" as "all dead" once emptied sessions.json.
            targets = sorted(reg_ips)
        else:
            # Auto-cleanup: a node absent from the Ray cluster is unreachable by
            # definition (we only reach panes via Ray), so its sessions are gone.
            # Guarded by `live` being non-empty — an empty cluster answer is suspect.
            if live:
                dead = [k for k, v in reg.items() if v.get("node_ip") not in live]
                if dead:
                    with _registry_txn():
                        cur = _load_registry()
                        for k in dead:
                            cur.pop(k, None)
                        _save_registry(cur)
                    reg = {k: v for k, v in reg.items() if k not in dead}
                    pruned = sorted(dead)
            targets = sorted(reg_ips & live) or sorted(live)

    sessions, unreachable = [], []
    for ip in targets:
        try:
            r = sh(ip, f"tmux list-sessions 2>/dev/null | grep '^{TMUX_PREFIX}' || echo NONE", 20)
        except Exception:
            unreachable.append(ip)
            continue
        if "NONE" in r["stdout"]:
            continue
        facts = jm.get(ip, {})
        for line in r["stdout"].strip().splitlines():
            name = line.split(":")[0].strip()
            # The name comes from remote `tmux ls` and goes straight back into a remote
            # command, so hold it to the same regex as caller-supplied names.
            if not name or not SESSION_NAME_RE.match(name):
                continue
            pane = ""
            try:
                pane = _capture_ip(ip, name, 40)
            except Exception:
                pass
            entry = {
                "name": name, "node_ip": ip,
                "job_name": facts.get("job_name", ""), "alias": facts.get("alias", ""),
                "role": (reg.get(name) or {}).get("role", ""),
                "cwd": (reg.get(name) or {}).get("cwd", ""),
                "state": "busy" if _is_busy(pane) else "idle",
            }
            if with_summary:
                entry["summary"] = _summarize(ip, name)
            sessions.append(entry)

    out = {"ok": True, "session_count": len(sessions), "sessions": sessions}
    if unreachable:
        out["unreachable_nodes"] = unreachable
    if pruned:
        out["pruned_sessions"] = pruned
    return out


def _list_single_node(node_ip: str) -> dict:
    run = _run_fn(node_ip)
    r = ray.get(
        run.remote(
            f"tmux list-sessions 2>/dev/null | grep '^{TMUX_PREFIX}' || echo NONE"
        ),
        timeout=15,
    )
    if "NONE" in r["stdout"]:
        return {"ok": True, "sessions": []}
    sessions = []
    for line in r["stdout"].strip().splitlines():
        parts = line.split(":")
        if parts:
            sessions.append({"name": parts[0].strip(), "info": line.strip()})
    return {"ok": True, "sessions": sessions}


def cc_stop(node_ip: str, name: str) -> dict:
    run = _run_fn(node_ip)
    name = _norm_name(name)
    with session_lock(node_ip, name):
        r = ray.get(
            run.remote(f"tmux kill-session -t {name} 2>&1 && echo OK || echo FAIL"),
            timeout=15,
        )
        if "OK" in r["stdout"]:
            _unregister_session(name)
            return {"ok": True, "name": name, "action": "stopped"}
    return {"ok": False, "error": r["stdout"]}


# ---------------------------------------------------------------------------
# Public surface for external callers (workbench/ccops.py). The driving logic
# above stays the single source of truth; these just expose it under the names
# ccops expects, plus cluster-level helpers ported from the RSI ccbridge.
# ---------------------------------------------------------------------------

# aliases: ccops calls the public spellings
norm_name = _norm_name
clean = _clean
validate_ip = _validate_ip
is_busy = _is_busy


def ray_mod():
    """Return the ray module, connected (ccops asks for it this way)."""
    _ensure_ray()
    return ray


def sh(node_ip: str, cmd: str, timeout: int = 30) -> dict:
    """Run one shell command on a node via Ray; returns {stdout,stderr,returncode}."""
    return ray.get(_run_fn(node_ip).remote(cmd, timeout), timeout=timeout + 15)




def alive_ips() -> list:
    """IPs of Ray nodes currently alive in the cluster."""
    return sorted(ip for ip in _live_node_ips() if ip)


def job_map(force: bool = False) -> dict:
    """ip -> job facts (job_name/job_id/alias/user/gpus/status), from loop-ray's registry
    actor + owners.yaml. Memoized JOB_MAP_TTL seconds; force=True bypasses the cache."""
    return _get_job_name_map(force=force)


def cc_cluster(force: bool = False) -> dict:
    """Cluster roster: one entry per node (union of loop-ray jobs + live Ray nodes).

    Each node carries `alive` (its raylet is in ray.nodes()) and `status` (loop-ray job
    state; "" = unknown, which downstream treats as live). ccops' node_live() reads both
    to decide whether a session is writable.
    """
    jm = job_map(force=force)
    live_or_none = _live_node_ips_or_none()
    if live_or_none is None:
        # Liveness unknown: report the nodes we know of without claiming any is dead.
        # ccops' node_live() refuses writes on alive=False, so guessing "dead" here
        # would lock out every write on a transient Ray blip.
        nodes = [{"node_ip": ip, **facts, "alive": True, "status": facts.get("status", "")}
                 for ip, facts in sorted(jm.items())]
        return {"ok": True, "node_count": len(nodes), "nodes": nodes,
                "liveness": "unknown (ray.nodes() unavailable)"}
    live = {ip for ip in live_or_none if ip}
    nodes = []
    for ip in sorted(set(jm) | live):
        facts = jm.get(ip, {})
        nodes.append({"node_ip": ip, **facts, "alive": ip in live, "status": facts.get("status", "")})
    return {"ok": True, "node_count": len(nodes), "nodes": nodes}


def main():
    p = argparse.ArgumentParser(description="CC Bridge — Claude Code on workers")
    sub = p.add_subparsers(dest="action", required=True)

    s = sub.add_parser("start")
    s.add_argument("node_ip")
    s.add_argument("--cwd", default="~")
    s.add_argument("--name", default=None)
    s.add_argument("--claude-path", default=None, help="Absolute path to claude binary (default: 'claude' from PATH)")

    s = sub.add_parser("send")
    s.add_argument("node_ip")
    s.add_argument("session_name")
    s.add_argument("message")
    s.add_argument("--wait", action="store_true", help="poll until CC finishes")
    s.add_argument("--timeout", type=int, default=600)

    s = sub.add_parser("read")
    s.add_argument("node_ip")
    s.add_argument("session_name")
    s.add_argument("--lines", type=int, default=80)

    s = sub.add_parser("keys")
    s.add_argument("node_ip")
    s.add_argument("session_name")
    s.add_argument("key", help="tmux key: Enter/Escape/Up/Down/C-c or a literal like '2'")

    s = sub.add_parser("status")
    s.add_argument("node_ip")
    s.add_argument("session_name")

    s = sub.add_parser("list")
    s.add_argument("node_ip", nargs="?", default=None)

    s = sub.add_parser("stop")
    s.add_argument("node_ip")
    s.add_argument("session_name")

    args = p.parse_args()
    ray.init("auto", ignore_reinit_error=True)

    if args.action == "start":
        out = cc_start(args.node_ip, cwd=args.cwd, name=args.name, claude_path=args.claude_path)
    elif args.action == "send":
        out = cc_send(args.node_ip, args.session_name, args.message,
                      wait=args.wait, timeout=args.timeout)
    elif args.action == "read":
        out = cc_read(args.node_ip, args.session_name, lines=args.lines)
    elif args.action == "keys":
        out = cc_keys(args.node_ip, args.session_name, args.key)
    elif args.action == "status":
        out = cc_status(args.node_ip, args.session_name)
    elif args.action == "list":
        out = cc_list(args.node_ip)
    elif args.action == "stop":
        out = cc_stop(args.node_ip, args.session_name)
    else:
        p.print_help()
        sys.exit(1)

    # Print deterministic delimiter before JSON so consumers can reliably
    # find the start of structured output (skips Ray warnings on stdout).
    print(JSON_DELIMITER)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
