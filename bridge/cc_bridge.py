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
# Codex (OpenAI CLI) equivalents. These scraped markers are best-effort and only back the
# CLI/MCP path (cc_send --wait, cc_status/cc_list); the workbench board reads the authoritative
# @cc_state tmux option written by Codex hooks. Tune freely if Codex changes its TUI strings.
CODEX_BUSY_MARKERS = (
    "Esc to interrupt",
    "esc to interrupt",
    "Working",
    "Thinking",
    "Esc to cancel",
)
# Tightened 2026-11-16 from real captured panes (codex v0.169.0). The ONLY reliable "the
# interactive prompt is live" signals are the input box and the ready-screen hint bar — both
# absent while a popup/onboarding screen is up. The old generic ">" / "Codex" matched the
# startup banner (">_ OpenAI Codex") and almost any pane, so they falsely reported ready while
# a popup was still on screen; removed.
CODEX_READY_MARKERS = (
    "Ask Codex to do anything",   # the live input box
    "? for shortcuts",            # ready-screen hint bar
)
# Popup/onboarding screens that must NOT be read as ready. Captured 2026-11-16 from a fresh
# CODEX_HOME first-run (a sign-in MENU). With tools/config_codex.sh applied, codex goes
# straight to the prompt in an untrusted dir (no trust popup) — verified live two ways — so in
# production these mainly guard the unconfigured/first-run case. A sign-in menu must NOT be
# blind-passed (Enter would start ChatGPT device auth), so codex carries no auto-pass key
# (see AGENT_KINDS 'prompt_key'): a popup leaves start 'unconfirmed' with the pane surfaced,
# never mistaken for ready.
CODEX_PROMPT_MARKERS = (
    "Welcome to Codex",
    "Choose how you want to use Codex",
    "Press enter to continue",
    "Sign in with ChatGPT",
)
# Claude's first-boot "trust this folder" confirmation — a yes/no, safe to auto-pass with Enter.
CLAUDE_PROMPT_MARKERS = (
    "trust this folder",
    "Yes, I trust",
)

# Per-agent launch + detection spec. `kind` selects which binary/flags to start and which
# marker sets to scrape. Binaries are env-overridable so a node can point at a non-PATH
# install or dodge the Noah /usr/bin/codex conflict (CC_CODEX_BIN=/usr/bin/codex-cli).
AGENT_KINDS = {
    "claude": {
        "bin": os.environ.get("CC_CLAUDE_BIN", "claude"),
        "args": ["--dangerously-skip-permissions"],
        "env": "IS_SANDBOX=1 ",   # bypasses root+skip-permissions block in CC 2.1+
        "proc": "claude",
        "busy": BUSY_MARKERS,
        "ready": READY_MARKERS,
        "prompts": CLAUDE_PROMPT_MARKERS,
        "prompt_key": "Enter",    # trust-folder is a yes/no → Enter passes it
    },
    "codex": {
        "bin": os.environ.get("CC_CODEX_BIN", "codex"),
        # -a never -s danger-full-access = yolo mode; --dangerously-bypass-hook-trust lets our
        # @cc_state hooks run unattended (same risk posture as danger-full-access).
        # --no-alt-screen = inline TUI (no alternate screen) so the board's scrollback/history
        # is preserved and scrollable, matching claude.
        "args": ["-a", "never", "-s", "danger-full-access",
                 "--dangerously-bypass-hook-trust", "--no-alt-screen"],
        "env": "",
        "proc": "codex",
        "busy": CODEX_BUSY_MARKERS,
        "ready": CODEX_READY_MARKERS,
        "prompts": CODEX_PROMPT_MARKERS,
        "prompt_key": None,       # sign-in menu → do NOT blind-Enter; surface it instead
        # OPENAI_API_KEY is resolved at launch from loop's config/envs (the owner-managed source
        # of truth, same precedence config_codex.sh uses) and injected into the start command,
        # so codex has the key regardless of shell dotfiles. The value never lands in this
        # source/logs. See _resolve_env_prefix.
        "inject_env": "OPENAI_API_KEY",
        "key_vars": ("CODEX_API_KEY", "ANTHROPIC_API_KEY"),
    },
}
DEFAULT_KIND = "claude"


def _kind_spec(kind: str) -> dict:
    """Resolve an agent-kind spec; unknown kinds fall back to the default (claude)."""
    return AGENT_KINDS.get(kind or DEFAULT_KIND, AGENT_KINDS[DEFAULT_KIND])


def _registered_kind(name: str) -> str:
    """Agent kind recorded for a session; legacy/unregistered sessions default to claude."""
    return (_load_registry().get(name) or {}).get("kind") or DEFAULT_KIND


def _boot_state(pane: str, kind: str) -> str:
    """Classify a booting pane for `kind`, pure (unit-testable):
      'prompt'  — a popup/onboarding screen is up; NEVER treat as ready (checked first, so a
                  banner that also contains a ready marker can't sneak through during a popup).
      'ready'   — the interactive prompt is live.
      'waiting' — neither yet; keep polling.
    """
    spec = _kind_spec(kind)
    if any(m in pane for m in spec.get("prompts", ())):
        return "prompt"
    if any(m in pane for m in spec["ready"]):
        return "ready"
    return "waiting"


def _config_envs_path() -> str:
    """Path to loop's `config/envs` (the owner-managed key source). `CC_ENVS_FILE` overrides;
    default is derived from this file's real location (loop/3rdparty/cc-bridge-plugin/bridge →
    loop/config/envs), so a symlinked deploy still points back at the real repo."""
    return os.environ.get("CC_ENVS_FILE") or os.path.normpath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", "config", "envs"))


def _read_envs_var(path: str, names: tuple) -> str:
    """Return the first of `names` set in a shell env file (`export NAME=val` / `NAME=val`),
    honoring later overrides (last assignment wins, like sourcing). Quotes stripped. Does NOT
    execute the file. '' if the file is unreadable or none of the names are set non-empty."""
    try:
        txt = open(path).read()
    except OSError:
        return ""
    for name in names:
        found = None
        for line in txt.splitlines():
            m = re.match(rf"^\s*(?:export\s+)?{re.escape(name)}\s*=\s*(.*)$", line)
            if m:
                found = m.group(1).strip()
        if found:
            if (found[:1] == '"' and found[-1:] == '"') or (found[:1] == "'" and found[-1:] == "'"):
                found = found[1:-1]
            if found:
                return found
    return ""


def _resolve_env_prefix(kind: str) -> str:
    """Env-var prefix prepended to a kind's launch command.

    Claude → the static `env` (IS_SANDBOX=1). Codex → the static `env` plus its API key injected
    as `OPENAI_API_KEY=…`, so codex has the key regardless of shell dotfiles (the tmux pane may
    not source ~/.bashrc). The key is resolved from `CC_CODEX_API_KEY` (direct, for callers that
    inject it) else loop's `config/envs` (CODEX_API_KEY, then ANTHROPIC_API_KEY — same precedence
    as config_codex.sh). The VALUE never appears in this source, logs, or comments — only its
    env-var name and the config/envs path. No key found → a HARD error naming config/envs and the
    vars, so we never start a session doomed to 401. Raises ValueError on that failure."""
    spec = _kind_spec(kind)
    base = spec.get("env", "")
    inject = spec.get("inject_env")
    if not inject:
        return base
    envs = _config_envs_path()
    val = os.environ.get("CC_CODEX_API_KEY") or _read_envs_var(envs, spec.get("key_vars", ()))
    if not val:
        raise ValueError(
            f"{kind}: no API key for {inject} — set one of "
            f"{', '.join(spec.get('key_vars', ()))} in {envs} (or export CC_CODEX_API_KEY)")
    return f"{base}{inject}={shlex.quote(val)} "


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


def _register_session(name: str, node_ip: str, cwd: str, role: str = "", kind: str = DEFAULT_KIND):
    """Register a session in the registry."""
    with _registry_txn():
        registry = _load_registry()
        registry[name] = {
            "node_ip": node_ip,
            "cwd": cwd,
            "role": role,
            "kind": kind,
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


def _is_busy(pane: str, kind: str = DEFAULT_KIND) -> bool:
    return any(m in pane for m in _kind_spec(kind)["busy"])


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


def _launch_cmd(kind: str, name: str, q_cwd: str, bin_override: str = None,
                env_prefix: str = None) -> str:
    """Build the remote shell command that starts an agent of `kind` in a fresh tmux.

    Pure string construction (no Ray) so launch logic is unit-testable. `name` is already
    validated (SESSION_NAME_RE) and `q_cwd` already quoted by the caller. Sets a session-scoped
    `@cc_kind` tmux option right after new-session so the roster/board and the kind-aware
    hooks can tell claude from codex. `bin_override` (claude's legacy claude_path) wins over the
    configured binary. `env_prefix` overrides the kind's static env (cc_start passes the
    secret-resolved prefix here for codex); default keeps the static `env`, so the claude
    command stays byte-identical and this function never has to read secrets.
    """
    spec = _kind_spec(kind)
    kind_key = kind if kind in AGENT_KINDS else DEFAULT_KIND
    agent_bin = bin_override or spec["bin"]
    agent_args = " ".join(shlex.quote(a) for a in spec["args"])
    env = spec["env"] if env_prefix is None else env_prefix
    # cwd/bin come from a caller (LLM/MCP/workbench) and land in a remote shell → quote them.
    inner = f"cd {q_cwd} && {env}{shlex.quote(agent_bin)} {agent_args}"
    return (
        f"tmux new-session -d -s {name} -c {q_cwd} && "
        f"tmux set -t {name} @cc_kind {shlex.quote(kind_key)} && "
        f"tmux send-keys -t {name} {shlex.quote(inner)} Enter && "
        f"echo OK"
    )


def cc_start(node_ip: str, name: str = None, cwd: str = "~", role: str = "",
             claude_path: str = None, kind: str = DEFAULT_KIND) -> dict:
    """Start an agent session in a fresh tmux on `node_ip`.

    `kind` selects the agent (claude | codex); unknown kinds fall back to claude. Positional
    order is (node_ip, name, cwd, role) — ccops.py calls it positionally that way; do not
    reorder without fixing that caller. `claude_path` is kept as a legacy binary override.
    """
    run = _run_fn(node_ip)
    kind = kind if kind in AGENT_KINDS else DEFAULT_KIND
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

        # Resolve the env prefix BEFORE touching tmux: for codex this reads the API key and
        # fails loudly if its secret file is missing, so we never start a 401-doomed session.
        try:
            env_prefix = _resolve_env_prefix(kind)
        except ValueError as exc:
            return {"ok": False, "error": str(exc), "name": name}

        q_cwd = quote_path(resolve_cwd)
        cmd = _launch_cmd(kind, name, q_cwd, bin_override=claude_path, env_prefix=env_prefix)
        r = ray.get(run.remote(cmd, timeout=20), timeout=25)
        if "OK" not in r["stdout"]:
            return {"ok": False, "error": r["stderr"] or r["stdout"], "name": name}

        # Wait for the agent to boot. A popup/onboarding screen is NEVER ready: if this kind has
        # a safe auto-pass key (claude's trust-folder → Enter) we send it; otherwise (codex's
        # sign-in menu) we leave it for a human/agent rather than blind-navigating a menu.
        prompt_key = _kind_spec(kind).get("prompt_key")
        for _ in range(20):
            time.sleep(1)
            pane = _capture(run, name, 40)
            st = _boot_state(pane, kind)
            if st == "prompt":
                if prompt_key:
                    _send_keys_unlocked(run, name, key=prompt_key)
                    time.sleep(2)
                continue  # popup on screen → not ready (passed or surfaced), keep waiting
            if st == "ready":
                _register_session(name, node_ip, cwd, role, kind)
                return {"ok": True, "name": name, "node": node_ip, "node_ip": node_ip,
                        "cwd": cwd, "role": role, "kind": kind, "state": "ready"}

        # started but readiness unconfirmed (still booting, or a popup we won't auto-pass) —
        # register anyway and surface the current pane so the caller can see what it's stuck on.
        _register_session(name, node_ip, cwd, role, kind)
        return {"ok": True, "name": name, "node": node_ip, "node_ip": node_ip,
                "cwd": cwd, "role": role, "kind": kind, "state": "started_unconfirmed",
                "pane": _clean(_capture(run, name, 40))}


def cc_send(node_ip: str, name: str, message: str, wait: bool = False,
            timeout: int = 120) -> dict:
    run = _run_fn(node_ip)
    name = _norm_name(name)
    if not _has_session(run, name):
        return {"ok": False, "error": f"Session '{name}' not found"}
    kind = _registered_kind(name)

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
        if _is_busy(pane, kind):
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
    kind = _registered_kind(name)
    return {"ok": True, "name": name, "kind": kind, "busy": _is_busy(pane, kind), "output": _clean(pane)}


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

    kind = _registered_kind(name)
    proc = _kind_spec(kind)["proc"]

    # 1. tmux session exists?
    if not _has_session(run, name):
        return {"ok": True, "name": name, "kind": kind, "tmux": False, "claude": False, "state": "dead"}

    # 2. agent process alive inside that tmux? (proc name depends on kind: claude / codex)
    r = ray.get(
        run.remote(
            f"tmux list-panes -t {name} -F '#{{pane_pid}}' | head -1 | "
            f"xargs -I{{}} sh -c 'pgrep -P {{}} -a 2>/dev/null | grep -q {shlex.quote(proc)} && echo ALIVE || echo GONE'"
        ),
        timeout=15,
    )
    proc_alive = "ALIVE" in r["stdout"]

    # 3. Capture pane for busy state
    pane = _capture(run, name, 40)
    busy = _is_busy(pane, kind)

    if not proc_alive:
        state = "tmux_only"  # tmux alive but the agent process exited/crashed
    elif busy:
        state = "busy"
    else:
        state = "idle"

    return {
        "ok": True,
        "name": name,
        "kind": kind,
        "tmux": True,
        "claude": proc_alive,  # kept key for back-compat; means "agent process alive"
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
            kind = (reg.get(name) or {}).get("kind") or DEFAULT_KIND
            entry = {
                "name": name, "node_ip": ip,
                "job_name": facts.get("job_name", ""), "alias": facts.get("alias", ""),
                "role": (reg.get(name) or {}).get("role", ""),
                "cwd": (reg.get(name) or {}).get("cwd", ""),
                "kind": kind,
                "state": "busy" if _is_busy(pane, kind) else "idle",
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
    s.add_argument("--kind", default=DEFAULT_KIND, choices=sorted(AGENT_KINDS), help="agent to launch (default: claude)")
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
        out = cc_start(args.node_ip, cwd=args.cwd, name=args.name,
                       claude_path=args.claude_path, kind=args.kind)
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
