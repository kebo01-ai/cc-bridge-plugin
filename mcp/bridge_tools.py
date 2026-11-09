#!/usr/bin/env python3
"""Bridge tool group — thin wrappers over bridge/cc_bridge.py (Ray -> tmux).

Each wrapper calls the existing cc_bridge function and returns its dict verbatim,
converting unexpected exceptions into {"ok": False, "error": ...} so a single bad
call never takes the MCP server down. Ray must already be initialized by the
server lifespan before these run.
"""
import os
import sys
import time

# bridge/cc_bridge.py is a sibling of this mcp/ dir.
_BRIDGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bridge")
if _BRIDGE_DIR not in sys.path:
    sys.path.insert(0, _BRIDGE_DIR)

try:
    import cc_bridge  # 577-line canonical bridge — single source of truth
    _IMPORT_ERR = None
except Exception as e:  # ray missing / not a Head box → mailbox-only server still runs
    cc_bridge = None
    _IMPORT_ERR = f"{type(e).__name__}: {e}"


def _guard(fn_name, *args, **kwargs) -> dict:
    if cc_bridge is None:
        return {"ok": False, "error": f"bridge unavailable (needs ray on this host): {_IMPORT_ERR}"}
    try:
        return getattr(cc_bridge, fn_name)(*args, **kwargs)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# --- node-name echo (addressing stays by IP; we only annotate results) ---
# Resolving ip -> job_name parses `loop-ray status` (~seconds), so cache it with a TTL
# and reuse the bridge's existing _get_job_name_map() rather than duplicating the logic.
_JOBMAP = {"ts": 0.0, "map": {}}
_JOBMAP_TTL = 120.0


def _node_label(node_ip: str) -> dict:
    ident = {"node_ip": node_ip}
    if cc_bridge is None or not node_ip:
        return ident
    now = time.time()
    if now - _JOBMAP["ts"] > _JOBMAP_TTL:
        try:
            _JOBMAP["map"] = cc_bridge._get_job_name_map() or {}
            _JOBMAP["ts"] = now
        except Exception:
            pass
    info = _JOBMAP["map"].get(node_ip) or {}
    if info.get("job_name"):
        ident["job_name"] = info["job_name"]
    if info.get("alias"):
        ident["alias"] = info["alias"]
    return ident


def _enrich(res: dict, node_ip: str) -> dict:
    """Annotate a successful result with node_ip + job_name (node name). Non-destructive."""
    if isinstance(res, dict):
        for k, v in _node_label(node_ip).items():
            res.setdefault(k, v)
    return res


def cc_send(node_ip: str, session: str, message: str, wait: bool = True, timeout: int = 600) -> dict:
    """Send a message to a running CC session's tmux and (by default) wait until it goes idle."""
    return _enrich(_guard("cc_send", node_ip, session, message, wait=wait, timeout=timeout), node_ip)


def cc_read(node_ip: str, session: str, lines: int = 80) -> dict:
    """Read the last N lines of a CC session's tmux pane."""
    return _enrich(_guard("cc_read", node_ip, session, lines=lines), node_ip)


def cc_list(node_ip: str = None) -> dict:
    """List CC sessions on a node (omit node_ip to scan all registered nodes)."""
    return _guard("cc_list", node_ip)


def cc_status(node_ip: str, session: str) -> dict:
    """Health of a CC session: tmux alive + claude process alive + busy/idle."""
    return _enrich(_guard("cc_status", node_ip, session), node_ip)


def cc_keys(node_ip: str, session: str, key: str) -> dict:
    """Send a raw key to a CC session (Enter/Escape/Up/Down/C-c/Y/N or a literal like '2').

    Use to interrupt a busy session (Escape), answer confirmation prompts / menus,
    or send a bare Enter — things cc_send (which types text) can't do.
    """
    return _enrich(_guard("cc_keys", node_ip, session, key), node_ip)


def cc_stop(node_ip: str, session: str) -> dict:
    """Kill a CC session's tmux and unregister it."""
    return _enrich(_guard("cc_stop", node_ip, session), node_ip)
