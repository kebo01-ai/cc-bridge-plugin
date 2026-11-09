#!/usr/bin/env python3
"""cc-mcp — Streamable-HTTP MCP server exposing CC Bridge over the Ray cluster.

Two tool groups on one server:
  - bridge (needs Ray):   cc_send / cc_read / cc_list / cc_status  -> drives worker tmux
  - mailbox (sqlite):     cc_register / cc_post / cc_inbox         -> agent-to-agent channel

Deploy on the Ray Head. Remote Claude clients point .mcp.json at this server's URL
with a bearer token. See README.md.

Run:
  CC_MCP_TOKEN=xxx python mcp/server.py --host 0.0.0.0 --port 8765
"""
import argparse
import contextlib
import hmac
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import bridge_tools  # noqa: E402
import mailbox  # noqa: E402
from mcp.server.mcpserver import MCPServer  # noqa: E402  (mcp 2.x; FastMCP was renamed MCPServer)
from mcp.server.transport_security import TransportSecuritySettings  # noqa: E402

# Set by main() before the app is built.
CONFIG = {"token": None, "ray_address": "auto", "use_ray": True, "allowed_hosts": None}


@contextlib.asynccontextmanager
async def lifespan(_server: "MCPServer"):
    """Init Ray once at startup (bridge tools need it); mailbox tools don't.

    A Ray failure degrades to mailbox-only instead of taking the server down.
    """
    if CONFIG["use_ray"]:
        try:
            import ray

            if not ray.is_initialized():
                ray.init(address=CONFIG["ray_address"], ignore_reinit_error=True)
            print("[cc-mcp] Ray connected; bridge tools live", flush=True)
        except Exception as e:
            print(f"[cc-mcp] WARNING: Ray init failed ({e!r}); bridge tools disabled, mailbox still up",
                  flush=True)
    yield


def build_mcp() -> "MCPServer":
    mcp = MCPServer(
        "cc-bridge",
        instructions="Drive and observe Claude Code sessions on Ray worker nodes, "
        "and exchange messages between agents via a shared mailbox.",
        lifespan=lifespan,
    )
    # bridge group (Ray -> tmux)
    mcp.add_tool(bridge_tools.cc_send)
    mcp.add_tool(bridge_tools.cc_read)
    mcp.add_tool(bridge_tools.cc_list)
    mcp.add_tool(bridge_tools.cc_status)
    mcp.add_tool(bridge_tools.cc_keys)
    mcp.add_tool(bridge_tools.cc_stop)
    # mailbox group (sqlite); rename to cc_* for a clean tool namespace
    mcp.add_tool(mailbox.register, name="cc_register",
                 description="Create/claim a mailbox inbox under `name` (idempotent).")
    mcp.add_tool(mailbox.post, name="cc_post",
                 description="Post a message to recipient `to`'s inbox.")
    mcp.add_tool(mailbox.inbox, name="cc_inbox",
                 description="Fetch messages with id > since_id for `me`; wait=true long-polls until "
                             "some arrive or timeout. Pass the returned `cursor` as next since_id.")
    return mcp


class BearerAuth:
    """ASGI middleware: require `Authorization: Bearer <token>` on every HTTP request.

    A token is MANDATORY unless the operator passes --insecure. These tools type into a
    root `claude --dangerously-skip-permissions` session on cluster workers, so an open
    port here is remote code execution, not just an open dashboard.
    """

    def __init__(self, app, token: str | None):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not self.token:
            return await self.app(scope, receive, send)
        raw = dict(scope.get("headers") or []).get(b"authorization", b"")
        try:
            presented = raw.decode("latin-1")
        except Exception:
            presented = ""
        # Constant-time: a plain == leaks the token prefix through response timing.
        if hmac.compare_digest(presented, f"Bearer {self.token}"):
            return await self.app(scope, receive, send)
        await send({"type": "http.response.start", "status": 401,
                    "headers": [(b"content-type", b"text/plain"), (b"www-authenticate", b"Bearer")]})
        await send({"type": "http.response.body", "body": b"unauthorized"})


def main():
    p = argparse.ArgumentParser(description="cc-mcp Streamable-HTTP server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--token-env", default="CC_MCP_TOKEN",
                   help="env var holding the bearer token (required unless --insecure)")
    p.add_argument("--insecure", action="store_true",
                   help="run with NO authentication. These tools drive a root "
                        "--dangerously-skip-permissions Claude on workers, so this is "
                        "remote code execution for anyone who can reach the port. "
                        "Localhost experiments only.")
    p.add_argument("--ray-address", default="auto")
    p.add_argument("--no-ray", action="store_true", help="skip Ray init (mailbox-only / testing)")
    p.add_argument("--allowed-hosts", default=None,
                   help="comma-list of Host:port to allow (keeps DNS-rebinding protection on). "
                        "Omit to disable the check and accept any Host (internal + token-gated).")
    opt = p.parse_args()

    CONFIG["token"] = os.environ.get(opt.token_env) or None
    CONFIG["ray_address"] = opt.ray_address
    CONFIG["use_ray"] = not opt.no_ray
    CONFIG["allowed_hosts"] = [h.strip() for h in opt.allowed_hosts.split(",")] if opt.allowed_hosts else None

    if not CONFIG["token"]:
        if not opt.insecure:
            p.error(
                f"no token in ${opt.token_env}. These tools type into a root "
                f"`claude --dangerously-skip-permissions` session on cluster workers, so an "
                f"unauthenticated port is remote code execution.\n"
                f"  Set one:   export {opt.token_env}=$(python3 -c 'import secrets;print(secrets.token_hex(16))')\n"
                f"  Or accept the risk explicitly with --insecure (localhost only)."
            )
        print("[cc-mcp] *** --insecure: NO AUTHENTICATION. Anyone who can reach this port "
              "can run commands on cluster workers. ***", flush=True)

    import uvicorn

    # DNS-rebinding protection rejects any Host not in the allowlist with HTTP 421.
    # Default (internal cluster + bearer token): disable it so clients can use the real
    # host:port. Pass --allowed-hosts to keep protection on with an explicit allowlist.
    if CONFIG["allowed_hosts"]:
        sec = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=CONFIG["allowed_hosts"],
            allowed_origins=CONFIG["allowed_hosts"],
        )
    else:
        sec = TransportSecuritySettings(enable_dns_rebinding_protection=False)

    app = BearerAuth(
        build_mcp().streamable_http_app(
            streamable_http_path="/mcp", json_response=True, stateless_http=True,
            transport_security=sec,
        ),
        CONFIG["token"],
    )
    print(f"[cc-mcp] serving on http://{opt.host}:{opt.port}/mcp  (ray={'off' if opt.no_ray else CONFIG['ray_address']})",
          flush=True)
    uvicorn.run(app, host=opt.host, port=opt.port)


if __name__ == "__main__":
    main()
