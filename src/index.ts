import { Type, type Static } from "@sinclair/typebox";
import { defineToolPlugin } from "openclaw/plugin-sdk/tool-plugin";
import { execFile } from "node:child_process";
import { resolve } from "node:path";

/**
 * CC Bridge — OpenClaw Plugin
 *
 * Manage Claude Code sessions on worker nodes via tmux + Ray remote.
 * All paths and settings are configurable via plugin config.
 */

const JSON_DELIMITER = "__CC_BRIDGE_JSON__";

/** Per-session mutex: prevents concurrent sends to the same session */
const sessionLocks = new Map<string, Promise<unknown>>();

/** Active CC session state — only one session can be active at a time */
let activeSession: { node_ip: string; session_name: string } | null = null;

function lockKey(nodeIp: string, sessionName: string): string {
  return `${nodeIp}:${sessionName}`;
}

/** Configuration schema — all paths configurable, sensible defaults for standalone use */
const ConfigSchema = Type.Object({
  pythonPath: Type.Optional(
    Type.String({
      description: "Path to Python interpreter with ray installed (default: python3)",
      default: "python3",
    })
  ),
  bridgePath: Type.Optional(
    Type.String({
      description: "Path to cc_bridge.py script (default: ./bridge/cc_bridge.py relative to plugin dir)",
    })
  ),
  bridgeCwd: Type.Optional(
    Type.String({
      description: "Working directory for bridge script execution (default: plugin directory)",
    })
  ),
  rayAddress: Type.Optional(
    Type.String({
      description: "Ray cluster address (default: 'auto' — connects to local cluster)",
      default: "auto",
    })
  ),
  sessionPrefix: Type.Optional(
    Type.String({
      description: "Prefix for tmux session names (default: 'cc-')",
      default: "cc-",
    })
  ),
  claudePath: Type.Optional(
    Type.String({
      description: "Absolute path to claude binary on worker nodes (default: 'claude' from PATH)",
    })
  ),
  defaultCwd: Type.Optional(
    Type.String({
      description: "Default working directory for new CC sessions on workers",
    })
  ),
});

type Config = Static<typeof ConfigSchema>;

/**
 * Parse JSON output from the bridge script using the deterministic delimiter.
 * Falls back to indexOf("{") if delimiter not found (backward compat).
 */
function parseOutput(output: string): unknown {
  const delimIdx = output.indexOf(JSON_DELIMITER);
  if (delimIdx >= 0) {
    const jsonStr = output.slice(delimIdx + JSON_DELIMITER.length).trim();
    try {
      return JSON.parse(jsonStr);
    } catch {}
  }
  const braceIdx = output.indexOf("{");
  if (braceIdx >= 0) {
    try {
      return JSON.parse(output.slice(braceIdx));
    } catch {}
  }
  return null;
}

/**
 * Resolve bridge script path and python path from config.
 */
function resolvePaths(config: Config): { pythonPath: string; bridgePath: string; cwd: string } {
  // import.meta.dirname points to dist/ after compilation; bridge/ lives at project root
  const distDir = resolve(import.meta.dirname || __dirname);
  const pluginDir = resolve(distDir, "..");
  const pythonPath = config.pythonPath || "python3";
  const bridgePath = config.bridgePath || resolve(pluginDir, "bridge", "cc_bridge.py");
  const cwd = config.bridgeCwd || pluginDir;
  return { pythonPath, bridgePath, cwd };
}

/**
 * Run the Python bridge script asynchronously via child_process.execFile.
 * No shell involved — args passed as array (injection-safe).
 * Concurrent calls to the same session are serialized via per-session lock.
 */
async function runBridge(args: string[], config: Config, lock?: string): Promise<unknown> {
  const { pythonPath, bridgePath, cwd } = resolvePaths(config);

  const doRun = (): Promise<unknown> =>
    new Promise((res) => {
      execFile(
        pythonPath,
        [bridgePath, ...args],
        {
          cwd,
          timeout: 660_000,
          encoding: "utf-8",
          env: {
            ...process.env,
            RAY_ADDRESS: config.rayAddress || "auto",
            CC_BRIDGE_PREFIX: config.sessionPrefix || "cc-",
          },
          maxBuffer: 4 * 1024 * 1024,
        },
        (error, stdout, _stderr) => {
          const output = stdout || (error as any)?.stdout || "";
          const parsed = parseOutput(output);
          if (parsed) {
            res(parsed);
            return;
          }
          if (error) {
            res({ ok: false, error: error.message?.slice(0, 500) || String(error) });
          } else {
            res({ ok: false, error: "No JSON output", raw: output.slice(0, 500) });
          }
        }
      );
    });

  if (!lock) return doRun();

  const prev = sessionLocks.get(lock) ?? Promise.resolve();
  const next = prev.then(doRun, doRun);
  sessionLocks.set(lock, next);
  next.finally(() => {
    if (sessionLocks.get(lock) === next) sessionLocks.delete(lock);
  });
  return next;
}

export default defineToolPlugin({
  id: "cc-bridge-plugin",
  name: "CC Bridge",
  description: "Manage Claude Code sessions on worker nodes via tmux + Ray remote.",
  configSchema: ConfigSchema,
  tools: (tool) => [
    tool({
      name: "cc_start",
      label: "Start CC Session",
      description:
        "Start a new Claude Code session in a tmux window on a worker node. Returns session name.",
      parameters: Type.Object({
        node_ip: Type.String({ description: "Worker node IP address" }),
        name: Type.Optional(
          Type.String({ description: "Session name (auto-generated if omitted)" })
        ),
        cwd: Type.Optional(
          Type.String({ description: "Working directory on the worker (default: ~)" })
        ),
      }),
      async execute({ node_ip, name, cwd }, config) {
        const args = ["start", node_ip];
        if (cwd) args.push("--cwd", cwd);
        else if (config.defaultCwd) args.push("--cwd", config.defaultCwd);
        if (name) args.push("--name", name);
        if (config.claudePath) args.push("--claude-path", config.claudePath);
        return runBridge(args, config);
      },
    }),

    tool({
      name: "cc_send",
      label: "Send to CC Session",
      description:
        "Send a message/instruction to a running Claude Code session. Use --wait to poll until CC finishes responding.",
      parameters: Type.Object({
        node_ip: Type.String({ description: "Worker node IP" }),
        session_name: Type.String({ description: "CC session name" }),
        message: Type.String({ description: "Message to send to Claude Code" }),
        wait: Type.Optional(
          Type.Boolean({ description: "Wait for CC to finish responding (default: true)", default: true })
        ),
        timeout: Type.Optional(
          Type.Number({ description: "Max seconds to wait (default: 600)", default: 600 })
        ),
      }),
      async execute({ node_ip, session_name, message, wait, timeout }, config) {
        const args = ["send", node_ip, session_name, message];
        if (wait !== false) args.push("--wait");
        if (timeout && timeout !== 600) args.push("--timeout", String(timeout));
        return runBridge(args, config, lockKey(node_ip, session_name));
      },
    }),

    tool({
      name: "cc_read",
      label: "Read CC Session",
      description:
        "Read the current tmux pane output of a CC session. Shows last N lines.",
      parameters: Type.Object({
        node_ip: Type.String({ description: "Worker node IP" }),
        session_name: Type.String({ description: "CC session name" }),
        lines: Type.Optional(
          Type.Number({ description: "Lines to capture (default: 80)", default: 80 })
        ),
      }),
      async execute({ node_ip, session_name, lines }, config) {
        const args = ["read", node_ip, session_name];
        if (lines && lines !== 80) args.push("--lines", String(lines));
        return runBridge(args, config, lockKey(node_ip, session_name));
      },
    }),

    tool({
      name: "cc_keys",
      label: "Send Keys to CC",
      description:
        "Send raw keys to a CC session (Enter/Escape/Up/Down/C-c/Y/N etc). For menus, confirmations, or interrupts.",
      parameters: Type.Object({
        node_ip: Type.String({ description: "Worker node IP" }),
        session_name: Type.String({ description: "CC session name" }),
        key: Type.String({ description: "tmux key token: Enter, Escape, Up, Down, C-c, Y, N, etc." }),
      }),
      async execute({ node_ip, session_name, key }, config) {
        return runBridge(["keys", node_ip, session_name, key], config, lockKey(node_ip, session_name));
      },
    }),

    tool({
      name: "cc_list",
      label: "List CC Sessions",
      description: "List all CC Bridge sessions running on a worker node.",
      parameters: Type.Object({
        node_ip: Type.Optional(
          Type.String({ description: "Worker node IP (omit to list all known nodes)" })
        ),
      }),
      async execute({ node_ip }, config) {
        const args = ["list"];
        if (node_ip) args.push(node_ip);
        const result = (await runBridge(args, config)) as any;
        // Annotate each session with active status (only one can be active)
        if (result && result.ok && Array.isArray(result.sessions)) {
          for (const s of result.sessions) {
            s.active =
              activeSession !== null &&
              activeSession.node_ip === s.node_ip &&
              activeSession.session_name === s.name;
          }
        }
        return result;
      },
    }),

    tool({
      name: "cc_stop",
      label: "Stop CC Session",
      description: "Kill a CC Bridge session on a worker node.",
      parameters: Type.Object({
        node_ip: Type.String({ description: "Worker node IP" }),
        session_name: Type.String({ description: "CC session name to stop" }),
      }),
      async execute({ node_ip, session_name }, config) {
        // If stopping the active session, auto-deactivate
        if (
          activeSession &&
          activeSession.node_ip === node_ip &&
          activeSession.session_name === session_name
        ) {
          activeSession = null;
        }
        return runBridge(["stop", node_ip, session_name], config);
      },
    }),

    tool({
      name: "cc_on",
      label: "Activate CC Session",
      description:
        "Set a CC session as the active foreground session. Only one session can be active at a time. " +
        "When active, user messages are forwarded to this CC session automatically. " +
        "If another session is already active, it will be deactivated first.",
      parameters: Type.Object({
        node_ip: Type.String({ description: "Worker node IP" }),
        session_name: Type.String({ description: "CC session name to activate" }),
      }),
      async execute({ node_ip, session_name }, config) {
        // Verify session exists before activating
        const args = ["read", node_ip, session_name, "--lines", "5"];
        const result = (await runBridge(args, config, lockKey(node_ip, session_name))) as any;
        if (result && result.ok === false) {
          return { ok: false, error: result.error || `Session not found: ${session_name}` };
        }
        const prev = activeSession;
        activeSession = { node_ip, session_name };
        return {
          ok: true,
          active: { node_ip, session_name },
          previous: prev,
          note: "Session activated. User messages will be forwarded to this CC session. Use cc_off to deactivate.",
        };
      },
    }),

    tool({
      name: "cc_off",
      label: "Deactivate CC Session",
      description:
        "Deactivate the current active CC session. Returns to normal conversation mode. " +
        "If no session is active, this is a no-op.",
      parameters: Type.Object({}),
      async execute(_params, _config) {
        const prev = activeSession;
        activeSession = null;
        if (prev) {
          return {
            ok: true,
            deactivated: prev,
            note: "CC session deactivated. Back to normal conversation mode.",
          };
        }
        return { ok: true, note: "No active session to deactivate." };
      },
    }),

    tool({
      name: "cc_status",
      label: "CC Bridge Status",
      description:
        "Show the current CC Bridge routing state: which session is active (if any).",
      parameters: Type.Object({}),
      async execute(_params, _config) {
        return {
          ok: true,
          active: activeSession,
          mode: activeSession ? "cc_forward" : "normal",
        };
      },
    }),
  ],
});
