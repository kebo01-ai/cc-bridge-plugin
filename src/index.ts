import { Type } from "@sinclair/typebox";
import { defineToolPlugin } from "openclaw/plugin-sdk/tool-plugin";
import { execFile } from "node:child_process";
import { resolve } from "node:path";

const LOOP_DIR = "/root/paddlejob/workspace/env_run/kebo01/loop";
const VENV_PY = resolve(LOOP_DIR, ".venv/bin/python3");
const BRIDGE_SCRIPT = resolve(LOOP_DIR, ".claude/skills/cc-bridge/cc_bridge.py");
const JSON_DELIMITER = "__CC_BRIDGE_JSON__";

/** Per-session mutex: prevents concurrent sends to the same session */
const sessionLocks = new Map<string, Promise<unknown>>();

function lockKey(nodeIp: string, sessionName: string): string {
  return `${nodeIp}:${sessionName}`;
}

/**
 * Parse JSON output from the bridge script using the deterministic delimiter.
 * Falls back to indexOf("{") if delimiter not found (backward compat).
 */
function parseOutput(output: string): unknown {
  // Prefer deterministic delimiter
  const delimIdx = output.indexOf(JSON_DELIMITER);
  if (delimIdx >= 0) {
    const jsonStr = output.slice(delimIdx + JSON_DELIMITER.length).trim();
    try {
      return JSON.parse(jsonStr);
    } catch {}
  }
  // Fallback: find first '{'
  const braceIdx = output.indexOf("{");
  if (braceIdx >= 0) {
    try {
      return JSON.parse(output.slice(braceIdx));
    } catch {}
  }
  return null;
}

/**
 * Run the Python bridge script asynchronously via child_process.execFile.
 * No shell is involved — args are passed as an array (injection-safe).
 * Concurrent calls to the same session are serialized via a per-session lock.
 */
async function runBridge(args: string[], lock?: string): Promise<unknown> {
  const doRun = (): Promise<unknown> =>
    new Promise((resolve) => {
      execFile(
        VENV_PY,
        [BRIDGE_SCRIPT, ...args],
        {
          cwd: LOOP_DIR,
          timeout: 180_000,
          encoding: "utf-8",
          env: { ...process.env, RAY_ADDRESS: "auto" },
          maxBuffer: 4 * 1024 * 1024, // 4MB
        },
        (error, stdout, _stderr) => {
          const output = stdout || (error as any)?.stdout || "";
          const parsed = parseOutput(output);
          if (parsed) {
            resolve(parsed);
            return;
          }
          if (error) {
            resolve({ ok: false, error: error.message?.slice(0, 500) || String(error) });
          } else {
            resolve({ ok: false, error: "No JSON output", raw: output.slice(0, 500) });
          }
        }
      );
    });

  if (!lock) return doRun();

  // Serialize calls to the same session
  const prev = sessionLocks.get(lock) ?? Promise.resolve();
  const next = prev.then(doRun, doRun);
  sessionLocks.set(lock, next);
  // Clean up when done to avoid memory leak
  next.finally(() => {
    if (sessionLocks.get(lock) === next) sessionLocks.delete(lock);
  });
  return next;
}

export default defineToolPlugin({
  id: "cc-bridge-plugin",
  name: "CC Bridge",
  description: "Manage Claude Code sessions on worker nodes via tmux + Ray remote.",
  configSchema: Type.Object({
    defaultCwd: Type.Optional(
      Type.String({ description: "Default cwd for new sessions (default: ~)" })
    ),
  }),
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
          Type.String({
            description: "Working directory on the worker (default: ~)",
          })
        ),
      }),
      async execute({ node_ip, name, cwd }, config) {
        const args = ["start", node_ip];
        if (cwd) args.push("--cwd", cwd);
        else if (config.defaultCwd) args.push("--cwd", config.defaultCwd);
        if (name) args.push("--name", name);
        return runBridge(args);
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
          Type.Boolean({
            description: "Wait for CC to finish responding (default: true)",
            default: true,
          })
        ),
        timeout: Type.Optional(
          Type.Number({
            description: "Max seconds to wait (default: 120)",
            default: 120,
          })
        ),
      }),
      async execute({ node_ip, session_name, message, wait, timeout }) {
        const args = ["send", node_ip, session_name, message];
        if (wait !== false) args.push("--wait");
        if (timeout && timeout !== 120) args.push("--timeout", String(timeout));
        return runBridge(args, lockKey(node_ip, session_name));
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
      async execute({ node_ip, session_name, lines }) {
        const args = ["read", node_ip, session_name];
        if (lines && lines !== 80) args.push("--lines", String(lines));
        return runBridge(args, lockKey(node_ip, session_name));
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
        key: Type.String({
          description: "tmux key token: Enter, Escape, Up, Down, C-c, Y, N, etc.",
        }),
      }),
      async execute({ node_ip, session_name, key }) {
        return runBridge(["keys", node_ip, session_name, key], lockKey(node_ip, session_name));
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
      async execute({ node_ip }) {
        const args = ["list"];
        if (node_ip) args.push(node_ip);
        return runBridge(args);
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
      async execute({ node_ip, session_name }) {
        return runBridge(["stop", node_ip, session_name]);
      },
    }),
  ],
});
