# CC Bridge — OpenClaw Plugin

Remote Claude Code session management via tmux + Ray.

> Control Claude Code sessions running on GPU worker nodes from your phone — through 如流 → OpenClaw agent → Ray → tmux.

## Architecture

```
You (如流/phone)
  └── OpenClaw agent (Head node)
        └── Ray remote execution
              └── Worker node tmux session
                    └── Claude Code CLI (interactive)
```

The agent acts as a transparent relay. Messages flow through without reinterpretation.

## Features

- **Start/stop** CC sessions on any Ray worker node
- **Send messages** with optional wait-for-idle (polls until CC finishes thinking)
- **Read output** from tmux pane (configurable line count)
- **Send raw keys** (Enter/Escape/C-c/Up/Down) for menus and interrupts
- **List sessions** across all known nodes (or a specific one)
- **Health check** — verifies tmux alive + claude process alive + busy state
- **Session registry** — persists across gateway restarts
- **Per-session concurrency lock** — prevents interleaved commands
- **Input validation** — IP + session name regex, no shell injection possible

## Installation

1. Clone this repo into your OpenClaw extensions directory:
   ```bash
   cd ~/.openclaw/extensions/
   git clone <this-repo> cc-bridge-plugin
   cd cc-bridge-plugin
   npm install
   npm run build
   ```

2. Copy the Python bridge script to your loop skills directory:
   ```bash
   cp bridge/cc_bridge.py /path/to/loop/.claude/skills/cc-bridge/cc_bridge.py
   ```

3. Register in your `openclaw.json`:
   ```json
   {
     "plugins": {
       "installs": ["~/.openclaw/extensions/cc-bridge-plugin"]
     },
     "tools": {
       "alsoAllow": ["cc_start", "cc_send", "cc_read", "cc_keys", "cc_list", "cc_stop"]
     }
   }
   ```

4. Restart your gateway:
   ```bash
   openclaw gateway restart
   ```

## Prerequisites

- **Ray cluster** — Head node with workers connected
- **tmux** on worker nodes (use `tools/config_tmux.sh` for offline nodes)
- **Claude Code CLI** installed on workers (`claude` in PATH)
- **Python 3.10+** with `ray` package on Head node

## Tools

| Tool | Description |
|------|-------------|
| `cc_start` | Start a new CC session on a worker |
| `cc_send` | Send a message (with optional wait-for-completion) |
| `cc_read` | Read current tmux pane output |
| `cc_keys` | Send raw keys (Enter/Escape/C-c/Up/Down/Y/N) |
| `cc_list` | List sessions (omit node_ip → scan all nodes) |
| `cc_stop` | Kill a session |

## Usage Examples

```typescript
// Start a session
cc_start({ node_ip: "10.95.110.148", cwd: "/workspace/my-project", name: "dev" })
// → { ok: true, name: "cc-dev", node: "10.95.110.148", state: "ready" }

// Send a task and wait for completion
cc_send({ node_ip: "10.95.110.148", session_name: "cc-dev", message: "git status", wait: true })
// → { ok: true, state: "idle", output: "On branch main\n..." }

// List all active sessions
cc_list({})
// → { ok: true, sessions: [{ name: "cc-dev", node_ip: "10.95.110.148", ... }] }

// Read current output (last 120 lines)
cc_read({ node_ip: "10.95.110.148", session_name: "cc-dev", lines: 120 })

// Interrupt CC
cc_keys({ node_ip: "10.95.110.148", session_name: "cc-dev", key: "Escape" })

// Stop session
cc_stop({ node_ip: "10.95.110.148", session_name: "cc-dev" })
```

## Manual Access

You can attach to any session directly via SSH + tmux:

```bash
ssh <worker_ip>
tmux attach -t cc-<name>
```

- **Interrupt CC**: press `Esc` (not Ctrl+C — that kills the process)
- **Detach without killing**: `Ctrl+b` then `d`
- Sessions you create without the `cc-` prefix are invisible to this plugin

## Security

- **No shell interpolation**: Plugin uses `execFile` (args as array), not `exec` with string concatenation
- **Input validation**: IPs must match `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}` with valid octets; session names are `[a-zA-Z0-9_-]{1,64}` only
- **IS_SANDBOX=1**: Required to bypass CC's root + `--dangerously-skip-permissions` check. Workers are assumed to be isolated containers.
- **Per-session mutex**: Concurrent sends to the same session are serialized

## Configuration

In `openclaw.json` plugin config:

```json
{
  "cc-bridge-plugin": {
    "defaultCwd": "~/workspace"
  }
}
```

## File Structure

```
cc-bridge-plugin/
├── src/
│   └── index.ts              # OpenClaw tool plugin (async, typed)
├── bridge/
│   └── cc_bridge.py          # Python bridge (Ray remote → tmux)
├── tools/
│   └── config_tmux.sh        # tmux installer (3-level fallback)
├── dist/                     # Compiled output (gitignored)
├── openclaw.plugin.json      # Plugin manifest
├── package.json
├── tsconfig.json
└── README.md
```

## How It Works

1. **Plugin layer** (`src/index.ts`): Registers tools with OpenClaw, handles async execution and per-session locking
2. **Python bridge** (`bridge/cc_bridge.py`): Connects to Ray cluster, schedules remote functions on worker nodes, drives tmux via `send-keys` and `capture-pane`
3. **Session registry** (`~/.cc-bridge/sessions.json`): Tracks which sessions live on which nodes, auto-reconciles dead sessions on list

### Idle Detection

`cc_send --wait` polls the tmux pane for busy markers:
- `esc to interrupt` / `esc to cancel`
- `Thinking` / `Topsy-turvying` / `Generating`

Session is idle when no markers appear for 2 consecutive polls (2s interval).

### JSON Output

The bridge prints a deterministic delimiter (`__CC_BRIDGE_JSON__`) before JSON output, so the TS layer can reliably find structured data even when Ray prints warnings to stdout.

## Known Limitations

- Output capture is bounded by tmux pane scrollback history
- CC auto-compact is indistinguishable from "thinking" (both show busy markers)
- Workers need tmux installed (offline nodes need the AppImage fallback)
- Single-user design — no auth layer between agent and workers

## License

MIT
