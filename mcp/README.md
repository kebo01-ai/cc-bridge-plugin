# cc-mcp — CC Bridge as an MCP server

把 CC Bridge 的会话控制 + 一个 sqlite 信箱,包成一个 **Streamable-HTTP MCP server**。
部署在 Ray Head,任意机器上的任意 Claude 配 `.mcp.json` 即可驱动/读取远程 CC 会话、
或在 agent 之间收发消息。

## 两组工具

**桥组(需 Ray,驱动 worker tmux)**
| tool | 作用 |
|---|---|
| `cc_send(node_ip, session, message, wait=true, timeout=600)` | 往会话发消息,`wait` 轮询到空闲 |
| `cc_read(node_ip, session, lines=80)` | 读 tmux pane 末 N 行 |
| `cc_list(node_ip?)` | 列会话(省 node_ip 则扫所有已知节点,自动跳过/清理死节点) |
| `cc_status(node_ip, session)` | tmux/claude 存活 + 忙闲 |
| `cc_keys(node_ip, session, key)` | 发原始键(Enter/Escape/C-c/Y/N/方向键)——打断、确认、菜单 |
| `cc_stop(node_ip, session)` | 杀会话并注销 |

**信箱组(sqlite,无 Ray,`~/.cc-bridge/mailbox.db`)**
| tool | 作用 |
|---|---|
| `cc_register(name)` | 建/认领收件箱(幂等) |
| `cc_post(to, body, sender?)` | 投递消息 |
| `cc_inbox(me, since_id=0, wait=false, timeout=30)` | 取 id>since_id 的新消息;`wait` 服务端 long-poll。回包带 `cursor`,下次当 since_id |

> **语义红线**:信箱**叫不醒**空闲的 Claude——接收方必须自己处于活跃循环、主动 `cc_inbox` 才收得到。
> 要唤醒没在循环里的会话,用桥组 `cc_send` 往它 TTY 注入。两条通道互补。

## 起服务(Head)

```bash
CC_MCP_TOKEN=<随机串> \
  <loop>/.venv/bin/python 3rdparty/cc-bridge-plugin/mcp/server.py \
  --host 0.0.0.0 --port 8765
```

参数:`--token-env`(默认 `CC_MCP_TOKEN`,空则无鉴权、仅本地调试)、`--ray-address`(默认 `auto`)、
`--no-ray`(只跑信箱、跳过 Ray,用于测试)。

## 客户端配置(远端 Claude 的 `.mcp.json`)

```json
{
  "mcpServers": {
    "cc-bridge": {
      "type": "http",
      "url": "http://10.12.202.11:8765/mcp",
      "headers": { "Authorization": "Bearer <同一串>" }
    }
  }
}
```

## 依赖

见 `requirements.txt`。运行 venv 需 `mcp>=2.3`(官方 SDK 2.x,FastMCP 已更名 MCPServer) + `ray`(桥组);
loop 的 `.venv` 即运行环境,`mcp` 走 PyPI(见 loop `pyproject.toml` 的 `[tool.uv.sources]`)。部署前
`uv sync` 把 venv 对齐到 lock 的 2.3.0。

## 代码

- `server.py` — FastMCP 装配 + lifespan 起 Ray + bearer 鉴权中间件 + uvicorn。
- `bridge_tools.py` — 薄封装 `../bridge/cc_bridge.py`(唯一真源)。
- `mailbox.py` — sqlite 信箱,独立无 Ray。
