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
| `cc_peek(me, since_id=0, wait=false, timeout=30)` | 相同的游标/等待语义，但不标记已读；供通知 hook 使用 |
| `cc_directory(query?, active_only?)` | 按 session/node 查稳定地址、旧地址 alias 和接收实例租约 |
| `cc_heartbeat(name, instance_id, ...)` | watcher 专用；独立续租，旧实例不能冒充新实例在线 |
| `cc_receipt(me, ids, stage)` | handed_off / acknowledged / completed；不制造新消息、不唤醒对端 |
| `cc_delivery_status(me?, message_id?)` | 查看投递阶段、时间线和延迟，不消费消息或返回正文 |

客户端通过原生 hook 唤醒：Claude asyncRewake / Codex 原生 queue，由 watcher 等待消息，
不需要模型不断调用 inbox 或向 TTY 注入。中心存储本身不运行模型；handoff 不表示任务完成。

`cc_register` 可带 node/session/instance_id/aliases；旧地址和完整历史保留，session 短名重名时拒绝寻址。
`cc_post` 可带 topic_id、reply_to、kind=request/update/result/ack、requires_reply、wake、idempotency_key。
ack 默认不唤醒、不要求回复；回执优先用 cc_receipt。重试复用相同 idempotency_key，不能换正文。
自动接收用 `cc_peek(pending_only=true, auto_only=true, instance_id=...)`；手动 inbox 按游标读历史。
pending 是待自动交付数，unread 仅保留旧 read 标记语义，不能用它推断模型是否已收到。

SQLite 原表做增量迁移，保留 ID、正文和 read。单条上限 256 KiB，批次最多 200 条/通常 64 KiB。
同进程投递通知条件变量，跨进程写入兜底扫描最长 5 秒；空 peek 不再每秒写在线时间。
数据库旁 mailbox-events.jsonl 为 5 MiB × 4 份轮转，记录元数据，不记录正文/凭据。
HTTP access log 默认关闭，调试时可传 `--access-log`；异常日志仍保留。

## 起服务(Head)

```bash
CC_MCP_TOKEN=<随机串> \
  <loop>/.venv/bin/python 3rdparty/cc-bridge-plugin/mcp/server.py \
  --host 0.0.0.0 --port 8765
```

参数:`--token-env`(默认 `CC_MCP_TOKEN`,缺失默认拒绝启动；调试需显式 `--insecure`)、`--ray-address`(默认 `auto`)、
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
