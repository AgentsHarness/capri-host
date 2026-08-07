# acp-host

本机 Host 服务（Go）：拉起 `grok agent stdio`，把 ACP 会话暴露给前端 / acp-hub。

## 设计

- **纯 Agent 执行**：`clientCapabilities.fs` / `terminal` 均为 `false`，**不**实现 `fs/*`、`terminal/*`。
- 读盘、写盘、跑命令由 **Agent（grok）自身工具** 在本机完成。
- Host 只做：进程管理、会话、流式事件转发、可选 `session/request_permission` 审批中继。

```
浏览器 (acp-fe) ──HTTP/SSE──▶ acp-host ──stdio──▶ grok agent
浏览器 (acp-fe) ──HTTP/SSE──▶ acp-hub ──SSE+HTTP──▶ acp-host × N ──stdio──▶ grok agent
```

## 运行

```bash
# 需要：Go >= 1.22，已安装并登录的 grok CLI
cd acp-host
go run ./cmd/acp-host
```

默认监听 `http://localhost:8765`。

### 环境变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `PORT` | `8765` | HTTP 端口 |
| `GROK_BIN` | `grok` | grok 可执行文件 |
| `HOST_ID` | `local` | Host 标识（多 Host 时用） |
| `HOST_NAME` | `Local Host` | 展示名 |
| `XAI_API_KEY` | — | 可选；否则用 `grok login` 缓存 |
| `HUB_URL` | — | 设置后进入 **Hub 中继模式**：配对并连接 acp-hub |
| `HUB_PAIR_CODE` | — | 一次性配对码（Hub 启动日志 / `GET /api/pairing` 查看） |
| `HOST_TOKEN` | — | 已配对的 token（优先级最高；否则用 `~/.acp-host/hub.json` 持久化的 token，再否则用配对码） |

Hub 中继模式示例：

```bash
HUB_URL=http://hub-host:8787 HUB_PAIR_CODE=DDVZRR HOST_ID=macbook HOST_NAME="MacBook" go run ./cmd/acp-host
```

首次配对成功后 token 保存在 `~/.acp-host/hub.json`，之后重启无需配对码。
Host 与 Hub 断开会自动重连（指数退避）；Hub 端 token 失效且提供了配对码时会自动重新配对。

## API（Local / 经 Hub 中转一致）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/events` | SSE 事件流 |
| GET | `/api/status` | 状态快照 |
| GET | `/api/hosts` | 本机单 Host 列表（Hub 模式下此端点由 Hub 提供注册表） |
| POST | `/api/prompt` | `{ "blocks": [{ "type":"text", "text":"..." }] }` |
| POST | `/api/cancel` | 取消当前回合 |
| POST | `/api/permission-response` | `{ requestId, optionId? , cancelled? }` |
| POST | `/api/session` | 重建会话 `{ cwd?, additionalDirectories?, mcpServers? }` |
| POST | `/api/set-mode` | `{ modeId }` |
| POST | `/api/sessions` | 列会话（若 Agent 支持） |

Hub 模式下所有 `/api/*` 请求经 Hub 中转（`?host=` 选择目标），本机直连照常可用。

## 与 acp-chat 差异

| | acp-chat (旧) | acp-host (新) |
|--|---------------|---------------|
| 语言 | Node | Go |
| Client fs/terminal | 有，可审批执行 | **无**，依赖 Agent |
| 前端 | 内嵌 HTML | 独立 `acp-fe` |
