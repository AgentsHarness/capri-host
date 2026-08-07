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

### 核心 / 事件流

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/events` | SSE 事件流 |
| GET | `/api/status` | 状态快照 |
| GET | `/api/hosts` | 本机单 Host 列表（Hub 模式下此端点由 Hub 提供注册表） |
| POST | `/api/prompt` | `{ "blocks": [{ "type":"text", "text":"..." }] }` |
| POST | `/api/cancel` | 取消当前回合 |
| POST | `/api/permission-response` | `{ requestId, optionId?, cancelled? }` |
| POST | `/api/client-response` | 透传 client_request 的响应给 Agent |
| POST | `/api/shell` | 本地执行 `sh -c {command}`（不经 Agent；`cwd?` 默认活动会话目录，`timeoutMs?` 默认 10s、上限 60s） |

### 会话

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/session` | 创建会话 `{ cwd?, additionalDirectories?, mcpServers? }`，返回 `sessionId` |
| POST | `/api/session-load` | 切换到历史会话 `{ sessionId, cwd }`（随后的 prompt 继续该对话） |
| POST | `/api/session-state` | 单会话的宿主侧实时状态 `{ sessionId }` |
| POST | `/api/session-updates` | 分页拉取会话历史消息 `{ sessionId, cwd, offset?, limit? }` → `{ updates, totalCount, hasMore }` |
| POST | `/api/session-running-tasks` | 会话中仍在运行的后台任务 `{ sessionId, cwd }` |
| POST | `/api/session-fork` | 复制会话 `{ sourceCwd?, newCwd?, newSessionId? }` |
| POST | `/api/session-rename` | 重命名当前会话 `{ title }` |
| POST | `/api/session-delete` | 删除会话 `{ sessionId?, cwd? }`（缺省为活动会话） |
| POST | `/api/session-info` | 活动会话详情（/session-info 模拟） |
| POST | `/api/sessions` | 列会话（若 Agent 支持） |
| POST | `/api/set-mode` | `{ modeId }` |
| POST | `/api/set-model` | `{ modelId, reasoningEffort? }` |
| POST | `/api/git-info` | 会话工作区的 git 分支 / worktree 状态 `{ cwd }` |
| POST | `/api/recap` | 触发 "where was I" 摘要 `{ auto? }` |

### 任务 / 调度

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/task-list` | 后台任务列表 |
| POST | `/api/task-output` | 任务 stdout：`{ taskId }`（活动会话实时注册表）或 `{ taskId, sessionId, cwd }`（从持久化时间线重建） |
| POST | `/api/task-kill` | 终止后台任务 `{ taskId }` |
| POST | `/api/subagent-cancel` | 取消子代理 `{ subagentId }` |
| POST | `/api/scheduler-delete` | 删除定时任务 `{ taskId, sessionId? }` |

### 上下文工具

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/compact` | 手动压缩上下文 `{ sessionId?, note? }` |
| POST | `/api/rewind-points` | 列出可回退点 `{ sessionId?, cwd? }`（统一归一化为 `{ index, timestamp, summary? }`） |
| POST | `/api/rewind-execute` | 回退到指定回退点 `{ sessionId?, targetIndex }` |

### 管理 / Agent 控制

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/billing` | 账单 / 用量查询 |
| POST | `/api/memory-flush` | 持久化会话记忆 `{ sessionId? }` |
| POST | `/api/memory-rewrite` | 重写会话记忆 `{ sessionId?, rawText, contextSummary? }`（rawText 必填） |
| POST | `/api/toggle-plan-mode` | 切换计划模式 `{ sessionId? }`（以 notification 发送，返回 `{ ok }`） |
| POST | `/api/permissions-reset` | 清除记住的权限决定 `{ sessionId? }` |
| GET | `/api/extensions` | 本地扩展盘点（`~/.grok` 的 hooks / plugins / skills，不经 Agent） |
| GET | `/api/settings` | 只读的 `~/.grok/config.toml` 安全子集（ui / session / models / cli，标量值） |

### MCP

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/mcp/list` | MCP 服务器注册表 |
| POST | `/api/mcp-toggle` | 启用 / 禁用 `{ name, enabled }` |
| POST | `/api/mcp-add` | 新增或更新 `{ server: { name, command, args?, env? } }` |
| POST | `/api/mcp-remove` | 删除 `{ name }` |
| POST | `/api/mcp-auth-trigger` | 触发 MCP 服务器的 OAuth 流程 `{ name }` |

Hub 模式下所有 `/api/*` 请求经 Hub 中转（`?host=` 选择目标），本机直连照常可用。

## 与 acp-chat 差异

| | acp-chat (旧) | acp-host (新) |
|--|---------------|---------------|
| 语言 | Node | Go |
| Client fs/terminal | 有，可审批执行 | **无**，依赖 Agent |
| 前端 | 内嵌 HTML | 独立 `acp-fe` |
