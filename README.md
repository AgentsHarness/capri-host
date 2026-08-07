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

### x.ai 扩展直通（完整对齐）

以下端点全部 **POST + JSON**，与 grok agent 的 `x.ai/*` 扩展方法一一对应
（经 `bridge.XaiCall` 直通；`sessionId` / `session_id` 为 `""` 时自动填入活动
会话，无会话则 404；agent 侧失败统一降级为 `200 {ok:false, error}`）。
成功应答统一为 `{ "ok": true, "result": <agent 原始结果> }`。
SSE hello 事件新增 `agentCapabilities` 字段（agent initialize 声明的能力）；
`/api/prompt` 新增可选字段 `messageId`（UUID）与 `meta`（透传为
session/prompt 的 `_meta`）。

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/xai-call` | 通用直通 `{ method, params? }`（method 形如 `"x.ai/foo"`） |
| POST | `/api/session-resume` | 恢复会话 `{ sessionId, cwd }`（两字段必填） |
| POST | `/api/session-close` | 关闭会话 `{ sessionId? }`（缺省活动会话） |
| POST | `/api/git/status` | git 状态 `{ cwd? }`（cwd 空则不带 gitRoot） |
| POST | `/api/git/diffs` | 差异 `{ cwd?, from, to, paths? }`（from/to 必填） |
| POST | `/api/git/stage` | 暂存 `{ cwd?, paths? }` |
| POST | `/api/git/unstage` | 取消暂存 `{ cwd?, paths? }` |
| POST | `/api/git/discard` | 丢弃改动 `{ cwd?, paths?, includeUntracked? }` |
| POST | `/api/git/commit` | 提交 `{ cwd?, message, amend?, signoff?, push? }`（message 必填） |
| POST | `/api/git/checkout` | 切换分支 `{ cwd?, branch, create? }`（branch 必填） |
| POST | `/api/git/checkout-commit` | 检出提交 `{ cwd?, commit, stashIfDirty? }`（commit 必填） |
| POST | `/api/git/stash` | 暂存全部改动 `{ cwd?, includeUntracked? }` |
| POST | `/api/git/branches` | 分支列表 `{ cwd? }` |
| POST | `/api/git/current-commit` | 当前提交 `{ cwd? }` |
| POST | `/api/git/repo-root` | git 仓库根目录 `{ cwd? }` |
| POST | `/api/queue/remove` | 移除队列条目 `{ id }`（需活动会话） |
| POST | `/api/queue/clear` | 清空队列（需活动会话） |
| POST | `/api/queue/reorder` | 重排队列 `{ ids: []string }`（wire 键为 `orderedIds`） |
| POST | `/api/queue/edit` | 编辑队列条目 `{ id, newText }`（需活动会话） |
| POST | `/api/queue/interject` | 队列插入 `{ id, newText? }`（需活动会话） |
| POST | `/api/skills/list` | 技能列表 `{ cwd? }` |
| POST | `/api/skills/toggle` | 启用/禁用技能 `{ name, enabled }` |
| POST | `/api/skills/add` | 添加技能路径 `{ path?, cwd? }`（原样透传） |
| POST | `/api/skills/remove` | 移除技能 `{ name }`（wire 键为 `path`） |
| POST | `/api/skills/refresh-baseline` | 刷新技能基线 |
| POST | `/api/plugins/list` | 插件列表 `{ sessionId? }` |
| POST | `/api/plugins/action` | 插件操作 `{ sessionId?, action }`（action 为 tagged 对象） |
| POST | `/api/plugins/reload` | 重载插件 |
| POST | `/api/hooks/list` | hooks 列表 `{ sessionId? }` |
| POST | `/api/hooks/action` | hook 操作 `{ sessionId?, action }`（action 为 tagged 对象） |
| POST | `/api/marketplace/list` | 插件市场源列表 |
| POST | `/api/marketplace/action` | 市场操作 `{ action }`（tagged 对象；需活动会话） |
| POST | `/api/workflows/list` | 工作流列表 `{ sessionId? }` |
| POST | `/api/session/info` | 会话信息 `{ sessionId? }` |
| POST | `/api/session/usage` | 会话用量 `{ sessionId? }` |
| POST | `/api/session/search` | 会话全文搜索 `{ query, cwd?, limit?, offset?, includeContent? }`（query 必填） |
| POST | `/api/sessions/list` | 全部会话（FleetView 名册，无参） |
| POST | `/api/prompt-history` | 提示历史 `{ cwd?, sessionId? }`（wire snake_case：cwd / session_id） |
| POST | `/api/btw` | 旁路提问 `{ question }`（需活动会话） |
| POST | `/api/interject` | 回合中插入 `{ text }`（需活动会话） |
| POST | `/api/commands-list` | 可用斜杠命令 `{ sessionId? }` |
| POST | `/api/workspaces/list` | 远端工作区列表（无参） |
| POST | `/api/subagent/list-running` | 运行中的子代理 `{ sessionId? }` |
| POST | `/api/session/share` | 分享会话 `{ sessionId? }`（wire snake_case：session_id） |
| POST | `/api/mcp/read-resource` | 读 MCP 资源 `{ server, uri }`（camelCase wire） |
| POST | `/api/mcp/auth-status` | MCP 认证状态 `{ sessionId? }`（wire snake_case：session_id） |
| POST | `/api/mcp/setup` | MCP 设置提交 `{ serverName, values }`（camelCase wire） |
| POST | `/api/mcp/toggle-tool` | 启停 MCP 工具 `{ serverName, toolName, enabled }`（wire snake_case） |
| POST | `/api/mcp/call` | 调用 MCP 工具 `{ server, tool, arguments?, serverUrl?, sessionId? }`（camelCase wire） |
| POST | `/api/auth/info` | 当前登录信息（无参） |
| POST | `/api/auth/logout` | 登出（无参） |
| POST | `/api/auth/get-url` | 获取登录 URL（无参） |
| POST | `/api/auth/submit-code` | 提交登录码 `{ code }`（必填） |
| POST | `/api/fs/list` | 列目录 `{ path, depth?, includeHidden?, limit?, ... }`（path 必填） |
| POST | `/api/fs/read-file` | 读文件 `{ path, maxBytes?, ... }`（path 必填） |
| POST | `/api/fs/exists` | 路径存在性 `{ path }`（必填） |
| POST | `/api/capabilities` | 能力查询（grok 无对应请求分支，agent 侧降级 200 ok:false） |
| POST | `/api/folder-trust-request` | 目录信任请求（grok 侧为反向请求，agent 侧降级 200 ok:false） |
| POST | `/api/suggest` | 补全建议 `{ text, cwd? }`（text 必填） |
| POST | `/api/suggest-prompt` | 下一提示预测 `{ generation? }` |
| POST | `/api/pr/status` | PR 状态 `{ cwd, branch }`（均必填） |
| POST | `/api/hunk-tracker/hunks` | hunk 列表 `{ path?, source? }`（get-hunks） |
| POST | `/api/bundle/status` | bundle 状态（无参） |
| POST | `/api/terminal/list` | 终端列表（无参） |
| POST | `/api/search/content` | 内容搜索（扁平键原样透传） |
| POST | `/api/billing/auto-topup-rule` | 自动充值规则（无参） |
| POST | `/api/feedback` | 反馈 `{ text }`（wire snake_case：session_id / feedback_text） |
| POST | `/api/cloud/env/list` | 云端环境列表（无参） |

> 注：grok 侧的队列方法（`x.ai/queue/*`）是 ext_notification 型，本层经
> `XaiCall` 以 request 型发送，结果原样返回；真实 agent 会对这类方法回
> `-32601 method_not_found`（宿主降级为 `200 {ok:false}`，不会崩）。

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
