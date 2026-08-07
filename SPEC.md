# SPEC — align/ext2（SA2b：会话/mcp/auth/skills/plugins/hooks/marketplace/queue/hunk-tracker 等扩展包装）

## 目标
为 grok 支持的 x.ai 扩展方法补齐 host 侧 typed 包装（bridge 层），全部基于 SA1 提供的 `XaiCall`。只写**新建文件**。

## 文件所有权（严格）
- **只允许新建**：`internal/acp/bridge_ext_session.go`（会话/历史/子代理/队列类）、`internal/acp/bridge_ext_admin.go`（mcp/auth/skills/plugins/hooks/marketplace/workflows/bundle/suggest/pr/misc/cloud/hunk-tracker 类）、`internal/acp/bridge_ext_session_test.go` 与 `internal/acp/bridge_ext_admin_test.go`（测试，包 acp）
- **禁止触碰**：`internal/acp/bridge.go`、`types.go`、`session_tasks.go`、`ext_methods_test.go`、`session_meta_test.go`、`internal/server/*`、README.md
- 依赖 SA1 的 `XaiCall`（语义同 ext1 SPEC 所述）与 SA2a 的 `UnwrapExtResult`（在 align/ext1 分支）。**对策**：开始开发前先 `git fetch . align/core align/ext1` 并把两个分支合并进来（`git merge align/core`、`git merge align/ext1`）。若冲突（文件不重叠，不应发生），以 core/ext1 为准。

## 背景：线级字段契约（已从 grok-build 源码核实）

### ⚠️ MCP 家族是**混合约定**（最容易错，逐方法照抄）：
| 方法 | wire 键 | 说明 |
|---|---|---|
| `x.ai/mcp/list` | `sessionId?`, `cache?`（默认 true） | camelCase；已有 Bridge.MCPList，不要重复 |
| `x.ai/mcp/call` | `sessionId?`, `server`, `serverUrl?`, `tool`, `arguments` | camelCase |
| `x.ai/mcp/read_resource` | `sessionId?`, `server`, `uri` | camelCase |
| `x.ai/mcp/auth_status` | `session_id` | **snake_case** |
| `x.ai/mcp/auth_trigger` | `session_id`, `server_name` | **snake_case**（已有 Bridge.MCPAuthTrigger，不要重复） |
| `x.ai/mcp/setup` | `sessionId`, `serverName`, `values` | camelCase |
| `x.ai/mcp/toggle` | `session_id`, `server_name`, `enabled` | **snake_case**（已有，不要重复） |
| `x.ai/mcp/toggle_tool` | `session_id`, `server_name`, `tool_name`, `enabled` | **snake_case** |
| `x.ai/mcp/upsert` | `session_id`, `server_name`, + 扁平 config | **snake_case**（已有，不要重复） |
| `x.ai/mcp/delete` | `session_id`, `server_name` | **snake_case**（已有，不要重复） |
| `x.ai/mcp/sdk_call` / `x.ai/mcp/sdk` / `x.ai/mcp/servers` | 读 `extensions/mcp.rs` 核实 | 按源码写 |

### 会话类（先读对应 grok 源文件核实，下列为已核实项）
- `x.ai/session/info`、`x.ai/session/usage`、`x.ai/session/list`、`x.ai/sessions/list`：无参或 `{sessionId?}`（读 `agent/handlers/session.rs` 与 `extensions/usage.rs`、`session_state.rs`）
- `x.ai/session/state`：`{sessionId}`（读 session_state.rs 的 StateRequest）
- `x.ai/session/import`：读 session_state.rs 的 ImportRequest
- `x.ai/session/search`：读 `extensions/session_search.rs`
- `x.ai/session/repair`：读 `extensions/repair.rs`
- `x.ai/session/load_history`：读 `extensions/chat_conversation_history.rs`
- `x.ai/session/update_mcp_servers`：读 `extensions/session_admin.rs`（与 rename/delete/fork 同一 handler）
- `x.ai/session/rehydrate`、`x.ai/session/resolve_local_for_worktree_resume`：读 `extensions/worktree.rs`（ResolveLocalForWorktreeResumeRequest）
- `x.ai/session/add_local_workspace`：读 `extensions/session_admin.rs`（feature local-workspace）
- `x.ai/session/prompt_complete`、`x.ai/session/interjection`：读对应 handler（interject.rs / notification.rs）核实
- `x.ai/session/close`（扩展版，区别于官方 session/close）、`x.ai/session/fork`、`x.ai/session/rename`、`x.ai/session/delete`：**已有** Bridge.ForkSession/RenameSession/SessionDelete，不要重复；`x.ai/session/close` 扩展版读 session_admin.rs 核实（注意与官方 session/close 是不同方法）
- `x.ai/share_session`：读 `extensions/share.rs`
- `x.ai/session_summaries/session_list`、`workspace_list`、`workspace_list_recent`：读 `extensions/session_summaries` 相关文件（可能是 session_admin.rs 或单独文件，先 ls extensions/）
- `x.ai/workspaces/list`：读 `agent/handlers/workspaces.rs`
- `x.ai/prompt_history`：读 `extensions/prompt_history.rs`
- `x.ai/btw`、`x.ai/feedback`、`x.ai/feedback/dismiss`：读 `extensions/feedback.rs`
- `x.ai/interject`：读 `extensions/interject.rs`
- `x.ai/commands/list`：读 `extensions/session_admin.rs`
- `x.ai/subagent/list_running`、`x.ai/subagent/get`：读 `extensions/task.rs` 或对应文件核实
- `x.ai/queue/*`（remove/reorder/clear/edit/hold_edit/release_edit/interject）：读 `extensions/` 下 queue 相关文件（可能在 prompt_meta.rs 或单独 queue.rs，先 ls 核实）

### admin 类（先读对应源文件核实）
- `x.ai/skills/*`（list/add/remove/reset/config/toggle/refresh-baseline）：读 `extensions/skills.rs`（SkillsListRequest/SkillsAddRequest/SkillsRemoveRequest/SkillsToggleRequest）
- `x.ai/plugins/*`（list/action/reload/notify-updates）：读 `extensions/plugins.rs`
- `x.ai/hooks/*`（list/action/run/event）：读 `extensions/hooks.rs`
- `x.ai/marketplace/*`（list/action）：读 `extensions/marketplace.rs`
- `x.ai/workflows/list`：读 `extensions/skills.rs`（WorkflowsListRequest）
- `x.ai/bundle/*`（sync/status/entry/get）：读 `extensions/bundle.rs`（BundleSyncRequest/BundleStatusRequest/EntryGetRequest）
- `x.ai/suggest`、`x.ai/suggestPrompt`：读 `extensions/suggest/mod.rs`（SuggestRequest/SuggestPromptRequest）
- `x.ai/pr/status`：读 `extensions/pr.rs`（PrStatusRequest）
- `x.ai/capabilities`、`x.ai/folder_trust/request`、`x.ai/settings/update`、`x.ai/probe`/`noop`/`test`/`log`：读对应 handler 核实
- `x.ai/auth/*`（get_url/submit_code/cancel/logout/info/check_subscription/getBearerToken）+ `x.ai/getApiKey`/`x.ai/setApiKey`：读 `extensions/auth.rs`
- `x.ai/auto-topup-rule`：读 `extensions/billing.rs`
- `x.ai/privacy/setCodingDataRetention`：读 `extensions/privacy.rs`
- `x.ai/review/comment`、`x.ai/review/comment/delete`：读 `extensions/` 下 review 相关文件
- `x.ai/rollout/survey`：读 `extensions/rollout.rs`
- `x.ai/hunk-tracker/*`（get-hunks/get-files/get-all-file-contents/get-summary/hunk-action/file-action/turn-action/all-action）：读 `extensions/hunk_tracker.rs`（GetHunksRequest/GetFilesRequest/HunkActionRequest/FileActionRequest/TurnActionRequest/AllActionRequest/GetSummaryRequest）
- `x.ai/cloud/env/list|create|update|delete`、`x.ai/cloud/terminate`：读 acp_agent.rs 中已核实的形状（list 无参；create `{name?, description?}`；terminate `{sandbox_id}`——再去源码复核）

## 任务
1. bridge_ext_session.go：上面「会话类」全部方法的 typed 包装。
2. bridge_ext_admin.go：上面「admin 类」全部方法的 typed 包装。
3. 命名约定：`SessionInfo`、`SessionUsage`、`SessionsList`、`SessionState`、`SessionSearch`、`QueueRemove(ctx, id string)`、`SkillsList`、`PluginsList`、`HooksList`、`MarketplaceList`、`WorkflowsList`、`McpCall`、`McpReadResource`、`McpAuthStatus`、`McpSetup`、`McpToggleTool`、`AuthInfo`、`AuthLogout`、`GetApiKey`、`HunkGetHunks` 等（同 ext1 的规则：ctx 第一参；可选字段省略；sessionId 可选的一律省略，必填的传 `""` 让 XaiCall 填充；返回 UnwrapExtResult 后的结果）。
4. 每个方法都要有 golang 文档注释写明 wire 键与省略规则。
5. 测试：每个测试文件至少覆盖 8 个代表性方法（含 MCP 混合约定验证：`McpSetup` 断言 wire 键是 camelCase `sessionId/serverName/values`；`McpToggleTool` 断言 wire 键是 snake_case `session_id/server_name/tool_name/enabled`；`AuthLogout`、`SkillsList`、`QueueRemove`、`HunkGetHunks`、`SessionState`、`Suggest`、`PrStatus`、`ShareSession`），用 readyBridge/resolveNext/recordingStdin。

## 完成标准
- `gofmt -l .` 无输出；`go build ./...`、`go vet ./...`、`go test ./...` 全绿。
- 提交：`git add -A && git commit -m "bridge_ext: session/admin x.ai wrappers"`。
- 报告：文件清单、方法清单（含与 grok 源码的对照）、测试清单、任何偏离及原因。
