# SPEC — align/core（SA1：bridge 核心对齐）

## 目标
让 acp-host 的 bridge 核心与 grok agent 的 ACP 协议完全对齐：
1. `session/update` 载体上的**全部**自定义 sessionUpdate kind 全量透传（不再白名单截断）
2. `agent_message_chunk` / `agent_thought_chunk` 转发 `_meta` 细节
3. `initialize` 响应解析 `agentCapabilities`、校验 `protocolVersion`
4. 官方 `session/resume`、`session/close` 支持
5. `session/prompt` 支持官方可选字段 `messageId` + `_meta`
6. 导出通用扩展调用 `XaiCall`（供 ext1/ext2/http 三个工作包使用）

## 文件所有权（严格）
- **只允许改**：`internal/acp/bridge.go`、`internal/acp/types.go`
- **可新建**：`internal/acp/bridge_full_passthrough_test.go`（或你自己的测试文件名，包 acp）
- **禁止触碰**：`internal/server/*`（含 fake_agent_test.go）、`internal/acp/session_tasks.go`、`internal/acp/ext_methods_test.go`、`internal/acp/session_meta_test.go`、README.md
- 其它 SA 会在**新建文件**里加代码，不会碰 bridge.go/types.go；你的改动是它们编译的前提（特别是 `XaiCall`）。

## 背景：协议事实（已从 grok-build 源码核实）
- grok 的自定义 sessionUpdate 枚举（`xai-grok-shell/src/extensions/notification.rs`）有约 49 种 kind，其中很多会走**官方 `session/update` 载体**下发（同一通知既可能走 `x.ai/session_notification` 也可能走 `session/update`）。
- 现在 bridge.go 的 `handleSessionUpdate` 的 `default:` 分支只白名单 16 种 kind，其余发 `unknown_update` 事件（前端完全不处理 = 丢弃）。必须改为全量透传。
- `session/prompt` 官方请求（agent-client-protocol-schema 0.11.4，`PromptRequest`，`#[serde(rename_all="camelCase")]`）：`sessionId`、`messageId`（unstable 可选，UUID）、`prompt`、`_meta`。**没有** prompt_id/parent_id/context/preferences（那是 1.x 的字段，grok 绑定的 0.11.4 没有）。
- `session/resume`（`ResumeSessionRequest`，camelCase）：`sessionId`、`cwd`、`additionalDirectories`、`mcpServers`、`_meta`。
- `session/close`（`CloseSessionRequest`，camelCase）：`sessionId`、`_meta`。
- `initialize` 响应字段：`protocolVersion`、`agentCapabilities`、`authMethods`、`agentInfo`、`configOptions`、`_meta`。

## 任务

### 1. session/update 全量透传（bridge.go handleSessionUpdate）
把 `default:` 分支的二次 switch（16 种白名单）**删除**，改为：**任意**未在显式 case 中处理的 kind 都转发为：
```go
b.Broadcast(Event{
    "type":      "session_notification",
    "method":    "session/update",
    "params":    map[string]any{"update": update},
    "sessionId": sid,
})
```
- 保留现有所有显式 case（agent_message_chunk / user_message_chunk / agent_thought_chunk / tool_call / tool_call_update / plan / usage_update / current_mode_update / config_option_update / available_commands_update / session_info_update）不动。
- 删除 `unknown_update` 广播（前端无处理）。
- **附加（对齐 handleXaiNotification 的用法提取）**：当 kind 为 `turn_completed` 或 `response_completed` 时，在转发 session_notification 事件**之外**，额外提取 usage：
  - `params._meta.totalTokens`（>0 时）→ `trackUsage(sid, used, 0)` + 发 `Event{"type":"usage","used":used,"size":nil,"sessionId":sid}`
  - `update.usage`（map 时）→ 追加 `ev["usage"] = update["usage"]`
  - 与 handleXaiNotification 中现有逻辑保持一致（空事件不广播：`len(ev) > 1` 才发）。

### 2. agent_message_chunk / agent_thought_chunk 的 _meta 转发
- `agent_message_chunk`：镜像 user_message_chunk 的处理：
  - `update["_meta"].hideFromScrollback` → `ev["hideFromScrollback"]`
  - `update["content"]._meta` 的 `displayText` / `displayAsCron` → 对应键
  - 保留现有 messageId 转发。
- `agent_thought_chunk`：若 `update["_meta"]` 存在 → `ev["meta"] = update["_meta"]`（原样）。

### 3. initialize 响应解析（bridge.go ensureBooted + types.go Status）
- Bridge 新增字段 `initAgentCapabilities any`（或 map[string]any；用 any 稳妥）。
- types.go `Status` 新增：`AgentCapabilities any `json:"agentCapabilities,omitempty"``。
- `ensureBooted` 拿到 initRes 后存 `b.initAgentCapabilities = initRes["agentCapabilities"]`；`Snapshot()` 里 `AgentCapabilities: b.initAgentCapabilities`。
- protocolVersion 校验：initRes["protocolVersion"] 存在且 != 1（注意 JSON 数字是 float64）时仅 `log.Printf` 警告（不 fail、不断开）。

### 4. session/resume / session/close
- `func (b *Bridge) ResumeSession(ctx context.Context, sessionID, cwd string) (map[string]any, error)`：
  - `Boot` 后发 `session/resume`，params：`{"sessionId": sessionID, "cwd": cwd, "mcpServers": []any{}, "additionalDirectories": []any{}}`，timeout bootTimeout。
  - 结果处理**镜像 LoadSession**：注册/更新 roster（sessions[sid] 不存在则新建 SessionState）、`act.Cwd = cwd`、`activeSessionID = sid`、`rememberSessionLocked`、`textBuf = ""`、`ready = true`、`bootError = ""`；响应里的 models/modes/configOptions 优先写入 act；发与 LoadSession 同形状的 `ready` 事件（sessionId/cwd/agentInfo/modes/configOptions/models/hostId/hostName）；`broadcastRosterChange()`；返回 sessRes。
  - 若目标会话已在 roster 且 Busy：走 LoadSession 的 focus-only 路径（重新聚焦 + re-broadcast busy），不真正调 agent。
- `func (b *Bridge) CloseSession(ctx context.Context, sessionID string) (map[string]any, error)`：
  - sessionID 为空时取 activeSessionID；无活跃会话 → `&HTTPError{Code: 404, Msg: "暂无活动会话"}`。
  - `session/close` params：`{"sessionId": sessionID}`，timeout 30s。
  - 成功后在 roster 删除该会话；若它是 activeSessionID → 清空 activeSessionID；若 `b.lastSessionID == sessionID` → 同时清 `lastSessionID`/`lastSessionCwd`（并 persist 空指针文件？不需要——保持 lastSessionFile 原样即可，但内存指针必须清，否则下次 Prompt 会 restore 一个已删除会话。**决定：清内存指针，不清磁盘文件**——磁盘文件只是提示，restore 失败会走 404 提示）。
  - `broadcastRosterChange()`。

### 5. session/prompt 可选字段（bridge.go）
- 新增：
  ```go
  type PromptOpts struct {
      MessageID string         // wire: messageId (UUID; 非空才发)
      Meta      map[string]any // wire: _meta (非空才发)
  }
  ```
- 把现有 `Prompt(ctx, sessionID string, blocks []ContentBlock)` 重构为内部 `PromptWithOpts(ctx, sessionID string, blocks []ContentBlock, opts PromptOpts) (string, error)`：
  - params：`{"sessionId": sessionID, "prompt": prompt}`；opts.MessageID 非空 → `params["messageId"] = opts.MessageID`；len(opts.Meta)>0 → `params["_meta"] = opts.Meta`。
  - 其余逻辑（busy 检查、restore last session、ensureBooted、超时、错误处理、self-heal、done 事件）**逐字保留**。
  - `Prompt` 变为薄包装：`return b.PromptWithOpts(ctx, sessionID, blocks, PromptOpts{})`。
- 导出 `func (b *Bridge) PromptWithOpts(...)`（http 包要用）。

### 6. 导出通用扩展调用 XaiCall（bridge.go，供其它 SA 使用）
```go
// XaiCall sends a client→agent x.ai extension request with the official
// "_" wire prefix ("_x.ai/<method>") and returns the RAW result map
// (ExtMethodResult envelopes are NOT unwrapped here — callers may unwrap
// with UnwrapExtResult).
// Session defaulting rule: if params contains "sessionId" or "session_id"
// whose value is "" (empty string), it is replaced with the active
// session's id; when no session is active this returns HTTPError 404.
// Keys absent from params are left absent. 60s timeout.
func (b *Bridge) XaiCall(ctx context.Context, method string, params map[string]any) (map[string]any, error)
```
实现：`Boot` → 若 `params["sessionId"]==""` 或 `params["session_id"]==""` → `sid := b.resolveSessionID("")`，仍为空 → `&HTTPError{404, "暂无活动会话"}`；否则写入该键 → `b.request(ctx, "_"+method, params, 60*time.Second)`（method 形如 "x.ai/foo"，前缀加 `_` 即可）。
- 文档注释必须写清上述规则（其它 SA 和未来维护者都依赖它）。

## 测试要求（新建 internal/acp/bridge_full_passthrough_test.go，包 acp）
用现有脚手架 `readyBridge()` / `resolveNext(t,b,w,result)` / `recordingStdin`（见 ext_methods_test.go，同包可直接用）：
1. 全量透传：直接调 `b.handleSessionUpdate(map[string]any{"sessionId":"s1","update":map[string]any{"sessionUpdate":"workflow_updated",...}})` 后，从 `b.subscribers` 或 Broadcast 侧断言收到 `session_notification` 事件（可临时 Subscribe() 一个 channel 收事件）；至少覆盖：`workflow_updated`、`tool_call_delta_chunk`、`diff_review`、`memory_files`、`model_changed`（>5 个抽样即可，但**必须**包含 diff_review 与 tool_call_delta_chunk）。
2. `turn_completed` 经 session/update 载体：params 带 `_meta.totalTokens=1234`、update 带 usage → 断言收到 usage 事件（used=1234）且 session_notification 也收到。
3. agent_message_chunk：content 带 `_meta.displayText` → chunk 事件带 displayText。
4. agent_thought_chunk：update 带 `_meta` → thought 事件带 meta。
5. ResumeSession：resolveNext 断言 wire method=="session/resume" 且 params 有 sessionId/cwd/mcpServers/additionalDirectories；再断言 roster 注册 + ready 事件广播（Subscribe 收）。
6. CloseSession：断言 wire method=="session/close"、params.sessionId；成功后 roster 移除、activeSessionID 清空。
7. PromptWithOpts：MessageID="uuid-1"、Meta={"yoloMode":true} → resolveNext 断言 wire params 含 messageId、_meta。
8. XaiCall：`XaiCall(ctx,"x.ai/foo",map[string]any{"sessionId":""})` → wire method=="_x.ai/foo" 且 params.sessionId=="s1"；`XaiCall(ctx,"x.ai/foo",map[string]any{})` → params 无 sessionId 键；`XaiCall(ctx,"x.ai/foo",map[string]any{"session_id":""})` → 填 "s1"。
   - 注意 readyBridge 里 b.ready=true 且已有会话 s1，XaiCall 内部 Boot 会直接返回。

## 完成标准
- `gofmt -l .` 无输出；`go build ./...`、`go vet ./...`、`go test ./...` 全绿（你改动的包全过）。
- 提交：`git add -A && git commit -m "bridge: full session/update passthrough, initialize caps, resume/close, prompt opts, XaiCall"`（在 align/core 分支）。
- 报告：改动文件清单、测试清单、任何偏离本 SPEC 的决策及原因。
