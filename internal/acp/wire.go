package acp

// ─────────────────────────────────────────────────────────────────────
// wire.go — ACP / JSON-RPC wire 键词汇表（bridge 核心 dispatch 路径）。
//
// 背景：领域负载大量使用 map[string]any（agent 的 _meta 等动态结构必须
// 透传），wire 键名此前以裸字符串散布在 dispatch 热路径中——camel/snake
// 分歧（sessionId vs session_id）尤其易错，拼错一个键只会静默失效，编译
// 期无感。本文件把高频键收敛为常量，供信封解析、session/update 分发、
// x.ai 通知与事件总线使用。
//
// 范围约定：bridge_ext_* 等直通文件按 handler 逐一注释的 agent 私有键
// 保持裸字符串（键随 agent 源码逐字段核验，见各文件头），不强行统一；
// 本表只覆盖 host 自有的、跨路径复用的核心词汇。
// ─────────────────────────────────────────────────────────────────────

const (
	// JSON-RPC 2.0 信封键（agent ↔ host 双向）。
	kJSONRPC = "jsonrpc"
	kID      = "id"
	kMethod  = "method"
	kParams  = "params"
	kResult  = "result"
	kError   = "error"

	// 事件与参数通用键。
	kType = "type"
	kSeq  = "seq"
	// sessionId：ACP 官方 camelCase；session_id：部分 x.ai 轨道的
	// snake_case。agent 方法参数两者都可能出现（见 resolveSessionKey）。
	kSessionID  = "sessionId"
	kSessionIDS = "session_id"
	// _meta 是 agent 侧动态元数据（透传保真）；meta 是 host 事件里对
	// 流式 chunk _meta 的转发别名。
	kMeta    = "_meta"
	kMetaOut = "meta"
	// session/update 载荷：params.update 是负载对象，其 sessionUpdate
	// 字段是 kind（serde tag，agent-client-protocol-schema client.rs）。
	kUpdate        = "update"
	kSessionUpdate = "sessionUpdate"

	// kReplayInternal 是 host 内部标记键，从不上 wire：agent 在
	// session/load 重放的每条通知上打 params._meta.isReplay（replay.rs），
	// 派生事件盖上该标记后由 Broadcast 在分配 seq 之前整条拦掉
	// （见 bridge.go Broadcast）。
	kReplayInternal = "_replay"

	// session/update 各 kind 广播出的事件载荷键。
	kText           = "text"
	kContent        = "content"
	kToolCall       = "toolCall"
	kToolCallUpdate = "toolCallUpdate"
	kEntries        = "entries"
	kCommands       = "commands"
	kModeState      = "modeState"
	kConfigOptions  = "configOptions"
	kTitle          = "title"
	kUpdatedAt      = "updatedAt"
	kUsed           = "used"
	kSize           = "size"
	kCost           = "cost"
	kRate           = "rate"
	kActive         = "active"
)

// 供 server 层复用的导出键：扩展端点构造 agent 参数时需要同样的
// camel/snake 会话键（见 server.sessionKey）。
const (
	WireSessionID  = kSessionID
	WireSessionIDS = kSessionIDS
)
