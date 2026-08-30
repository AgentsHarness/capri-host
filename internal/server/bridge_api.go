package server

import (
	"context"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// ─────────────────────────────────────────────────────────────────────
// bridge_api.go — HTTP 层对 *acp.Bridge 的消费者侧窄接口。
//
// Go 惯例：接口定义在消费者一侧。server 不再持有具体 *acp.Bridge，而是
// 声明它实际需要的 63 个方法（按 handler 域分组）；*acp.Bridge 隐式满足，
// main.go 无需适配。收益：
//
//   - 依赖面成为显式契约：Bridge 新增方法不会悄悄进入 server 的依赖；
//     server 依赖了什么一目了然；
//   - 测试可以用假实现替换单个域（不必再为每个 handler 域起假 agent
//     子进程）；
//   - 域分组本身是路由→桥方法的映射索引。
// ─────────────────────────────────────────────────────────────────────

// lifecycleAPI：宿主状态快照、事件订阅、agent 进程生命周期。
type lifecycleAPI interface {
	Snapshot() acp.Status
	Subscribe() (ch chan acp.Event, unsubscribe func())
	RestartAgent(ctx context.Context) error
	HasSession(sessionID string) bool
	SessionStateOf(sessionID string) *acp.SessionState
}

// promptAPI：回合驱动（prompt / cancel / 队列状态）。
type promptAPI interface {
	PromptWithOpts(ctx context.Context, sessionID string, blocks []acp.ContentBlock, opts acp.PromptOpts) (stopReason string, meta map[string]any, err error)
	CancelWithMeta(sessionID string, meta map[string]any)
	QueueStatus(sessionID string) map[string]any
}

// sessionAPI：会话生命周期与历史/回溯/任务视图。
type sessionAPI interface {
	NewSession(ctx context.Context, sc acp.SessionConfig) error
	LoadSession(ctx context.Context, sessionID, cwd string, meta ...map[string]any) (map[string]any, error)
	ResumeSession(ctx context.Context, sessionID, cwd string, meta ...map[string]any) (map[string]any, error)
	ListSessions(ctx context.Context, opts ...acp.ListSessionsOpts) (sessions []any, nextCursor string, meta map[string]any, err error)
	SessionDelete(ctx context.Context, sessionID string) (map[string]any, error)
	CloseSession(ctx context.Context, sessionID string) (map[string]any, error)
	RenameSession(ctx context.Context, sessionID, title string) (map[string]any, error)
	ForkSession(ctx context.Context, sessionID string, params map[string]any) (map[string]any, error)
	SessionInfo(sessionID string) *acp.SessionInfoDetail
	SessionPlan(sessionID, cwd string) (string, bool)
	SessionLoadHistory(ctx context.Context, beforeID string) (map[string]any, error)
	SessionUpdates(ctx context.Context, sessionID, cwd string, opts ...acp.SessionUpdatesOpts) (acp.UpdatesPage, error)
	SessionRunningTasks(sessionID, cwd string) ([]acp.TaskEvent, error)
	CompactConversation(ctx context.Context, sessionID, note string) (map[string]any, error)
	Recap(ctx context.Context, sessionID string, auto bool) (map[string]any, error)
	RewindPoints(ctx context.Context, sessionID string) (map[string]any, error)
	RewindExecute(ctx context.Context, sessionID string, targetIndex int, mode string) (map[string]any, error)
	SchedulerDelete(ctx context.Context, sessionID, taskID string) (map[string]any, error)
}

// permissionAPI：权限模式与 agent→client 请求的应答。
type permissionAPI interface {
	SetPermissionMode(ctx context.Context, mode string) (map[string]any, error)
	PermissionsReset(ctx context.Context, sessionID string) (map[string]any, error)
	RespondPermissionWithMeta(requestID, optionID string, cancelled bool, scope *acp.PermissionScope, followupMessage string) error
	RespondClientRequest(requestID string, result map[string]any, errMsg string) error
}

// modelAPI：模型/模式选择与自定义模型配置。
type modelAPI interface {
	SetModel(ctx context.Context, sessionID, modelID, reasoningEffort string) error
	SetMode(ctx context.Context, sessionID, modeID string) (map[string]any, error)
	TogglePlanMode(ctx context.Context, sessionID string) (map[string]any, error)
	SetDefaultModelConfig(modelID, effort string) error
	ListCustomModels() ([]map[string]any, error)
	UpsertCustomModel(id string, values map[string]any) error
	DeleteCustomModel(id string) (defaultCleared bool, err error)
	ReloadModels(ctx context.Context) error
}

// mcpAPI：MCP 服务器配置管理。
type mcpAPI interface {
	MCPList(ctx context.Context) (map[string]any, error)
	MCPUpsert(ctx context.Context, server map[string]any) (map[string]any, error)
	MCPDelete(ctx context.Context, name string) (map[string]any, error)
	MCPToggle(ctx context.Context, name string, enabled bool) (map[string]any, error)
	MCPAuthTrigger(ctx context.Context, name string) (map[string]any, error)
}

// taskAPI：后台任务与终端/子代理控制。
type taskAPI interface {
	TaskList(ctx context.Context) (map[string]any, error)
	TaskKill(ctx context.Context, sessionID, taskID string) (map[string]any, error)
	TaskLog(sessionID, cwd, taskID string) (*acp.TaskLog, error)
	SubagentCancel(ctx context.Context, sessionID, subagentID string) (map[string]any, error)
	TerminalPtyInput(ctx context.Context, terminalID, data string) (map[string]any, error)
}

// statsAPI：用量/统计/账单/git 元信息。
type statsAPI interface {
	UsageReport(ctx context.Context, cwd, sessionID string, from, to int64) (*acp.UsageReport, error)
	SessionStats(ctx context.Context, cwd, sessionID string) (*acp.SessionStats, error)
	Billing(ctx context.Context, sessionID string) (map[string]any, error)
	GitInfo(ctx context.Context, sessionID, cwd string) (map[string]any, error)
}

// goalAPI：host 侧 goal 引擎（实现见 acp.goalEngine）。
type goalAPI interface {
	GoalSet(ctx context.Context, sessionID, objective string, tokenBudget int64) (map[string]any, error)
	GoalStatus(sessionID string) (map[string]any, error)
	GoalPause(sessionID string) (map[string]any, error)
	GoalResume(sessionID string) (map[string]any, error)
	GoalClear(sessionID string) (map[string]any, error)
}

// miscAPI：记忆、UI 设置、模型默认广告位、agent 配置路径。
type miscAPI interface {
	MemoryFlush(ctx context.Context, sessionID string) (map[string]any, error)
	MemoryRewrite(ctx context.Context, sessionID, rawText, contextSummary string) (map[string]any, error)
	SetUiSettings(patch map[string]any) error
	SetToolsetSettings(patch map[string]any) error
	DismissModelDefaultCampaigns(ctx context.Context) error
	ConfigTOMLPath() (string, error)
}

// extAPI：x.ai/* 扩展直通（typed 端点与自由透传共用）。
type extAPI interface {
	XaiCall(ctx context.Context, method string, params map[string]any) (any, error)
	XaiNotify(ctx context.Context, method string, params map[string]any) (map[string]any, error)
}

// bridgeAPI 是 HTTP 层眼中"桥"的全部能力。handler 统一经 s.bridge
// （bridgeAPI 类型）调用；具体实现始终是 *acp.Bridge。
type bridgeAPI interface {
	lifecycleAPI
	promptAPI
	sessionAPI
	permissionAPI
	modelAPI
	mcpAPI
	taskAPI
	statsAPI
	goalAPI
	miscAPI
	extAPI
}

// 编译期锚定：*acp.Bridge 必须持续满足本接口（若 Bridge 演进破坏了
// server 的依赖契约，这里在构建期报错，而不是运行期 404/500）。
var _ bridgeAPI = (*acp.Bridge)(nil)
