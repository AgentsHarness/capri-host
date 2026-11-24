package server

import (
	"net/http"
)

// routes 装配完整处理链：核心 API → 模型/goal/统计/用量 → 扩展域（每个
// register*Routes 与其 handler 同文件，路由与实现同址）→ 嵌入前端兜底。
// CORS for Vite dev. withAuth sits INSIDE withCORS so preflight OPTIONS
// (which never carries Authorization) is answered by CORS without tripping
// the token gate.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	s.registerCoreRoutes(mux)
	s.registerModelRoutes(mux)
	s.registerGoalRoutes(mux)
	s.registerStatsRoutes(mux)
	s.registerUsageRoutes(mux)
	s.registerExtRoutes(mux)
	s.registerExtSessionRoutes(mux)
	s.registerExtQueueRoutes(mux)
	s.registerExtGitRoutes(mux)
	s.registerExtCodeRoutes(mux)
	s.registerExtMCPRoutes(mux)
	s.registerExtEcosystemRoutes(mux)
	s.registerExtAuthRoutes(mux)
	s.registerExtTerminalRoutes(mux)
	s.registerExtFSRoutes(mux)
	s.registerExtCloudRoutes(mux)
	s.registerExtMiscRoutes(mux)
	s.registerAgentRoutes(mux)
	s.registerLocalFSRoutes(mux)
	// 嵌入的 capri-fe SPA（web/dist）：兜底 GET 路由，静态文件 +
	// 非 API 路径回退 index.html（实现见 web.go）
	mux.HandleFunc("GET /", s.handleWeb)
	// Host guard 在最外层：DNS rebinding 页面的请求 Host 是攻击者域名，
	// 必须在 CORS / 鉴权 / 路由之前拒绝，否则同源请求可以不带 Origin。
	// withIdempotency 在 withAuth 内侧：只有认证通过的请求才进入幂等表，
	// 免得未授权请求用 key 污染 store。withRequestLog 只对带
	// X-Capri-Request-ID 的 relay 请求生效，不改变本地请求路径。
	return s.withHostGuard(s.withCORS(withAuth(s.withIdempotency(s.withRequestLog(mux)), s.cfg.AccessToken, s.authLimiter)))
}

// registerCoreRoutes 注册核心 API（回合、会话、权限、任务、终端外的主链路）。
func (s *Server) registerCoreRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /events", s.handleSSE)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	// Hub link: read the pairing/connection state, and pair at runtime with
	// a code read off the hub. See http_hub.go.
	mux.HandleFunc("GET /api/hub/state", s.handleHubState)
	mux.HandleFunc("POST /api/hub/pair", s.handleHubPair)
	// Use a hub this host has already paired with, no code needed.
	mux.HandleFunc("POST /api/hub/reuse", s.handleHubReuse)
	// Disconnect from the hub, switching back to local mode.
	mux.HandleFunc("POST /api/hub/disconnect", s.handleHubDisconnect)
	// Host identity: the display name lives in the bridge and the hub
	// registry, so only this process can change it. See http_hub.go.
	mux.HandleFunc("POST /api/host/rename", s.handleHostRename)
	// Host configuration read and update.
	mux.HandleFunc("GET /api/host/config", s.handleGetHostConfig)
	mux.HandleFunc("POST /api/host/config", s.handlePostHostConfig)
	mux.HandleFunc("POST /api/host/detect-grok", s.handleDetectGrok)
	// Graceful stop, for the supervisor process that spawned this host.
	mux.HandleFunc("POST /api/host/quit", s.handleHostQuit)
	mux.HandleFunc("GET /api/probe", s.handleProbe)
	mux.HandleFunc("POST /api/prompt", s.handlePrompt)
	mux.HandleFunc("POST /api/cancel", s.handleCancel)
	mux.HandleFunc("POST /api/agent-restart", s.handleAgentRestart)
	mux.HandleFunc("POST /api/permission-response", s.handlePermission)
	mux.HandleFunc("POST /api/client-response", s.handleClientResponse)
	mux.HandleFunc("POST /api/session", s.handleSession)
	mux.HandleFunc("POST /api/session-load", s.handleSessionLoad)
	mux.HandleFunc("POST /api/set-mode", s.handleSetMode)
	mux.HandleFunc("POST /api/set-model", s.handleSetModel)
	mux.HandleFunc("POST /api/session/config-option", s.handleSetConfigOption)
	mux.HandleFunc("POST /api/session/set-config-option", s.handleSetConfigOption)
	mux.HandleFunc("POST /api/config-option", s.handleSetConfigOption)
	mux.HandleFunc("POST /api/sessions", s.handleListSessions)
	mux.HandleFunc("POST /api/session-state", s.handleSessionState)
	mux.HandleFunc("POST /api/session-updates", s.handleSessionUpdates)
	mux.HandleFunc("POST /api/session-running-tasks", s.handleSessionRunningTasks)
	mux.HandleFunc("POST /api/git-info", s.handleGitInfo)
	mux.HandleFunc("POST /api/session-fork", s.handleSessionFork)
	mux.HandleFunc("POST /api/session-rename", s.handleSessionRename)
	mux.HandleFunc("POST /api/recap", s.handleRecap)
	mux.HandleFunc("POST /api/session-info", s.handleSessionInfo)
	mux.HandleFunc("POST /api/session-plan", s.handleSessionPlan)
	mux.HandleFunc("POST /api/subagent-cancel", s.handleSubagentCancel)
	mux.HandleFunc("POST /api/subagent/message", s.handleSubagentMessage)
	mux.HandleFunc("POST /api/subagent-message", s.handleSubagentMessage)
	mux.HandleFunc("POST /api/task-kill", s.handleTaskKill)
	mux.HandleFunc("POST /api/task-list", s.handleTaskList)
	mux.HandleFunc("POST /api/task-output", s.handleTaskOutput)
	mux.HandleFunc("POST /api/session-delete", s.handleSessionDelete)
	mux.HandleFunc("POST /api/compact", s.handleCompact)
	mux.HandleFunc("POST /api/rewind-points", s.handleRewindPoints)
	mux.HandleFunc("POST /api/rewind-execute", s.handleRewindExecute)
	mux.HandleFunc("POST /api/scheduler-delete", s.handleSchedulerDelete)
	mux.HandleFunc("POST /api/shell", s.handleShell)
	mux.HandleFunc("GET /api/hosts", s.handleHosts)
	mux.HandleFunc("POST /api/billing", s.handleBilling)
	mux.HandleFunc("POST /api/memory-flush", s.handleMemoryFlush)
	mux.HandleFunc("POST /api/memory-rewrite", s.handleMemoryRewrite)
	mux.HandleFunc("POST /api/memory-list", s.handleMemoryList)
	mux.HandleFunc("POST /api/memory-toggle", s.handleMemoryToggle)
	mux.HandleFunc("POST /api/memory-dream", s.handleMemoryDream)
	mux.HandleFunc("POST /api/memory-forget", s.handleMemoryForget)
	mux.HandleFunc("POST /api/toggle-plan-mode", s.handleTogglePlanMode)
	mux.HandleFunc("POST /api/permissions-reset", s.handlePermissionsReset)
	mux.HandleFunc("GET /api/mcp/list", s.handleMCPList)
	mux.HandleFunc("POST /api/mcp-toggle", s.handleMCPToggle)
	mux.HandleFunc("POST /api/mcp-add", s.handleMCPAdd)
	mux.HandleFunc("POST /api/mcp-remove", s.handleMCPRemove)
	mux.HandleFunc("POST /api/mcp-auth-trigger", s.handleMCPAuthTrigger)
	mux.HandleFunc("GET /api/extensions", s.handleExtensions)
	mux.HandleFunc("GET /api/settings", s.handleSettings)
	mux.HandleFunc("POST /api/settings", s.handleSettingsUpdate)
}
