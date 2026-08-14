package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

type Server struct {
	cfg    config.Config
	bridge *acp.Bridge
	http   *http.Server
	// promptFn runs a turn when injected (tests only; signature unchanged).
	// When nil, handlePrompt uses bridge.PromptWithOpts — the default path —
	// so the optional messageId/meta fields ride the session/prompt params.
	// The handler runs it detached from the client connection so a browser
	// crash mid-turn does not cancel the turn (the grok agent keeps running).
	promptFn func(ctx context.Context, sessionID string, blocks []acp.ContentBlock) (string, error)
}

func New(cfg config.Config, bridge *acp.Bridge) *Server {
	s := &Server{cfg: cfg, bridge: bridge}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", s.handleSSE)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("POST /api/prompt", s.handlePrompt)
	mux.HandleFunc("POST /api/cancel", s.handleCancel)
	mux.HandleFunc("POST /api/agent-restart", s.handleAgentRestart)
	mux.HandleFunc("POST /api/permission-response", s.handlePermission)
	mux.HandleFunc("POST /api/client-response", s.handleClientResponse)
	mux.HandleFunc("POST /api/session", s.handleSession)
	mux.HandleFunc("POST /api/session-load", s.handleSessionLoad)
	mux.HandleFunc("POST /api/set-mode", s.handleSetMode)
	mux.HandleFunc("POST /api/set-model", s.handleSetModel)
	mux.HandleFunc("POST /api/set-default-model", s.handleSetDefaultModel)
	mux.HandleFunc("POST /api/custom-models", s.handleCustomModels)
	mux.HandleFunc("POST /api/custom-model", s.handleCustomModelUpsert)
	mux.HandleFunc("POST /api/custom-model-delete", s.handleCustomModelDelete)
	mux.HandleFunc("POST /api/sessions", s.handleListSessions)
	mux.HandleFunc("POST /api/session-state", s.handleSessionState)
	mux.HandleFunc("POST /api/session-updates", s.handleSessionUpdates)
	mux.HandleFunc("POST /api/session-running-tasks", s.handleSessionRunningTasks)
	mux.HandleFunc("POST /api/usage-report", s.handleUsageReport)
	mux.HandleFunc("POST /api/session-stats", s.handleSessionStats)
	mux.HandleFunc("POST /api/git-info", s.handleGitInfo)
	mux.HandleFunc("POST /api/session-fork", s.handleSessionFork)
	mux.HandleFunc("POST /api/session-rename", s.handleSessionRename)
	mux.HandleFunc("POST /api/recap", s.handleRecap)
	mux.HandleFunc("POST /api/session-info", s.handleSessionInfo)
	mux.HandleFunc("POST /api/subagent-cancel", s.handleSubagentCancel)
	mux.HandleFunc("POST /api/task-kill", s.handleTaskKill)
	mux.HandleFunc("POST /api/task-list", s.handleTaskList)
	mux.HandleFunc("POST /api/task-output", s.handleTaskOutput)
	mux.HandleFunc("POST /api/session-delete", s.handleSessionDelete)
	mux.HandleFunc("POST /api/compact", s.handleCompact)
	mux.HandleFunc("POST /api/rewind-points", s.handleRewindPoints)
	mux.HandleFunc("POST /api/rewind-execute", s.handleRewindExecute)
	mux.HandleFunc("POST /api/scheduler-delete", s.handleSchedulerDelete)
	// ── goal engine (TUI /goal parity; host-side, see http_goal.go) ──
	mux.HandleFunc("POST /api/goal/set", s.handleGoalSet)
	mux.HandleFunc("POST /api/goal/status", s.handleGoalStatus)
	mux.HandleFunc("POST /api/goal/pause", s.handleGoalPause)
	mux.HandleFunc("POST /api/goal/resume", s.handleGoalResume)
	mux.HandleFunc("POST /api/goal/clear", s.handleGoalClear)
	mux.HandleFunc("POST /api/shell", s.handleShell)
	mux.HandleFunc("GET /api/hosts", s.handleHosts)
	mux.HandleFunc("POST /api/billing", s.handleBilling)
	mux.HandleFunc("POST /api/memory-flush", s.handleMemoryFlush)
	mux.HandleFunc("POST /api/memory-rewrite", s.handleMemoryRewrite)
	mux.HandleFunc("POST /api/toggle-plan-mode", s.handleTogglePlanMode)
	mux.HandleFunc("POST /api/permissions-reset", s.handlePermissionsReset)
	mux.HandleFunc("GET /api/mcp/list", s.handleMCPList)
	mux.HandleFunc("POST /api/mcp-toggle", s.handleMCPToggle)
	mux.HandleFunc("POST /api/mcp-add", s.handleMCPAdd)
	mux.HandleFunc("POST /api/mcp-remove", s.handleMCPRemove)
	mux.HandleFunc("POST /api/mcp-auth-trigger", s.handleMCPAuthTrigger)
	mux.HandleFunc("GET /api/extensions", s.handleExtensions)
	mux.HandleFunc("GET /api/settings", s.handleSettings)
	// ── x.ai 扩展直通（完整对齐；实现见 http_ext.go）──
	mux.HandleFunc("POST /api/xai-call", s.handleXaiCall)
	mux.HandleFunc("POST /api/session-resume", s.handleSessionResume)
	mux.HandleFunc("POST /api/session-close", s.handleSessionClose)
	mux.HandleFunc("POST /api/session-load-history", s.handleSessionLoadHistory)
	mux.HandleFunc("POST /api/git/status", s.handleGitStatus)
	mux.HandleFunc("POST /api/git/diffs", s.handleGitDiffs)
	mux.HandleFunc("POST /api/git/stage", s.handleGitStage)
	mux.HandleFunc("POST /api/git/unstage", s.handleGitUnstage)
	mux.HandleFunc("POST /api/git/discard", s.handleGitDiscard)
	mux.HandleFunc("POST /api/git/commit", s.handleGitCommit)
	mux.HandleFunc("POST /api/git/checkout", s.handleGitCheckout)
	mux.HandleFunc("POST /api/git/checkout-commit", s.handleGitCheckoutCommit)
	mux.HandleFunc("POST /api/git/stash", s.handleGitStash)
	mux.HandleFunc("POST /api/git/branches", s.handleGitBranches)
	mux.HandleFunc("POST /api/git/current-commit", s.handleGitCurrentCommit)
	mux.HandleFunc("POST /api/git/repo-root", s.handleGitRepoRoot)
	mux.HandleFunc("POST /api/queue/remove", s.handleQueueRemove)
	mux.HandleFunc("POST /api/queue/clear", s.handleQueueClear)
	mux.HandleFunc("POST /api/queue/reorder", s.handleQueueReorder)
	mux.HandleFunc("POST /api/queue/edit", s.handleQueueEdit)
	mux.HandleFunc("POST /api/queue/interject", s.handleQueueInterject)
	mux.HandleFunc("POST /api/queue/hold-edit", s.handleQueueHoldEdit)
	mux.HandleFunc("POST /api/queue/release-edit", s.handleQueueReleaseEdit)
	mux.HandleFunc("POST /api/queue/status", s.handleQueueStatus)
	mux.HandleFunc("POST /api/skills/list", s.handleSkillsList)
	mux.HandleFunc("POST /api/skills/toggle", s.handleSkillsToggle)
	mux.HandleFunc("POST /api/skills/add", s.handleSkillsAdd)
	mux.HandleFunc("POST /api/skills/remove", s.handleSkillsRemove)
	mux.HandleFunc("POST /api/skills/refresh-baseline", s.handleSkillsRefreshBaseline)
	mux.HandleFunc("POST /api/plugins/list", s.handlePluginsList)
	mux.HandleFunc("POST /api/plugins/action", s.handlePluginsAction)
	mux.HandleFunc("POST /api/plugins/reload", s.handlePluginsReload)
	mux.HandleFunc("POST /api/hooks/list", s.handleHooksList)
	mux.HandleFunc("POST /api/hooks/action", s.handleHooksAction)
	mux.HandleFunc("POST /api/marketplace/list", s.handleMarketplaceList)
	mux.HandleFunc("POST /api/marketplace/action", s.handleMarketplaceAction)
	mux.HandleFunc("POST /api/workflows/list", s.handleWorkflowsList)
	mux.HandleFunc("POST /api/session/info", s.handleSessionInfoExt)
	mux.HandleFunc("POST /api/session/usage", s.handleSessionUsage)
	mux.HandleFunc("POST /api/session/search", s.handleSessionSearch)
	mux.HandleFunc("POST /api/sessions/list", s.handleSessionsList)
	mux.HandleFunc("POST /api/prompt-history", s.handlePromptHistory)
	mux.HandleFunc("POST /api/btw", s.handleBtw)
	mux.HandleFunc("POST /api/interject", s.handleInterject)
	mux.HandleFunc("POST /api/commands-list", s.handleCommandsList)
	mux.HandleFunc("POST /api/workspaces/list", s.handleWorkspacesList)
	mux.HandleFunc("POST /api/subagent/list-running", s.handleSubagentListRunning)
	mux.HandleFunc("POST /api/session/share", s.handleSessionShare)
	mux.HandleFunc("POST /api/mcp/read-resource", s.handleMCPReadResource)
	mux.HandleFunc("POST /api/mcp/auth-status", s.handleMCPAuthStatus)
	mux.HandleFunc("POST /api/mcp/setup", s.handleMCPSetup)
	mux.HandleFunc("POST /api/mcp/toggle-tool", s.handleMCPToggleTool)
	mux.HandleFunc("POST /api/mcp/call", s.handleMCPCall)
	mux.HandleFunc("POST /api/auth/info", s.handleAuthInfo)
	mux.HandleFunc("POST /api/auth/logout", s.handleAuthLogout)
	mux.HandleFunc("POST /api/auth/get-url", s.handleAuthGetURL)
	mux.HandleFunc("POST /api/auth/submit-code", s.handleAuthSubmitCode)
	mux.HandleFunc("POST /api/fs/list", s.handleFSList)
	mux.HandleFunc("POST /api/fs/read-file", s.handleFSReadFile)
	mux.HandleFunc("POST /api/fs/exists", s.handleFSExists)
	mux.HandleFunc("POST /api/capabilities", s.handleCapabilities)
	mux.HandleFunc("POST /api/folder-trust-request", s.handleFolderTrustRequest)
	mux.HandleFunc("POST /api/suggest", s.handleSuggest)
	mux.HandleFunc("POST /api/suggest-prompt", s.handleSuggestPrompt)
	mux.HandleFunc("POST /api/pr/status", s.handlePRStatus)
	mux.HandleFunc("POST /api/hunk-tracker/hunks", s.handleHunkTrackerHunks)
	mux.HandleFunc("POST /api/bundle/status", s.handleBundleStatus)
	mux.HandleFunc("POST /api/terminal/list", s.handleTerminalList)
	mux.HandleFunc("POST /api/search/content", s.handleSearchContent)
	mux.HandleFunc("POST /api/billing/auto-topup-rule", s.handleAutoTopupRule)
	mux.HandleFunc("POST /api/feedback", s.handleFeedback)
	mux.HandleFunc("POST /api/cloud/env/list", s.handleCloudEnvList)
	// ── x.ai 扩展直通（完整对齐，第二批；实现见 http_ext2.go）──
	mux.HandleFunc("POST /api/session/state", s.handleSessionStateExt)
	mux.HandleFunc("POST /api/session-import", s.handleSessionImport)
	mux.HandleFunc("POST /api/session-repair", s.handleSessionRepair)
	mux.HandleFunc("POST /api/session-update-mcp-servers", s.handleSessionUpdateMcpServers)
	mux.HandleFunc("POST /api/session-add-local-workspace", s.handleSessionAddLocalWorkspace)
	mux.HandleFunc("POST /api/session-resolve-worktree-resume", s.handleSessionResolveWorktreeResume)
	mux.HandleFunc("POST /api/session-rehydrate", s.handleSessionRehydrate)
	mux.HandleFunc("POST /api/session-summaries/session-list", s.handleSessionSummariesSessionList)
	mux.HandleFunc("POST /api/session-summaries/workspace-list", s.handleSessionSummariesWorkspaceList)
	mux.HandleFunc("POST /api/session-summaries/workspace-list-recent", s.handleSessionSummariesWorkspaceListRecent)
	mux.HandleFunc("POST /api/cloud/terminate", s.handleCloudTerminate)
	mux.HandleFunc("POST /api/cloud/env/create", s.handleCloudEnvCreate)
	mux.HandleFunc("POST /api/cloud/env/update", s.handleCloudEnvUpdate)
	mux.HandleFunc("POST /api/cloud/env/delete", s.handleCloudEnvDelete)
	mux.HandleFunc("POST /api/api-key-get", s.handleApiKeyGet)
	mux.HandleFunc("POST /api/api-key-set", s.handleApiKeySet)
	mux.HandleFunc("POST /api/auth/get-bearer-token", s.handleAuthGetBearerToken)
	mux.HandleFunc("POST /api/auth/cancel", s.handleAuthCancel)
	mux.HandleFunc("POST /api/auth/check-subscription", s.handleAuthCheckSubscription)
	mux.HandleFunc("POST /api/privacy/set-coding-data-retention", s.handlePrivacySetCodingDataRetention)
	mux.HandleFunc("POST /api/rollout/survey", s.handleRolloutSurvey)
	mux.HandleFunc("POST /api/git/files", s.handleGitFiles)
	mux.HandleFunc("POST /api/git/stage-content", s.handleGitStageContent)
	mux.HandleFunc("POST /api/git/checkout-session-head", s.handleGitCheckoutSessionHead)
	mux.HandleFunc("POST /api/git/worktree/create", s.handleWorktreeCreate)
	mux.HandleFunc("POST /api/git/worktree/remove", s.handleWorktreeRemove)
	mux.HandleFunc("POST /api/git/worktree/apply", s.handleWorktreeApply)
	mux.HandleFunc("POST /api/git/worktree/create-from-worktree", s.handleWorktreeCreateFromWorktree)
	mux.HandleFunc("POST /api/git/worktree/create-from-worktree-sync", s.handleWorktreeCreateFromWorktreeSync)
	mux.HandleFunc("POST /api/git/worktree/resume-session", s.handleWorktreeResumeSession)
	mux.HandleFunc("POST /api/git/worktree/list", s.handleWorktreeList)
	mux.HandleFunc("POST /api/git/worktree/show", s.handleWorktreeShow)
	mux.HandleFunc("POST /api/git/worktree/gc", s.handleWorktreeGc)
	mux.HandleFunc("POST /api/git/worktree/db/stats", s.handleWorktreeDbStats)
	mux.HandleFunc("POST /api/git/worktree/db/rebuild", s.handleWorktreeDbRebuild)
	mux.HandleFunc("POST /api/git/worktree/db/path", s.handleWorktreeDbPath)
	mux.HandleFunc("POST /api/hunk-tracker/files", s.handleHunkTrackerFiles)
	mux.HandleFunc("POST /api/hunk-tracker/file-contents", s.handleHunkTrackerFileContents)
	mux.HandleFunc("POST /api/hunk-tracker/summary", s.handleHunkTrackerSummary)
	mux.HandleFunc("POST /api/hunk-tracker/hunk-action", s.handleHunkTrackerHunkAction)
	mux.HandleFunc("POST /api/hunk-tracker/file-action", s.handleHunkTrackerFileAction)
	mux.HandleFunc("POST /api/hunk-tracker/turn-action", s.handleHunkTrackerTurnAction)
	mux.HandleFunc("POST /api/hunk-tracker/all-action", s.handleHunkTrackerAllAction)
	mux.HandleFunc("POST /api/skills/reset", s.handleSkillsReset)
	mux.HandleFunc("POST /api/skills/config", s.handleSkillsConfig)
	mux.HandleFunc("POST /api/plugins/notify-updates", s.handlePluginsNotifyUpdates)
	mux.HandleFunc("POST /api/subagent/get", s.handleSubagentGet)
	mux.HandleFunc("POST /api/terminal/create", s.handleTerminalCreate)
	mux.HandleFunc("POST /api/terminal/kill", s.handleTerminalKill)
	mux.HandleFunc("POST /api/terminal/output", s.handleTerminalOutput)
	mux.HandleFunc("POST /api/terminal/wait-for-exit", s.handleTerminalWaitForExit)
	mux.HandleFunc("POST /api/terminal/release", s.handleTerminalRelease)
	mux.HandleFunc("POST /api/terminal/background", s.handleTerminalBackground)
	mux.HandleFunc("POST /api/terminal/pty/create", s.handleTerminalPtyCreate)
	mux.HandleFunc("POST /api/terminal/pty/load", s.handleTerminalPtyLoad)
	mux.HandleFunc("POST /api/terminal/pty/resize", s.handleTerminalPtyResize)
	mux.HandleFunc("POST /api/terminal/pty/input", s.handleTerminalPtyInput)
	mux.HandleFunc("POST /api/fs/write-file", s.handleFSWriteFile)
	mux.HandleFunc("POST /api/fs/delete-file", s.handleFSDeleteFile)
	mux.HandleFunc("POST /api/search/fuzzy/open", s.handleSearchFuzzyOpen)
	mux.HandleFunc("POST /api/search/fuzzy/change", s.handleSearchFuzzyChange)
	mux.HandleFunc("POST /api/search/fuzzy/close", s.handleSearchFuzzyClose)
	mux.HandleFunc("POST /api/bundle/sync", s.handleBundleSync)
	mux.HandleFunc("POST /api/bundle/entry-get", s.handleBundleEntryGet)
	mux.HandleFunc("POST /api/code/goto-definition", s.handleCodeGotoDefinition)
	mux.HandleFunc("POST /api/code/goto-references", s.handleCodeGotoReferences)
	mux.HandleFunc("POST /api/code/find-definitions", s.handleCodeFindDefinitions)
	mux.HandleFunc("POST /api/code/find-references", s.handleCodeFindReferences)
	mux.HandleFunc("POST /api/code/status", s.handleCodeStatus)
	mux.HandleFunc("POST /api/review/comment", s.handleReviewComment)
	mux.HandleFunc("POST /api/review/comment-delete", s.handleReviewCommentDelete)
	mux.HandleFunc("POST /api/debug/trigger-feedback", s.handleDebugTriggerFeedback)
	mux.HandleFunc("POST /api/debug/arm-auto-compact", s.handleDebugArmAutoCompact)
	mux.HandleFunc("POST /api/debug/agent", s.handleDebugAgent)
	// ── 嵌入的 capri-fe SPA（web/dist）：兜底 GET 路由，静态文件 +
	// 非 API 路径回退 index.html（实现见 web.go）──
	mux.HandleFunc("GET /", s.handleWeb)
	// CORS for Vite dev. withAuth sits INSIDE withCORS so preflight
	// OPTIONS (which never carries Authorization) is answered by CORS
	// without tripping the token gate.
	s.http = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           withCORS(withAuth(mux, cfg.AccessToken)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// ── local-origin guard for sensitive endpoints ────────────────────────

// sensitiveEndpointPaths are the routes that expose privileged host
// capabilities — arbitrary local command execution (/api/shell) and
// API-key/auth material — and must never be reachable cross-origin from a
// random web page. Any /api/auth/* route is sensitive too.
var sensitiveEndpointPaths = []string{
	"/api/shell",
	"/api/api-key-get",
	"/api/api-key-set",
}

// isSensitiveEndpoint reports whether r targets a sensitive endpoint.
func isSensitiveEndpoint(r *http.Request) bool {
	for _, p := range sensitiveEndpointPaths {
		if r.URL.Path == p {
			return true
		}
	}
	return strings.HasPrefix(r.URL.Path, "/api/auth/")
}

// isLocalOrigin reports whether the request's Origin header (when present)
// names a local origin: localhost / 127.0.0.1 / ::1 on any port (covers the
// Vite dev server on another localhost port), or the exact host:port of the
// request's own Host header (same-origin calls made through the machine's
// LAN address). A missing Origin — curl, server-side callers, the host UI's
// own same-origin requests — is allowed: browsers only attach Origin on
// cross-origin or non-GET requests, and a hostile page cannot forge it to a
// local value. `Origin: null` (sandboxed iframes) has no host and is
// rejected.
func isLocalOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	host := u.Hostname()
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	return strings.EqualFold(u.Host, r.Host)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sensitive := isSensitiveEndpoint(r)
		if sensitive {
			// Sensitive endpoints never answer `*`: echo the request Origin
			// back only when it is a local origin, so a cross-origin web
			// page can neither read nor invoke them.
			if origin := r.Header.Get("Origin"); origin != "" && isLocalOrigin(r) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
			if !isLocalOrigin(r) {
				writeJSON(w, http.StatusForbidden, map[string]any{
					"ok": false, "error": "cross-origin request to sensitive endpoint denied",
				})
				return
			}
		} else {
			// CORS for Vite dev: the FE runs on another localhost port and
			// must reach every non-sensitive endpoint.
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		// Authorization: the FE attaches Bearer on every API call; the
		// preflight must admit it or cross-origin direct host calls (hub
		// mode 双连接) would be blocked by the browser.
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── inbound access-token gate ─────────────────────────────────────────

// authRequiredForPath reports whether a path requires the configured
// access token. Exemptions (by design, the FE's boot probes):
//   - `GET /`            — SPA static assets; browsers cannot attach custom
//     headers to script/style loads, and the page itself is public.
//   - `GET /api/hosts`   — detectMode()/probeAccess() must reach it before
//     any token exists. It carries no conversation data; the response
//     declares `authRequired` so the FE knows to show the gate. Its 401
//     semantics belong to the HUB (the FE uses 401 to tell "this is a
//     hub, not a host") — a host must never 401 here.
//
// Everything else under /api/* and the /events SSE stream is gated.
func authRequiredForPath(path string) bool {
	if strings.HasPrefix(path, "/api/") {
		return path != "/api/hosts"
	}
	return path == "/events"
}

// withAuth gates /api/* (except /api/hosts) and /events with the host's
// access token when one is configured; empty token disables the gate
// (local trusted default). Accepted transports:
//   - Authorization: Bearer <token>   (the FE's apiFetch always sends it)
//   - ?token=<token>                  (EventSource cannot set headers)
func withAuth(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || !authRequiredForPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if tokenEqual(bearerToken(r.Header.Get("Authorization")), token) ||
			tokenEqual(strings.TrimSpace(r.URL.Query().Get("token")), token) {
			next.ServeHTTP(w, r)
			return
		}
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"ok": false, "error": "需要有效的访问 token",
		})
	})
}

// bearerToken extracts the token from an Authorization header
// ("Bearer <token>", case-insensitive prefix, trimmed).
func bearerToken(auth string) string {
	auth = strings.TrimSpace(auth)
	const prefix = "Bearer "
	if len(auth) >= len(prefix) && strings.EqualFold(auth[:len(prefix)], prefix) {
		return strings.TrimSpace(auth[len(prefix):])
	}
	return ""
}

// tokenEqual compares two tokens in constant time so the shared secret
// cannot be fingerprinted via timing (same scheme as capri-hub).
func tokenEqual(a, b string) bool {
	if a == "" || b == "" || len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

func (s *Server) ListenAndServe() error {
	log.Printf("[capri-host] listening on http://localhost:%d", s.cfg.Port)
	log.Printf("[capri-host] grok bin=%s hostId=%s name=%q", s.cfg.GrokBin, s.cfg.HostID, s.cfg.HostName)
	log.Printf("[capri-host] mode=agent-only (no client fs/terminal)")
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// 防御性：告知前置代理（nginx 等）不要缓冲流式响应——SSE 一旦被
	// 缓冲，回合事件会成批到达，前端实时性受损（有时还会撑爆代理缓冲）。
	w.Header().Set("X-Accel-Buffering", "no-buffering")

	snap := s.bridge.Snapshot()
	// hello 的 busy 只反映被 announce 的会话（snap.SessionID）本身，而非
	// 所有会话的 OR 聚合（聚合版保留在 Status.Busy，供 /api/status）：
	// 另一个会话在后台跑回合时页面刷新，前台视图不应被永久置 busy ——
	// 后台会话的 done 事件会被客户端按 sessionId 过滤掉，永远清不掉。
	// 与 handleSessionLoad 返回 per-session busy 的语义一致。
	helloBusy := false
	if st := s.bridge.SessionStateOf(snap.SessionID); st != nil {
		helloBusy = st.Busy
	}
	hello := map[string]any{
		"type":              "hello",
		"ready":             snap.Ready,
		"busy":              helloBusy,
		"sessionId":         snap.SessionID,
		"cwd":               snap.Cwd,
		"text":              snap.Text,
		"error":             snap.BootError,
		"agentInfo":         snap.AgentInfo,
		"modes":             snap.Modes,
		"configOptions":     snap.ConfigOptions,
		"models":            snap.Models,
		"pendingRequests":   snap.PendingRequests,
		"hostId":            snap.HostID,
		"hostName":          snap.HostName,
		"homeDir":           snap.HomeDir,
		"capabilities":      snap.Capabilities,
		"agentCapabilities": snap.AgentCapabilities,
		"roster":            snap.Roster,
		"agentStartedAt":    snap.AgentStartedAt,
		"permissionMode":    snap.PermissionMode,
	}
	data, _ := json.Marshal(hello)
	if !writeSSEFrame(w, flusher, data) {
		return // client gone before the hello even landed
	}

	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !writeSSEFrame(w, flusher, nil) {
				return
			}
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// 复制后附加 hostId：本地 SSE 事件需带 hostId 供前端按
			// (hostId, seq) 与 hub 中继推回的同源事件去重（双连接时
			// 本地 SSE 与 /ws/fe 各来一份）。不改 bridge 共享 map ——
			// hub-client 与 SSE 订阅同一事件，改 map 会污染上传路径。
			// 与 hub 的 stripFullUpdate 对齐：去掉大体积 fullUpdate，
			// 避免撑爆 SSE 帧与 Subscribe 缓冲导致 Broadcast drop。
			out := make(map[string]any, len(ev)+1)
			for k, v := range ev {
				out[k] = v
			}
			delete(out, "fullUpdate")
			out["hostId"] = snap.HostID
			b, err := json.Marshal(out)
			if err != nil {
				continue
			}
			if !writeSSEFrame(w, flusher, b) {
				return
			}
		}
	}
}

// writeSSEFrame writes one SSE frame and flushes. Returns false when the
// client went away (write failed) or the flush failed, so the SSE handler
// can return and unsubscribe immediately instead of blocking forever on a
// dead reader (TCP window full / RST) — a stuck handler would leak the
// subscription and stall every other subscriber's Broadcast.
func writeSSEFrame(w http.ResponseWriter, flusher http.Flusher, data []byte) bool {
	var err error
	if data == nil {
		// Comment frame (ticker ping); `%s` of nil would print "<nil>".
		_, err = fmt.Fprintf(w, ": ping\n\n")
	} else {
		_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	}
	if err != nil {
		return false
	}
	flusher.Flush()
	return true
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.bridge.Snapshot()
	// 部署模式由 host 配置决定：配了 HUB_URL → hub 模式（内嵌前端跨源
	// 直连 hub 看全部 host）；否则 local 模式（前端锁定本机）。
	if s.cfg.HubURL != "" {
		snap.Mode = "hub"
		snap.HubURL = s.cfg.HubURL
	} else {
		snap.Mode = "local"
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleHosts(w http.ResponseWriter, r *http.Request) {
	// Local mode: single host (this process). Multi-host comes via Hub.
	snap := s.bridge.Snapshot()
	// authRequired: this host requires the access token on its API (the
	// FE reads it in probeAccess to decide whether to show the gate —
	// /api/hosts itself stays open so boot probing works pre-token).
	writeJSON(w, http.StatusOK, map[string]any{
		"hosts": []map[string]any{
			{
				"hostId":   snap.HostID,
				"hostName": snap.HostName,
				"online":   true,
				"ready":    snap.Ready,
				"local":    true,
			},
		},
		"authRequired": s.cfg.AccessToken != "",
	})
}

type promptBody struct {
	Blocks []acp.ContentBlock `json:"blocks"`
	// Optional: target session (defaults to the active session).
	SessionID string `json:"sessionId,omitempty"`
	// Optional: messageId (UUID) / _meta, forwarded on the session/prompt
	// params only when set (absent key ≠ off, matching the TUI).
	MessageID string         `json:"messageId,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
}

func (s *Server) handlePrompt(w http.ResponseWriter, r *http.Request) {
	var body promptBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if len(body.Blocks) == 0 {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "blocks 不能为空"})
		return
	}
	// 显式指向的会话必须存在（纯内存检查，无 agent 交互）：未知会话在
	// 受理前同步 404，不走受理流程。未显式指定会话（缺省 active）时
	// 跳过——无活动会话时的恢复/新建是 agent roundtrip，留在后台完成，
	// 失败经 SSE error 事件送达（bridge.PromptWithOpts 的广播）。
	if body.SessionID != "" && !s.bridge.HasSession(body.SessionID) {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "会话不存在"})
		return
	}
	// 受理即返回：POST 只确认"已受理"（{ok:true}），回合在后台跑，结果
	// （done / error / cancelled + meta）全部经 live 通道（SSE/WS）送达。
	// 请求不再在反代（Cloudflare ~100s / nginx）前挂到回合结束，反代超时
	// （524/504）从根上消失。回合执行路径不变：context.Background() 使
	// 客户端断开不会取消回合——浏览器 crash / 关页后回合照常跑完，进度
	// 与结果仍走 live 通道，只有显式 /api/cancel 能停它。
	go func() {
		if s.promptFn != nil {
			// Test-injected promptFn keeps its exact contract.
			_, _ = s.promptFn(context.Background(), body.SessionID, body.Blocks)
			return
		}
		_, _, _ = s.bridge.PromptWithOpts(context.Background(), body.SessionID, body.Blocks,
			acp.PromptOpts{MessageID: body.MessageID, Meta: body.Meta})
	}()
	writeJSON(w, 200, map[string]any{"ok": true})
}

type cancelBody struct {
	// Optional: target session (defaults to the active session).
	SessionID string `json:"sessionId,omitempty"`
	// Optional session/cancel `_meta` seeds (grok reads them at
	// acp_agent.rs:2079-2108): cancelTrigger ("esc"|"ctrl_c"),
	// cancelSubagents (default true), rewindIfNoOutput / rewindIfPristine.
	// Only the keys the client sends are forwarded; absent = agent default.
	CancelTrigger    string `json:"cancelTrigger,omitempty"`
	CancelSubagents  *bool  `json:"cancelSubagents,omitempty"`
	RewindIfNoOutput *bool  `json:"rewindIfNoOutput,omitempty"`
	RewindIfPristine *bool  `json:"rewindIfPristine,omitempty"`
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var body cancelBody
	_ = readJSON(r, &body)
	meta := map[string]any{}
	if body.CancelTrigger != "" {
		meta["cancelTrigger"] = body.CancelTrigger
	}
	if body.CancelSubagents != nil {
		meta["cancelSubagents"] = *body.CancelSubagents
	}
	if body.RewindIfNoOutput != nil {
		meta["rewindIfNoOutput"] = *body.RewindIfNoOutput
	}
	if body.RewindIfPristine != nil {
		meta["rewindIfPristine"] = *body.RewindIfPristine
	}
	s.bridge.CancelWithMeta(body.SessionID, meta)
	writeJSON(w, 200, map[string]any{"ok": true})
}

// handleAgentRestart restarts the grok agent process on user request:
// kill the current process (if any), clear host state, boot a fresh agent
// and restore the last session. The host never restarts the agent on its
// own — this endpoint is the only restart path (assume the agent is
// reliable; failures surface as errors, and the user decides when to
// restart).
func (s *Server) handleAgentRestart(w http.ResponseWriter, r *http.Request) {
	// 重启是服务端状态操作，用独立超时而非 r.Context()：客户端中途断
	// 开不应取消重启流程 —— kill 之后 boot 失败会留下无进程状态。
	// 4 分钟 = acp 包 bootTimeout(2min) × 2，覆盖杀进程 + boot + 恢复。
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := s.bridge.RestartAgent(ctx); err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

type permBody struct {
	RequestID string `json:"requestId"`
	OptionID  string `json:"optionId"`
	Cancelled bool   `json:"cancelled"`
	// Scope and FollowupMessage are optional and forwarded to the agent
	// via the ACP response `_meta` (bash command scope / reject followup),
	// aligned with the TUI's permission dispatch.
	Scope           *acp.PermissionScope `json:"scope,omitempty"`
	FollowupMessage string               `json:"followupMessage,omitempty"`
}

func (s *Server) handlePermission(w http.ResponseWriter, r *http.Request) {
	var body permBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RequestID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 requestId"})
		return
	}
	if err := s.bridge.RespondPermissionWithMeta(body.RequestID, body.OptionID, body.Cancelled, body.Scope, body.FollowupMessage); err != nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// handleClientResponse resolves a forwarded x.ai/* request (ask_user_question,
// exit_plan_mode, …) with a raw result object or an error message.
type clientResponseBody struct {
	RequestID string         `json:"requestId"`
	Result    map[string]any `json:"result"`
	Error     string         `json:"error"`
}

func (s *Server) handleClientResponse(w http.ResponseWriter, r *http.Request) {
	var body clientResponseBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RequestID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 requestId"})
		return
	}
	if err := s.bridge.RespondClientRequest(body.RequestID, body.Result, body.Error); err != nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	var sc acp.SessionConfig
	_ = readJSON(r, &sc)
	if err := s.bridge.NewSession(r.Context(), sc); err != nil {
		writeAgentError(w, "session/new", err)
		return
	}
	snap := s.bridge.Snapshot()
	writeJSON(w, 200, map[string]any{"ok": true, "sessionId": snap.SessionID})
}

type modeBody struct {
	ModeID    string `json:"modeId"`
	SessionID string `json:"sessionId,omitempty"`
}

func (s *Server) handleSetMode(w http.ResponseWriter, r *http.Request) {
	var body modeBody
	if err := readJSON(r, &body); err != nil || body.ModeID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modeId"})
		return
	}
	// Permission modes ride the x.ai/yolo_mode_changed notification (the
	// agent's ask/auto/always-approve channel, TUI parity) — session/set_mode
	// only understands the session-mode ids and would silently no-op on
	// normal/auto/always-approve. Session-mode ids keep the legacy path.
	switch body.ModeID {
	case "normal", "auto", "always-approve", "always_approve", "yolo":
		if _, err := s.bridge.SetPermissionMode(r.Context(), body.ModeID); err != nil {
			writeAgentError(w, "set-permission-mode", err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	res, err := s.bridge.SetMode(r.Context(), body.SessionID, body.ModeID)
	if err != nil {
		writeAgentError(w, "session/set-mode", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

type setModelBody struct {
	ModelID         string `json:"modelId"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
	SessionID       string `json:"sessionId,omitempty"`
}

func (s *Server) handleSetModel(w http.ResponseWriter, r *http.Request) {
	var body setModelBody
	if err := readJSON(r, &body); err != nil || body.ModelID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modelId"})
		return
	}
	if err := s.bridge.SetModel(r.Context(), body.SessionID, body.ModelID, body.ReasoningEffort); err != nil {
		writeAgentError(w, "session/set-model", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// listSessionsBody — POST /api/sessions. cwd/cursor/meta are OPTIONAL
// official session/list fields forwarded on the wire when present (absent
// = the existing `{}` request exactly); the response keeps the existing
// local enrichment (status/badges) untouched.
type listSessionsBody struct {
	Cwd    string         `json:"cwd,omitempty"`
	Cursor string         `json:"cursor,omitempty"`
	Meta   map[string]any `json:"meta,omitempty"`
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	var body listSessionsBody
	_ = readJSON(r, &body)
	var opts acp.ListSessionsOpts
	if body.Cwd != "" || body.Cursor != "" || len(body.Meta) > 0 {
		opts = acp.ListSessionsOpts{Cwd: body.Cwd, Cursor: body.Cursor, Meta: body.Meta}
	}
	sessions, nextCursor, meta, err := s.bridge.ListSessions(r.Context(), opts)
	if err != nil {
		writeAgentError(w, "session/list", err)
		return
	}
	out := map[string]any{"ok": true, "sessions": sessions}
	// 响应游标与 `_meta` 原样透传（仅非空才带；absent key ≠ off）。
	if nextCursor != "" {
		out["nextCursor"] = nextCursor
	}
	if len(meta) > 0 {
		out["meta"] = meta
	}
	writeJSON(w, 200, out)
}

type sessionStateBody struct {
	SessionID string `json:"sessionId"`
}

// handleSessionState — x.ai/session/state analog: host-side live state of
// one session (dashboard active/idle/awaiting classification).
func (s *Server) handleSessionState(w http.ResponseWriter, r *http.Request) {
	var body sessionStateBody
	_ = readJSON(r, &body)
	if body.SessionID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId"})
		return
	}
	st := s.bridge.SessionStateOf(body.SessionID)
	if st == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "会话不在线（未在本进程创建/加载）"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "session": st})
}

type sessionLoadBody struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	// Meta: client-supplied session seeds (permission-mode flags, the
	// TUI's yoloMode/autoMode analog) forwarded as the session/load
	// params `_meta`. Omitted = current behavior exactly.
	Meta map[string]any `json:"meta,omitempty"`
}

// handleSessionLoad switches the active session to a historical one
// (session/load), so subsequent prompts continue that conversation.
func (s *Server) handleSessionLoad(w http.ResponseWriter, r *http.Request) {
	var body sessionLoadBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	sessRes, err := s.bridge.LoadSession(r.Context(), body.SessionID, body.Cwd, body.Meta)
	if err != nil {
		writeAgentError(w, "session/load", err)
		return
	}
	// Echo models / busy so the FE can update caption + spinner even if the
	// SSE ready/busy events race with historyLoading.
	out := map[string]any{"ok": true, "sessionId": body.SessionID}
	if sessRes != nil {
		if m, ok := sessRes["models"]; ok && m != nil {
			out["models"] = m
		}
		if modes, ok := sessRes["modes"]; ok && modes != nil {
			out["modes"] = modes
		}
		if co, ok := sessRes["configOptions"]; ok && co != nil {
			out["configOptions"] = co
		}
		if busy, ok := sessRes["busy"].(bool); ok {
			out["busy"] = busy
		}
	}
	writeJSON(w, 200, out)
}

type sessionUpdatesBody struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	Offset    *int64 `json:"offset"`
	Limit     *int   `json:"limit"`
	// Optional x.ai/session/updates request fields (camelCase on the wire):
	// stream delivers updates as chunked notifications, chunkSize sets the
	// per-chunk size (default 64), turnIndex tails by user-message turn
	// count. Absent = the existing offset/limit behavior exactly.
	Stream    bool `json:"stream,omitempty"`
	ChunkSize *int `json:"chunkSize,omitempty"`
	TurnIndex *int `json:"turnIndex,omitempty"`
}

// handleSessionUpdates fetches a session's stored updates (message history)
// and returns them as raw envelopes. The frontend replays them locally
// (deterministic ordering, no SSE broadcast drops).
func (s *Server) handleSessionUpdates(w http.ResponseWriter, r *http.Request) {
	var body sessionUpdatesBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	opts := acp.SessionUpdatesOpts{Offset: body.Offset, Limit: body.Limit, Stream: body.Stream}
	if body.ChunkSize != nil {
		opts.ChunkSize = body.ChunkSize
	}
	if body.TurnIndex != nil {
		opts.TurnIndex = body.TurnIndex
	}
	page, err := s.bridge.SessionUpdates(r.Context(), body.SessionID, body.Cwd, opts)
	if err != nil {
		writeAgentError(w, "session/updates", err)
		return
	}
	out := map[string]any{
		"ok":         true,
		"sessionId":  body.SessionID,
		"totalCount": page.TotalCount,
		"hasMore":    page.HasMore,
		"updates":    page.Updates,
	}
	// promptStarts: pass through so FE can page history one user-turn at a
	// time (see chat.ts previousTurnWindow). Omit when empty — older agents
	// and pure offset/limit pages never set it.
	if len(page.PromptStarts) > 0 {
		out["promptStarts"] = page.PromptStarts
	}
	if body.Stream {
		// stream=true: the agent does not return the updates in this
		// response — the real data arrives as session_updates_chunk SSE
		// notifications. Mark the response so the FE knows this is the
		// stream ack (updates is empty by design); totalCount/hasMore are
		// still meaningful from the stream start.
		out["streamed"] = true
	}
	writeJSON(w, 200, out)
}

// handleSessionRunningTasks returns the session's STILL-RUNNING tasks
// (task_backgrounded orphans whose output log was written recently) — the
// web equivalent of the TUI's live tasks pane. The persisted timeline is
// only used to surface current work, not to replay history.
func (s *Server) handleSessionRunningTasks(w http.ResponseWriter, r *http.Request) {
	var body sessionUpdatesBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	events, err := s.bridge.SessionRunningTasks(body.SessionID, body.Cwd)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":        true,
		"sessionId": body.SessionID,
		"events":    events,
	})
}

// handleGitInfo returns the git branch/worktree state for a session cwd
// (x.ai/git/info + local worktree probe), so the frontend can show the
// status-bar branch without waiting for a git_head_changed notification.
type gitInfoBody struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
}

func (s *Server) handleGitInfo(w http.ResponseWriter, r *http.Request) {
	var body gitInfoBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	info, err := s.bridge.GitInfo(r.Context(), body.Cwd)
	if err != nil {
		// Non-repo / agent error → empty state, not a hard failure.
		writeJSON(w, 200, map[string]any{
			"ok": true, "branch": "", "isWorktree": false, "mainRepo": "",
		})
		return
	}
	out := map[string]any{"ok": true, "sessionId": body.SessionID}
	for k, v := range info {
		out[k] = v
	}
	writeJSON(w, 200, out)
}

// handleSessionFork forks the given session via x.ai/session/fork
// ({sessionId?} empty resolves to the active one).
type forkBody struct {
	SessionID    string `json:"sessionId,omitempty"`
	SourceCwd    string `json:"sourceCwd"`
	NewCwd       string `json:"newCwd"`
	NewSessionID string `json:"newSessionId"`
	SourceWorkDir string `json:"sourceWorkspaceDir"`
}

func (s *Server) handleSessionFork(w http.ResponseWriter, r *http.Request) {
	var body forkBody
	_ = readJSON(r, &body)
	params := map[string]any{}
	if body.SourceCwd != "" {
		params["sourceCwd"] = body.SourceCwd
	}
	if body.NewCwd != "" {
		params["newCwd"] = body.NewCwd
	}
	if body.NewSessionID != "" {
		params["newSessionId"] = body.NewSessionID
	}
	if body.SourceWorkDir != "" {
		params["sourceWorkspaceDir"] = body.SourceWorkDir
	}
	res, err := s.bridge.ForkSession(r.Context(), body.SessionID, params)
	if err != nil {
		writeAgentError(w, "session/fork", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionRename renames the given session via x.ai/session/rename
// ({sessionId?} empty resolves to the active one).
func (s *Server) handleSessionRename(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Title     string `json:"title"`
	}
	if err := readJSON(r, &body); err != nil || body.Title == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 title"})
		return
	}
	res, err := s.bridge.RenameSession(r.Context(), body.SessionID, body.Title)
	if err != nil {
		writeAgentError(w, "session/rename", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleRecap fires x.ai/recap (fire-and-forget "where was I" summary).
// {sessionId?} empty resolves to the active session.
func (s *Server) handleRecap(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Auto      bool   `json:"auto"`
	}
	_ = readJSON(r, &body)
	res, err := s.bridge.Recap(r.Context(), body.SessionID, body.Auto)
	if err != nil {
		writeAgentError(w, "session/recap", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionInfo serves the given session's details on demand
// (TUI /session-info analog) — no client-side state reconstruction.
// {sessionId?}: empty resolves to the active session.
func (s *Server) handleSessionInfo(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	_ = readJSON(r, &body)
	info := s.bridge.SessionInfo(body.SessionID)
	if info == nil {
		msg := "暂无活动会话"
		if body.SessionID != "" {
			msg = "未知会话"
		}
		writeJSON(w, 404, map[string]any{"ok": false, "error": msg})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "session": info})
}

// handleSubagentCancel cancels a subagent via x.ai/subagent/cancel
// ({sessionId?} empty resolves to the active session).
func (s *Server) handleSubagentCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID  string `json:"sessionId,omitempty"`
		SubagentID string `json:"subagentId"`
	}
	if err := readJSON(r, &body); err != nil || body.SubagentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 subagentId"})
		return
	}
	res, err := s.bridge.SubagentCancel(r.Context(), body.SessionID, body.SubagentID)
	if err != nil {
		writeAgentError(w, "x.ai/subagent/cancel", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskKill kills a background task via x.ai/task/kill
// ({sessionId?} empty resolves to the active session).
func (s *Server) handleTaskKill(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		TaskID    string `json:"taskId"`
	}
	if err := readJSON(r, &body); err != nil || body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	res, err := s.bridge.TaskKill(r.Context(), body.SessionID, body.TaskID)
	if err != nil {
		writeAgentError(w, "x.ai/task/kill", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskList lists background tasks via x.ai/task/list.
func (s *Server) handleTaskList(w http.ResponseWriter, r *http.Request) {
	res, err := s.bridge.TaskList(r.Context())
	if err != nil {
		writeAgentError(w, "x.ai/task/list", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskOutput returns one task's stdout for the FE block viewer.
// Body: { taskId } — the ACTIVE session's live registry
// (x.ai/task/list); or { taskId, sessionId, cwd } — reconstructed from
// that session's persisted timeline + on-disk log, so history-replay
// rows and top-strip restored (TUI-held) tasks get their full log
// regardless of which history page is loaded.
func (s *Server) handleTaskOutput(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TaskID    string `json:"taskId"`
		SessionID string `json:"sessionId,omitempty"`
		Cwd       string `json:"cwd,omitempty"`
	}
	if err := readJSON(r, &body); err != nil || body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	if body.SessionID != "" && body.Cwd != "" {
		tl, err := s.bridge.TaskLog(body.SessionID, body.Cwd, body.TaskID)
		if err != nil {
			if errors.Is(err, acp.ErrTaskLogNotFound) {
				writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
				return
			}
			writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "task": tl})
		return
	}
	res, err := s.bridge.TaskList(r.Context())
	if err != nil {
		writeAgentError(w, "x.ai/task/list", err)
		return
	}
	// ExtMethodResult envelope: { result: { tasks: [...] } } or flat { tasks }.
	tasks := extractTaskList(res)
	var found map[string]any
	for _, t := range tasks {
		id, _ := t["task_id"].(string)
		if id == "" {
			id, _ = t["taskId"].(string)
		}
		if id == body.TaskID {
			found = t
			break
		}
	}
	if found == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "任务不存在或已清理"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "task": found})
}

// handleSessionDelete deletes a session via x.ai/session/delete.
// Body: {sessionId, cwd} — sessionId defaults to the active session; cwd
// is accepted for symmetry with the other session endpoints but the wire
// method only carries {sessionId}.
func (s *Server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.SessionDelete(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "session/delete", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleCompact compacts a session's context via x.ai/compact_conversation.
// Body: {sessionId, note?} — sessionId defaults to the active session.
func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Note      string `json:"note"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.CompactConversation(r.Context(), body.SessionID, body.Note)
	if err != nil {
		writeAgentError(w, "session/compact", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleRewindPoints lists a session's rewindable points via
// x.ai/rewind/points. Body: {sessionId, cwd} — sessionId defaults to the
// active session. Points are normalized to {index, timestamp, summary?}
// regardless of the agent's snake_case/camelCase field names.
func (s *Server) handleRewindPoints(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.RewindPoints(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "session/rewind_points", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "points": normalizeRewindPoints(res)})
}

// handleRewindExecute rolls a session back to a rewind point via
// x.ai/rewind/execute. Body: {sessionId, targetIndex, mode?} — mode is
// optional ("all" | "conversation_only"); empty defaults to
// "conversation_only" (TUI /rewind behavior, conversation-only rollback).
func (s *Server) handleRewindExecute(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID   string `json:"sessionId"`
		TargetIndex *int   `json:"targetIndex"`
		Mode        string `json:"mode"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.TargetIndex == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 targetIndex"})
		return
	}
	if body.Mode != "" && body.Mode != "all" && body.Mode != "conversation_only" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "mode 必须是 all 或 conversation_only"})
		return
	}
	res, err := s.bridge.RewindExecute(r.Context(), body.SessionID, *body.TargetIndex, body.Mode)
	if err != nil {
		writeAgentError(w, "session/rewind", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSchedulerDelete deletes a scheduled task via x.ai/scheduler/delete.
// Body: {sessionId, taskId} — sessionId defaults to the active session.
func (s *Server) handleSchedulerDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		TaskID    string `json:"taskId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	res, err := s.bridge.SchedulerDelete(r.Context(), body.SessionID, body.TaskID)
	if err != nil {
		writeAgentError(w, "scheduler/delete", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// shellBody is POST /api/shell — local command execution, NOT routed
// through the agent.
type shellBody struct {
	Command   string `json:"command"`
	Cwd       string `json:"cwd"`
	TimeoutMs int    `json:"timeoutMs"`
}

const (
	shellDefaultTimeout = 10 * time.Second
	shellMaxTimeout     = 60 * time.Second
	// shellMaxOutput caps each captured stream (stdout and stderr alike) so
	// a runaway command cannot balloon memory; overflow truncates the
	// response and flags it via `truncated`.
	shellMaxOutput = 16 << 20 // 16 MiB
)

// cappedBuffer is a bytes.Buffer that stores at most max bytes and records
// whether any write overflowed. Writes past the cap are dropped but report
// the full length, so exec's io.Copy keeps draining the pipe and the child
// never blocks on a full OS pipe buffer.
type cappedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.buf.Len() >= c.max {
		c.overflow = true
		return len(p), nil
	}
	room := c.max - c.buf.Len()
	if len(p) > room {
		c.buf.Write(p[:room])
		c.overflow = true
		return len(p), nil
	}
	return c.buf.Write(p)
}

// handleShell runs a command locally via os/exec (sh -c) — a pure host
// facility that never touches the agent. Body: {command, cwd?, timeoutMs?}
// (cwd defaults to the active session's cwd, timeout defaults to 10s and
// is capped at 60s). Like handlePrompt, the command runs on an independent
// context, so a browser disconnect just releases the handler while the
// command keeps running; only the timeout stops it. No stdin is attached.
func (s *Server) handleShell(w http.ResponseWriter, r *http.Request) {
	var body shellBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if strings.TrimSpace(body.Command) == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "command 不能为空"})
		return
	}
	cwd := body.Cwd
	if cwd == "" {
		cwd = s.bridge.Snapshot().Cwd // active session cwd
	}
	if cwd == "" {
		wd, err := os.Getwd()
		if err == nil {
			cwd = wd
		}
	}
	timeout := shellDefaultTimeout
	if body.TimeoutMs > 0 {
		t := time.Duration(body.TimeoutMs) * time.Millisecond
		if t > shellMaxTimeout {
			t = shellMaxTimeout
		}
		timeout = t
	}

	type shellResult struct {
		exitCode  int
		stdout    string
		stderr    string
		timedOut  bool
		truncated bool
	}
	done := make(chan shellResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", body.Command)
		cmd.Dir = cwd
		// stdout and stderr share the same 16 MiB cap; stderr is folded into
		// the same truncation accounting, so `truncated` flips when either
		// stream overflows.
		stdout := &cappedBuffer{max: shellMaxOutput}
		stderr := &cappedBuffer{max: shellMaxOutput}
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		// No stdin by design.
		res := shellResult{}
		if err := cmd.Run(); err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				res.exitCode = ee.ExitCode()
			} else {
				res.exitCode = -1
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				res.timedOut = true
			}
		}
		res.stdout = stdout.buf.String()
		res.stderr = stderr.buf.String()
		res.truncated = stdout.overflow || stderr.overflow
		done <- res
	}()

	select {
	case <-r.Context().Done():
		// Client went away mid-run; the command continues in the background.
		return
	case res := <-done:
		writeJSON(w, 200, map[string]any{
			"ok":        true,
			"exitCode":  res.exitCode,
			"stdout":    res.stdout,
			"stderr":    res.stderr,
			"timedOut":  res.timedOut,
			"truncated": res.truncated,
		})
	}
}

// firstAny returns the first present value under any of the given keys.
func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

// asInt64 is the lenient int reader for JSON-decoded numbers.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	case int:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}

// normalizeRewindPoints unwraps the x.ai/rewind/points response — flat
// {points:[…]}, ExtMethodResult {result:{points:[…]}}, or the agent's raw
// snake_case {rewind_points:[…]} — and normalizes each point's field names
// (snake_case or camelCase on the wire) into {index, timestamp, summary?}.
// Points without a usable index are dropped.
func normalizeRewindPoints(res map[string]any) []map[string]any {
	if res == nil {
		return nil
	}
	inner := res
	if r, ok := res["result"].(map[string]any); ok {
		inner = r
	}
	var raw []any
	if p, ok := inner["points"].([]any); ok {
		raw = p
	} else if p, ok := inner["rewindPoints"].([]any); ok {
		raw = p
	} else if p, ok := inner["rewind_points"].([]any); ok {
		raw = p
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		idx, hasIdx := asInt64(firstAny(m, "index", "prompt_index", "target_prompt_index", "targetIndex"))
		if !hasIdx {
			continue
		}
		pt := map[string]any{"index": idx}
		if ts := firstAny(m, "timestamp", "ts", "created_at"); ts != nil {
			pt["timestamp"] = ts
		}
		if sum := firstAny(m, "summary", "description", "prompt_preview"); sum != nil {
			pt["summary"] = sum
		}
		if hfc := firstAny(m, "has_file_changes", "hasFileChanges"); hfc != nil {
			pt["hasFileChanges"] = hfc
		}
		out = append(out, pt)
	}
	return out
}

// extractTaskList unwraps x.ai/task/list response shapes into a []map.
func extractTaskList(res map[string]any) []map[string]any {
	if res == nil {
		return nil
	}
	// Nested ExtMethodResult: { result: { tasks: [...] } }
	inner := res
	if r, ok := res["result"].(map[string]any); ok {
		inner = r
	}
	raw, ok := inner["tasks"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// ── admin endpoints (billing / memory / plan / permissions / MCP) ────

// writeAgentError maps an agent-backed endpoint failure to a response.
// Three-tier contract (the frontend classifies on exactly this):
//   - host-side HTTPError          → keep its status code (404 会话不存在
//     等；goal 端点仍用 409 表示状态冲突);
//   - agent JSON-RPC rejection (RPCError) → 200 + {ok:false, error, code?} —
//     the agent is alive and answered; the FE renders an operation/turn
//     error row, never a connection error;
//   - transport failure (timeout / write error / boot failure) → 502
//     (bad gateway) — the host is fine, the upstream agent is not; the FE
//     still renders an operation error row but can tell the two apart.
//
// Unsupported-method replies keep their friendly 200 + ok:false form.
func writeAgentError(w http.ResponseWriter, method string, err error) {
	var he *acp.HTTPError
	if errors.As(err, &he) {
		writeJSON(w, he.Code, map[string]any{"ok": false, "error": he.Msg})
		return
	}
	msg := err.Error()
	if methodUnsupported(msg) {
		writeJSON(w, 200, map[string]any{"ok": false, "error": fmt.Sprintf("「%s」不受支持: %s", method, msg)})
		return
	}
	var rpcErr *acp.RPCError
	if errors.As(err, &rpcErr) {
		body := map[string]any{"ok": false, "error": msg}
		if rpcErr.Code != 0 {
			body["code"] = rpcErr.Code
		}
		writeJSON(w, 200, body)
		return
	}
	writeJSON(w, 502, map[string]any{"ok": false, "error": msg})
}

// methodUnsupported reports whether an agent error message indicates the
// wire method is not implemented (-32601 method_not_found and friends).
func methodUnsupported(msg string) bool {
	lower := strings.ToLower(msg)
	for _, pat := range []string{
		"-32601", "method not found", "unknown method", "no such method",
		"not supported", "unsupported", "not implemented",
	} {
		if strings.Contains(lower, pat) {
			return true
		}
	}
	return false
}

// normalizeMCPServers unwraps an x.ai/mcp/list reply into a []any of server
// entries, accepting three shapes: {servers:[…]} at the top level, an
// ExtMethodResult envelope {result:{servers:[…]}}, or a bare result array
// ({result:[…]}). Bare-string entries (server names) become {name: …};
// object entries pass through untouched.
func normalizeMCPServers(res map[string]any) []any {
	if res == nil {
		return nil
	}
	var raw []any
	if arr, ok := res["servers"].([]any); ok {
		raw = arr
	} else if r, ok := res["result"].(map[string]any); ok {
		if arr, ok := r["servers"].([]any); ok {
			raw = arr
		}
	} else if arr, ok := res["result"].([]any); ok {
		raw = arr
	}
	out := make([]any, 0, len(raw))
	for _, item := range raw {
		if item == nil {
			continue
		}
		if name, ok := item.(string); ok {
			out = append(out, map[string]any{"name": name})
			continue
		}
		out = append(out, item)
	}
	return out
}

// handleBilling — POST /api/billing {sessionId?} → _x.ai/billing.
// sessionId defaults to the active session.
func (s *Server) handleBilling(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.Billing(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/billing", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryFlush — POST /api/memory-flush {sessionId?} → _x.ai/memory/flush.
func (s *Server) handleMemoryFlush(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.MemoryFlush(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/flush", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryRewrite — POST /api/memory-rewrite {sessionId?, rawText,
// contextSummary?} → _x.ai/memory/rewrite. rawText is required by the
// agent's request contract (rawText + contextSummary).
func (s *Server) handleMemoryRewrite(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID      string `json:"sessionId"`
		RawText        string `json:"rawText"`
		ContextSummary string `json:"contextSummary"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RawText == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 rawText"})
		return
	}
	res, err := s.bridge.MemoryRewrite(r.Context(), body.SessionID, body.RawText, body.ContextSummary)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/rewrite", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTogglePlanMode — POST /api/toggle-plan-mode {sessionId?} →
// _x.ai/toggle_plan_mode (sent as a fire-and-forget notification; the
// agent has no request branch for it). The frontend applies its local
// desired planMode, so a bare ok is the contract.
func (s *Server) handleTogglePlanMode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if _, err := s.bridge.TogglePlanMode(r.Context(), body.SessionID); err != nil {
		writeAgentError(w, "_x.ai/toggle_plan_mode", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// handlePermissionsReset — POST /api/permissions-reset {sessionId?} →
// _x.ai/permissions/reset.
func (s *Server) handlePermissionsReset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.PermissionsReset(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/permissions/reset", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPList — GET /api/mcp/list → _x.ai/mcp/list. The agent's server
// registry is returned defensively normalized under {servers:[…]}.
func (s *Server) handleMCPList(w http.ResponseWriter, r *http.Request) {
	res, err := s.bridge.MCPList(r.Context())
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/list", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "servers": normalizeMCPServers(res)})
}

// handleMCPToggle — POST /api/mcp-toggle {name, enabled} → _x.ai/mcp/toggle.
func (s *Server) handleMCPToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Name == "" || body.Enabled == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name 和 enabled"})
		return
	}
	res, err := s.bridge.MCPToggle(r.Context(), body.Name, *body.Enabled)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/toggle", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPAdd — POST /api/mcp-add {server:{name, command, args?, env?}} →
// _x.ai/mcp/upsert. The server object is passed through verbatim — the
// agent is the authority on its schema — only server.name is validated
// host-side.
func (s *Server) handleMCPAdd(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Server map[string]any `json:"server"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Server == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 server"})
		return
	}
	if name, _ := body.Server["name"].(string); name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 server.name"})
		return
	}
	res, err := s.bridge.MCPUpsert(r.Context(), body.Server)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/upsert", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPRemove — POST /api/mcp-remove {name} → _x.ai/mcp/delete.
func (s *Server) handleMCPRemove(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &body); err != nil || body.Name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	res, err := s.bridge.MCPDelete(r.Context(), body.Name)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/delete", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPAuthTrigger — POST /api/mcp-auth-trigger {name} →
// _x.ai/mcp/auth_trigger.
func (s *Server) handleMCPAuthTrigger(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &body); err != nil || body.Name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	res, err := s.bridge.MCPAuthTrigger(r.Context(), body.Name)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/auth_trigger", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleExtensions — GET /api/extensions: the LOCAL extension inventory
// (~/.grok on disk), never routed through the agent. Missing/unreadable
// paths are skipped silently; the response is always 200 with
// {hooks:[], plugins:[], skills:[]}.
func (s *Server) handleExtensions(w http.ResponseWriter, r *http.Request) {
	home, err := os.UserHomeDir()
	if err != nil {
		writeJSON(w, 200, map[string]any{"hooks": []any{}, "plugins": []any{}, "skills": []any{}})
		return
	}
	writeJSON(w, 200, scanExtensions(home))
}

// handleSettings — GET /api/settings: the SAFE, read-only subset of
// ~/.grok/config.toml ({ui, session, models, cli} sections, scalar values
// only). The file is never written; a missing file yields {}.
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	home, err := os.UserHomeDir()
	if err != nil {
		writeJSON(w, 200, map[string]any{})
		return
	}
	sections := parseConfigTOML(filepath.Join(home, ".grok", "config.toml"))
	out := map[string]any{}
	for _, name := range []string{"ui", "session", "models", "cli"} {
		if kv, ok := sections[name]; ok && len(kv) > 0 {
			out[name] = kv
		}
	}
	writeJSON(w, 200, out)
}
