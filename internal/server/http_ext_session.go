package server

import (
	"net/http"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// http_ext_session.go — 会话扩展端点：生命周期/元信息/历史摘要、插话（btw/interject/feedback）、子代理查询。

// ── 官方 ACP 补齐 ─────────────────────────────────────────────────────

// handleSessionResume — POST /api/session-resume {sessionId, cwd, meta?} →
// ResumeSession. sessionId/cwd are required (400); the optional meta map
// (client-supplied seeds) is forwarded as the session/resume params `_meta`
// (the session-load analog), omitted when absent.
func (s *Server) handleSessionResume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string         `json:"sessionId"`
		Cwd       string         `json:"cwd"`
		Meta      map[string]any `json:"meta,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	res, err := s.bridge.ResumeSession(r.Context(), body.SessionID, body.Cwd, body.Meta)
	if err != nil {
		writeAgentError(w, "session/resume", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionClose — POST /api/session-close {sessionId?} → CloseSession。
// sessionId 缺省为活动会话。
func (s *Server) handleSessionClose(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	res, err := s.bridge.CloseSession(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "session/close", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// ── 会话 / 历史 / 子代理 ───────────────────────────────────────────────

func (s *Server) handleSessionInfoExt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/session/info", sessionKey(acp.WireSessionID, body.SessionID))
}

func (s *Server) handleSessionUsage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/session/usage", sessionKey(acp.WireSessionID, body.SessionID))
}

// handleSessionSearch — {query, cwd?, limit?, offset?, includeContent?}
// （camelCase，与 grok 侧 session_search.rs 一致）。
func (s *Server) handleSessionSearch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Query          string `json:"query"`
		Cwd            string `json:"cwd"`
		Limit          *int   `json:"limit"`
		Offset         *int   `json:"offset"`
		IncludeContent *bool  `json:"includeContent"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Query == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 query"})
		return
	}
	params := map[string]any{"query": body.Query}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	if body.Limit != nil {
		params["limit"] = *body.Limit
	}
	if body.Offset != nil {
		params["offset"] = *body.Offset
	}
	if body.IncludeContent != nil {
		params["includeContent"] = *body.IncludeContent
	}
	s.xaiCall(w, r, "x.ai/session/search", params)
}

func (s *Server) handleSessionsList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/sessions/list", map[string]any{})
}

// handlePromptHistory — {cwd?, sessionId?}；wire snake_case（cwd / session_id）。
func (s *Server) handlePromptHistory(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd       string `json:"cwd"`
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	if body.SessionID != "" {
		params["session_id"] = body.SessionID
	}
	s.xaiCall(w, r, "x.ai/prompt_history", params)
}

// handleBtw — POST /api/btw {question, sessionId?} → x.ai/btw。
// sessionId 显式给出时透传（浏览器可能在别的会话上发 /btw——多标签页 /
// 恢复的历史会话）；缺省沿用 "" → XaiCall 填活动会话（向后兼容）。
func (s *Server) handleBtw(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Question  string `json:"question"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Question == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 question"})
		return
	}
	params := sessionKey(acp.WireSessionID, body.SessionID)
	params["question"] = body.Question
	s.xaiCall(w, r, "x.ai/btw", params)
}

func (s *Server) handleInterject(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Text == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 text"})
		return
	}
	s.xaiCall(w, r, "x.ai/interject", map[string]any{"sessionId": "", "text": body.Text})
}

func (s *Server) handleWorkspacesList(w http.ResponseWriter, r *http.Request) {
	// Optional x.ai/workspaces/list fields (camelCase, workspaces.rs):
	// pageSize/pageToken/query/kind — forwarded only when present (absent
	// = the existing no-arg request exactly).
	var body struct {
		PageSize  *int   `json:"pageSize,omitempty"`
		PageToken string `json:"pageToken,omitempty"`
		Query     string `json:"query,omitempty"`
		Kind      string `json:"kind,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.PageSize != nil {
		params["pageSize"] = *body.PageSize
	}
	if body.PageToken != "" {
		params["pageToken"] = body.PageToken
	}
	if body.Query != "" {
		params["query"] = body.Query
	}
	if body.Kind != "" {
		params["kind"] = body.Kind
	}
	s.xaiCall(w, r, "x.ai/workspaces/list", params)
}

func (s *Server) handleSubagentListRunning(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/subagent/list_running", sessionKey(acp.WireSessionID, body.SessionID))
}

// handleSessionShare — {sessionId?}；wire snake_case（session_id），空则填活动会话。
func (s *Server) handleSessionShare(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/share_session", sessionKey(acp.WireSessionIDS, body.SessionID))
}

// handleFeedback — {text}；wire 为 ClientFeedbackInput snake_case
// （session_id 填活动会话 + feedback_text）。
func (s *Server) handleFeedback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Text == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 text"})
		return
	}
	s.xaiCall(w, r, "x.ai/feedback", map[string]any{"session_id": "", "feedback_text": body.Text})
}

// ── 会话（x.ai/session/*）────────────────────────────────────────────

// handleSessionStateExt — POST /api/session/state {cwd} →
// x.ai/session/state {sessionId, cwd}（camelCase，两者必填；sessionId 空则
// 填活动会话）。与宿主侧 /api/session-state 不同，此端点直通 agent。
func (s *Server) handleSessionStateExt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	s.xaiCall(w, r, "x.ai/session/state", map[string]any{"sessionId": "", "cwd": body.Cwd})
}

// handleSessionImport — POST /api/session-import {cwd, state?, updates?} →
// x.ai/session/import {sessionId, cwd, state?, updates?}（camelCase）。
func (s *Server) handleSessionImport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string         `json:"cwd"`
		State   map[string]any `json:"state,omitempty"`
		Updates []any          `json:"updates,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	params := map[string]any{"sessionId": "", "cwd": body.Cwd}
	if body.State != nil {
		params["state"] = body.State
	}
	if len(body.Updates) > 0 {
		params["updates"] = body.Updates
	}
	s.xaiCall(w, r, "x.ai/session/import", params)
}

// handleSessionRepair — POST /api/session-repair {dryRun?} →
// x.ai/session/repair {sessionId, dryRun?}（camelCase；dryRun 缺省 false）。
func (s *Server) handleSessionRepair(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DryRun *bool `json:"dryRun,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if body.DryRun != nil {
		params["dryRun"] = *body.DryRun
	}
	s.xaiCall(w, r, "x.ai/session/repair", params)
}

// handleSessionUpdateMcpServers — POST /api/session-update-mcp-servers
// {mcpServers} → x.ai/session/update_mcp_servers {sessionId, mcpServers}
// （camelCase；mcpServers 为 ACP McpServer 数组）。
func (s *Server) handleSessionUpdateMcpServers(w http.ResponseWriter, r *http.Request) {
	var body struct {
		MCPServers []map[string]any `json:"mcpServers"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.MCPServers == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 mcpServers"})
		return
	}
	s.xaiCall(w, r, "x.ai/session/update_mcp_servers", map[string]any{
		"sessionId":  "",
		"mcpServers": body.MCPServers,
	})
}

// handleSessionAddLocalWorkspace — POST /api/session-add-local-workspace
// {meta?} → x.ai/session/add_local_workspace {sessionId, meta?}（camelCase）。
func (s *Server) handleSessionAddLocalWorkspace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Meta map[string]any `json:"meta,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if body.Meta != nil {
		params["meta"] = body.Meta
	}
	s.xaiCall(w, r, "x.ai/session/add_local_workspace", params)
}

// handleSessionResolveWorktreeResume — POST
// /api/session-resolve-worktree-resume {cwd} →
// x.ai/session/resolve_local_for_worktree_resume {sessionId, cwd}
// （camelCase，两者必填）。
func (s *Server) handleSessionResolveWorktreeResume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	s.xaiCall(w, r, "x.ai/session/resolve_local_for_worktree_resume", map[string]any{
		"sessionId": "",
		"cwd":       body.Cwd,
	})
}

// handleSessionRehydrate — POST /api/session-rehydrate {sourceCwd, repoRoot,
// worktreePath?} → x.ai/session/rehydrate {sessionId, sourceCwd, repoRoot,
// worktreePath?}（camelCase；sourceCwd/repoRoot 必填）。
func (s *Server) handleSessionRehydrate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceCwd    string `json:"sourceCwd"`
		RepoRoot     string `json:"repoRoot"`
		WorktreePath string `json:"worktreePath,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SourceCwd == "" || body.RepoRoot == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sourceCwd 和 repoRoot"})
		return
	}
	params := map[string]any{"sessionId": "", "sourceCwd": body.SourceCwd, "repoRoot": body.RepoRoot}
	if body.WorktreePath != "" {
		params["worktreePath"] = body.WorktreePath
	}
	s.xaiCall(w, r, "x.ai/session/rehydrate", params)
}

// handleSessionLoadHistory — POST /api/session-load-history {beforeId?} →
// x.ai/session/load_history {beforeId?}（camelCase；beforeId 为客户端持有
// 的游标，空则省略 = 第一页，grok-build chat_conversation_history.rs：
// `beforeId` → `nextBeforeId`）。gateway 型会话，不传 sessionId。成功返回
// {ok:true, result:<agent 原始 result>}，错误经 writeAgentError 降级。
func (s *Server) handleSessionLoadHistory(w http.ResponseWriter, r *http.Request) {
	var body struct {
		BeforeID string `json:"beforeId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	res, err := s.bridge.SessionLoadHistory(r.Context(), body.BeforeID)
	if err != nil {
		writeAgentError(w, "x.ai/session/load_history", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// ── 会话摘要（x.ai/session_summaries/*）───────────────────────────────

// handleSessionSummariesSessionList — POST /api/session-summaries/session-list
// {workspaceDirectory} → x.ai/session_summaries/session_list
// {workspace_directory}（SNAKE_CASE — 结构体无 rename 属性）。
func (s *Server) handleSessionSummariesSessionList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkspaceDirectory string `json:"workspaceDirectory"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.WorkspaceDirectory == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 workspaceDirectory"})
		return
	}
	s.xaiCall(w, r, "x.ai/session_summaries/session_list", map[string]any{
		"workspace_directory": body.WorkspaceDirectory,
	})
}

// handleSessionSummariesWorkspaceList — POST
// /api/session-summaries/workspace-list → x.ai/session_summaries/workspace_list（无参）。
func (s *Server) handleSessionSummariesWorkspaceList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/session_summaries/workspace_list", map[string]any{})
}

// handleSessionSummariesWorkspaceListRecent — POST
// /api/session-summaries/workspace-list-recent {limit} →
// x.ai/session_summaries/workspace_list_recent {limit}（必填）。
func (s *Server) handleSessionSummariesWorkspaceListRecent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Limit *int `json:"limit"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Limit == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 limit"})
		return
	}
	s.xaiCall(w, r, "x.ai/session_summaries/workspace_list_recent", map[string]any{
		"limit": *body.Limit,
	})
}

// ── 子代理（x.ai/subagent/get）───────────────────────────────────────

// handleSubagentGet — POST /api/subagent/get {subagentId, block?,
// timeoutMs?} → x.ai/subagent/get（camelCase；无 sessionId 字段；
// subagentId 必填）。
func (s *Server) handleSubagentGet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SubagentID string  `json:"subagentId"`
		Block      *bool   `json:"block,omitempty"`
		TimeoutMs  *uint64 `json:"timeoutMs,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SubagentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 subagentId"})
		return
	}
	params := map[string]any{"subagentId": body.SubagentID}
	if body.Block != nil {
		params["block"] = *body.Block
	}
	if body.TimeoutMs != nil {
		params["timeoutMs"] = *body.TimeoutMs
	}
	s.xaiCall(w, r, "x.ai/subagent/get", params)
}

// registerExtSessionRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtSessionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/session-resume", s.handleSessionResume)
	mux.HandleFunc("POST /api/session-close", s.handleSessionClose)
	mux.HandleFunc("POST /api/session-load-history", s.handleSessionLoadHistory)
	mux.HandleFunc("POST /api/session/info", s.handleSessionInfoExt)
	mux.HandleFunc("POST /api/session/usage", s.handleSessionUsage)
	mux.HandleFunc("POST /api/session/search", s.handleSessionSearch)
	mux.HandleFunc("POST /api/sessions/list", s.handleSessionsList)
	mux.HandleFunc("POST /api/prompt-history", s.handlePromptHistory)
	mux.HandleFunc("POST /api/btw", s.handleBtw)
	mux.HandleFunc("POST /api/interject", s.handleInterject)
	mux.HandleFunc("POST /api/workspaces/list", s.handleWorkspacesList)
	mux.HandleFunc("POST /api/subagent/list-running", s.handleSubagentListRunning)
	mux.HandleFunc("POST /api/session/share", s.handleSessionShare)
	mux.HandleFunc("POST /api/feedback", s.handleFeedback)
	// ── x.ai 扩展直通（会话域，原第二批端点）──
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
	mux.HandleFunc("POST /api/subagent/get", s.handleSubagentGet)
}
