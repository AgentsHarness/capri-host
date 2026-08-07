package server

import "net/http"

// ── x.ai 扩展直通（完整对齐）────────────────────────────────────────────
//
// 本文件实现与 grok agent 的 x.ai/* 扩展方法一一对应的 typed HTTP 端点：
// 全部 POST + JSON；错误统一经 writeAgentError 映射（宿主侧 HTTPError 保留
// 状态码，agent 侧失败降级为 200 {ok:false, error}）；成功统一应答
// {ok:true, result:<agent 原始 result>}。需要活动会话的端点在 params 里放
// "sessionId": ""（或该方法要求的 snake 键 "session_id": ""），由
// bridge.XaiCall 填入活动会话；无会话时 XaiCall 返回 404 → writeAgentError
// 透传。
//
// 队列端点（x.ai/queue/*）在 grok 侧是 ext_notification 型，本层经 XaiCall
// 以 request 型发送，结果原样返回；真实 agent 会对这类方法回 -32601
// method_not_found（宿主降级为 200 {ok:false}，不会崩）。未来如需通知型
// 可在 bridge 增加导出包装。

// xaiCall 是共享直通：调用 bridge.XaiCall 并统一应答 {ok:true, result}。
func (s *Server) xaiCall(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
	res, err := s.bridge.XaiCall(r.Context(), method, params)
	if err != nil {
		writeAgentError(w, "_"+method, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// readBody 解码请求体；失败时写出 400 并返回 false。
func readBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := readJSON(r, dst); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return false
	}
	return true
}

// gitRootParams 把 git 端点的 {cwd?} 映射为 wire params（cwd 为空则不带 gitRoot）。
func gitRootParams(cwd string) map[string]any {
	params := map[string]any{}
	if cwd != "" {
		params["gitRoot"] = cwd
	}
	return params
}

// sessionKey 生成会话 id 参数：客户端显式给的 id 优先；否则 "" 让 XaiCall
// 填活动会话（无会话时 XaiCall 返回 404 → writeAgentError 透传）。
// key 是该方法要求的 wire 键（"sessionId" 或 snake 的 "session_id"）。
func sessionKey(key, sid string) map[string]any {
	if sid != "" {
		return map[string]any{key: sid}
	}
	return map[string]any{key: ""}
}

// ── 通用直通 ──────────────────────────────────────────────────────────

// handleXaiCall — POST /api/xai-call {method, params?} → XaiCall。
// method 形如 "x.ai/foo"；params 缺省时空 map；成功返回 {ok:true, result:<原始 result>}。
func (s *Server) handleXaiCall(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Method == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 method"})
		return
	}
	params := body.Params
	if params == nil {
		params = map[string]any{}
	}
	s.xaiCall(w, r, body.Method, params)
}

// ── 官方 ACP 补齐 ─────────────────────────────────────────────────────

// handleSessionResume — POST /api/session-resume {sessionId, cwd} → ResumeSession。
// 两个字段必填（400）。
func (s *Server) handleSessionResume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	res, err := s.bridge.ResumeSession(r.Context(), body.SessionID, body.Cwd)
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

// ── Git（camelCase 键；cwd 空则不带 gitRoot）──────────────────────────

func (s *Server) handleGitStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/git/status", gitRootParams(body.Cwd))
}

func (s *Server) handleGitDiffs(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string   `json:"cwd"`
		From  string   `json:"from"`
		To    string   `json:"to"`
		Paths []string `json:"paths"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.From == "" || body.To == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 from 和 to"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["from"] = body.From
	params["to"] = body.To
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	s.xaiCall(w, r, "x.ai/git/diffs", params)
}

func (s *Server) handleGitStage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string   `json:"cwd"`
		Paths []string `json:"paths"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	s.xaiCall(w, r, "x.ai/git/stage", params)
}

func (s *Server) handleGitUnstage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string   `json:"cwd"`
		Paths []string `json:"paths"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	s.xaiCall(w, r, "x.ai/git/unstage", params)
}

func (s *Server) handleGitDiscard(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd              string   `json:"cwd"`
		Paths            []string `json:"paths"`
		IncludeUntracked *bool    `json:"includeUntracked"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	if body.IncludeUntracked != nil {
		params["includeUntracked"] = *body.IncludeUntracked
	}
	s.xaiCall(w, r, "x.ai/git/discard", params)
}

func (s *Server) handleGitCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string `json:"cwd"`
		Message string `json:"message"`
		Amend   *bool  `json:"amend"`
		Signoff *bool  `json:"signoff"`
		Push    *bool  `json:"push"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Message == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 message"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["message"] = body.Message
	if body.Amend != nil {
		params["amend"] = *body.Amend
	}
	if body.Signoff != nil {
		params["signoff"] = *body.Signoff
	}
	if body.Push != nil {
		params["push"] = *body.Push
	}
	s.xaiCall(w, r, "x.ai/git/commit", params)
}

func (s *Server) handleGitCheckout(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Branch string `json:"branch"`
		Create *bool  `json:"create"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Branch == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 branch"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["branch"] = body.Branch
	if body.Create != nil {
		params["create"] = *body.Create
	}
	s.xaiCall(w, r, "x.ai/git/checkout", params)
}

func (s *Server) handleGitCheckoutCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd          string `json:"cwd"`
		Commit       string `json:"commit"`
		StashIfDirty *bool  `json:"stashIfDirty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Commit == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 commit"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["commit"] = body.Commit
	if body.StashIfDirty != nil {
		params["stashIfDirty"] = *body.StashIfDirty
	}
	s.xaiCall(w, r, "x.ai/git/checkout_commit", params)
}

func (s *Server) handleGitStash(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd              string `json:"cwd"`
		IncludeUntracked *bool  `json:"includeUntracked"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if body.IncludeUntracked != nil {
		params["includeUntracked"] = *body.IncludeUntracked
	}
	s.xaiCall(w, r, "x.ai/git/stash", params)
}

func (s *Server) handleGitBranches(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/git/branches", gitRootParams(body.Cwd))
}

func (s *Server) handleGitCurrentCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/git/current_commit", gitRootParams(body.Cwd))
}

func (s *Server) handleGitRepoRoot(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/git/git_repo_root", gitRootParams(body.Cwd))
}

// ── 队列（grok 侧为 ext_notification 型；此处经 XaiCall 以 request 型发送，
//    结果原样返回。需活动会话：sessionId 由 XaiCall 填充，否则 404）────

func (s *Server) handleQueueRemove(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	s.xaiCall(w, r, "x.ai/queue/remove", map[string]any{"sessionId": "", "id": body.ID})
}

func (s *Server) handleQueueClear(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/queue/clear", map[string]any{"sessionId": ""})
}

// handleQueueReorder — {ids: []string}；wire 键为 orderedIds（grok 侧
// parse_queue_edit_command 的约定）。
func (s *Server) handleQueueReorder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs []string `json:"ids"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if len(body.IDs) > 0 {
		params["orderedIds"] = body.IDs
	}
	s.xaiCall(w, r, "x.ai/queue/reorder", params)
}

func (s *Server) handleQueueEdit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID      string `json:"id"`
		NewText string `json:"newText"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if body.ID != "" {
		params["id"] = body.ID
	}
	if body.NewText != "" {
		params["newText"] = body.NewText
	}
	s.xaiCall(w, r, "x.ai/queue/edit", params)
}

func (s *Server) handleQueueInterject(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID      string `json:"id"`
		NewText string `json:"newText"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if body.ID != "" {
		params["id"] = body.ID
	}
	if body.NewText != "" {
		params["newText"] = body.NewText
	}
	s.xaiCall(w, r, "x.ai/queue/interject", params)
}

// ── Skills / Plugins / Hooks / Marketplace / Workflows ────────────────

func (s *Server) handleSkillsList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	s.xaiCall(w, r, "x.ai/skills/list", params)
}

func (s *Server) handleSkillsToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
		Cwd     string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Name == "" || body.Enabled == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name 和 enabled"})
		return
	}
	params := map[string]any{"name": body.Name, "enabled": *body.Enabled}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	s.xaiCall(w, r, "x.ai/skills/toggle", params)
}

// handleSkillsAdd — {path?, cwd?} 原样透传（grok 侧 SkillsAddRequest）。
func (s *Server) handleSkillsAdd(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readBody(w, r, &body) {
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	s.xaiCall(w, r, "x.ai/skills/add", body)
}

// handleSkillsRemove — {name}；grok 侧 SkillsRemoveRequest 的 wire 键为 path，
// 这里把 name 映射为 path。
func (s *Server) handleSkillsRemove(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
		Cwd  string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	params := map[string]any{"path": body.Name}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	s.xaiCall(w, r, "x.ai/skills/remove", params)
}

func (s *Server) handleSkillsRefreshBaseline(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/skills/refresh-baseline", map[string]any{})
}

func (s *Server) handlePluginsList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/plugins/list", sessionKey("sessionId", body.SessionID))
}

// handlePluginsAction — {sessionId?, action}；action 为 grok 侧 tagged 对象
// （{type: "reload"|"install"|...}）。
func (s *Server) handlePluginsAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Action    any    `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Action == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 action"})
		return
	}
	params := sessionKey("sessionId", body.SessionID)
	params["action"] = body.Action
	s.xaiCall(w, r, "x.ai/plugins/action", params)
}

func (s *Server) handlePluginsReload(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/plugins/reload", map[string]any{})
}

func (s *Server) handleHooksList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/hooks/list", sessionKey("sessionId", body.SessionID))
}

func (s *Server) handleHooksAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Action    any    `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Action == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 action"})
		return
	}
	params := sessionKey("sessionId", body.SessionID)
	params["action"] = body.Action
	s.xaiCall(w, r, "x.ai/hooks/action", params)
}

func (s *Server) handleMarketplaceList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/marketplace/list", map[string]any{})
}

// handleMarketplaceAction — {action}（tagged 对象，如 {type:"refresh"}）；
// wire 还需要 sessionId，由 XaiCall 填活动会话。
func (s *Server) handleMarketplaceAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Action any `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Action == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 action"})
		return
	}
	s.xaiCall(w, r, "x.ai/marketplace/action", map[string]any{"sessionId": "", "action": body.Action})
}

func (s *Server) handleWorkflowsList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/workflows/list", sessionKey("sessionId", body.SessionID))
}

// ── 会话 / 历史 / 子代理 ───────────────────────────────────────────────

func (s *Server) handleSessionInfoExt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/session/info", sessionKey("sessionId", body.SessionID))
}

func (s *Server) handleSessionUsage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/session/usage", sessionKey("sessionId", body.SessionID))
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

func (s *Server) handleBtw(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Question string `json:"question"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Question == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 question"})
		return
	}
	s.xaiCall(w, r, "x.ai/btw", map[string]any{"sessionId": "", "question": body.Question})
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

func (s *Server) handleCommandsList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/commands/list", sessionKey("sessionId", body.SessionID))
}

func (s *Server) handleWorkspacesList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/workspaces/list", map[string]any{})
}

func (s *Server) handleSubagentListRunning(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/subagent/list_running", sessionKey("sessionId", body.SessionID))
}

// handleSessionShare — {sessionId?}；wire snake_case（session_id），空则填活动会话。
func (s *Server) handleSessionShare(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/share_session", sessionKey("session_id", body.SessionID))
}

// ── MCP（混合约定：read-resource/setup/call 为 camelCase wire，
//    auth-status/toggle-tool 为 snake wire，照 SPEC 表）────────────────

func (s *Server) handleMCPReadResource(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Server string `json:"server"`
		URI    string `json:"uri"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Server == "" || body.URI == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 server 和 uri"})
		return
	}
	// sessionId 可选省略：缺省走 agent 池。
	s.xaiCall(w, r, "x.ai/mcp/read_resource", map[string]any{"server": body.Server, "uri": body.URI})
}

func (s *Server) handleMCPAuthStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	// wire 键为 session_id（必填），空则填活动会话。
	s.xaiCall(w, r, "x.ai/mcp/auth_status", sessionKey("session_id", body.SessionID))
}

func (s *Server) handleMCPSetup(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServerName string            `json:"serverName"`
		Values     map[string]string `json:"values"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ServerName == "" || body.Values == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 serverName 和 values"})
		return
	}
	// camelCase wire：sessionId / serverName / values。
	s.xaiCall(w, r, "x.ai/mcp/setup", map[string]any{
		"sessionId":  "",
		"serverName": body.ServerName,
		"values":     body.Values,
	})
}

func (s *Server) handleMCPToggleTool(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServerName string `json:"serverName"`
		ToolName   string `json:"toolName"`
		Enabled    *bool  `json:"enabled"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ServerName == "" || body.ToolName == "" || body.Enabled == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 serverName、toolName 和 enabled"})
		return
	}
	// snake wire：session_id / server_name / tool_name / enabled。
	s.xaiCall(w, r, "x.ai/mcp/toggle_tool", map[string]any{
		"session_id":  "",
		"server_name": body.ServerName,
		"tool_name":   body.ToolName,
		"enabled":     *body.Enabled,
	})
}

func (s *Server) handleMCPCall(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Server    string `json:"server"`
		ServerURL string `json:"serverUrl,omitempty"`
		Tool      string `json:"tool"`
		Arguments any    `json:"arguments,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Server == "" || body.Tool == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 server 和 tool"})
		return
	}
	// camelCase wire：sessionId（可选，缺省走 agent 池）/ server / serverUrl / tool / arguments。
	params := map[string]any{"server": body.Server, "tool": body.Tool}
	if body.SessionID != "" {
		params["sessionId"] = body.SessionID
	}
	if body.ServerURL != "" {
		params["serverUrl"] = body.ServerURL
	}
	if body.Arguments != nil {
		params["arguments"] = body.Arguments
	}
	s.xaiCall(w, r, "x.ai/mcp/call", params)
}

// ── Auth / FS / 其它 ──────────────────────────────────────────────────

func (s *Server) handleAuthInfo(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/info", map[string]any{})
}

func (s *Server) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	// scope 可选项按 SPEC 省略。
	s.xaiCall(w, r, "x.ai/auth/logout", map[string]any{})
}

func (s *Server) handleAuthGetURL(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/get_url", map[string]any{})
}

func (s *Server) handleAuthSubmitCode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Code string `json:"code"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Code == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 code"})
		return
	}
	s.xaiCall(w, r, "x.ai/auth/submit_code", map[string]any{"code": body.Code})
}

// handleFSList — {path, depth?, includeHidden?, limit?, ...}（camelCase；
// path 必填，其余键原样透传）。
func (s *Server) handleFSList(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readBody(w, r, &body) {
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	if path, _ := body["path"].(string); path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	s.xaiCall(w, r, "x.ai/fs/list", body)
}

// handleFSReadFile — {path, maxBytes?, ...}（path 必填，其余键原样透传）。
func (s *Server) handleFSReadFile(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readBody(w, r, &body) {
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	if path, _ := body["path"].(string); path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	s.xaiCall(w, r, "x.ai/fs/read_file", body)
}

func (s *Server) handleFSExists(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path string `json:"path"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	s.xaiCall(w, r, "x.ai/fs/exists", map[string]any{"path": body.Path})
}

// handleCapabilities — x.ai/capabilities。grok 侧无对应请求分支（该能力经
// initialize meta 宣告），真实 agent 会回 -32601 → writeAgentError 降级
// 200 {ok:false}。
func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/capabilities", map[string]any{})
}

// handleFolderTrustRequest — x.ai/folder_trust/request。grok 侧该方法是
// agent → 客户端的反向请求，客户端 → agent 调用会回 -32601 → 降级 200 {ok:false}。
func (s *Server) handleFolderTrustRequest(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/folder_trust/request", map[string]any{})
}

func (s *Server) handleSuggest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
		Cwd  string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Text == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 text"})
		return
	}
	params := map[string]any{"text": body.Text}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	s.xaiCall(w, r, "x.ai/suggest", params)
}

func (s *Server) handleSuggestPrompt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Generation *uint64 `json:"generation"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Generation != nil {
		params["generation"] = *body.Generation
	}
	s.xaiCall(w, r, "x.ai/suggestPrompt", params)
}

func (s *Server) handlePRStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Branch string `json:"branch"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" || body.Branch == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd 和 branch"})
		return
	}
	s.xaiCall(w, r, "x.ai/pr/status", map[string]any{"cwd": body.Cwd, "branch": body.Branch})
}

// handleHunkTrackerHunks — {path?, source?}（get-hunks；sessionId 按 SPEC 省略）。
func (s *Server) handleHunkTrackerHunks(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path   string `json:"path"`
		Source string `json:"source"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Path != "" {
		params["path"] = body.Path
	}
	if body.Source != "" {
		params["source"] = body.Source
	}
	s.xaiCall(w, r, "x.ai/hunk-tracker/get-hunks", params)
}

func (s *Server) handleBundleStatus(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/bundle/status", map[string]any{})
}

func (s *Server) handleTerminalList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/terminal/list", map[string]any{})
}

// handleSearchContent — 扁平键原样透传（cwd / sessionId / 搜索参数，照
// search.rs 的 ContentSearchRequest flatten 约定）。
func (s *Server) handleSearchContent(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readBody(w, r, &body) {
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	s.xaiCall(w, r, "x.ai/search/content", body)
}

func (s *Server) handleAutoTopupRule(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auto-topup-rule", map[string]any{})
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

func (s *Server) handleCloudEnvList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/cloud/env/list", map[string]any{})
}
