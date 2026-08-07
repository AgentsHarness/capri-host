package server

import "net/http"

// ── x.ai 扩展直通（完整对齐 · 第二批）────────────────────────────────
//
// 第二批 typed HTTP 端点：与 grok agent 的 x.ai/* 扩展方法一一对应（wire
// 键均在 grok 源码中逐字段核验过，见各 handler 注释）。约定与 http_ext.go
// 完全一致：POST + JSON；readBody 解码（失败 400）；s.xaiCall 直通并统一
// 应答 {ok:true, result:<agent 原始 result>}；sessionId 为 "" 时由
// bridge.XaiCall 填活动会话（无会话 404）；agent 侧失败经 writeAgentError
// 降级。需要活动会话的方法照 http_ext.go 惯例传 "sessionId": ""，可选
// sessionId 的方法（grok 侧 Option 字段）在客户端显式给出时才转发。

// envVars converts a {name: value} map to the agent's [{name, value}]
// EnvVar array (terminal create / pty create wire shape).
func envVars(env map[string]string) []map[string]any {
	out := make([]map[string]any, 0, len(env))
	for name, value := range env {
		out = append(out, map[string]any{"name": name, "value": value})
	}
	return out
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

// ── 云端沙箱（x.ai/cloud/*）──────────────────────────────────────────

// handleCloudTerminate — POST /api/cloud/terminate {sandboxId} →
// x.ai/cloud/terminate {sandbox_id}（SNAKE_CASE，必填）。
func (s *Server) handleCloudTerminate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SandboxID string `json:"sandboxId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SandboxID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sandboxId"})
		return
	}
	s.xaiCall(w, r, "x.ai/cloud/terminate", map[string]any{"sandbox_id": body.SandboxID})
}

// handleCloudEnvCreate — POST /api/cloud/env/create {name?, description?,
// repository?, defaultBranch?, containerImage?, setupScript?} →
// x.ai/cloud/env/create（SNAKE_CASE：default_branch / container_image /
// setup_script；均可选，空则省略）。
func (s *Server) handleCloudEnvCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name           string `json:"name,omitempty"`
		Description    string `json:"description,omitempty"`
		Repository     string `json:"repository,omitempty"`
		DefaultBranch  string `json:"defaultBranch,omitempty"`
		ContainerImage string `json:"containerImage,omitempty"`
		SetupScript    string `json:"setupScript,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Name != "" {
		params["name"] = body.Name
	}
	if body.Description != "" {
		params["description"] = body.Description
	}
	if body.Repository != "" {
		params["repository"] = body.Repository
	}
	if body.DefaultBranch != "" {
		params["default_branch"] = body.DefaultBranch
	}
	if body.ContainerImage != "" {
		params["container_image"] = body.ContainerImage
	}
	if body.SetupScript != "" {
		params["setup_script"] = body.SetupScript
	}
	s.xaiCall(w, r, "x.ai/cloud/env/create", params)
}

// handleCloudEnvUpdate — POST /api/cloud/env/update {environmentId, name?,
// …} → x.ai/cloud/env/update {environment_id, …}（SNAKE_CASE，environment_id
// 必填）。
func (s *Server) handleCloudEnvUpdate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		EnvironmentID  string `json:"environmentId"`
		Name           string `json:"name,omitempty"`
		Description    string `json:"description,omitempty"`
		Repository     string `json:"repository,omitempty"`
		DefaultBranch  string `json:"defaultBranch,omitempty"`
		ContainerImage string `json:"containerImage,omitempty"`
		SetupScript    string `json:"setupScript,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.EnvironmentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 environmentId"})
		return
	}
	params := map[string]any{"environment_id": body.EnvironmentID}
	if body.Name != "" {
		params["name"] = body.Name
	}
	if body.Description != "" {
		params["description"] = body.Description
	}
	if body.Repository != "" {
		params["repository"] = body.Repository
	}
	if body.DefaultBranch != "" {
		params["default_branch"] = body.DefaultBranch
	}
	if body.ContainerImage != "" {
		params["container_image"] = body.ContainerImage
	}
	if body.SetupScript != "" {
		params["setup_script"] = body.SetupScript
	}
	s.xaiCall(w, r, "x.ai/cloud/env/update", params)
}

// handleCloudEnvDelete — POST /api/cloud/env/delete {environmentId} →
// x.ai/cloud/env/delete {environment_id}（SNAKE_CASE，必填）。
func (s *Server) handleCloudEnvDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		EnvironmentID string `json:"environmentId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.EnvironmentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 environmentId"})
		return
	}
	s.xaiCall(w, r, "x.ai/cloud/env/delete", map[string]any{"environment_id": body.EnvironmentID})
}

// ── 认证（x.ai/getApiKey / setApiKey / auth/*）───────────────────────

// handleApiKeyGet — POST /api/api-key-get → x.ai/getApiKey（无参）。
func (s *Server) handleApiKeyGet(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/getApiKey", map[string]any{})
}

// handleApiKeySet — POST /api/api-key-set {key?} → x.ai/setApiKey {key?}
// （key 缺省或空串 = 清除已存 key）。
func (s *Server) handleApiKeySet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key string `json:"key,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/setApiKey", map[string]any{"key": body.Key})
}

// handleAuthGetBearerToken — POST /api/auth/get-bearer-token →
// x.ai/auth/getBearerToken（无参）。
func (s *Server) handleAuthGetBearerToken(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/getBearerToken", map[string]any{})
}

// handleAuthCancel — POST /api/auth/cancel {requestSeq?} →
// x.ai/auth/cancel {request_seq?}（SNAKE_CASE，可选；缺省取消任意进行中的
// 登录）。
func (s *Server) handleAuthCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RequestSeq *uint64 `json:"requestSeq,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.RequestSeq != nil {
		params["request_seq"] = *body.RequestSeq
	}
	s.xaiCall(w, r, "x.ai/auth/cancel", params)
}

// handleAuthCheckSubscription — POST /api/auth/check-subscription →
// x.ai/auth/check_subscription（无参）。
func (s *Server) handleAuthCheckSubscription(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/check_subscription", map[string]any{})
}

// ── 隐私 / 灰度（x.ai/privacy/*、x.ai/rollout/*）────────────────────

// handlePrivacySetCodingDataRetention — POST
// /api/privacy/set-coding-data-retention {codingDataRetentionOptOut} →
// x.ai/privacy/setCodingDataRetention {codingDataRetentionOptOut}
// （camelCase，必填 bool）。
func (s *Server) handlePrivacySetCodingDataRetention(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CodingDataRetentionOptOut *bool `json:"codingDataRetentionOptOut"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.CodingDataRetentionOptOut == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 codingDataRetentionOptOut"})
		return
	}
	s.xaiCall(w, r, "x.ai/privacy/setCodingDataRetention", map[string]any{
		"codingDataRetentionOptOut": *body.CodingDataRetentionOptOut,
	})
}

// handleRolloutSurvey — POST /api/rollout/survey {preferences, feedback} →
// x.ai/rollout/survey {sessionId, preferences, feedback}（camelCase；
// preferences/feedback 必填）。
func (s *Server) handleRolloutSurvey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Preferences []string `json:"preferences"`
		Feedback    string   `json:"feedback"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Preferences == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 preferences"})
		return
	}
	s.xaiCall(w, r, "x.ai/rollout/survey", map[string]any{
		"sessionId":   "",
		"preferences": body.Preferences,
		"feedback":    body.Feedback,
	})
}

// ── Git（x.ai/git/*，camelCase；cwd 空则不带 gitRoot）────────────────

// handleGitFiles — POST /api/git/files {cwd?, paths, version?} →
// x.ai/git/files {gitRoot?, paths, version?}（paths 必填）。
func (s *Server) handleGitFiles(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string   `json:"cwd"`
		Paths   []string `json:"paths"`
		Version string   `json:"version,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if len(body.Paths) == 0 {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 paths"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["paths"] = body.Paths
	if body.Version != "" {
		params["version"] = body.Version
	}
	s.xaiCall(w, r, "x.ai/git/files", params)
}

// handleGitStageContent — POST /api/git/stage-content {cwd?, path, content}
// → x.ai/git/stage/content {gitRoot?, path, content}（path/content 必填）。
func (s *Server) handleGitStageContent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string `json:"cwd"`
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["path"] = body.Path
	params["content"] = body.Content
	s.xaiCall(w, r, "x.ai/git/stage/content", params)
}

// handleGitCheckoutSessionHead — POST /api/git/checkout-session-head {cwd?,
// stashIfDirty?} → x.ai/git/checkout_session_head {sessionId, gitRoot?,
// stashIfDirty?}（sessionId 必填 → 填活动会话）。
func (s *Server) handleGitCheckoutSessionHead(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd          string `json:"cwd"`
		StashIfDirty *bool  `json:"stashIfDirty,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	params["sessionId"] = ""
	if body.StashIfDirty != nil {
		params["stashIfDirty"] = *body.StashIfDirty
	}
	s.xaiCall(w, r, "x.ai/git/checkout_session_head", params)
}

// ── Worktree（x.ai/git/worktree/*，camelCase）────────────────────────

// handleWorktreeCreate — POST /api/git/worktree/create {sourcePath,
// worktreePath?, copyMode?, gitRef?, copyIgnoredInBackground?,
// ignoredSkipPatterns?, worktreeType?, label?} → 同名 wire 方法
// （sessionId 必填 → 填活动会话；sourcePath 必填）。
func (s *Server) handleWorktreeCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourcePath              string   `json:"sourcePath"`
		WorktreePath            string   `json:"worktreePath,omitempty"`
		CopyMode                string   `json:"copyMode,omitempty"`
		GitRef                  string   `json:"gitRef,omitempty"`
		CopyIgnoredInBackground bool     `json:"copyIgnoredInBackground,omitempty"`
		IgnoredSkipPatterns     []string `json:"ignoredSkipPatterns,omitempty"`
		WorktreeType            string   `json:"worktreeType,omitempty"`
		Label                   string   `json:"label,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SourcePath == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sourcePath"})
		return
	}
	params := map[string]any{"sessionId": "", "sourcePath": body.SourcePath}
	if body.WorktreePath != "" {
		params["worktreePath"] = body.WorktreePath
	}
	if body.CopyMode != "" {
		params["copyMode"] = body.CopyMode
	}
	if body.GitRef != "" {
		params["gitRef"] = body.GitRef
	}
	if body.CopyIgnoredInBackground {
		params["copyIgnoredInBackground"] = true
	}
	if len(body.IgnoredSkipPatterns) > 0 {
		params["ignoredSkipPatterns"] = body.IgnoredSkipPatterns
	}
	if body.WorktreeType != "" {
		params["worktreeType"] = body.WorktreeType
	}
	if body.Label != "" {
		params["label"] = body.Label
	}
	s.xaiCall(w, r, "x.ai/git/worktree/create", params)
}

// handleWorktreeRemove — POST /api/git/worktree/remove {worktreePath?,
// idOrPath?, force?, dryRun?} → 同名 wire 方法（worktreePath/idOrPath 至少
// 其一）。
func (s *Server) handleWorktreeRemove(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorktreePath string `json:"worktreePath,omitempty"`
		IDOrPath     string `json:"idOrPath,omitempty"`
		Force        bool   `json:"force,omitempty"`
		DryRun       bool   `json:"dryRun,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.WorktreePath == "" && body.IDOrPath == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 worktreePath 或 idOrPath"})
		return
	}
	params := map[string]any{"force": body.Force, "dryRun": body.DryRun}
	if body.WorktreePath != "" {
		params["worktreePath"] = body.WorktreePath
	}
	if body.IDOrPath != "" {
		params["idOrPath"] = body.IDOrPath
	}
	s.xaiCall(w, r, "x.ai/git/worktree/remove", params)
}

// handleWorktreeApply — POST /api/git/worktree/apply {worktreePath, mode?} →
// x.ai/git/worktree/apply {sessionId, worktreePath, mode?}（worktreePath
// 必填；mode 缺省 "overwrite"）。
func (s *Server) handleWorktreeApply(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorktreePath string `json:"worktreePath"`
		Mode         string `json:"mode,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.WorktreePath == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 worktreePath"})
		return
	}
	params := map[string]any{"sessionId": "", "worktreePath": body.WorktreePath}
	if body.Mode != "" {
		params["mode"] = body.Mode
	}
	s.xaiCall(w, r, "x.ai/git/worktree/apply", params)
}

// handleWorktreeCreateFromWorktree — POST /api/git/worktree/create-from-worktree
// {sourceWorktreePath, newSessionId, copyMode?, gitRef?, worktreeType?,
// label?} → x.ai/git/worktree/create_from_worktree（无 sessionId 字段）。
func (s *Server) handleWorktreeCreateFromWorktree(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceWorktreePath string `json:"sourceWorktreePath"`
		NewSessionID       string `json:"newSessionId"`
		CopyMode           string `json:"copyMode,omitempty"`
		GitRef             string `json:"gitRef,omitempty"`
		WorktreeType       string `json:"worktreeType,omitempty"`
		Label              string `json:"label,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SourceWorktreePath == "" || body.NewSessionID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sourceWorktreePath 和 newSessionId"})
		return
	}
	params := map[string]any{
		"sourceWorktreePath": body.SourceWorktreePath,
		"newSessionId":       body.NewSessionID,
	}
	if body.CopyMode != "" {
		params["copyMode"] = body.CopyMode
	}
	if body.GitRef != "" {
		params["gitRef"] = body.GitRef
	}
	if body.WorktreeType != "" {
		params["worktreeType"] = body.WorktreeType
	}
	if body.Label != "" {
		params["label"] = body.Label
	}
	s.xaiCall(w, r, "x.ai/git/worktree/create_from_worktree", params)
}

// handleWorktreeCreateFromWorktreeSync — POST
// /api/git/worktree/create-from-worktree-sync（参数同上）→
// x.ai/git/worktree/create_from_worktree_sync（同步变体）。
func (s *Server) handleWorktreeCreateFromWorktreeSync(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceWorktreePath string `json:"sourceWorktreePath"`
		NewSessionID       string `json:"newSessionId"`
		CopyMode           string `json:"copyMode,omitempty"`
		GitRef             string `json:"gitRef,omitempty"`
		WorktreeType       string `json:"worktreeType,omitempty"`
		Label              string `json:"label,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SourceWorktreePath == "" || body.NewSessionID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sourceWorktreePath 和 newSessionId"})
		return
	}
	params := map[string]any{
		"sourceWorktreePath": body.SourceWorktreePath,
		"newSessionId":       body.NewSessionID,
	}
	if body.CopyMode != "" {
		params["copyMode"] = body.CopyMode
	}
	if body.GitRef != "" {
		params["gitRef"] = body.GitRef
	}
	if body.WorktreeType != "" {
		params["worktreeType"] = body.WorktreeType
	}
	if body.Label != "" {
		params["label"] = body.Label
	}
	s.xaiCall(w, r, "x.ai/git/worktree/create_from_worktree_sync", params)
}

// handleWorktreeResumeSession — POST /api/git/worktree/resume-session
// {sourceCwd, copyMode?, worktreeType?, restoreCode?, gitRef?} →
// x.ai/git/worktree/resume_session {sessionId, sourceCwd, …}（sessionId
// 必填 → 填活动会话；sourceCwd 必填）。
func (s *Server) handleWorktreeResumeSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceCwd    string `json:"sourceCwd"`
		CopyMode     string `json:"copyMode,omitempty"`
		WorktreeType string `json:"worktreeType,omitempty"`
		RestoreCode  *bool  `json:"restoreCode,omitempty"`
		GitRef       string `json:"gitRef,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SourceCwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sourceCwd"})
		return
	}
	params := map[string]any{"sessionId": "", "sourceCwd": body.SourceCwd}
	if body.CopyMode != "" {
		params["copyMode"] = body.CopyMode
	}
	if body.WorktreeType != "" {
		params["worktreeType"] = body.WorktreeType
	}
	if body.RestoreCode != nil {
		params["restoreCode"] = *body.RestoreCode
	}
	if body.GitRef != "" {
		params["gitRef"] = body.GitRef
	}
	s.xaiCall(w, r, "x.ai/git/worktree/resume_session", params)
}

// handleWorktreeList — POST /api/git/worktree/list {repo?, type?,
// includeAll?} → x.ai/git/worktree/list（grok 的 ListWorktreeRequest：
// repo? / type?（小写数组键）/ includeAll?；空体 = 无参请求）。
func (s *Server) handleWorktreeList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Repo       string   `json:"repo,omitempty"`
		Types      []string `json:"type,omitempty"`
		IncludeAll bool     `json:"includeAll,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Repo != "" {
		params["repo"] = body.Repo
	}
	if len(body.Types) > 0 {
		params["type"] = body.Types
	}
	if body.IncludeAll {
		params["includeAll"] = true
	}
	s.xaiCall(w, r, "x.ai/git/worktree/list", params)
}

// handleWorktreeShow — POST /api/git/worktree/show {idOrPath} →
// x.ai/git/worktree/show {idOrPath}（必填）。
func (s *Server) handleWorktreeShow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDOrPath string `json:"idOrPath"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.IDOrPath == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 idOrPath"})
		return
	}
	s.xaiCall(w, r, "x.ai/git/worktree/show", map[string]any{"idOrPath": body.IDOrPath})
}

// handleWorktreeGc — POST /api/git/worktree/gc {dryRun?, maxAge?, force?} →
// x.ai/git/worktree/gc（maxAge 为 "7d"/"24h"/"30m"/"60s" 时长串）。
func (s *Server) handleWorktreeGc(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DryRun bool   `json:"dryRun,omitempty"`
		MaxAge string `json:"maxAge,omitempty"`
		Force  bool   `json:"force,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"dryRun": body.DryRun, "force": body.Force}
	if body.MaxAge != "" {
		params["maxAge"] = body.MaxAge
	}
	s.xaiCall(w, r, "x.ai/git/worktree/gc", params)
}

// handleWorktreeDbStats — POST /api/git/worktree/db/stats →
// x.ai/git/worktree/db/stats（无参）。
func (s *Server) handleWorktreeDbStats(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/git/worktree/db/stats", map[string]any{})
}

// handleWorktreeDbRebuild — POST /api/git/worktree/db/rebuild →
// x.ai/git/worktree/db/rebuild（无参）。
func (s *Server) handleWorktreeDbRebuild(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/git/worktree/db/rebuild", map[string]any{})
}

// handleWorktreeDbPath — POST /api/git/worktree/db/path →
// x.ai/git/worktree/db/path（无参）。
func (s *Server) handleWorktreeDbPath(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/git/worktree/db/path", map[string]any{})
}

// ── Hunk tracker（x.ai/hunk-tracker/*，camelCase；sessionId 可选）──────

// hunkSessionParams forwards the optional sessionId when the client
// supplied it (grok's structs declare it Option — omitted = agent falls
// back to the active session).
func hunkSessionParams(sessionID string) map[string]any {
	if sessionID != "" {
		return map[string]any{"sessionId": sessionID}
	}
	return map[string]any{}
}

// handleHunkTrackerFiles — POST /api/hunk-tracker/files {sessionId?} →
// x.ai/hunk-tracker/get-files。
func (s *Server) handleHunkTrackerFiles(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/hunk-tracker/get-files", hunkSessionParams(body.SessionID))
}

// handleHunkTrackerFileContents — POST /api/hunk-tracker/file-contents
// {sessionId?} → x.ai/hunk-tracker/get-all-file-contents。
func (s *Server) handleHunkTrackerFileContents(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/hunk-tracker/get-all-file-contents", hunkSessionParams(body.SessionID))
}

// handleHunkTrackerSummary — POST /api/hunk-tracker/summary {sessionId?} →
// x.ai/hunk-tracker/get-summary。
func (s *Server) handleHunkTrackerSummary(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/hunk-tracker/get-summary", hunkSessionParams(body.SessionID))
}

// handleHunkTrackerHunkAction — POST /api/hunk-tracker/hunk-action
// {sessionId?, hunkId, action} → x.ai/hunk-tracker/hunk-action
// （hunkId/action 必填；action 为 "accept"|"reject"）。
func (s *Server) handleHunkTrackerHunkAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		HunkID    string `json:"hunkId"`
		Action    string `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.HunkID == "" || body.Action == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 hunkId 和 action"})
		return
	}
	params := hunkSessionParams(body.SessionID)
	params["hunkId"] = body.HunkID
	params["action"] = body.Action
	s.xaiCall(w, r, "x.ai/hunk-tracker/hunk-action", params)
}

// handleHunkTrackerFileAction — POST /api/hunk-tracker/file-action
// {sessionId?, path, action} → x.ai/hunk-tracker/file-action。
func (s *Server) handleHunkTrackerFileAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Path      string `json:"path"`
		Action    string `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" || body.Action == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path 和 action"})
		return
	}
	params := hunkSessionParams(body.SessionID)
	params["path"] = body.Path
	params["action"] = body.Action
	s.xaiCall(w, r, "x.ai/hunk-tracker/file-action", params)
}

// handleHunkTrackerTurnAction — POST /api/hunk-tracker/turn-action
// {sessionId?, promptIndex, action} → x.ai/hunk-tracker/turn-action。
func (s *Server) handleHunkTrackerTurnAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID   string `json:"sessionId,omitempty"`
		PromptIndex *int   `json:"promptIndex"`
		Action      string `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.PromptIndex == nil || body.Action == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 promptIndex 和 action"})
		return
	}
	params := hunkSessionParams(body.SessionID)
	params["promptIndex"] = *body.PromptIndex
	params["action"] = body.Action
	s.xaiCall(w, r, "x.ai/hunk-tracker/turn-action", params)
}

// handleHunkTrackerAllAction — POST /api/hunk-tracker/all-action
// {sessionId?, action} → x.ai/hunk-tracker/all-action。
func (s *Server) handleHunkTrackerAllAction(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Action    string `json:"action"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Action == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 action"})
		return
	}
	params := hunkSessionParams(body.SessionID)
	params["action"] = body.Action
	s.xaiCall(w, r, "x.ai/hunk-tracker/all-action", params)
}

// ── 技能（x.ai/skills/reset、config）──────────────────────────────────

// handleSkillsReset — POST /api/skills/reset {cwd?} →
// x.ai/skills/reset（cwd 缺省 "."）。
func (s *Server) handleSkillsReset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	s.xaiCall(w, r, "x.ai/skills/reset", params)
}

// handleSkillsConfig — POST /api/skills/config {cwd?} →
// x.ai/skills/config（cwd 缺省 "."）。
func (s *Server) handleSkillsConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	s.xaiCall(w, r, "x.ai/skills/config", params)
}

// ── 插件（x.ai/plugins/notify-updates）───────────────────────────────

// handlePluginsNotifyUpdates — POST /api/plugins/notify-updates {sessionId?,
// updates} → x.ai/plugins/notify-updates {sessionId, updates}（camelCase；
// updates 为 (name, old_ver, new_ver) 三元组数组）。
func (s *Server) handlePluginsNotifyUpdates(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Updates   []any  `json:"updates"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Updates == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 updates"})
		return
	}
	params := map[string]any{"sessionId": "", "updates": body.Updates}
	if body.SessionID != "" {
		params["sessionId"] = body.SessionID
	}
	s.xaiCall(w, r, "x.ai/plugins/notify-updates", params)
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

// ── 终端（x.ai/terminal/*，camelCase）────────────────────────────────

// handleTerminalCreate — POST /api/terminal/create {command, args?, env?,
// cwd?, outputByteLimit?} → x.ai/terminal/create {sessionId, command, args?,
// env?, cwd?, outputByteLimit?}（sessionId 必填 → 填活动会话；command 必填；
// env 为 {name:value} 映射，wire 转 [{name,value}]）。
func (s *Server) handleTerminalCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Command         string            `json:"command"`
		Args            []string          `json:"args,omitempty"`
		Env             map[string]string `json:"env,omitempty"`
		Cwd             string            `json:"cwd,omitempty"`
		OutputByteLimit *int              `json:"outputByteLimit,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Command == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 command"})
		return
	}
	params := map[string]any{"sessionId": "", "command": body.Command}
	if len(body.Args) > 0 {
		params["args"] = body.Args
	}
	if len(body.Env) > 0 {
		params["env"] = envVars(body.Env)
	}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	if body.OutputByteLimit != nil {
		params["outputByteLimit"] = *body.OutputByteLimit
	}
	s.xaiCall(w, r, "x.ai/terminal/create", params)
}

// terminalIDParams builds the common {sessionId, terminalId} wire params
// for output / wait_for_exit / release / background (session_id REQUIRED →
// "" filled by XaiCall).
func terminalIDParams(method, terminalID string) map[string]any {
	return map[string]any{"sessionId": "", "terminalId": terminalID}
}

// handleTerminalKill — POST /api/terminal/kill {terminalId} →
// x.ai/terminal/kill {terminalId, sessionId?}（sessionId 可选，缺省省略；
// PTY 按 id 查找）。
func (s *Server) handleTerminalKill(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId"})
		return
	}
	s.xaiCall(w, r, "x.ai/terminal/kill", map[string]any{"terminalId": body.TerminalID})
}

// handleTerminalOutput — POST /api/terminal/output {terminalId} →
// x.ai/terminal/output {sessionId, terminalId}。
func (s *Server) handleTerminalOutput(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId"})
		return
	}
	s.xaiCall(w, r, "x.ai/terminal/output", terminalIDParams("x.ai/terminal/output", body.TerminalID))
}

// handleTerminalWaitForExit — POST /api/terminal/wait-for-exit {terminalId}
// → x.ai/terminal/wait_for_exit {sessionId, terminalId}。
func (s *Server) handleTerminalWaitForExit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId"})
		return
	}
	s.xaiCall(w, r, "x.ai/terminal/wait_for_exit", terminalIDParams("x.ai/terminal/wait_for_exit", body.TerminalID))
}

// handleTerminalRelease — POST /api/terminal/release {terminalId} →
// x.ai/terminal/release {sessionId, terminalId}。
func (s *Server) handleTerminalRelease(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId"})
		return
	}
	s.xaiCall(w, r, "x.ai/terminal/release", terminalIDParams("x.ai/terminal/release", body.TerminalID))
}

// handleTerminalBackground — POST /api/terminal/background {terminalId} →
// x.ai/terminal/background {sessionId, terminalId}。
func (s *Server) handleTerminalBackground(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId"})
		return
	}
	s.xaiCall(w, r, "x.ai/terminal/background", terminalIDParams("x.ai/terminal/background", body.TerminalID))
}

// handleTerminalPtyCreate — POST /api/terminal/pty/create {shell?, cwd?,
// sessionId?, env?, rows?, cols?, name?, meta?} → x.ai/terminal/pty/create
// （sessionId/_meta 可选；meta → `_meta` 透传）。
func (s *Server) handleTerminalPtyCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Shell     string            `json:"shell,omitempty"`
		Cwd       string            `json:"cwd,omitempty"`
		SessionID string            `json:"sessionId,omitempty"`
		Env       map[string]string `json:"env,omitempty"`
		Rows      *int              `json:"rows,omitempty"`
		Cols      *int              `json:"cols,omitempty"`
		Name      string            `json:"name,omitempty"`
		Meta      map[string]any    `json:"meta,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Shell != "" {
		params["shell"] = body.Shell
	}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	if body.SessionID != "" {
		params["sessionId"] = body.SessionID
	}
	if len(body.Env) > 0 {
		params["env"] = envVars(body.Env)
	}
	if body.Rows != nil {
		params["rows"] = *body.Rows
	}
	if body.Cols != nil {
		params["cols"] = *body.Cols
	}
	if body.Name != "" {
		params["name"] = body.Name
	}
	if len(body.Meta) > 0 {
		params["_meta"] = body.Meta
	}
	s.xaiCall(w, r, "x.ai/terminal/pty/create", params)
}

// handleTerminalPtyLoad — POST /api/terminal/pty/load {terminalId, meta?} →
// x.ai/terminal/pty/load {terminalId, _meta?}（terminalId 必填）。
func (s *Server) handleTerminalPtyLoad(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string         `json:"terminalId"`
		Meta       map[string]any `json:"meta,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId"})
		return
	}
	params := map[string]any{"terminalId": body.TerminalID}
	if len(body.Meta) > 0 {
		params["_meta"] = body.Meta
	}
	s.xaiCall(w, r, "x.ai/terminal/pty/load", params)
}

// handleTerminalPtyResize — POST /api/terminal/pty/resize {terminalId, rows,
// cols} → x.ai/terminal/pty/resize（三者必填）。
func (s *Server) handleTerminalPtyResize(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
		Rows       *int   `json:"rows"`
		Cols       *int   `json:"cols"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" || body.Rows == nil || body.Cols == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId、rows 和 cols"})
		return
	}
	s.xaiCall(w, r, "x.ai/terminal/pty/resize", map[string]any{
		"terminalId": body.TerminalID,
		"rows":       *body.Rows,
		"cols":       *body.Cols,
	})
}

// handleTerminalPtyInput — POST /api/terminal/pty/input {terminalId, data}
// → `_x.ai/terminal/pty/input` 通知（data 为 base64；grok 侧仅 notification
// 型，以 fire-and-forget 发送，无 JSON-RPC id）。
func (s *Server) handleTerminalPtyInput(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TerminalID string `json:"terminalId"`
		Data       string `json:"data"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.TerminalID == "" || body.Data == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 terminalId 和 data"})
		return
	}
	res, err := s.bridge.TerminalPtyInput(r.Context(), body.TerminalID, body.Data)
	if err != nil {
		writeAgentError(w, "_x.ai/terminal/pty/input", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// ── 文件系统（x.ai/fs/write_file、delete_file）────────────────────────

// handleFSWriteFile — POST /api/fs/write-file {path, content, createDirs?}
// → x.ai/fs/write_file {sessionId?, path, content, createDirs?}（camelCase；
// createDirs 缺省 true）。
func (s *Server) handleFSWriteFile(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path       string `json:"path"`
		Content    string `json:"content"`
		CreateDirs *bool  `json:"createDirs,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	params := map[string]any{"path": body.Path, "content": body.Content}
	if body.CreateDirs != nil {
		params["createDirs"] = *body.CreateDirs
	}
	s.xaiCall(w, r, "x.ai/fs/write_file", params)
}

// handleFSDeleteFile — POST /api/fs/delete-file {path} →
// x.ai/fs/delete_file {sessionId?, path}（path 必填）。
func (s *Server) handleFSDeleteFile(w http.ResponseWriter, r *http.Request) {
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
	s.xaiCall(w, r, "x.ai/fs/delete_file", map[string]any{"path": body.Path})
}

// ── 搜索（x.ai/search/fuzzy/*，camelCase）────────────────────────────

// handleSearchFuzzyOpen — POST /api/search/fuzzy/open {cwd?, root?,
// requestId?, hidden?, meta?} → x.ai/search/fuzzy/open {sessionId?, cwd?,
// root?, requestId?, hidden?, _meta?}（sessionId/_meta 可选，仅显式给出时
// 转发）。
func (s *Server) handleSearchFuzzyOpen(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string         `json:"sessionId,omitempty"`
		Cwd       string         `json:"cwd,omitempty"`
		Root      string         `json:"root,omitempty"`
		RequestID string         `json:"requestId,omitempty"`
		Hidden    bool           `json:"hidden,omitempty"`
		Meta      map[string]any `json:"meta,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.SessionID != "" {
		params["sessionId"] = body.SessionID
	}
	if body.Cwd != "" {
		params["cwd"] = body.Cwd
	}
	if body.Root != "" {
		params["root"] = body.Root
	}
	if body.RequestID != "" {
		params["requestId"] = body.RequestID
	}
	if body.Hidden {
		params["hidden"] = true
	}
	if len(body.Meta) > 0 {
		params["_meta"] = body.Meta
	}
	s.xaiCall(w, r, "x.ai/search/fuzzy/open", params)
}

// handleSearchFuzzyChange — POST /api/search/fuzzy/change {searchId, query,
// dirsOnly?, limit?} → x.ai/search/fuzzy/change（searchId/query 必填）。
func (s *Server) handleSearchFuzzyChange(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SearchID string `json:"searchId"`
		Query    string `json:"query"`
		DirsOnly bool   `json:"dirsOnly,omitempty"`
		Limit    *int   `json:"limit,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SearchID == "" || body.Query == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 searchId 和 query"})
		return
	}
	params := map[string]any{"searchId": body.SearchID, "query": body.Query, "dirsOnly": body.DirsOnly}
	if body.Limit != nil {
		params["limit"] = *body.Limit
	}
	s.xaiCall(w, r, "x.ai/search/fuzzy/change", params)
}

// handleSearchFuzzyClose — POST /api/search/fuzzy/close {searchId} →
// x.ai/search/fuzzy/close（searchId 必填）。
func (s *Server) handleSearchFuzzyClose(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SearchID string `json:"searchId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SearchID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 searchId"})
		return
	}
	s.xaiCall(w, r, "x.ai/search/fuzzy/close", map[string]any{"searchId": body.SearchID})
}

// ── Bundle（x.ai/bundle/sync、entry/get）─────────────────────────────

// handleBundleSync — POST /api/bundle/sync {force?} →
// x.ai/bundle/sync {force?}（force 缺省 false）。
func (s *Server) handleBundleSync(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Force bool `json:"force,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Force {
		params["force"] = true
	}
	s.xaiCall(w, r, "x.ai/bundle/sync", params)
}

// handleBundleEntryGet — POST /api/bundle/entry-get {kind, name} →
// x.ai/bundle/entry/get（kind/name 必填）。
func (s *Server) handleBundleEntryGet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Kind == "" || body.Name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 kind 和 name"})
		return
	}
	s.xaiCall(w, r, "x.ai/bundle/entry/get", map[string]any{"kind": body.Kind, "name": body.Name})
}

// ── 代码导航（x.ai/code/*，camelCase）────────────────────────────────

// codeNavParams builds the common {sessionId, cwd?} prefix for the code-nav
// methods (sessionId REQUIRED for eligibility → "" filled by XaiCall).
func codeNavParams(cwd string) map[string]any {
	params := map[string]any{"sessionId": ""}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return params
}

// handleCodeGotoDefinition — POST /api/code/goto-definition {cwd?, path, row,
// column} → x.ai/code/goto-definition {sessionId, cwd?, path, row, column}
// （row/column 1 起始）。
func (s *Server) handleCodeGotoDefinition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd,omitempty"`
		Path   string `json:"path"`
		Row    *int   `json:"row"`
		Column *int   `json:"column"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" || body.Row == nil || body.Column == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path、row 和 column"})
		return
	}
	params := codeNavParams(body.Cwd)
	params["path"] = body.Path
	params["row"] = *body.Row
	params["column"] = *body.Column
	s.xaiCall(w, r, "x.ai/code/goto-definition", params)
}

// handleCodeGotoReferences — POST /api/code/goto-references {cwd?, path, row,
// column} → x.ai/code/goto-references（参数同 goto-definition）。
func (s *Server) handleCodeGotoReferences(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd,omitempty"`
		Path   string `json:"path"`
		Row    *int   `json:"row"`
		Column *int   `json:"column"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" || body.Row == nil || body.Column == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path、row 和 column"})
		return
	}
	params := codeNavParams(body.Cwd)
	params["path"] = body.Path
	params["row"] = *body.Row
	params["column"] = *body.Column
	s.xaiCall(w, r, "x.ai/code/goto-references", params)
}

// handleCodeFindDefinitions — POST /api/code/find-definitions {cwd?, symbol,
// contextPath?} → x.ai/code/find-definitions {sessionId, cwd?, symbol,
// contextPath?}（symbol 必填）。
func (s *Server) handleCodeFindDefinitions(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd         string `json:"cwd,omitempty"`
		Symbol      string `json:"symbol"`
		ContextPath string `json:"contextPath,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Symbol == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 symbol"})
		return
	}
	params := codeNavParams(body.Cwd)
	params["symbol"] = body.Symbol
	if body.ContextPath != "" {
		params["contextPath"] = body.ContextPath
	}
	s.xaiCall(w, r, "x.ai/code/find-definitions", params)
}

// handleCodeFindReferences — POST /api/code/find-references {cwd?, symbol,
// contextPath?} → x.ai/code/find-references（参数同 find-definitions）。
func (s *Server) handleCodeFindReferences(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd         string `json:"cwd,omitempty"`
		Symbol      string `json:"symbol"`
		ContextPath string `json:"contextPath,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Symbol == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 symbol"})
		return
	}
	params := codeNavParams(body.Cwd)
	params["symbol"] = body.Symbol
	if body.ContextPath != "" {
		params["contextPath"] = body.ContextPath
	}
	s.xaiCall(w, r, "x.ai/code/find-references", params)
}

// handleCodeStatus — POST /api/code/status {cwd?} → x.ai/code/status
// {sessionId, cwd?}。
func (s *Server) handleCodeStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/code/status", codeNavParams(body.Cwd))
}

// ── 代码评审 / 调试（x.ai/review/*、x.ai/debug/*）─────────────────────

// handleReviewComment — POST /api/review/comment {promptIndex, comment,
// citation} → x.ai/review/comment {sessionId, promptIndex, comment,
// citation}（camelCase；citation 为 {path, startLine, endLine, text, side?}）。
func (s *Server) handleReviewComment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PromptIndex *int           `json:"promptIndex"`
		Comment     string         `json:"comment"`
		Citation    map[string]any `json:"citation"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.PromptIndex == nil || body.Citation == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 promptIndex 和 citation"})
		return
	}
	s.xaiCall(w, r, "x.ai/review/comment", map[string]any{
		"sessionId":   "",
		"promptIndex": *body.PromptIndex,
		"comment":     body.Comment,
		"citation":    body.Citation,
	})
}

// handleReviewCommentDelete — POST /api/review/comment-delete {commentId} →
// x.ai/review/comment/delete {sessionId, commentId}（commentId 必填）。
func (s *Server) handleReviewCommentDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CommentID string `json:"commentId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.CommentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 commentId"})
		return
	}
	s.xaiCall(w, r, "x.ai/review/comment/delete", map[string]any{
		"sessionId": "",
		"commentId": body.CommentID,
	})
}

// handleDebugTriggerFeedback — POST /api/debug/trigger-feedback {sessionId?,
// tier?, mode?} → x.ai/debug/trigger_feedback（tier: tier1|tier2|tier3；
// mode: thumbs|stars|text|thumbs_text|stars_text）。
func (s *Server) handleDebugTriggerFeedback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Tier      string `json:"tier,omitempty"`
		Mode      string `json:"mode,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if body.SessionID != "" {
		params["sessionId"] = body.SessionID
	}
	if body.Tier != "" {
		params["tier"] = body.Tier
	}
	if body.Mode != "" {
		params["mode"] = body.Mode
	}
	s.xaiCall(w, r, "x.ai/debug/trigger_feedback", params)
}

// handleDebugArmAutoCompact — POST /api/debug/arm-auto-compact {sessionId?}
// → x.ai/debug/arm_auto_compact（sessionId 必填 → 填活动会话）。
func (s *Server) handleDebugArmAutoCompact(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": ""}
	if body.SessionID != "" {
		params["sessionId"] = body.SessionID
	}
	s.xaiCall(w, r, "x.ai/debug/arm_auto_compact", params)
}

// handleDebugAgent — POST /api/debug/agent → x.ai/debug/agent（无参）。
func (s *Server) handleDebugAgent(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/debug/agent", map[string]any{})
}
