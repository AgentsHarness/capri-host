package server

import (
	"net/http"
)

// http_ext_git.go — git 与 worktree 端点（状态/暂存/提交/stash、worktree CRUD、PR 状态）。

// ── Git（camelCase 键；cwd 空则不带 gitRoot）──────────────────────────

func (s *Server) handleGitStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd              string `json:"cwd"`
		IncludeUntracked *bool  `json:"includeUntracked"`
	}
	if !readBody(w, r, &body) {
		return
	}
	// Agent 侧 x.ai/git/status 的 include_untracked 缺省自 2026-08-07 起为
	// false（此前为 true）；host 这里显式解析默认值并总是发送该键，保证
	// 行为不随 agent 版本漂移。默认 true 维持本端点历史语义（含 untracked）。
	includeUntracked := true
	if body.IncludeUntracked != nil {
		includeUntracked = *body.IncludeUntracked
	}
	params := gitRootParams(body.Cwd)
	params["includeUntracked"] = includeUntracked
	s.xaiCall(w, r, "x.ai/git/status", params)
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

// registerExtGitRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtGitRoutes(mux *http.ServeMux) {
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
	mux.HandleFunc("POST /api/pr/status", s.handlePRStatus)
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
}
