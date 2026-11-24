package server

import (
	"fmt"
	"net/http"
	"time"
)

// ── Worktree（x.ai/git/worktree/*，camelCase）────────────────────────

// handleWorktreeCreate — POST /api/git/worktree/create {sourcePath,
// worktreePath?, copyMode?, gitRef?, copyIgnoredInBackground?,
// ignoredSkipPatterns?, worktreeType?, label?} → 同名 wire 方法
// （sourcePath 必填）。sessionId：有活动会话时回落活动会话；home 空状态
// 场景没有会话，而 agent 侧 CreateWorktreeRequest.session_id 是 serde
// 必填字段——给它一个唯一占位，避免 XaiCall 把空串解析成活动会话而
// 404（该创建不需要真实会话：session_id 在 agent 侧只用于日志与进行中
// 注册表）。
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
	params := map[string]any{"sourcePath": body.SourcePath}
	sid := ""
	if info := s.bridge.SessionInfo(""); info != nil {
		sid = info.SessionID
	}
	if sid == "" {
		sid = fmt.Sprintf("nosession-%d", time.Now().UnixNano())
	}
	params["sessionId"] = sid
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
// includeAll?} → x.ai/git/worktree/list（grok 的 WorktreeListReq：
// repo? / type?（小写数组键）/ include_all?；双发 include_all 与 includeAll）。
func (s *Server) handleWorktreeList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Repo         string   `json:"repo,omitempty"`
		Types        []string `json:"type,omitempty"`
		IncludeAll   bool     `json:"includeAll,omitempty"`
		IncludeSnake bool     `json:"include_all,omitempty"`
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
	if body.IncludeAll || body.IncludeSnake {
		params["include_all"] = true
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

// handleWorktreeDetach — POST /api/git/worktree/detach {idOrPath, allowCopy?}
// → x.ai/git/worktree/detach {idOrPath, allowCopy?}。
func (s *Server) handleWorktreeDetach(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDOrPath  string `json:"idOrPath"`
		AllowCopy bool   `json:"allowCopy,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.IDOrPath == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 idOrPath"})
		return
	}
	params := map[string]any{"idOrPath": body.IDOrPath}
	if body.AllowCopy {
		params["allowCopy"] = true
	}
	s.xaiCall(w, r, "x.ai/git/worktree/detach", params)
}

// handleWorktreeSalvage — POST /api/git/worktree/salvage {idOrPath, out}
// → x.ai/git/worktree/salvage {idOrPath, out}。
func (s *Server) handleWorktreeSalvage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDOrPath string `json:"idOrPath"`
		Out      string `json:"out"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.IDOrPath == "" || body.Out == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 idOrPath 和 out"})
		return
	}
	s.xaiCall(w, r, "x.ai/git/worktree/salvage", map[string]any{
		"idOrPath": body.IDOrPath,
		"out":      body.Out,
	})
}

// handleWorktreeCleanArtifacts — POST /api/git/worktree/clean-artifacts {idOrPath}
// → x.ai/git/worktree/clean-artifacts {idOrPath}。
func (s *Server) handleWorktreeCleanArtifacts(w http.ResponseWriter, r *http.Request) {
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
	s.xaiCall(w, r, "x.ai/git/worktree/clean-artifacts", map[string]any{
		"idOrPath": body.IDOrPath,
	})
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
