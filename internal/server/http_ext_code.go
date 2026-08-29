package server

import (
	"net/http"
)

// http_ext_code.go — 代码智能与复核端点（goto/find、bundle 索引、hunk tracker、review 评论、suggest）。

func (s *Server) handleSuggest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text       string  `json:"text"`
		Cwd        string  `json:"cwd"`
		Cursor     *int    `json:"cursor,omitempty"`
		Limit      *int    `json:"limit,omitempty"`
		Generation *uint64 `json:"generation,omitempty"`
		IncludeAI  *bool   `json:"includeAi,omitempty"`
		AIModel    string  `json:"aiModel,omitempty"`
		TokenOnly  *bool   `json:"tokenOnly,omitempty"`
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
	// Optional x.ai/suggest fields (camelCase, suggest/mod.rs): forwarded
	// only when present (absent = the existing text/cwd request exactly).
	if body.Cursor != nil {
		params["cursor"] = *body.Cursor
	}
	if body.Limit != nil {
		params["limit"] = *body.Limit
	}
	if body.Generation != nil {
		params["generation"] = *body.Generation
	}
	if body.IncludeAI != nil {
		params["includeAi"] = *body.IncludeAI
	}
	if body.AIModel != "" {
		params["aiModel"] = body.AIModel
	}
	if body.TokenOnly != nil {
		params["tokenOnly"] = *body.TokenOnly
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

// registerExtCodeRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtCodeRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/suggest", s.handleSuggest)
	mux.HandleFunc("POST /api/suggest-prompt", s.handleSuggestPrompt)
	mux.HandleFunc("POST /api/hunk-tracker/hunks", s.handleHunkTrackerHunks)
	mux.HandleFunc("POST /api/bundle/status", s.handleBundleStatus)
	mux.HandleFunc("POST /api/hunk-tracker/files", s.handleHunkTrackerFiles)
	mux.HandleFunc("POST /api/hunk-tracker/file-contents", s.handleHunkTrackerFileContents)
	mux.HandleFunc("POST /api/hunk-tracker/summary", s.handleHunkTrackerSummary)
	mux.HandleFunc("POST /api/hunk-tracker/hunk-action", s.handleHunkTrackerHunkAction)
	mux.HandleFunc("POST /api/hunk-tracker/file-action", s.handleHunkTrackerFileAction)
	mux.HandleFunc("POST /api/hunk-tracker/turn-action", s.handleHunkTrackerTurnAction)
	mux.HandleFunc("POST /api/hunk-tracker/all-action", s.handleHunkTrackerAllAction)
	mux.HandleFunc("POST /api/bundle/sync", s.handleBundleSync)
	mux.HandleFunc("POST /api/bundle/entry-get", s.handleBundleEntryGet)
	mux.HandleFunc("POST /api/code/goto-definition", s.handleCodeGotoDefinition)
	mux.HandleFunc("POST /api/code/goto-references", s.handleCodeGotoReferences)
	mux.HandleFunc("POST /api/code/find-definitions", s.handleCodeFindDefinitions)
	mux.HandleFunc("POST /api/code/find-references", s.handleCodeFindReferences)
	mux.HandleFunc("POST /api/code/status", s.handleCodeStatus)
	mux.HandleFunc("POST /api/review/comment", s.handleReviewComment)
	mux.HandleFunc("POST /api/review/comment-delete", s.handleReviewCommentDelete)
}
