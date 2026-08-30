package server

import (
	"net/http"
)

// http_ext_fs.go — 文件系统与内容检索端点（list/read/write/delete、content/fuzzy search）。

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

// registerExtFSRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtFSRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/fs/list", s.handleFSList)
	mux.HandleFunc("POST /api/fs/read-file", s.handleFSReadFile)
	mux.HandleFunc("POST /api/fs/exists", s.handleFSExists)
	mux.HandleFunc("POST /api/search/content", s.handleSearchContent)
	mux.HandleFunc("POST /api/fs/write-file", s.handleFSWriteFile)
	mux.HandleFunc("POST /api/fs/delete-file", s.handleFSDeleteFile)
	mux.HandleFunc("POST /api/search/fuzzy/open", s.handleSearchFuzzyOpen)
	mux.HandleFunc("POST /api/search/fuzzy/change", s.handleSearchFuzzyChange)
	mux.HandleFunc("POST /api/search/fuzzy/close", s.handleSearchFuzzyClose)
}
