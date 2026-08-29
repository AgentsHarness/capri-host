package server

import (
	"net/http"
)

// http_ext_terminal.go — 终端与 PTY 端点（创建/输出/等待/PTY 流）。

func (s *Server) handleTerminalList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/terminal/list", map[string]any{})
}

// 本域端点与 grok agent 的 x.ai/* 扩展方法一一对应（wire 键均在 grok
// 源码中逐字段核验过，见各 handler 注释）。公共约定见 http_ext.go：
// POST + JSON；readBody 解码（失败 400）；s.xaiCall 直通并统一应答
// {ok:true, result:<agent 原始 result>}；sessionId 为 "" 时由
// bridge.XaiCall 填活动会话（无会话 404）；agent 侧失败经 writeAgentError
// 降级。可选 sessionId 的方法（grok 侧 Option 字段）在客户端显式给出时
// 才转发。

// envVars converts a {name: value} map to the agent's [{name, value}]
// EnvVar array (terminal create / pty create wire shape).
func envVars(env map[string]string) []map[string]any {
	out := make([]map[string]any, 0, len(env))
	for name, value := range env {
		out = append(out, map[string]any{"name": name, "value": value})
	}
	return out
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

// registerExtTerminalRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtTerminalRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/terminal/list", s.handleTerminalList)
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
}
