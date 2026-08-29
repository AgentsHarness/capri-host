package server

import (
	"net/http"
)

// ─────────────────────────────────────────────────────────────────────
// http_ext.go — x.ai 扩展直通的公共层。
//
// 域文件索引：http_ext_{session,queue,git,code,mcp,ecosystem,auth,terminal,
// fs,cloud,misc}.go 各自持有一组 typed 端点与其 register*Routes；公共约定：
// 全部 POST + JSON；错误统一经 writeAgentError 映射（宿主侧 HTTPError 保留
// 状态码，agent 侧失败降级为 200 {ok:false, error}）；成功统一应答
// {ok:true, result:<agent 原始 result>}；需要活动会话的端点在 params 里放
// "sessionId": ""（snake 轨道用 "session_id": ""），由 bridge.XaiCall 填入
// 活动会话，无会话时 404 透传。
// ─────────────────────────────────────────────────────────────────────

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
// 队列端点（x.ai/queue/*）在 grok 侧是 ext_notification 型：经
// bridge.XaiNotify 以 _x.ai/queue/* 通知（无 JSON-RPC id）发送，端点即写
// 即回 {ok:true}；权威队列状态经 x.ai/queue/changed 广播回传（见
// handleQueue* 各端点）。

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

// registerExtRoutes 注册公共直通端点（自由透传入口）。
func (s *Server) registerExtRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/xai-call", s.handleXaiCall)
}
