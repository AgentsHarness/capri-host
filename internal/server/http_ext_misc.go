package server

import (
	"net/http"
)

// http_ext_misc.go — 能力声明、目录信任与 debug 端点。

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

// registerExtMiscRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtMiscRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/capabilities", s.handleCapabilities)
	mux.HandleFunc("POST /api/folder-trust-request", s.handleFolderTrustRequest)
	mux.HandleFunc("POST /api/debug/trigger-feedback", s.handleDebugTriggerFeedback)
	mux.HandleFunc("POST /api/debug/arm-auto-compact", s.handleDebugArmAutoCompact)
	mux.HandleFunc("POST /api/debug/agent", s.handleDebugAgent)
}
