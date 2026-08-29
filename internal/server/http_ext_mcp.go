package server

import (
	"net/http"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// http_ext_mcp.go — MCP 工具直通端点（读资源/认证/安装/开关/调用）。

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
	s.xaiCall(w, r, "x.ai/mcp/auth_status", sessionKey(acp.WireSessionIDS, body.SessionID))
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

// registerExtMCPRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtMCPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/mcp/read-resource", s.handleMCPReadResource)
	mux.HandleFunc("POST /api/mcp/auth-status", s.handleMCPAuthStatus)
	mux.HandleFunc("POST /api/mcp/setup", s.handleMCPSetup)
	mux.HandleFunc("POST /api/mcp/toggle-tool", s.handleMCPToggleTool)
	mux.HandleFunc("POST /api/mcp/call", s.handleMCPCall)
}
