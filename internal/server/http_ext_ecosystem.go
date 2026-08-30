package server

import (
	"net/http"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// http_ext_ecosystem.go — skills / plugins / hooks / marketplace / workflows / commands 扩展生态端点。

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
	s.xaiCall(w, r, "x.ai/plugins/list", sessionKey(acp.WireSessionID, body.SessionID))
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
	params := sessionKey(acp.WireSessionID, body.SessionID)
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
	s.xaiCall(w, r, "x.ai/hooks/list", sessionKey(acp.WireSessionID, body.SessionID))
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
	params := sessionKey(acp.WireSessionID, body.SessionID)
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
	s.xaiCall(w, r, "x.ai/workflows/list", sessionKey(acp.WireSessionID, body.SessionID))
}

func (s *Server) handleCommandsList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/commands/list", sessionKey(acp.WireSessionID, body.SessionID))
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

// registerExtEcosystemRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtEcosystemRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/skills/list", s.handleSkillsList)
	mux.HandleFunc("POST /api/skills/toggle", s.handleSkillsToggle)
	mux.HandleFunc("POST /api/skills/add", s.handleSkillsAdd)
	mux.HandleFunc("POST /api/skills/remove", s.handleSkillsRemove)
	mux.HandleFunc("POST /api/skills/refresh-baseline", s.handleSkillsRefreshBaseline)
	mux.HandleFunc("POST /api/plugins/list", s.handlePluginsList)
	mux.HandleFunc("POST /api/plugins/action", s.handlePluginsAction)
	mux.HandleFunc("POST /api/plugins/reload", s.handlePluginsReload)
	mux.HandleFunc("POST /api/hooks/list", s.handleHooksList)
	mux.HandleFunc("POST /api/hooks/action", s.handleHooksAction)
	mux.HandleFunc("POST /api/marketplace/list", s.handleMarketplaceList)
	mux.HandleFunc("POST /api/marketplace/action", s.handleMarketplaceAction)
	mux.HandleFunc("POST /api/workflows/list", s.handleWorkflowsList)
	mux.HandleFunc("POST /api/commands-list", s.handleCommandsList)
	mux.HandleFunc("POST /api/skills/reset", s.handleSkillsReset)
	mux.HandleFunc("POST /api/skills/config", s.handleSkillsConfig)
	mux.HandleFunc("POST /api/plugins/notify-updates", s.handlePluginsNotifyUpdates)
}
