package server

import (
	"github.com/AgentsHarness/capri-host/internal/acp"
	"net/http"
)

type permBody struct {
	RequestID string `json:"requestId"`
	OptionID  string `json:"optionId"`
	Cancelled bool   `json:"cancelled"`
	// Scope and FollowupMessage are optional and forwarded to the agent
	// via the ACP response `_meta` (bash command scope / reject followup),
	// aligned with the TUI's permission dispatch.
	Scope           *acp.PermissionScope `json:"scope,omitempty"`
	FollowupMessage string               `json:"followupMessage,omitempty"`
}

func (s *Server) handlePermission(w http.ResponseWriter, r *http.Request) {
	var body permBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RequestID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 requestId"})
		return
	}
	if err := s.bridge.RespondPermissionWithMeta(body.RequestID, body.OptionID, body.Cancelled, body.Scope, body.FollowupMessage); err != nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// handleClientResponse resolves a forwarded x.ai/* request (ask_user_question,
// exit_plan_mode, …) with a raw result object or an error message.
type clientResponseBody struct {
	RequestID string         `json:"requestId"`
	Result    map[string]any `json:"result"`
	Error     string         `json:"error"`
}

func (s *Server) handleClientResponse(w http.ResponseWriter, r *http.Request) {
	var body clientResponseBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RequestID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 requestId"})
		return
	}
	if err := s.bridge.RespondClientRequest(body.RequestID, body.Result, body.Error); err != nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	var sc acp.SessionConfig
	_ = readJSON(r, &sc)
	if err := s.bridge.NewSession(r.Context(), sc); err != nil {
		writeAgentError(w, "session/new", err)
		return
	}
	snap := s.bridge.Snapshot()
	writeJSON(w, 200, map[string]any{"ok": true, "sessionId": snap.SessionID})
}

type modeBody struct {
	ModeID    string `json:"modeId"`
	SessionID string `json:"sessionId,omitempty"`
}

func (s *Server) handleSetMode(w http.ResponseWriter, r *http.Request) {
	var body modeBody
	if err := readJSON(r, &body); err != nil || body.ModeID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modeId"})
		return
	}
	// Permission modes ride the x.ai/yolo_mode_changed notification (the
	// agent's ask/auto/always-approve channel, TUI parity) — session/set_mode
	// only understands the session-mode ids and would silently no-op on
	// normal/auto/always-approve. Session-mode ids keep the legacy path.
	switch body.ModeID {
	case "normal", "auto", "always-approve", "always_approve", "yolo":
		if _, err := s.bridge.SetPermissionMode(r.Context(), body.ModeID); err != nil {
			writeAgentError(w, "set-permission-mode", err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	res, err := s.bridge.SetMode(r.Context(), body.SessionID, body.ModeID)
	if err != nil {
		writeAgentError(w, "session/set-mode", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

type setModelBody struct {
	ModelID         string `json:"modelId"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
	ContextWindow   uint64 `json:"contextWindow,omitempty"`
	SessionID       string `json:"sessionId,omitempty"`
}

func (s *Server) handleSetModel(w http.ResponseWriter, r *http.Request) {
	var body setModelBody
	if err := readJSON(r, &body); err != nil || body.ModelID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modelId"})
		return
	}
	warning, err := s.bridge.SetModel(r.Context(), body.SessionID, body.ModelID, body.ReasoningEffort, body.ContextWindow)
	if err != nil {
		writeAgentError(w, "session/set-model", err)
		return
	}
	// warning：模型已切换但档位未生效（非致命）。HTTP 仍 200，靠 body 里的
	// warning 让前端弹 toast 并回滚 caption 上的档位，而不是静默显示成功。
	out := map[string]any{"ok": true}
	if warning != "" {
		out["warning"] = warning
	}
	writeJSON(w, 200, out)
}

type setConfigOptionBody struct {
	SessionID string `json:"sessionId,omitempty"`
	ConfigID  string `json:"configId"`
	Value     string `json:"value"`
}

// handleSetConfigOption — POST /api/session/config-option {sessionId?, configId, value}
// 调用官方 ACP session/set_config_option 端点。支持 configId="model" 或 "reasoning_effort"。
func (s *Server) handleSetConfigOption(w http.ResponseWriter, r *http.Request) {
	var body setConfigOptionBody
	if err := readJSON(r, &body); err != nil || body.ConfigID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 configId"})
		return
	}
	res, err := s.bridge.SetConfigOption(r.Context(), body.SessionID, body.ConfigID, body.Value)
	if err != nil {
		writeAgentError(w, "session/set_config_option", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}
