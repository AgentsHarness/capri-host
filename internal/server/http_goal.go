package server

import (
	"net/http"
)

// ── /api/goal/* — host-side goal engine control (TUI /goal parity) ──
// The ACP wire has no goal control methods; these endpoints drive the
// host's GoalEngine directly (see internal/acp/goal.go). Each returns
// {ok:true, goal:<state|null>} where state mirrors the TUI's
// goal_updated payload so the frontend renders it unchanged.

// handleGoalSet — POST /api/goal/set {objective, tokenBudget?, sessionId?}
// → start (or restart) an autonomous goal bound to the given session
// (empty sessionId resolves to the active one). The first turn is fired
// automatically by the host with the goal instruction injected.
func (s *Server) handleGoalSet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Objective   string `json:"objective"`
		TokenBudget int64  `json:"tokenBudget"`
		SessionID   string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	state, err := s.bridge.GoalSet(r.Context(), body.SessionID, body.Objective, body.TokenBudget)
	if err != nil {
		writeAgentError(w, "goal/set", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalStatus — POST /api/goal/status {sessionId?} → current goal
// state of the given session (no agent round-trip; instant). goal is null
// when no goal is set; 404 when a goal exists on another session.
func (s *Server) handleGoalStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	state, err := s.bridge.GoalStatus(body.SessionID)
	if err != nil {
		writeAgentError(w, "goal/status", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalPause — POST /api/goal/pause {sessionId?} → pause an active
// goal on the given session.
func (s *Server) handleGoalPause(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	state, err := s.bridge.GoalPause(body.SessionID)
	if err != nil {
		writeAgentError(w, "goal/pause", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalResume — POST /api/goal/resume {sessionId?} → resume a paused
// goal on the given session.
func (s *Server) handleGoalResume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	state, err := s.bridge.GoalResume(body.SessionID)
	if err != nil {
		writeAgentError(w, "goal/resume", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalClear — POST /api/goal/clear {sessionId?} → clear the goal on
// the given session.
func (s *Server) handleGoalClear(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	state, err := s.bridge.GoalClear(body.SessionID)
	if err != nil {
		writeAgentError(w, "goal/clear", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}
