package server

import (
	"net/http"
)

// ── /api/goal/* — host-side goal engine control (TUI /goal parity) ──
// The ACP wire has no goal control methods; these endpoints drive the
// host's GoalEngine directly (see internal/acp/goal.go). Each returns
// {ok:true, goal:<state|null>} where state mirrors the TUI's
// goal_updated payload so the frontend renders it unchanged.

// handleGoalSet — POST /api/goal/set {objective, tokenBudget?} → start
// (or restart) an autonomous goal on the active session. The first turn
// is fired automatically by the host with the goal instruction injected.
func (s *Server) handleGoalSet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Objective   string `json:"objective"`
		TokenBudget int64  `json:"tokenBudget"`
	}
	if !readBody(w, r, &body) {
		return
	}
	state, err := s.bridge.GoalSet(r.Context(), body.Objective, body.TokenBudget)
	if err != nil {
		writeAgentError(w, "goal/set", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalStatus — POST /api/goal/status → current goal state (no
// agent round-trip; instant). goal is null when no goal is set.
func (s *Server) handleGoalStatus(w http.ResponseWriter, r *http.Request) {
	state := s.bridge.GoalStatus()
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalPause — POST /api/goal/pause → pause an active goal.
func (s *Server) handleGoalPause(w http.ResponseWriter, r *http.Request) {
	state, err := s.bridge.GoalPause()
	if err != nil {
		writeAgentError(w, "goal/pause", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalResume — POST /api/goal/resume → resume a paused goal.
func (s *Server) handleGoalResume(w http.ResponseWriter, r *http.Request) {
	state, err := s.bridge.GoalResume()
	if err != nil {
		writeAgentError(w, "goal/resume", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}

// handleGoalClear — POST /api/goal/clear → clear the goal.
func (s *Server) handleGoalClear(w http.ResponseWriter, r *http.Request) {
	state, err := s.bridge.GoalClear()
	if err != nil {
		writeAgentError(w, "goal/clear", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "goal": state})
}
