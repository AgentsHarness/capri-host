package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ── POST /api/goal/* — host-side goal engine ─────────────────────────

func TestGoalEndpointsLifecycle(t *testing.T) {
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s)

	// Subscribe to goal_updated broadcasts to verify the wire events.
	evCh, unsub := b.Subscribe()
	defer unsub()

	// set → active
	rec := postJSON(t, s, "/api/goal/set", `{"objective":"修复登录模块","tokenBudget":500000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("set status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("set resp = %s, want ok:true", rec.Body.String())
	}
	g, _ := m["goal"].(map[string]any)
	if g["status"] != "active" || g["objective"] != "修复登录模块" {
		t.Fatalf("set goal = %v, want active with objective", g)
	}
	if g["token_budget"] != float64(500000) {
		t.Fatalf("set token_budget = %v, want 500000", g["token_budget"])
	}
	if g["started_at"] == nil || g["elapsed_ms"] == nil {
		t.Fatalf("set goal missing timing fields: %v", g)
	}

	// status → still active, instant (no agent round-trip)
	rec = postJSON(t, s, "/api/goal/status", `{}`)
	g = goalFrom(t, rec)
	if g["status"] != "active" {
		t.Fatalf("status goal = %v, want active", g)
	}

	// pause → user_paused
	rec = postJSON(t, s, "/api/goal/pause", `{}`)
	g = goalFrom(t, rec)
	if g["status"] != "user_paused" {
		t.Fatalf("pause goal = %v, want user_paused", g)
	}

	// pause again → 409
	rec = postJSON(t, s, "/api/goal/pause", `{}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("double-pause status = %d, body=%s", rec.Code, rec.Body.String())
	}

	// resume → active
	rec = postJSON(t, s, "/api/goal/resume", `{}`)
	g = goalFrom(t, rec)
	if g["status"] != "active" {
		t.Fatalf("resume goal = %v, want active", g)
	}

	// clear → cleared
	rec = postJSON(t, s, "/api/goal/clear", `{}`)
	g = goalFrom(t, rec)
	if g["status"] != "cleared" {
		t.Fatalf("clear goal = %v, want cleared", g)
	}

	// status after clear → still cleared
	rec = postJSON(t, s, "/api/goal/status", `{}`)
	g = goalFrom(t, rec)
	if g["status"] != "cleared" {
		t.Fatalf("status after clear = %v, want cleared", g)
	}

	// resume on cleared → 409 (not paused)
	rec = postJSON(t, s, "/api/goal/resume", `{}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("resume-cleared status = %d, body=%s", rec.Code, rec.Body.String())
	}

	// Every mutation (set/pause/resume/clear — status is a snapshot query
	// and does not broadcast) emitted a goal_updated typed event.
	deadline := time.After(2 * time.Second)
	saw := 0
	for saw < 4 {
		select {
		case ev := <-evCh:
			if ev["type"] == "goal_updated" {
				saw++
				if _, ok := ev["update"].(map[string]any); !ok {
					t.Fatalf("goal_updated without update map: %v", ev)
				}
			}
		case <-deadline:
			t.Fatalf("only saw %d goal_updated events, want >= 4", saw)
		}
	}
}

func TestGoalSetValidation(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// No active session → 404.
	rec := postJSON(t, s, "/api/goal/set", `{"objective":"x"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec.Code, rec.Body.String())
	}

	createActiveSession(t, s)

	// Empty objective → 400.
	rec = postJSON(t, s, "/api/goal/set", `{"objective":"  "}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty-objective status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if !strings.Contains(m["error"].(string), "缺少目标描述") {
		t.Fatalf("resp = %s, want 缺少目标描述", rec.Body.String())
	}

	// status with no goal → goal null (not an error).
	rec = postJSON(t, s, "/api/goal/status", `{}`)
	m = decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("empty status resp = %s, want ok:true", rec.Body.String())
	}
	if m["goal"] != nil {
		t.Fatalf("empty status goal = %v, want null", m["goal"])
	}

	// pause/clear with no goal → 404.
	for _, path := range []string{"/api/goal/pause", "/api/goal/clear", "/api/goal/resume"} {
		rec = postJSON(t, s, path, `{}`)
		if rec.Code != http.StatusNotFound && rec.Code != http.StatusConflict {
			t.Fatalf("%s no-goal status = %d, body=%s", path, rec.Code, rec.Body.String())
		}
	}
}

// goalFrom decodes a {ok, goal} response and returns the goal map.
func goalFrom(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	g, _ := m["goal"].(map[string]any)
	if g == nil {
		t.Fatalf("resp = %s, want goal map", rec.Body.String())
	}
	return g
}
