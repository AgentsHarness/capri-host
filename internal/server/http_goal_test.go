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

// TestGoalSessionBinding — the goal is a host-local single goal BOUND to
// the session it was set on (the sessionId it resolved to). status/pause/
// resume/clear for another session are 404; /goal set on another session
// replaces the goal and re-binds it to that session.
func TestGoalSessionBinding(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	sid := createActiveSession(t, s) // fake agent answers session/new with "sess-new"
	sidJSON := `"sessionId":"` + sid + `"`

	// set with the explicit sessionId → the goal binds to that session.
	rec := postJSON(t, s, "/api/goal/set", `{"objective":"修复登录模块",`+sidJSON+`}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("set status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if g := goalFrom(t, rec); g["status"] != "active" {
		t.Fatalf("set goal = %v, want active", g)
	}

	// The bound sessionId sees the goal…
	rec = postJSON(t, s, "/api/goal/status", `{`+sidJSON+`}`)
	if g := goalFrom(t, rec); g["status"] != "active" {
		t.Fatalf("status(bound sid) goal = %v, want active", g)
	}

	// …another sessionId does not → 404 当前会话没有目标.
	rec = postJSON(t, s, "/api/goal/status", `{"sessionId":"other-sess"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status(other sid) code = %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); !strings.Contains(m["error"].(string), "当前会话没有目标") {
		t.Fatalf("resp = %s, want 当前会话没有目标", rec.Body.String())
	}

	// pause/resume/clear on the wrong session are 404 and leave the goal
	// untouched on the bound one.
	for _, path := range []string{"/api/goal/pause", "/api/goal/resume", "/api/goal/clear"} {
		rec = postJSON(t, s, path, `{"sessionId":"other-sess"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s(other sid) code = %d, want 404, body=%s", path, rec.Code, rec.Body.String())
		}
	}
	if g := goalFrom(t, postJSON(t, s, "/api/goal/status", `{`+sidJSON+`}`)); g["status"] != "active" {
		t.Fatalf("goal must survive wrong-session ops, status = %v", g["status"])
	}

	// pause on the bound session works.
	if g := goalFrom(t, postJSON(t, s, "/api/goal/pause", `{`+sidJSON+`}`)); g["status"] != "user_paused" {
		t.Fatalf("pause(bound sid) goal = %v, want user_paused", g)
	}

	// /goal set on another session replaces the goal and re-binds it.
	rec = postJSON(t, s, "/api/goal/set", `{"objective":"新目标","sessionId":"other-sess"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("re-set status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if g := goalFrom(t, rec); g["objective"] != "新目标" {
		t.Fatalf("re-set goal = %v, want 新目标", g)
	}
	rec = postJSON(t, s, "/api/goal/status", `{`+sidJSON+`}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status(old sid) after re-bind = %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
	if g := goalFrom(t, postJSON(t, s, "/api/goal/status", `{"sessionId":"other-sess"}`)); g["status"] != "active" {
		t.Fatalf("status(new sid) = %v, want active", g["status"])
	}

	// clear on the new bound session.
	if g := goalFrom(t, postJSON(t, s, "/api/goal/clear", `{"sessionId":"other-sess"}`)); g["status"] != "cleared" {
		t.Fatalf("clear(new sid) = %v, want cleared", g["status"])
	}

	// The cleared goal is still bound to other-sess (status keeps showing
	// the cleared state on that session, mirroring the no-sessionId flow) —
	// the active session's view stays a 404.
	if g := goalFrom(t, postJSON(t, s, "/api/goal/status", `{"sessionId":"other-sess"}`)); g["status"] != "cleared" {
		t.Fatalf("status(new sid) after clear = %v, want cleared", g["status"])
	}
	rec = postJSON(t, s, "/api/goal/status", `{}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("empty-sid status after clear = %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); !strings.Contains(m["error"].(string), "当前会话没有目标") {
		t.Fatalf("resp = %s, want 当前会话没有目标", rec.Body.String())
	}
}
