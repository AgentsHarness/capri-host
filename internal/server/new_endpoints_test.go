package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
	}
	return m
}

// ── POST /api/session-delete ────────────────────────────────────────

func TestSessionDeleteEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// Explicit sessionId → 200 {ok:true}.
	rec := postJSON(t, s, "/api/session-delete", `{"sessionId":"sess-1","cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// Missing sessionId with no active session → 404.
	rec2 := postJSON(t, s, "/api/session-delete", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
	if m := decodeBody(t, rec2); !strings.Contains(m["error"].(string), "暂无活动会话") {
		t.Fatalf("resp = %s, want 暂无活动会话", rec2.Body.String())
	}

	// Malformed body → 400.
	rec3 := postJSON(t, s, "/api/session-delete", `{`)
	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("bad-json status = %d, body=%s", rec3.Code, rec3.Body.String())
	}
}

// SessionDelete with no sessionId falls back to the active session.
func TestSessionDeleteDefaultsToActiveSession(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// Create the active session (cwd /tmp) through the real handler.
	rec := postJSON(t, s, "/api/session", `{"cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("session/new status = %d, body=%s", rec.Code, rec.Body.String())
	}

	rec2 := postJSON(t, s, "/api/session-delete", `{}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
	if m := decodeBody(t, rec2); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec2.Body.String())
	}
}

// ── POST /api/session-info ──────────────────────────────────────────

// TestSessionInfoEndpoint — {sessionId?}: empty resolves to the active
// session; an explicit unknown sessionId is a local 404.
func TestSessionInfoEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// No active session, empty sessionId → 404 暂无活动会话.
	rec := postJSON(t, s, "/api/session-info", `{}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); !strings.Contains(m["error"].(string), "暂无活动会话") {
		t.Fatalf("resp = %s, want 暂无活动会话", rec.Body.String())
	}

	// Unknown explicit sessionId → 404 未知会话.
	rec = postJSON(t, s, "/api/session-info", `{"sessionId":"nope"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown-sid status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); !strings.Contains(m["error"].(string), "未知会话") {
		t.Fatalf("resp = %s, want 未知会话", rec.Body.String())
	}

	// Active session: empty sessionId resolves to it; explicit id matches.
	sid := createActiveSession(t, s)
	for _, body := range []string{`{}`, `{"sessionId":"` + sid + `"}`} {
		rec = postJSON(t, s, "/api/session-info", body)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		m := decodeBody(t, rec)
		info, _ := m["session"].(map[string]any)
		if info == nil || info["sessionId"] != sid {
			t.Fatalf("resp = %s, want session.sessionId = %s", rec.Body.String(), sid)
		}
	}
}

// ── POST /api/compact ───────────────────────────────────────────────

func TestCompactEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/compact", `{"sessionId":"sess-1","note":"cleanup"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/compact", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/rewind-points ─────────────────────────────────────────

func TestRewindPointsEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/rewind-points", `{"sessionId":"sess-1","cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	raw, ok := m["points"].([]any)
	if !ok || len(raw) != 2 {
		t.Fatalf("points = %v, want 2 entries", m["points"])
	}
	// The fake agent returns a wrapped {result:{points:[…]}} envelope with
	// mixed snake_case/camelCase fields; the handler must normalize.
	first := raw[0].(map[string]any)
	if first["index"] != float64(0) || first["timestamp"] != float64(1000) || first["summary"] != "hello" {
		t.Errorf("point[0] = %v, want {index:0 timestamp:1000 summary:hello}", first)
	}
	second := raw[1].(map[string]any)
	if second["index"] != float64(1) || second["timestamp"] != float64(2000) {
		t.Errorf("point[1] = %v, want {index:1 timestamp:2000} (from prompt_index/ts)", second)
	}
	if _, has := second["summary"]; has {
		t.Errorf("point[1] summary = %v, want absent", second["summary"])
	}
}

func TestNormalizeRewindPoints(t *testing.T) {
	cases := []struct {
		name string
		res  map[string]any
		want []map[string]any
	}{
		{
			name: "flat camelCase",
			res:  map[string]any{"points": []any{map[string]any{"index": float64(0), "timestamp": float64(100), "summary": "s"}}},
			want: []map[string]any{{"index": int64(0), "timestamp": float64(100), "summary": "s"}},
		},
		{
			name: "wrapped snake_case",
			res: map[string]any{"result": map[string]any{"points": []any{
				map[string]any{"prompt_index": float64(1), "ts": float64(200)},
			}}},
			want: []map[string]any{{"index": int64(1), "timestamp": float64(200)}},
		},
		{
			name: "rewindPoints key and target_prompt_index",
			res:  map[string]any{"rewindPoints": []any{map[string]any{"target_prompt_index": float64(3), "timestamp": float64(300)}}},
			want: []map[string]any{{"index": int64(3), "timestamp": float64(300)}},
		},
		{
			// The real grok agent serializes RewindPointsResponse verbatim:
			// a top-level snake_case `rewind_points` key with
			// prompt_index / created_at / prompt_preview fields.
			name: "agent raw snake_case rewind_points",
			res: map[string]any{"rewind_points": []any{
				map[string]any{
					"prompt_index":       float64(2),
					"created_at":         "2026-08-07T14:00:26Z",
					"num_file_snapshots": float64(1),
					"has_file_changes":   true,
					"prompt_preview":     "删除history上方的按钮们",
				},
			}},
			want: []map[string]any{{
				"index":          int64(2),
				"timestamp":      "2026-08-07T14:00:26Z",
				"summary":        "删除history上方的按钮们",
				"hasFileChanges": true,
			}},
		},
		{
			name: "points without index dropped",
			res:  map[string]any{"points": []any{map[string]any{"timestamp": float64(1)}, "not-a-map"}},
			want: nil,
		},
		{
			name: "nil input",
			res:  nil,
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeRewindPoints(c.res)
			if len(got) != len(c.want) {
				t.Fatalf("got %d points, want %d: %+v", len(got), len(c.want), got)
			}
			for i := range got {
				for k, wantV := range c.want[i] {
					if got[i][k] != wantV {
						t.Errorf("point[%d].%s = %v, want %v (point=%+v)", i, k, got[i][k], wantV, got[i])
					}
				}
			}
		})
	}
}

// ── POST /api/rewind-execute ────────────────────────────────────────

func TestRewindExecuteEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/rewind-execute", `{"sessionId":"sess-1","targetIndex":2}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// targetIndex 0 is a legitimate rewind target — must not 400.
	rec0 := postJSON(t, s, "/api/rewind-execute", `{"sessionId":"sess-1","targetIndex":0}`)
	if rec0.Code != http.StatusOK {
		t.Fatalf("index-0 status = %d, body=%s", rec0.Code, rec0.Body.String())
	}

	// Missing targetIndex → 400.
	rec2 := postJSON(t, s, "/api/rewind-execute", `{"sessionId":"sess-1"}`)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing-index status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/scheduler-delete ──────────────────────────────────────

func TestSchedulerDeleteEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/scheduler-delete", `{"sessionId":"sess-1","taskId":"t-9"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// Missing taskId → 400.
	rec2 := postJSON(t, s, "/api/scheduler-delete", `{"sessionId":"sess-1"}`)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing-taskId status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/shell ─────────────────────────────────────────────────

func TestShellEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// Happy path: exit code 0, stdout captured, no stderr.
	rec := postJSON(t, s, "/api/shell", `{"command":"echo hello"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true || m["exitCode"] != float64(0) || m["stdout"] != "hello\n" ||
		m["stderr"] != "" || m["timedOut"] != false {
		t.Fatalf("resp = %s, want exitCode 0 stdout 'hello\\n'", rec.Body.String())
	}

	// Non-zero exit code is reported, not an HTTP error.
	rec2 := postJSON(t, s, "/api/shell", `{"command":"exit 3"}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("exit-3 status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
	if m := decodeBody(t, rec2); m["exitCode"] != float64(3) || m["timedOut"] != false {
		t.Fatalf("resp = %s, want exitCode 3", rec2.Body.String())
	}

	// Empty command → 400.
	rec3 := postJSON(t, s, "/api/shell", `{"command":""}`)
	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("empty-command status = %d, body=%s", rec3.Code, rec3.Body.String())
	}
	rec4 := postJSON(t, s, "/api/shell", `{"command":"   "}`)
	if rec4.Code != http.StatusBadRequest {
		t.Fatalf("whitespace-command status = %d, body=%s", rec4.Code, rec4.Body.String())
	}
}

// A command that outlives timeoutMs is killed and reported as timedOut.
func TestShellTimeout(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	start := time.Now()
	rec := postJSON(t, s, "/api/shell", `{"command":"sleep 5","timeoutMs":300}`)
	elapsed := time.Since(start)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["timedOut"] != true {
		t.Fatalf("resp = %s, want timedOut:true", rec.Body.String())
	}
	if code, _ := m["exitCode"].(float64); code == 0 {
		t.Fatalf("resp = %s, want non-zero exitCode on timeout", rec.Body.String())
	}
	if elapsed > 5*time.Second {
		t.Fatalf("took %v — the command was not killed by the timeout", elapsed)
	}
}

// Shell cwd defaults to the active session's cwd when not provided.
func TestShellCwdDefaultsToActiveSession(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session", `{"cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("session/new status = %d, body=%s", rec.Code, rec.Body.String())
	}

	rec2 := postJSON(t, s, "/api/shell", `{"command":"pwd"}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
	if m := decodeBody(t, rec2); m["stdout"] != "/tmp\n" {
		t.Fatalf("resp = %s, want stdout '/tmp\\n' (active session cwd)", rec2.Body.String())
	}

	// Explicit cwd wins over the session cwd.
	rec3 := postJSON(t, s, "/api/shell", `{"command":"pwd","cwd":"/"}`)
	if rec3.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec3.Code, rec3.Body.String())
	}
	if m := decodeBody(t, rec3); m["stdout"] != "/\n" {
		t.Fatalf("resp = %s, want stdout '/\\n'", rec3.Body.String())
	}
}
