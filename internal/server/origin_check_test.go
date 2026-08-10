package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── local-origin guard for sensitive endpoints ────────────────────────

// A hostile web page's fetch carries Origin: http://evil.example against
// Host 127.0.0.1:8765 — every sensitive endpoint must refuse it before the
// handler runs, and must not answer with any ACAO header.
func TestSensitiveEndpointRejectsCrossOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	paths := []string{
		"/api/shell",
		"/api/api-key-get",
		"/api/api-key-set",
		"/api/auth/info",
		"/api/auth/get-bearer-token",
	}
	for _, path := range paths {
		for _, origin := range []string{"http://evil.example", "null"} {
			req := httptest.NewRequest("POST", "http://127.0.0.1:8765"+path, strings.NewReader(`{}`))
			req.Header.Set("Origin", origin)
			rec := httptest.NewRecorder()
			s.http.Handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s with Origin %q: status = %d, body=%s; want 403",
					path, origin, rec.Code, rec.Body.String())
			}
			if h := rec.Header().Get("Access-Control-Allow-Origin"); h != "" {
				t.Fatalf("%s with Origin %q: ACAO = %q, want empty", path, origin, h)
			}
		}
	}
}

// Local origins — FE dev server on another localhost port, same-origin
// calls, and no Origin at all (curl / same-origin GET) — keep working.
func TestSensitiveEndpointAllowsLocalOrigins(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	cases := []struct{ name, origin, host string }{
		{"no origin (curl)", "", "127.0.0.1:8765"},
		{"localhost other port (Vite dev)", "http://localhost:5173", "127.0.0.1:8765"},
		{"127.0.0.1 same port", "http://127.0.0.1:8765", "127.0.0.1:8765"},
		{"::1 same port", "http://[::1]:8765", "[::1]:8765"},
		{"origin matches request Host", "http://192.168.1.5:8765", "192.168.1.5:8765"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest("POST", "http://"+tc.host+"/api/shell", strings.NewReader(`{"command":"echo hi"}`))
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, body=%s; want 200", tc.name, rec.Code, rec.Body.String())
		}
		if m := decodeBody(t, rec); m["ok"] != true || m["stdout"] != "hi\n" {
			t.Fatalf("%s: resp = %s", tc.name, rec.Body.String())
		}
		// Sensitive responses echo the local Origin instead of `*`.
		if tc.origin != "" {
			if h := rec.Header().Get("Access-Control-Allow-Origin"); h != tc.origin {
				t.Fatalf("%s: ACAO = %q, want %q", tc.name, h, tc.origin)
			}
		}
	}
}

// Preflight behaviour: evil preflights to sensitive endpoints are refused;
// local preflights pass with the Origin echoed; non-sensitive endpoints
// keep the wide-open `*` for the FE dev cross-port setup.
func TestSensitiveEndpointPreflight(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	req := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/shell", nil)
	req.Header.Set("Origin", "http://evil.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("evil preflight status = %d, want 403", rec.Code)
	}

	req2 := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/shell", nil)
	req2.Header.Set("Origin", "http://localhost:5173")
	rec2 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNoContent {
		t.Fatalf("local preflight status = %d, want 204", rec2.Code)
	}
	if h := rec2.Header().Get("Access-Control-Allow-Origin"); h != "http://localhost:5173" {
		t.Fatalf("local preflight ACAO = %q, want origin echo", h)
	}

	req3 := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/status", nil)
	req3.Header.Set("Origin", "http://evil.example")
	rec3 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusNoContent {
		t.Fatalf("non-sensitive preflight status = %d, want 204", rec3.Code)
	}
	if h := rec3.Header().Get("Access-Control-Allow-Origin"); h != "*" {
		t.Fatalf("non-sensitive preflight ACAO = %q, want *", h)
	}

	// Non-sensitive actual requests stay open to any origin.
	req4 := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/status", nil)
	req4.Header.Set("Origin", "http://evil.example")
	rec4 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("non-sensitive GET with evil Origin status = %d, want 200", rec4.Code)
	}
	if h := rec4.Header().Get("Access-Control-Allow-Origin"); h != "*" {
		t.Fatalf("non-sensitive GET ACAO = %q, want *", h)
	}
}

// ── /api/shell output cap ─────────────────────────────────────────────

// A command emitting more than shellMaxOutput is truncated, flagged
// truncated:true, and still reports a clean exit; small output is untouched.
func TestShellOutputTruncated(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/shell", `{"command":"yes x | head -c 20000000"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["truncated"] != true {
		t.Fatalf("truncated = %v, want true", m["truncated"])
	}
	out, _ := m["stdout"].(string)
	if len(out) != shellMaxOutput {
		t.Fatalf("capped stdout len = %d, want %d", len(out), shellMaxOutput)
	}
	if m["exitCode"] != float64(0) {
		t.Fatalf("exitCode = %v, want 0", m["exitCode"])
	}

	rec2 := postJSON(t, s, "/api/shell", `{"command":"yes a | head -c 1000"}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("small status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
	if m := decodeBody(t, rec2); m["truncated"] != false || len(m["stdout"].(string)) != 1000 {
		t.Fatalf("small resp = %s, want truncated false with 1000 bytes", rec2.Body.String())
	}
}
