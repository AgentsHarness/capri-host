package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// ── inbound access-token gate ─────────────────────────────────────────

// newTokenServer builds a fake-agent server with an access token
// configured (same harness as newFakeAgentServer, plus AccessToken).
func newTokenServer(t *testing.T, token string) *Server {
	t.Helper()
	t.Setenv(ACPHostFakeAgentEnv, "1")
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             os.Args[0],
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	t.Cleanup(b.Shutdown)
	return New(config.Config{Port: 0, GrokBin: "grok", AccessToken: token}, b)
}

// request issues a request against s with optional auth headers/query.
func request(t *testing.T, s *Server, method, path, authHeader, tokenQuery string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "http://127.0.0.1:8765"+path, strings.NewReader(`{}`))
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if tokenQuery != "" {
		q := req.URL.Query()
		q.Set("token", tokenQuery)
		req.URL.RawQuery = q.Encode()
	}
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

const testToken = "s3cret-token-abc"

// Missing or wrong credentials are refused on every gated path.
func TestAccessTokenRejectsMissingOrWrong(t *testing.T) {
	s := newTokenServer(t, testToken)

	cases := []struct{ name, method, path, auth, query string }{
		{"no auth", "GET", "/api/status", "", ""},
		{"wrong bearer", "GET", "/api/status", "Bearer wrong-token", ""},
		{"wrong query", "GET", "/api/status", "", "wrong-token"},
		{"prompt no auth", "POST", "/api/prompt", "", ""},
		{"shell no auth", "POST", "/api/shell", "", ""},
		{"events no auth", "GET", "/events", "", ""},
		{"events wrong query", "GET", "/events", "", "wrong-token"},
	}
	for _, tc := range cases {
		rec := request(t, s, tc.method, tc.path, tc.auth, tc.query)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", tc.name, rec.Code)
		}
		// The gate must not leak handler output on failure.
		if m := decodeBody(t, rec); m["ok"] != false {
			t.Fatalf("%s: body ok = %v, want false", tc.name, m["ok"])
		}
	}
}

// Valid credentials — Bearer header (any case), ?token= query — pass.
func TestAccessTokenAcceptsValid(t *testing.T) {
	s := newTokenServer(t, testToken)

	cases := []struct{ name, method, path, auth, query string }{
		{"bearer", "GET", "/api/status", "Bearer " + testToken, ""},
		{"bearer lowercase scheme", "GET", "/api/status", "bearer " + testToken, ""},
		{"query token", "GET", "/api/status", "", testToken},
		{"prompt with bearer", "POST", "/api/prompt", "Bearer " + testToken, ""},
	}
	for _, tc := range cases {
		rec := request(t, s, tc.method, tc.path, tc.auth, tc.query)
		if rec.Code == http.StatusUnauthorized {
			t.Fatalf("%s: status = 401, want accepted", tc.name)
		}
	}
}

// Boot probes stay open: /api/hosts (with authRequired flag) and the SPA
// shell are reachable without a token.
func TestAccessTokenExemptions(t *testing.T) {
	s := newTokenServer(t, testToken)

	// /api/hosts — the FE's detectMode/probeAccess path.
	rec := request(t, s, "GET", "/api/hosts", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/hosts status = %d, want 200 (boot probe)", rec.Code)
	}
	if m := decodeBody(t, rec); m["authRequired"] != true {
		t.Fatalf("GET /api/hosts authRequired = %v, want true", m["authRequired"])
	}

	// GET / — SPA static shell.
	rec2 := request(t, s, "GET", "/", "", "")
	if rec2.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200 (static shell open)", rec2.Code)
	}

	// Control: a gated path with no token still refuses.
	rec3 := request(t, s, "GET", "/api/status", "", "")
	if rec3.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/status status = %d, want 401", rec3.Code)
	}
}

// Host without a configured token keeps the open behavior (local trusted
// default) and reports authRequired:false.
func TestAccessTokenDisabledByDefault(t *testing.T) {
	s := newTokenServer(t, "")

	rec := request(t, s, "POST", "/api/prompt", "", "")
	if rec.Code == http.StatusUnauthorized {
		t.Fatal("no-token server rejected a request; want open")
	}
	rec2 := request(t, s, "GET", "/api/hosts", "", "")
	if m := decodeBody(t, rec2); m["authRequired"] != false {
		t.Fatalf("authRequired = %v, want false when no token configured", m["authRequired"])
	}
}

// Preflight OPTIONS passes without a token (the real request carries the
// Authorization header, which the preflight response must admit).
func TestAccessTokenPreflight(t *testing.T) {
	s := newTokenServer(t, testToken)

	req := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/prompt", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "content-type, authorization")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", rec.Code)
	}
	allowed := rec.Header().Get("Access-Control-Allow-Headers")
	if !strings.Contains(strings.ToLower(allowed), "authorization") {
		t.Fatalf("Allow-Headers = %q, want it to admit authorization", allowed)
	}
}

// The gate scope is exact: /api/hosts is the only /api/* exemption,
// /events is gated, the SPA fallback is not.
func TestAuthRequiredForPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/api/hosts", false},
		{"/api/status", true},
		{"/api/prompt", true},
		{"/api/shell", true},
		{"/api/nonexistent", true},
		{"/events", true},
		{"/", false},
		{"/assets/index-BvtOyS3E.css", false},
		{"/favicon.ico", false},
	}
	for _, tc := range cases {
		if got := authRequiredForPath(tc.path); got != tc.want {
			t.Errorf("authRequiredForPath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}
