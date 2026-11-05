package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
)

// ── local-origin guard for sensitive endpoints ────────────────────────

// A hostile web page's fetch carries Origin: http://evil.example against
// Host 127.0.0.1:8765 — every sensitive endpoint must refuse it before the
// handler runs, and must not answer with any ACAO header.
func TestSensitiveEndpointRejectsCrossOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	paths := []string{
		"/api/shell",
		"/api/local/dirs",
		"/api/local/mkdir",
		"/api/api-key-get",
		"/api/api-key-set",
		"/api/auth/info",
		"/api/auth/get-bearer-token",
		"/api/hub/pair",
		"/api/host/rename",
		"/api/host/quit",
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

// The interfaces that drive the agent or write files are just as powerful as
// /api/shell: a web page that can reach 127.0.0.1 must not be able to call
// them.
func TestAgentDrivingEndpointsRejectCrossOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	for _, path := range []string{"/api/prompt", "/api/fs/write-file", "/api/settings", "/api/session"} {
		req := httptest.NewRequest("POST", "http://127.0.0.1:8765"+path, strings.NewReader(`{}`))
		req.Header.Set("Origin", "http://evil.example")
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s with evil Origin: status = %d, want 403; body=%s", path, rec.Code, rec.Body.String())
		}
		if h := rec.Header().Get("Access-Control-Allow-Origin"); h != "" {
			t.Fatalf("%s with evil Origin: ACAO = %q, want empty", path, h)
		}
	}
}

// Deployed hub FE (Origin = HUB_URL) may call sensitive endpoints on the
// local host port — that is the "远程站优先本机" shortcut. Other public
// origins stay forbidden.
func TestSensitiveEndpointAllowsTrustedHubOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	s.cfg.HubURL = "https://agents.example"

	req := httptest.NewRequest("POST", "http://127.0.0.1:8765/api/shell", strings.NewReader(`{"command":"echo hi"}`))
	req.Header.Set("Origin", "https://agents.example")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("trusted hub Origin status = %d, body=%s; want 200", rec.Code, rec.Body.String())
	}
	if h := rec.Header().Get("Access-Control-Allow-Origin"); h != "https://agents.example" {
		t.Fatalf("ACAO = %q, want hub origin echo", h)
	}

	// Preflight with Private Network Access must advertise Allow-Private-Network.
	pre := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/shell", nil)
	pre.Header.Set("Origin", "https://agents.example")
	pre.Header.Set("Access-Control-Request-Method", "POST")
	pre.Header.Set("Access-Control-Request-Private-Network", "true")
	prerec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(prerec, pre)
	if prerec.Code != http.StatusNoContent {
		t.Fatalf("trusted hub preflight status = %d, want 204", prerec.Code)
	}
	if prerec.Header().Get("Access-Control-Allow-Private-Network") != "true" {
		t.Fatalf("missing Access-Control-Allow-Private-Network on hub preflight")
	}

	// Unrelated public origin still denied even when HubURL is set.
	evil := httptest.NewRequest("POST", "http://127.0.0.1:8765/api/shell", strings.NewReader(`{}`))
	evil.Header.Set("Origin", "https://evil.example")
	evilRec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(evilRec, evil)
	if evilRec.Code != http.StatusForbidden {
		t.Fatalf("untrusted Origin status = %d, want 403", evilRec.Code)
	}
}

// A runtime pairing updates the manager, not s.cfg.HubURL. The hub FE origin
// must still be trusted without a process restart.
func TestSensitiveEndpointAllowsLiveHubOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	s.SetHubController(&stubHub{state: hubstate.State{
		Configured: true,
		HubURL:     "https://agents.example",
	}})

	req := httptest.NewRequest("POST", "http://127.0.0.1:8765/api/shell", strings.NewReader(`{"command":"echo hi"}`))
	req.Header.Set("Origin", "https://agents.example")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("live hub Origin status = %d, body=%s; want 200", rec.Code, rec.Body.String())
	}
}

// Local origins — FE dev server on another localhost port, same-origin
// calls, and no Origin at all (curl / same-origin GET) — keep working.
func TestSensitiveEndpointAllowsLocalOrigins(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	// The "origin matches request Host" case below is the host UI opened via
	// a LAN address, which only exists when the listener is non-loopback.
	s.cfg.BindAddr = "0.0.0.0"

	cases := []struct{ name, origin, host string }{
		{"no origin (curl)", "", "127.0.0.1:8765"},
		{"localhost other port (Vite dev)", "http://localhost:5173", "127.0.0.1:8765"},
		{"127.0.0.1 same port", "http://127.0.0.1:8765", "127.0.0.1:8765"},
		{"::1 same port", "http://[::1]:8765", "[::1]:8765"},
		{"origin matches request Host", "http://192.168.1.5:8765", "192.168.1.5:8765"},
		{"LAN dev another port", "http://192.168.1.5:5173", "192.168.1.5:8765"},
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

	// Non-sensitive endpoints follow the same allowlist: a page that can
	// reach 127.0.0.1 still must not drive /api/prompt or /api/fs/*.
	req3 := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/status", nil)
	req3.Header.Set("Origin", "http://evil.example")
	rec3 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusForbidden {
		t.Fatalf("non-sensitive evil preflight status = %d, want 403", rec3.Code)
	}
	if h := rec3.Header().Get("Access-Control-Allow-Origin"); h != "" {
		t.Fatalf("non-sensitive evil preflight ACAO = %q, want empty", h)
	}

	req4 := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/status", nil)
	req4.Header.Set("Origin", "http://evil.example")
	rec4 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusForbidden {
		t.Fatalf("non-sensitive GET with evil Origin status = %d, want 403", rec4.Code)
	}
	if h := rec4.Header().Get("Access-Control-Allow-Origin"); h != "" {
		t.Fatalf("non-sensitive evil GET ACAO = %q, want empty", h)
	}

	// A page that merely resolves its own domain to 127.0.0.1 (DNS
	// rebinding) sends its domain in Host. It must be refused even though
	// Origin and Host agree — that equality is what used to bypass the
	// sensitive-endpoint check.
	for _, method := range []string{"GET", "POST"} {
		req5 := httptest.NewRequest(method, "http://evil.example:8765/api/host/config", strings.NewReader(`{}`))
		req5.Host = "evil.example:8765"
		req5.Header.Set("Origin", "http://evil.example:8765")
		rec5 := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec5, req5)
		if rec5.Code != http.StatusForbidden {
			t.Fatalf("DNS-rebinding %s status = %d, want 403", method, rec5.Code)
		}
	}
}

func TestIsAllowedHost(t *testing.T) {
	cases := []struct {
		name string
		bind string
		host string
		want bool
	}{
		{"loopback localhost", "", "localhost:8765", true},
		{"loopback v4", "", "127.0.0.1:8765", true},
		{"loopback v6", "", "[::1]:8765", true},
		{"loopback rejects lan ip", "", "192.168.1.5:8765", false},
		{"loopback rejects domain", "", "evil.example:8765", false},
		{"all ifaces accepts lan ip", "0.0.0.0", "192.168.1.5:8765", true},
		{"all ifaces accepts another ip", "0.0.0.0", "10.0.0.8:8765", true},
		{"all ifaces rejects domain", "0.0.0.0", "evil.example:8765", false},
		{"specific iface accepts its ip", "192.168.1.5", "192.168.1.5:8765", true},
		{"specific iface rejects other ip", "192.168.1.5", "192.168.1.6:8765", false},
		{"bind hostname", "myhost.local", "myhost.local:8765", true},
		{"other hostname", "myhost.local", "evil.example:8765", false},
	}
	for _, tc := range cases {
		s := &Server{cfg: config.Config{BindAddr: tc.bind}}
		if got := s.isAllowedHost(tc.host); got != tc.want {
			t.Errorf("%s: isAllowedHost(%q) = %v, want %v", tc.name, tc.host, got, tc.want)
		}
	}
}

// DNS rebinding can issue a same-origin GET without an Origin header. Host
// alone must reject it; otherwise /api/host/config would leak the config.
func TestHostGuardRejectsDNSRebindingWithoutOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	for _, path := range []string{"/api/host/config", "/api/status", "/api/hosts"} {
		req := httptest.NewRequest("GET", "http://evil.example:8765"+path, nil)
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s with rebinding Host: status = %d, want 403", path, rec.Code)
		}
	}
}

// The same allowlist covers non-sensitive endpoints too, and a trusted hub
// origin still gets both normal reads and the Private Network Access grant.
func TestNonSensitiveEndpointAllowsTrustedHubOrigin(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	// The browser omits the default HTTPS port from Origin; HubURL may spell
	// it out. Both must compare equal.
	s.cfg.HubURL = "https://agents.example:443"

	req := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/status", nil)
	req.Header.Set("Origin", "https://agents.example")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s; want 200", rec.Code, rec.Body.String())
	}
	if h := rec.Header().Get("Access-Control-Allow-Origin"); h != "https://agents.example" {
		t.Fatalf("ACAO = %q, want hub origin echo", h)
	}

	pre := httptest.NewRequest("OPTIONS", "http://127.0.0.1:8765/api/status", nil)
	pre.Header.Set("Origin", "https://agents.example")
	pre.Header.Set("Access-Control-Request-Method", "GET")
	pre.Header.Set("Access-Control-Request-Private-Network", "true")
	prerec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(prerec, pre)
	if prerec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", prerec.Code)
	}
	if prerec.Header().Get("Access-Control-Allow-Private-Network") != "true" {
		t.Fatalf("missing Access-Control-Allow-Private-Network for trusted hub")
	}

	evil := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/status", nil)
	evil.Header.Set("Origin", "https://evil.example")
	evilRec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(evilRec, evil)
	if evilRec.Code != http.StatusForbidden {
		t.Fatalf("untrusted Origin status = %d, want 403", evilRec.Code)
	}

	// A cross-site form POST can arrive without Origin; Sec-Fetch-Site still
	// identifies it and must not be treated as a local caller.
	form := httptest.NewRequest("POST", "http://127.0.0.1:8765/api/shell", strings.NewReader(`{}`))
	form.Header.Set("Sec-Fetch-Site", "cross-site")
	formRec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(formRec, form)
	if formRec.Code != http.StatusForbidden {
		t.Fatalf("cross-site form status = %d, want 403", formRec.Code)
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
