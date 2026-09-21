package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
)

// ── host control surface ──────────────────────────────────────────────
//
// These endpoints exist so a supervisor process (the Windows tray, Capri.app)
// can drive a host it does not own the memory of. The tests below pin the two
// properties that matter to such a caller: a well-formed request changes the
// host, and a missing one is refused with a status the caller can act on.

type stubHub struct {
	state     hubstate.State
	pairURL   string
	pairCode  string
	paired    int32
	renamedTo string
	renameErr error
	usedURL   string
	useErr    error
}

func (s *stubHub) State() hubstate.State { return s.state }

func (s *stubHub) PairWith(_ context.Context, hubURL, code string) error {
	atomic.AddInt32(&s.paired, 1)
	s.pairURL, s.pairCode = hubURL, code
	return nil
}

func (s *stubHub) Reuse(hubURL string) error {
	if s.useErr != nil {
		return s.useErr
	}
	s.usedURL = hubURL
	return nil
}

func (s *stubHub) Disconnect() error {
	s.state.Configured = false
	s.state.HubURL = ""
	s.state.Connected = false
	return nil
}

func (s *stubHub) Rename(_ context.Context, newName string) error {
	if s.renameErr != nil {
		return s.renameErr
	}
	s.renamedTo = newName
	return nil
}

func newControlServer(t *testing.T) *Server {
	t.Helper()
	t.Setenv(ACPHostFakeAgentEnv, "1")
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             os.Args[0],
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	t.Cleanup(b.Shutdown)
	return New(config.Config{Port: 0, GrokBin: "grok", HostID: "h", HostName: "host"}, b)
}

func postControlJSON(t *testing.T, s *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8765"+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func TestHostRenameAppliesTheName(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{state: hubstate.State{Configured: true, HostName: "old"}}
	s.SetHubController(stub)

	rec := postControlJSON(t, s, "/api/host/rename", `{"name":"  新名字  "}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	// Trimmed before it reaches the controller: a name that is only whitespace
	// must not become a legitimate-looking rename.
	if stub.renamedTo != "新名字" {
		t.Errorf("renamedTo = %q, want %q", stub.renamedTo, "新名字")
	}
}

func TestHostRenameRejectsEmptyName(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{}
	s.SetHubController(stub)

	for _, body := range []string{`{"name":""}`, `{"name":"   "}`, `{}`, `not json`} {
		rec := postControlJSON(t, s, "/api/host/rename", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, rec.Code)
		}
	}
	if stub.renamedTo != "" {
		t.Errorf("a rejected request still renamed to %q", stub.renamedTo)
	}
}

func TestHostRenameSurfacesControllerError(t *testing.T) {
	s := newControlServer(t)
	s.SetHubController(&stubHub{renameErr: errors.New("本机代号不能为空")})

	rec := postControlJSON(t, s, "/api/host/rename", `{"name":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	var out struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out.Error == "" {
		t.Errorf("body = %s, want a JSON error field", rec.Body.String())
	}
}

func TestControlEndpointsConflictWithoutAController(t *testing.T) {
	// A host built by an embedder that never wires the manager must say so,
	// rather than accept a request it cannot carry out.
	s := newControlServer(t)

	for _, tc := range []struct{ path, body string }{
		{"/api/host/rename", `{"name":"x"}`},
		{"/api/host/quit", `{}`},
		{"/api/hub/reuse", `{"hubUrl":"https://h.example"}`},
	} {
		rec := postControlJSON(t, s, tc.path, tc.body)
		if rec.Code != http.StatusConflict {
			t.Errorf("%s: status = %d, want 409", tc.path, rec.Code)
		}
	}
}

func TestHostQuitAnswersWithoutWaitingForShutdown(t *testing.T) {
	s := newControlServer(t)
	done := make(chan struct{})
	s.SetQuitFunc(func() {
		// A real shutdown tears down the bridge and the agent process. The
		// reply must not be hostage to that: a caller whose connection dies
		// mid-stop cannot tell success from failure.
		time.Sleep(300 * time.Millisecond)
		close(done)
	})

	start := time.Now()
	rec := postControlJSON(t, s, "/api/host/quit", `{}`)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("handler blocked for %s; it must answer before stopping", elapsed)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("quit func was never called")
	}
}

func TestHubStateFallsBackToConfigurationWithoutAController(t *testing.T) {
	// The window between "server is listening" and "main injected the client"
	// is real, and answering "configured but not yet paired" there is truthful
	// where an error would not be.
	s := newControlServer(t)
	req := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/hub/state", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var out struct {
		Hub hubstate.State `json:"hub"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Hub.Configured || out.Hub.HostID != "h" {
		t.Errorf("fallback state = %+v, want Configured=false HostID=h", out.Hub)
	}
}

func TestHostsEndpointFollowsLiveHubState(t *testing.T) {
	// Runtime pairing updates the manager, not s.cfg. FE probes /api/hosts
	// and /api/probe; those must follow the live snapshot. The tray already
	// does, via GET /api/hub/state.
	s := newControlServer(t)
	s.SetHubController(&stubHub{state: hubstate.State{
		Configured: true,
		HubURL:     "https://hub.example",
	}})

	req := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/hosts", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/hosts = %d", rec.Code)
	}
	var hosts struct {
		Mode   string `json:"mode"`
		HubURL string `json:"hubUrl"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &hosts); err != nil {
		t.Fatal(err)
	}
	if hosts.Mode != "hub" || hosts.HubURL != "https://hub.example" {
		t.Errorf("hosts = %+v, want mode=hub hubUrl=https://hub.example", hosts)
	}

	req2 := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/probe", nil)
	rec2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("GET /api/probe = %d", rec2.Code)
	}
	var probe struct {
		Mode   string `json:"mode"`
		HubURL string `json:"hubUrl"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &probe); err != nil {
		t.Fatal(err)
	}
	if probe.Mode != "hub" || probe.HubURL != "https://hub.example" {
		t.Errorf("probe = %+v, want mode=hub hubUrl=https://hub.example", probe)
	}
}

func TestHubPairKeepsHubURLFromLocalOrigin(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{}
	s.SetHubController(stub)

	rec := postControlJSON(t, s, "/api/hub/pair", `{"code":"ABC234","hubUrl":"https://hub.example"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if stub.pairURL != "https://hub.example" || stub.pairCode != "ABC234" {
		t.Errorf("PairWith(%q, %q), want hubUrl kept for a local origin", stub.pairURL, stub.pairCode)
	}
}

func TestHubPairIgnoresHubURLFromHubOrigin(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{state: hubstate.State{Configured: true, HubURL: "https://hub.example"}}
	s.SetHubController(stub)

	req := httptest.NewRequest("POST", "http://127.0.0.1:8765/api/hub/pair",
		strings.NewReader(`{"code":"ABC234","hubUrl":"https://evil.example"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://hub.example")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if stub.pairURL != "" {
		t.Errorf("hub FE retargeted the host to %q", stub.pairURL)
	}
	if stub.pairCode != "ABC234" {
		t.Errorf("code = %q, want ABC234 (re-pair the current hub)", stub.pairCode)
	}
}

func TestHubUseAdoptsTheStoredCredential(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{state: hubstate.State{Configured: true, HubURL: "https://h.example", Paired: true}}
	s.SetHubController(stub)

	rec := postControlJSON(t, s, "/api/hub/reuse", `{"hubUrl":"https://h.example"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if stub.usedURL != "https://h.example" {
		t.Errorf("usedURL = %q", stub.usedURL)
	}
	// The reuse path must not fall through to pairing: sending the user for a
	// code they do not need is the behaviour this endpoint exists to remove.
	if n := atomic.LoadInt32(&stub.paired); n != 0 {
		t.Errorf("pairing was attempted %d time(s) on the reuse path", n)
	}
}

func TestHubReuseIgnoresHubURLFromHubOrigin(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{state: hubstate.State{Configured: true, HubURL: "https://hub.example"}}
	s.SetHubController(stub)

	req := httptest.NewRequest("POST", "http://127.0.0.1:8765/api/hub/reuse",
		strings.NewReader(`{"hubUrl":"https://evil.example"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://hub.example")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if stub.usedURL != "" {
		t.Errorf("hub FE retargeted the host to %q", stub.usedURL)
	}
}

func TestHubUseSurfacesAMissingCredential(t *testing.T) {
	// No usable token on disk means the caller really does have to pair, and
	// the response has to say so rather than look like a success.
	s := newControlServer(t)
	s.SetHubController(&stubHub{useErr: errors.New("hub.json 里没有 https://h.example 的可用凭证")})

	rec := postControlJSON(t, s, "/api/hub/reuse", `{"hubUrl":"https://h.example"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	var out struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || !strings.Contains(out.Error, "可用凭证") {
		t.Errorf("body = %s, want an error naming the missing credential", rec.Body.String())
	}
}

func TestControlPathsAreSensitive(t *testing.T) {
	// Pairing, renaming and quitting are all privileged state changes; the
	// local-origin gate costs nothing and must cover every one of them. A new
	// endpoint that forgets to register here would silently be reachable from
	// a page the user merely visited.
	sensitive := func(path string) bool {
		return isSensitiveEndpoint(httptest.NewRequest("POST", "http://127.0.0.1:8765"+path, nil))
	}
	for _, p := range []string{"/api/hub/pair", "/api/hub/reuse", "/api/hub/disconnect", "/api/host/rename", "/api/host/config", "/api/host/quit"} {
		if !sensitive(p) {
			t.Errorf("%s is not registered as sensitive", p)
		}
	}
	// /api/hub/state is a read, and the tray polls it twice a second.
	if sensitive("/api/hub/state") {
		t.Error("/api/hub/state is marked sensitive; it is a read the tray polls")
	}
}

func TestHubDisconnect(t *testing.T) {
	s := newControlServer(t)
	stub := &stubHub{state: hubstate.State{Configured: true, HubURL: "https://h.example", Connected: true}}
	s.SetHubController(stub)

	rec := postControlJSON(t, s, "/api/hub/disconnect", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if stub.state.Configured {
		t.Errorf("stub still configured after disconnect")
	}
}

func TestHostConfigGetAndPost(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("CAPRI_HOME", tmpDir)

	s := newControlServer(t)

	// GET
	req := httptest.NewRequest("GET", "http://127.0.0.1:8765/api/host/config", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/host/config status = %d, body = %s", rec.Code, rec.Body.String())
	}

	// POST valid update
	postRec := postControlJSON(t, s, "/api/host/config", `{"host_name":"MyNewName","port":9000}`)
	if postRec.Code != http.StatusOK {
		t.Fatalf("POST /api/host/config status = %d, body = %s", postRec.Code, postRec.Body.String())
	}

	f, err := config.LoadFile()
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if f.HostName != "MyNewName" || f.Port != 9000 {
		t.Errorf("config on disk = %+v", f)
	}

	// POST invalid bind policy: non-loopback without fe_token
	badRec := postControlJSON(t, s, "/api/host/config", `{"bind":"0.0.0.0","fe_token":""}`)
	if badRec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bind 0.0.0.0 without token, got %d", badRec.Code)
	}
}
