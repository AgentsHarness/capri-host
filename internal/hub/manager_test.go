package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNormalizeHubURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		bad  bool
	}{
		{name: "empty means unchanged", in: "", want: ""},
		{name: "whitespace only", in: "   ", want: ""},
		{name: "bare host gets https", in: "hub.example.com", want: "https://hub.example.com"},
		// A documentation-range address (RFC 5737), so no real host of anyone's
		// ends up recorded in a public test file.
		{name: "bare ip gets https", in: "203.0.113.10", want: "https://203.0.113.10"},
		{name: "explicit http kept", in: "http://192.168.1.10:8787", want: "http://192.168.1.10:8787"},
		{name: "explicit https kept", in: "https://h.example.com", want: "https://h.example.com"},
		{name: "trailing slash dropped", in: "https://h.example.com/", want: "https://h.example.com"},
		// A pasted API path would otherwise be concatenated with the client's
		// own "/api/pair" and 404 with no hint why.
		{name: "pasted api path dropped", in: "https://h.example.com/api/pairing", want: "https://h.example.com"},
		{name: "query dropped", in: "https://h.example.com/?x=1", want: "https://h.example.com"},
		{name: "credentials dropped", in: "https://user:pw@h.example.com", want: "https://h.example.com"},
		{name: "surrounding spaces trimmed", in: "  https://h.example.com  ", want: "https://h.example.com"},
		{name: "port preserved", in: "https://h.example.com:8443", want: "https://h.example.com:8443"},
		{name: "wrong scheme refused", in: "ftp://h.example.com", bad: true},
		{name: "scheme with no host refused", in: "https://", bad: true},
	} {
		got, err := NormalizeHubURL(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("%s: NormalizeHubURL(%q) = %q, want an error", tc.name, tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: NormalizeHubURL(%q): %v", tc.name, tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: NormalizeHubURL(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}

// pairingHub is a stand-in for capri-hub that only implements POST /api/pair.
// The manager's pairing path never reaches any other endpoint, and a real hub
// would drag in QUIC and a session protocol this test does not exercise.
type pairingHub struct {
	srv   *httptest.Server
	code  string
	token string
	calls atomic.Int32
	// lastHostID records what identity the host claimed, which is how the
	// collision fix is observable from the hub's side.
	lastHostID atomic.Value
}

func newPairingHub(t *testing.T, code, token string) *pairingHub {
	t.Helper()
	h := &pairingHub{code: code, token: token}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/pair", func(w http.ResponseWriter, r *http.Request) {
		h.calls.Add(1)
		var body struct {
			Code     string `json:"code"`
			HostID   string `json:"hostId"`
			HostName string `json:"hostName"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		h.lastHostID.Store(body.HostID)
		w.Header().Set("Content-Type", "application/json")
		if body.Code != h.code {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "配对码无效或已过期"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "token": h.token})
	})
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
	return h
}

func (h *pairingHub) url() string { return h.srv.URL }

func managerFor(t *testing.T, hubURL string) (*Manager, string) {
	t.Helper()
	stateFile := filepath.Join(t.TempDir(), "hub.json")
	m := NewManager(Config{
		URL:       hubURL,
		HostID:    "test-host",
		HostName:  "Test Host",
		StateFile: stateFile,
	}, nil)
	return m, stateFile
}

func TestManagerStateInLocalModeIsNotConfigured(t *testing.T) {
	m, _ := managerFor(t, "")
	st := m.State()
	if st.Configured || st.Paired || st.Connected {
		t.Errorf("local mode should be unconfigured: %+v", st)
	}
	if m.HubURL() != "" {
		t.Errorf("HubURL = %q, want empty", m.HubURL())
	}
}

func TestManagerPairWithSetsHubFromLocalMode(t *testing.T) {
	h := newPairingHub(t, "ABC234", "tok-1")
	m, stateFile := managerFor(t, "")

	var persisted string
	m.persist = func(u string) error { persisted = u; return nil }

	// This is the new-user path: nothing configured, no client running, the
	// address and the code both arrive from the tray dialog.
	if err := m.PairWith(context.Background(), h.url(), "abc-234"); err != nil {
		t.Fatalf("PairWith: %v", err)
	}

	if got := m.HubURL(); got != h.url() {
		t.Errorf("HubURL = %q, want %q", got, h.url())
	}
	st := m.State()
	if !st.Configured || !st.Paired {
		t.Errorf("want configured+paired, got %+v", st)
	}
	if persisted != h.url() {
		t.Errorf("persist called with %q, want %q", persisted, h.url())
	}

	// The token must be on disk, or a restart would ask for a code again.
	b, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("state file not written: %v", err)
	}
	var sf stateFile2
	if err := json.Unmarshal(b, &sf); err != nil {
		t.Fatal(err)
	}
	if sf.Token != "tok-1" || sf.URL != h.url() {
		t.Errorf("state file = %+v", sf)
	}
	// The 0600 the code asks for is only meaningful where the OS implements
	// POSIX modes. Go's Windows layer maps nothing but the read-only bit, so a
	// file written 0600 reads back as 0666 there and asserting otherwise would
	// be testing the assertion, not the code. On Windows the protection is
	// that %USERPROFILE% is already ACL'd to the account.
	if runtime.GOOS != "windows" {
		if fi, err := os.Stat(stateFile); err == nil && fi.Mode().Perm() != 0o600 {
			t.Errorf("state file mode = %v, want 0600", fi.Mode().Perm())
		}
	}
}

// stateFile2 (the on-disk shape) is declared in control_test.go — same
// package, so it is shared rather than redeclared here.

func TestManagerPairWithNormalizesTheAddress(t *testing.T) {
	h := newPairingHub(t, "ABC234", "tok-1")
	m, _ := managerFor(t, "")

	// httptest gives "http://127.0.0.1:PORT"; append a path to prove it is
	// stripped rather than concatenated onto /api/pair.
	if err := m.PairWith(context.Background(), h.url()+"/api/pairing/", "ABC234"); err != nil {
		t.Fatalf("PairWith: %v", err)
	}
	if got := m.HubURL(); got != h.url() {
		t.Errorf("HubURL = %q, want the origin %q", got, h.url())
	}
}

func TestManagerPairWithRejectsBadCodeBeforeContactingHub(t *testing.T) {
	h := newPairingHub(t, "ABC234", "tok-1")
	m, _ := managerFor(t, "")

	// "I" is not in the hub's alphabet, so this cannot be a real code. Sending
	// it would spend the hub's per-IP pairing budget on a certain failure.
	if err := m.PairWith(context.Background(), h.url(), "ABCI34"); err == nil {
		t.Fatal("expected a rejection")
	}
	if n := h.calls.Load(); n != 0 {
		t.Errorf("hub was contacted %d times for an impossible code", n)
	}
	if m.HubURL() != "" {
		t.Errorf("HubURL was set to %q despite the failure", m.HubURL())
	}
}

func TestManagerPairWithRejectsBadAddressBeforeContactingHub(t *testing.T) {
	m, _ := managerFor(t, "")
	if err := m.PairWith(context.Background(), "ftp://nope", "ABC234"); err == nil {
		t.Fatal("expected a rejection")
	}
	if m.HubURL() != "" {
		t.Errorf("HubURL = %q, want empty", m.HubURL())
	}
}

func TestManagerPairWithEmptyURLInLocalModeAsksForOne(t *testing.T) {
	m, _ := managerFor(t, "")
	err := m.PairWith(context.Background(), "", "ABC234")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "hub 地址") {
		t.Errorf("error should mention the missing address, got: %v", err)
	}
}

func TestManagerRejectedCodeLeavesConfigUntouched(t *testing.T) {
	good := newPairingHub(t, "ABC234", "tok-good")
	bad := newPairingHub(t, "ZZZ999", "tok-bad")
	m, _ := managerFor(t, "")

	if err := m.PairWith(context.Background(), good.url(), "ABC234"); err != nil {
		t.Fatalf("first pair: %v", err)
	}

	// Now try to move to another hub with a code that hub will reject. The
	// working pairing must survive: the whole reason a throwaway client does
	// the probing is that a typo must not cost a live link.
	if err := m.PairWith(context.Background(), bad.url(), "ABC234"); err == nil {
		t.Fatal("expected the second hub to reject the code")
	}
	if got := m.HubURL(); got != good.url() {
		t.Errorf("HubURL moved to %q despite the failure, want %q", got, good.url())
	}
	if st := m.State(); !st.Paired {
		t.Errorf("lost the working pairing: %+v", st)
	}
}

func TestManagerPairWithRetargetsToAnotherHub(t *testing.T) {
	first := newPairingHub(t, "ABC234", "tok-1")
	second := newPairingHub(t, "XYZ789", "tok-2")
	m, stateFile := managerFor(t, "")

	if err := m.PairWith(context.Background(), first.url(), "ABC234"); err != nil {
		t.Fatalf("pair with first: %v", err)
	}
	if err := m.PairWith(context.Background(), second.url(), "XYZ789"); err != nil {
		t.Fatalf("pair with second: %v", err)
	}

	if got := m.HubURL(); got != second.url() {
		t.Errorf("HubURL = %q, want %q", got, second.url())
	}
	b, _ := os.ReadFile(stateFile)
	var sf stateFile2
	_ = json.Unmarshal(b, &sf)
	if sf.URL != second.url() || sf.Token != "tok-2" {
		t.Errorf("state file still points at the old hub: %+v", sf)
	}
}

func TestManagerPairWithSendsTheConfiguredHostID(t *testing.T) {
	h := newPairingHub(t, "ABC234", "tok-1")
	m, _ := managerFor(t, "")
	if err := m.PairWith(context.Background(), h.url(), "ABC234"); err != nil {
		t.Fatalf("PairWith: %v", err)
	}
	// The hub keys its host table by this value, so a host that claimed the
	// wrong one would displace another machine.
	if got, _ := h.lastHostID.Load().(string); got != "test-host" {
		t.Errorf("hub saw hostId %q, want %q", got, "test-host")
	}
}

func TestManagerPersistFailureDoesNotFailThePairing(t *testing.T) {
	h := newPairingHub(t, "ABC234", "tok-1")
	m, _ := managerFor(t, "")
	m.persist = func(string) error { return os.ErrPermission }

	// The link is already up by the time persist runs. Reporting failure would
	// tell the user pairing did not work when it plainly did.
	if err := m.PairWith(context.Background(), h.url(), "ABC234"); err != nil {
		t.Fatalf("PairWith failed because persistence did: %v", err)
	}
	if st := m.State(); !st.Paired {
		t.Errorf("want paired, got %+v", st)
	}
}

func TestManagerRunExitsOnContextCancel(t *testing.T) {
	m, _ := managerFor(t, "")
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { m.Run(ctx, nil); close(done) }()

	// Local mode parks on the retarget channel; cancelling must still get us
	// out, or shutdown would hang on this goroutine.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestManagerStateReadsTokenFromDiskWithNoClient(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "hub.json")
	b, _ := json.Marshal(stateFile2{URL: "https://h.example.com", HostID: "x", Token: "tok"})
	if err := os.WriteFile(stateFile, b, 0o600); err != nil {
		t.Fatal(err)
	}

	m := NewManager(Config{URL: "https://h.example.com", HostID: "x", StateFile: stateFile}, nil)
	// Run has not started, so there is no client. Reporting "未配对" here would
	// make the tray blink its hub entry out of existence during a retarget.
	if st := m.State(); !st.Paired {
		t.Errorf("want paired from the state file, got %+v", st)
	}

	// A token belonging to a different hub must not count.
	m2 := NewManager(Config{URL: "https://other.example.com", HostID: "x", StateFile: stateFile}, nil)
	if st := m2.State(); st.Paired {
		t.Errorf("token for another hub counted as paired: %+v", st)
	}
}
