package hub

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestNormalizePairCode(t *testing.T) {
	cases := map[string]string{
		"abc234":   "ABC234",
		" ABC234 ": "ABC234",
		"abc-234":  "ABC234",
		"ABC_234":  "ABC234",
		"abc 234":  "ABC234",
		"":         "",
	}
	for in, want := range cases {
		if got := NormalizePairCode(in); got != want {
			t.Errorf("NormalizePairCode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidatePairCode(t *testing.T) {
	if err := ValidatePairCode("ABC234"); err != nil {
		t.Fatalf("valid code rejected: %v", err)
	}
	// Length and alphabet are the two independent reasons to reject, and both
	// must report ErrBadPairCode so the HTTP layer can answer 400 rather than
	// blaming the hub with a 502.
	for _, bad := range []string{"", "ABC23", "ABC2345", "ABC23O", "ABC23I", "ABC230", "ABC231", "ABC23L", "abc234"} {
		err := ValidatePairCode(bad)
		if err == nil {
			t.Errorf("ValidatePairCode(%q) accepted an invalid code", bad)
			continue
		}
		if !errors.Is(err, ErrBadPairCode) {
			t.Errorf("ValidatePairCode(%q) = %v, want ErrBadPairCode", bad, err)
		}
	}
}

// pairFakeHub serves POST /api/pair. token is handed out on success; calls counts
// every request that reached the server.
type pairFakeHub struct {
	srv   *httptest.Server
	calls atomic.Int32
	token string
	deny  bool
}

func newPairFakeHub(t *testing.T, token string) *pairFakeHub {
	t.Helper()
	fh := &pairFakeHub{token: token}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pair", func(w http.ResponseWriter, r *http.Request) {
		fh.calls.Add(1)
		var body struct {
			Code   string `json:"code"`
			HostID string `json:"hostId"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if fh.deny {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "配对码无效或已过期"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "token": fh.token})
	})
	fh.srv = httptest.NewServer(mux)
	t.Cleanup(fh.srv.Close)
	return fh
}

func newPairTestClient(t *testing.T, url string) (*Client, string) {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "hub.json")
	c := NewClient(Config{
		URL:       url,
		HostID:    "h1",
		HostName:  "H1",
		StateFile: statePath,
	})
	return c, statePath
}

func TestPairPersistsTokenAndSignalsRepair(t *testing.T) {
	fh := newPairFakeHub(t, "tok-abc")
	c, statePath := newPairTestClient(t, fh.srv.URL)

	if st := c.State(); st.Paired {
		t.Fatal("fresh client reports paired")
	}

	if err := c.Pair(context.Background(), "abc-234"); err != nil {
		t.Fatalf("Pair: %v", err)
	}

	// Token in memory…
	c.stateMu.Lock()
	got := c.token
	c.stateMu.Unlock()
	if got != "tok-abc" {
		t.Errorf("in-memory token = %q, want tok-abc", got)
	}

	// …and on disk, so a restart does not need the code again.
	b, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("state file not written: %v", err)
	}
	var sf stateFile
	if err := json.Unmarshal(b, &sf); err != nil {
		t.Fatalf("state file unparsable: %v", err)
	}
	if sf.Token != "tok-abc" || sf.URL != fh.srv.URL {
		t.Errorf("persisted state = %+v, want token tok-abc for %s", sf, fh.srv.URL)
	}

	if st := c.State(); !st.Paired {
		t.Error("State().Paired is false after a successful pair")
	}

	// Run must be woken rather than left sleeping out its backoff.
	select {
	case <-c.repairCh:
	default:
		t.Error("Pair did not signal repairCh")
	}
	if !c.forceRepair.Load() {
		t.Error("Pair did not set forceRepair")
	}
}

// A rejected code must leave a working pairing untouched. This is the reason
// Pair calls the hub before replacing anything: overwriting first would let one
// typo knock a connected host offline until someone found a fresh code.
func TestPairRejectedKeepsExistingToken(t *testing.T) {
	fh := newPairFakeHub(t, "tok-good")
	c, _ := newPairTestClient(t, fh.srv.URL)

	if err := c.Pair(context.Background(), "ABC234"); err != nil {
		t.Fatalf("first Pair: %v", err)
	}
	fh.deny = true
	err := c.Pair(context.Background(), "ZZZ999")
	if err == nil {
		t.Fatal("Pair succeeded against a hub that denied it")
	}
	if errors.Is(err, ErrBadPairCode) {
		t.Errorf("hub rejection reported as a local format error: %v", err)
	}

	c.stateMu.Lock()
	got := c.token
	c.stateMu.Unlock()
	if got != "tok-good" {
		t.Errorf("token = %q after a failed re-pair, want the original tok-good", got)
	}
	if st := c.State(); !st.Paired {
		t.Error("a failed re-pair cleared the paired state")
	}
}

// A code that cannot be valid must never reach the hub: POST /api/pair is rate
// limited per IP, and spending that budget on a typo delays a real retry.
func TestPairRejectsBadCodeWithoutCallingHub(t *testing.T) {
	fh := newPairFakeHub(t, "tok")
	c, _ := newPairTestClient(t, fh.srv.URL)

	for _, bad := range []string{"abc", "ABC23O", "ABC2345"} {
		if err := c.Pair(context.Background(), bad); !errors.Is(err, ErrBadPairCode) {
			t.Errorf("Pair(%q) = %v, want ErrBadPairCode", bad, err)
		}
	}
	if n := fh.calls.Load(); n != 0 {
		t.Errorf("hub received %d requests for locally-invalid codes, want 0", n)
	}
}

// State must read a token left on disk by a previous run even before Run's
// first ensureToken has copied it into memory, or a tray opened during boot
// claims an established host is unpaired.
func TestStateReadsPersistedToken(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "hub.json")
	c := NewClient(Config{URL: "http://hub.example", HostID: "h1", StateFile: statePath})

	b, _ := json.Marshal(stateFile2{URL: "http://hub.example", HostID: "h1", Token: "persisted"})
	if err := os.WriteFile(statePath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if st := c.State(); !st.Paired {
		t.Error("State did not see the token on disk")
	}

	// A token belonging to a different hub must not count.
	b2, _ := json.Marshal(stateFile2{URL: "http://other.example", HostID: "h1", Token: "persisted"})
	_ = os.WriteFile(statePath, b2, 0o600)
	c2 := NewClient(Config{URL: "http://hub.example", HostID: "h1", StateFile: statePath})
	if st := c2.State(); st.Paired {
		t.Error("State accepted a token persisted for a different hub URL")
	}
}

// stateFile2 mirrors the on-disk shape without depending on the unexported
// type's field tags staying put.
type stateFile2 struct {
	URL    string `json:"url"`
	HostID string `json:"hostId"`
	Token  string `json:"token"`
}

func TestStateReportsTransportAndUptime(t *testing.T) {
	c := NewClient(Config{URL: "http://hub.example", HostID: "h1", HostName: "H1"})

	st := c.State()
	if !st.Configured || st.Connected || st.Transport != "" {
		t.Fatalf("fresh client state = %+v", st)
	}

	c.noteConnected(TransportQUIC)
	st = c.State()
	if !st.Connected {
		t.Error("Connected is false after noteConnected")
	}
	if st.Transport != TransportQUIC {
		t.Errorf("Transport = %q, want %q", st.Transport, TransportQUIC)
	}
	if st.ConnectedSince == "" {
		t.Error("ConnectedSince empty while connected")
	}

	c.noteDisconnected()
	st = c.State()
	if st.Connected || st.Transport != "" || st.ConnectedSince != "" {
		t.Errorf("state after noteDisconnected = %+v, want a disconnected snapshot", st)
	}
}

func TestStateReportsLastError(t *testing.T) {
	c := NewClient(Config{URL: "http://hub.example"})
	c.setLastErr(errors.New("boom"))
	if got := c.State().LastError; got != "boom" {
		t.Errorf("LastError = %q, want boom", got)
	}
	// Connecting clears it, so a recovered link does not keep showing a stale
	// failure in the tray.
	c.noteConnected(TransportWS)
	if got := c.State().LastError; got != "" {
		t.Errorf("LastError = %q after reconnect, want empty", got)
	}
}

// requestRepair must cancel the session context Run published, which is the
// mechanism that lets a pairing replace the credential on a live connection
// instead of waiting for the link to drop on its own.
func TestRequestRepairCancelsLiveSession(t *testing.T) {
	c := NewClient(Config{URL: "http://hub.example"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.setSessCancel(cancel)

	c.requestRepair()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("requestRepair did not cancel the session context")
	}
}

// requestRepair is also safe with no session in flight — Pair is reachable from
// the tray before Run has started.
func TestRequestRepairWithoutSession(t *testing.T) {
	c := NewClient(Config{URL: "http://hub.example"})
	c.requestRepair() // must not panic on a nil sessCancel
	select {
	case <-c.repairCh:
	default:
		t.Error("repairCh not signalled")
	}
	// The channel is buffered 1 and carries a level, not a queue: repeated
	// requests must not block.
	c.requestRepair()
	c.requestRepair()
}
