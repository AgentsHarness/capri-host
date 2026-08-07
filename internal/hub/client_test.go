package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
)

// fakeHub stands in for acp-hub in tests.
type fakeHub struct {
	t            *testing.T
	pairOK       bool // pair succeeds only when code == "ABC123"
	streamFrames []string

	mu          sync.Mutex
	pairCalls   int
	streamCalls int
	events      []map[string]any
	responds    []map[string]any

	eventsCh   chan map[string]any
	respondsCh chan map[string]any
}

func newFakeHub(t *testing.T) *fakeHub {
	return &fakeHub{
		t:          t,
		pairOK:     true,
		eventsCh:   make(chan map[string]any, 64),
		respondsCh: make(chan map[string]any, 64),
	}
}

func (f *fakeHub) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/pair", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.pairCalls++
		f.mu.Unlock()
		var body struct {
			Code     string `json:"code"`
			HostID   string `json:"hostId"`
			HostName string `json:"hostName"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if !f.pairOK || body.Code != "ABC123" {
			writeTestJSON(w, 401, map[string]any{"ok": false, "error": "配对码无效或已过期"})
			return
		}
		writeTestJSON(w, 200, map[string]any{"ok": true, "token": "tok123", "hostId": body.HostID})
	})

	mux.HandleFunc("POST /api/hub/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok123" {
			writeTestJSON(w, 401, map[string]any{"ok": false, "error": "token 无效"})
			return
		}
		var body struct {
			HostID string           `json:"hostId"`
			Events []map[string]any `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.events = append(f.events, body.Events...)
		f.mu.Unlock()
		for _, ev := range body.Events {
			f.eventsCh <- ev
		}
		writeTestJSON(w, 200, map[string]any{"ok": true})
	})

	mux.HandleFunc("POST /api/hub/respond", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok123" {
			writeTestJSON(w, 401, map[string]any{"ok": false, "error": "token 无效"})
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.responds = append(f.responds, body)
		f.mu.Unlock()
		f.respondsCh <- body
		writeTestJSON(w, 200, map[string]any{"ok": true})
	})

	mux.HandleFunc("GET /api/hub/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok123" {
			writeTestJSON(w, 401, map[string]any{"ok": false, "error": "token 无效"})
			return
		}
		f.mu.Lock()
		f.streamCalls++
		frames := f.streamFrames
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for _, fr := range frames {
			fmt.Fprintf(w, "data: %s\n\n", fr)
			fl.Flush()
		}
		<-r.Context().Done()
	})

	return mux
}

func writeTestJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func TestEnsureTokenPairsAndPersists(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()
	stateFile := filepath.Join(t.TempDir(), "hub.json")

	c := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", PairCode: "ABC123", StateFile: stateFile})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	if c.token != "tok123" {
		t.Errorf("token = %q, want tok123", c.token)
	}
	if got := fh.countPairCalls(); got != 1 {
		t.Errorf("pair calls = %d, want 1", got)
	}

	// A second client (e.g. after restart) loads the token from disk
	// without pairing again.
	c2 := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", StateFile: stateFile})
	if err := c2.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken (restart): %v", err)
	}
	if c2.token != "tok123" {
		t.Errorf("restart token = %q, want tok123", c2.token)
	}
	if got := fh.countPairCalls(); got != 1 {
		t.Errorf("pair calls after restart = %d, want 1 (no re-pair)", got)
	}
}

func TestEnsureTokenRejectsBadCode(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", PairCode: "WRONG", StateFile: filepath.Join(t.TempDir(), "hub.json")})
	if err := c.ensureToken(context.Background()); err == nil {
		t.Fatal("ensureToken should fail with a bad pair code")
	}
	if c.token != "" {
		t.Error("token must stay empty on pair failure")
	}
}

func TestForwardLoop(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json")})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.forwardLoop(ctx, bridge)

	time.Sleep(150 * time.Millisecond) // let the subscription register
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "hi from host"})

	select {
	case ev := <-fh.eventsCh:
		if ev["type"] != "chunk" || ev["text"] != "hi from host" {
			t.Errorf("forwarded event = %v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event forwarded to hub")
	}
}

func TestStreamRelayRoundTrip(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"type":"hello","service":"hub"}`,
		`{"type":"request","reqId":"r1","method":"POST","path":"/api/prompt","body":{"blocks":[{"type":"text","text":"hi"}]}}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/prompt" || r.Method != "POST" {
			writeTestJSON(w, 404, map[string]any{"ok": false, "error": "not found"})
			return
		}
		var body struct {
			Blocks []acp.ContentBlock `json:"blocks"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body.Blocks) != 1 {
			writeTestJSON(w, 400, map[string]any{"ok": false, "error": "bad body"})
			return
		}
		writeTestJSON(w, 200, map[string]any{"ok": true, "stopReason": "end_turn"})
	}))
	defer local.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", LocalBase: local.URL, StateFile: filepath.Join(t.TempDir(), "hub.json")})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.streamLoop(ctx, bridge) }()

	select {
	case resp := <-fh.respondsCh:
		if resp["reqId"] != "r1" || resp["hostId"] != "h1" {
			t.Errorf("respond = %v, want reqId r1 hostId h1", resp)
		}
		if resp["status"] != float64(200) {
			t.Errorf("respond status = %v, want 200", resp["status"])
		}
		body, _ := json.Marshal(resp["body"])
		if !bytes.Contains(body, []byte(`"stopReason":"end_turn"`)) {
			t.Errorf("respond body = %s", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no respond posted to hub")
	}
}

func TestRunForwardsEvents(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json")})
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, bridge)

	deadline := time.After(3 * time.Second)
	time.Sleep(300 * time.Millisecond) // let the stream connect + forwardLoop subscribe
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "via Run"})
	for {
		select {
		case ev := <-fh.eventsCh:
			if ev["type"] == "chunk" && ev["text"] == "via Run" {
				return // wired end to end
			}
		case <-deadline:
			t.Fatal("Run did not forward bridge events to the hub")
		}
	}
}

func TestStreamAuthFailure(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "stale-token", StateFile: filepath.Join(t.TempDir(), "hub.json")})
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})
	err := c.streamLoop(context.Background(), bridge)
	if !isAuthErr(err) {
		t.Errorf("streamLoop err = %v, want 401 auth error", err)
	}
}

func (f *fakeHub) countPairCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pairCalls
}
