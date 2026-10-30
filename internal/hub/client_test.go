package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
)

func replayItemsOf(evs []acp.Event) []replayItem {
	out := make([]replayItem, 0, len(evs))
	for _, ev := range evs {
		raw, err := json.Marshal(ev)
		if err != nil {
			panic(err)
		}
		out = append(out, replayItem{seq: eventSeq(ev), raw: raw})
	}
	return out
}

// fakeHub stands in for capri-hub in tests (WS transport).
type fakeHub struct {
	t            *testing.T
	pairOK       bool // pair succeeds only when code == "ABC123"
	streamFrames []string
	// rawStream sends streamFrames verbatim instead of filling in the
	// mandatory hello fields (see completeHello) — for tests that exercise
	// the version gate against an outdated hub.
	rawStream bool

	mu          sync.Mutex
	pairCalls   int
	streamCalls int
	events      []map[string]any
	responds    []map[string]any

	eventsCh   chan map[string]any
	respondsCh chan map[string]any
	// hostStatusCh receives every host_status frame, for heartbeat pacing.
	hostStatusCh chan map[string]any
	// acks receives every state_ack frame, for the confirmed-state tests.
	acks chan map[string]any
}

func newFakeHub(t *testing.T) *fakeHub {
	return &fakeHub{
		t:            t,
		pairOK:       true,
		eventsCh:     make(chan map[string]any, 64),
		respondsCh:   make(chan map[string]any, 64),
		hostStatusCh: make(chan map[string]any, 64),
		acks:         make(chan map[string]any, 64),
	}
}

// completeHello fills the hello fields every current host build requires but
// that a test does not care about, so a test can write only the fields it
// exercises. Non-hello frames pass through untouched.
func completeHello(frame string) string {
	var msg map[string]any
	if json.Unmarshal([]byte(frame), &msg) != nil || msg["type"] != "hello" {
		return frame
	}
	defaults := map[string]any{
		"seq":          float64(0),
		"subscribers":  float64(0),
		"deflate":      true,
		"pingInterval": float64(5),
		"idleTimeout":  float64(20),
	}
	for k, v := range defaults {
		if _, ok := msg[k]; !ok {
			msg[k] = v
		}
	}
	out, err := json.Marshal(msg)
	if err != nil {
		return frame
	}
	return string(out)
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

	mux.HandleFunc("GET /ws/host", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok123" {
			http.Error(w, "token 无效", http.StatusUnauthorized)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")

		f.mu.Lock()
		f.streamCalls++
		frames := append([]string(nil), f.streamFrames...)
		raw := f.rawStream
		f.mu.Unlock()

		ctx := r.Context()
		for _, fr := range frames {
			if !raw {
				fr = completeHello(fr)
			}
			if err := conn.Write(ctx, websocket.MessageText, []byte(fr)); err != nil {
				return
			}
		}

		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var frame struct {
				Type   string           `json:"type"`
				Events []map[string]any `json:"events"`
				ReqID  string           `json:"reqId"`
				Status int              `json:"status"`
				Body   json.RawMessage  `json:"body"`
			}
			if json.Unmarshal(data, &frame) != nil {
				continue
			}
			switch frame.Type {
			case "events":
				f.mu.Lock()
				f.events = append(f.events, frame.Events...)
				f.mu.Unlock()
				for _, ev := range frame.Events {
					f.eventsCh <- ev
				}
			case "host_status":
				var parsed map[string]any
				if json.Unmarshal(data, &parsed) == nil {
					f.hostStatusCh <- parsed
				}
			case "state_ack":
				var parsed map[string]any
				if json.Unmarshal(data, &parsed) == nil {
					f.acks <- parsed
				}
			case "respond":
				body := map[string]any{
					"hostId": "h1",
					"reqId":  frame.ReqID,
					"status": frame.Status,
				}
				var parsed any
				if json.Unmarshal(frame.Body, &parsed) == nil {
					body["body"] = parsed
				}
				f.mu.Lock()
				f.responds = append(f.responds, body)
				f.mu.Unlock()
				f.respondsCh <- body
			}
		}
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

	c := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", PairCode: "ABC123", StateFile: stateFile, DisableQUIC: true})
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
	c2 := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", StateFile: stateFile, DisableQUIC: true})
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

	c := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", PairCode: "WRONG", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
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

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	// Hub would push subscribers>0 when a browser is listening.
	c.setBrowserSubscribers(1)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Drain sendCh into eventsCh like a connected writeLoop would.
	go drainSendCh(ctx, c, fh)
	go c.forwardLoop(ctx, bridge)

	time.Sleep(80 * time.Millisecond) // let the subscription register
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

func TestForwardLoopPausedWhenNoBrowsers(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	// Explicitly idle: no browser subscribers → no event upload.
	c.setBrowserSubscribers(0)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drainSendCh(ctx, c, fh)
	go c.forwardLoop(ctx, bridge)

	time.Sleep(80 * time.Millisecond)
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "should not upload"})

	select {
	case ev := <-fh.eventsCh:
		// host_status heartbeats are OK; bridge chunk events must not appear.
		if ev["type"] == "chunk" {
			t.Fatalf("paused forwardLoop uploaded chunk: %v", ev)
		}
	case <-time.After(400 * time.Millisecond):
		// expected: nothing (or only future heartbeats, which we didn't wait for)
	}

	// Resume and confirm traffic starts.
	c.setBrowserSubscribers(1)
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "now online"})
	select {
	case ev := <-fh.eventsCh:
		if ev["type"] != "chunk" || ev["text"] != "now online" {
			// May receive host_status first if a heartbeat races; keep reading.
			deadline := time.After(3 * time.Second)
			for ev["type"] != "chunk" {
				select {
				case ev = <-fh.eventsCh:
				case <-deadline:
					t.Fatalf("no chunk after resume, last=%v", ev)
				}
			}
			if ev["text"] != "now online" {
				t.Errorf("chunk after resume = %v", ev)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event after resuming subscribers")
	}
}

func TestStreamHelloSubscribersGate(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"subscribers","count":1}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.wsSession(ctx, bridge) }()

	// Wait until the subscribers frame enables forwarding.
	deadline := time.After(3 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled after subscribers:1")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestStreamRelayRoundTrip(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"request","reqId":"r1","method":"POST","path":"/api/prompt","body":{"blocks":[{"type":"text","text":"hi"}]}}`,
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

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", LocalBase: local.URL, StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.wsSession(ctx, bridge) }()

	select {
	case resp := <-fh.respondsCh:
		if resp["reqId"] != "r1" {
			t.Errorf("respond = %v, want reqId r1", resp)
		}
		if resp["status"] != float64(200) && resp["status"] != 200 {
			t.Errorf("respond status = %v (%T), want 200", resp["status"], resp["status"])
		}
		body, _ := json.Marshal(resp["body"])
		if !bytes.Contains(body, []byte(`"stopReason":"end_turn"`)) {
			t.Errorf("respond body = %s", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no respond posted to hub")
	}
}

// A relayed request stamped with another host's hostId must be refused
// without touching the local HTTP API — the host only executes requests
// the hub routed to it.
func TestStreamRelayRejectsForeignHostID(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"request","reqId":"r9","hostId":"h2","method":"POST","path":"/api/prompt","body":{"blocks":[{"type":"text","text":"hi"}]}}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	hits := make(chan string, 1)
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits <- r.URL.Path // must never fire
		writeTestJSON(w, 200, map[string]any{"ok": true})
	}))
	defer local.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", LocalBase: local.URL, StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.wsSession(ctx, bridge) }()

	select {
	case resp := <-fh.respondsCh:
		if resp["reqId"] != "r9" {
			t.Errorf("respond = %v, want reqId r9", resp)
		}
		if resp["status"] != float64(404) && resp["status"] != 404 {
			t.Errorf("respond status = %v, want 404", resp["status"])
		}
		body, _ := json.Marshal(resp["body"])
		if !bytes.Contains(body, []byte("host 未配对")) {
			t.Errorf("respond body = %s, want host 未配对 error", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no respond posted to hub")
	}

	select {
	case p := <-hits:
		t.Fatalf("local API was hit with foreign hostId: %s", p)
	default:
		// expected — the foreign request never reached the local API
	}
}

func TestRunForwardsEvents(t *testing.T) {
	fh := newFakeHub(t)
	// Stream hello announces a listening browser so forwardLoop is armed.
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":1}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, bridge)

	deadline := time.After(3 * time.Second)
	// Wait until stream hello enables forwarding.
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled from stream hello")
		case <-time.After(20 * time.Millisecond):
		}
	}
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

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "stale-token", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})
	err := c.wsSession(context.Background(), bridge)
	if !isAuthErr(err) {
		t.Errorf("wsSession err = %v, want 401 auth error", err)
	}
}

// TestForwardLoopNoMerge is the P0 regression: hub must forward each
// bridge event with its original seq and text. Merging chunks into one
// "abc" event (keeping seq 1) collided with local SSE seq 1,2,3 "a","b","c"
// and broke dual-path FE dedup.
func TestForwardLoopNoMerge(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	c.setBrowserSubscribers(1)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drainSendCh(ctx, c, fh)
	go c.forwardLoop(ctx, bridge)

	time.Sleep(80 * time.Millisecond)
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "a", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "b", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "c", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "done", "sessionId": "s1"})

	var chunks []string
	var seqs []float64
	sawDone := false
	deadline := time.After(3 * time.Second)
	for !sawDone || len(chunks) < 3 {
		select {
		case ev := <-fh.eventsCh:
			typ, _ := ev["type"].(string)
			if typ != "chunk" && typ != "done" {
				continue
			}
			if s, ok := ev["seq"].(float64); ok {
				seqs = append(seqs, s)
			}
			if typ == "chunk" {
				if txt, ok := ev["text"].(string); ok {
					chunks = append(chunks, txt)
				}
			} else if typ == "done" {
				sawDone = true
			}
		case <-deadline:
			t.Fatalf("timed out: chunks=%v seqs=%v sawDone=%v", chunks, seqs, sawDone)
		}
	}
	if strings.Join(chunks, "") != "abc" {
		t.Errorf("chunk texts = %v, want a,b,c separately (joined abc)", chunks)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3 unmerged wire events (got %v)", len(chunks), chunks)
	}
	// Bridge assigns contiguous global seq 1,2,3,4 for a,b,c,done.
	want := []float64{1, 2, 3, 4}
	if len(seqs) != len(want) {
		t.Fatalf("seqs = %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seqs = %v, want %v (no merge, bridge seqs preserved)", seqs, want)
		}
	}
	c.seqMu.Lock()
	n := len(c.replay)
	c.seqMu.Unlock()
	if n != 4 {
		t.Errorf("replay len = %d, want 4 (all unmerged events)", n)
	}
}

// TestForwardLoopSeqAssignedAfterMerge kept as an alias name for
// greppability of the old regression; behavior is no-merge contiguous
// bridge seqs (same as TestForwardLoopNoMerge).
func TestForwardLoopSeqAssignedAfterMerge(t *testing.T) {
	TestForwardLoopNoMerge(t)
}

// TestHostStatusIsControlFrame: host_status must be a control frame
// {"v":1,"type":"host_status","ready":…,"busy":…,"booting":…,"pendingCount":…}
// — no seq, not an events frame, not in the replay buffer — so it cannot
// collide with bridge.Broadcast seq.
func TestHostStatusIsControlFrame(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", Port: 8765, DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	c.enqueueHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad frame: %s", payload)
		}
		if f["type"] != "host_status" {
			t.Fatalf("type = %v, want host_status (control frame), payload=%s", f["type"], payload)
		}
		if _, ok := f["seq"]; ok {
			t.Errorf("host_status must not carry seq, got %v", f["seq"])
		}
		if _, ok := f["events"]; ok {
			t.Errorf("host_status must not be an events frame, payload=%s", payload)
		}
		for _, k := range []string{"ready", "busy", "booting", "pendingCount", "port"} {
			if _, ok := f[k]; !ok {
				t.Errorf("host_status missing %s: %s", k, payload)
			}
		}
		if int(f["port"].(float64)) != 8765 {
			t.Errorf("port = %v, want 8765", f["port"])
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status frame")
	}
	// Not stored in replay (control plane only).
	c.seqMu.Lock()
	n := len(c.replay)
	next := c.nextSeq
	c.seqMu.Unlock()
	if n != 0 {
		t.Errorf("replay len = %d, want 0 (host_status is not data-plane)", n)
	}
	if next != 0 {
		t.Errorf("nextSeq = %d, want 0 (host_status must not bump data-plane seq)", next)
	}

	// After bridge Broadcast seq=1 and a host_status, next bridge event is seq=2.
	c.setBrowserSubscribers(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.forwardLoop(ctx, bridge)
	time.Sleep(50 * time.Millisecond)
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "one", "sessionId": "s1"})
	// Wait until seq lands in replay.
	deadline := time.After(2 * time.Second)
	for {
		c.seqMu.Lock()
		got := c.nextSeq
		c.seqMu.Unlock()
		if got >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("bridge event never reached hub client")
		case <-time.After(20 * time.Millisecond):
		}
	}
	c.enqueueHostStatus(bridge) // must not advance nextSeq
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "two", "sessionId": "s1"})
	deadline = time.After(2 * time.Second)
	for {
		c.seqMu.Lock()
		got := c.nextSeq
		c.seqMu.Unlock()
		if got >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("second bridge event never reached hub client (nextSeq=%d)", got)
		case <-time.After(20 * time.Millisecond):
		}
	}
	// Drain any host_status control frames; ensure they have no seq.
	for {
		select {
		case payload := <-c.sendCh:
			var f map[string]any
			if json.Unmarshal(payload, &f) != nil {
				continue
			}
			if f["type"] == "host_status" {
				if _, ok := f["seq"]; ok {
					t.Errorf("control host_status carried seq: %s", payload)
				}
			}
		default:
			return
		}
	}
}

// statusBridge is a minimal bridgeSource for host_status tests: Snapshot
// returns a Status the test mutates, and Subscribe hands back a channel
// the test pushes events into.
type statusBridge struct {
	mu sync.Mutex
	st acp.Status
	ch chan acp.Event
}

func newStatusBridge() *statusBridge {
	return &statusBridge{
		st: acp.Status{Ready: true},
		ch: make(chan acp.Event, 64),
	}
}

func (b *statusBridge) Subscribe() (chan acp.Event, func()) {
	return b.ch, func() {}
}

func (b *statusBridge) Snapshot() acp.Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.st
}

func (b *statusBridge) setBusy(v bool) {
	b.mu.Lock()
	b.st.Busy = v
	b.mu.Unlock()
}

func (b *statusBridge) setPending(n int) {
	b.mu.Lock()
	b.st.PendingRequests = make([]acp.PendingReq, n)
	b.mu.Unlock()
}

// TestMaybeNotifyHostStatus: a status change pushes a host_status frame
// immediately (control plane — no seq, no browser-subscriber gate);
// an unchanged status sends nothing.
func TestMaybeNotifyHostStatus(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)
	bridge := newStatusBridge()

	// Baseline frame records the sent fields.
	c.enqueueHostStatus(bridge)
	select {
	case <-c.sendCh:
	case <-time.After(time.Second):
		t.Fatal("no baseline host_status")
	}

	// Unchanged → silent.
	c.maybeNotifyHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		t.Fatalf("unchanged status must not resend, got %s", payload)
	default:
	}

	// Busy flips → immediate frame with busy=true.
	bridge.setBusy(true)
	c.maybeNotifyHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad frame: %s", payload)
		}
		if f["type"] != "host_status" || f["busy"] != true {
			t.Errorf("frame = %v, want host_status busy=true", f)
		}
		if _, ok := f["seq"]; ok {
			t.Errorf("host_status must not carry seq: %s", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status on busy flip")
	}

	// Busy → idle again → another immediate frame.
	bridge.setBusy(false)
	c.maybeNotifyHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad frame: %s", payload)
		}
		if f["busy"] != false {
			t.Errorf("frame = %v, want busy=false", f)
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status on idle flip")
	}

	// Unchanged again → silent.
	c.maybeNotifyHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		t.Fatalf("unchanged status must not resend, got %s", payload)
	default:
	}
}

// TestMaybeNotifyHostStatusPendingCount: pendingCount flips also push an
// immediate host_status frame.
func TestMaybeNotifyHostStatusPendingCount(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)
	bridge := newStatusBridge()

	c.enqueueHostStatus(bridge)
	select {
	case <-c.sendCh:
	case <-time.After(time.Second):
		t.Fatal("no baseline host_status")
	}

	bridge.setPending(2)
	c.maybeNotifyHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad frame: %s", payload)
		}
		if got, ok := f["pendingCount"].(float64); !ok || int(got) != 2 {
			t.Errorf("frame = %v, want pendingCount=2", f)
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status on pendingCount flip")
	}

	// A resolved request DECREASES pendingCount. The forward loop must push
	// that immediately; waiting for the heartbeat means 20s of stale state
	// on a probing hub.
	bridge.setPending(0)
	evCh, _ := bridge.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.forwardLoop(ctx, bridge)
	evCh <- acp.Event{"type": "client_request_resolved", "sessionId": "s1"}
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad frame: %s", payload)
		}
		if f["type"] != "host_status" {
			t.Fatalf("first frame after resolve = %v, want host_status", f)
		}
		if got, ok := f["pendingCount"].(float64); !ok || int(got) != 0 {
			t.Errorf("frame = %v, want pendingCount=0", f)
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status after client_request_resolved")
	}
}

// TestForwardLoopNotifiesHostStatus: a busy event in the forward loop
// pushes an immediate host_status frame even while event upload is paused
// (control frames are not gated on browser subscribers); a chunk event
// does not.
func TestForwardLoopNotifiesHostStatus(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 8)
	c.setBrowserSubscribers(0) // paused upload — control frames must still flow
	bridge := newStatusBridge()
	evCh, _ := bridge.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.forwardLoop(ctx, bridge)

	// Chunk traffic must not trigger a status frame.
	evCh <- acp.Event{"type": "chunk", "text": "x", "sessionId": "s1"}
	select {
	case payload := <-c.sendCh:
		t.Fatalf("chunk must not push host_status, got %s", payload)
	case <-time.After(120 * time.Millisecond):
		// expected (flush window passed)
	}

	// Busy transition → immediate host_status with busy=true.
	bridge.setBusy(true)
	evCh <- acp.Event{"type": "busy", "sessionId": "s1"}
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad frame: %s", payload)
		}
		if f["type"] != "host_status" || f["busy"] != true {
			t.Errorf("frame = %v, want host_status busy=true", f)
		}
		if _, ok := f["seq"]; ok {
			t.Errorf("host_status must not carry seq: %s", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status on busy event")
	}

	cancel()
}

// TestPausedStillReplaysOnResume: while browser subscribers are 0, bridge
// events must still enter the replay buffer (not discarded). On 0→1 the
// host catch-up via sendReplayAfter(lastSentSeq) must deliver them.
func TestPausedStillReplaysOnResume(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	c.setBrowserSubscribers(0)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drainSendCh(ctx, c, fh)
	go c.forwardLoop(ctx, bridge)

	time.Sleep(80 * time.Millisecond)
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "paused-a", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "paused-b", "sessionId": "s1"})

	// Wait until both events are in the replay buffer (flush is 50ms).
	deadline := time.After(2 * time.Second)
	for {
		c.seqMu.Lock()
		n := len(c.replay)
		c.seqMu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("paused events never entered replay (len=%d)", n)
		case <-time.After(20 * time.Millisecond):
		}
	}
	// While paused, nothing should have been enqueued for uplink.
	select {
	case ev := <-fh.eventsCh:
		if ev["type"] == "chunk" {
			t.Fatalf("paused forwardLoop uploaded chunk: %v", ev)
		}
	case <-time.After(150 * time.Millisecond):
		// expected
	}

	// Simulate subscribers 0→1 handler: resume catch-up from lastSentSeq.
	c.seqMu.Lock()
	after := c.lastSentSeq
	c.seqMu.Unlock()
	c.setBrowserSubscribers(1)
	if last := c.sendReplayAfter(after); last == 0 {
		t.Fatal("sendReplayAfter returned 0, want paused chunks replayed")
	}

	var got []string
	deadline = time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case ev := <-fh.eventsCh:
			if ev["type"] != "chunk" {
				continue
			}
			txt, _ := ev["text"].(string)
			got = append(got, txt)
		case <-deadline:
			t.Fatalf("resume replay timed out: got %v", got)
		}
	}
	if got[0] != "paused-a" || got[1] != "paused-b" {
		t.Errorf("replayed chunks = %v, want [paused-a paused-b]", got)
	}
}

// TestHandleHelloSeqEpochReset: after host process restart, local seq
// space restarts at 0/1 while hub still holds a high LastSeq. hello must
// NOT raise lastSentSeq to that alien value (or 0→1 catch-up dies), and
// must emit a seq_reset control frame so hub clears its watermark.
func TestHandleHelloSeqEpochReset(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 16)

	// Simulate a prior process that had reached seq 1000 on the hub, then
	// this fresh client process has only local events 1..2 in replay.
	c.seqAndReplay([]acp.Event{
		{"type": "chunk", "text": "a", "seq": uint64(1)},
		{"type": "chunk", "text": "b", "seq": uint64(2)},
	})
	// nextSeq should track 2; lastSentSeq still 0 (never enqueued).
	c.seqMu.Lock()
	if c.nextSeq != 2 {
		t.Fatalf("nextSeq = %d, want 2", c.nextSeq)
	}
	c.lastSentSeq = 0
	c.seqMu.Unlock()

	c.handleHelloSeq(1000)

	// lastSentSeq must stay 0 (not polluted by hub's 1000).
	c.seqMu.Lock()
	if c.lastSentSeq != 0 && c.lastSentSeq != 2 {
		// May be raised by noteLastSentSeq during critical replay enqueue.
		// Accept 0 (nothing enqueued yet if queue full) or 2 (replay landed).
		// Must NOT be 1000.
		if c.lastSentSeq >= 1000 {
			t.Fatalf("lastSentSeq = %d, must not adopt alien hub.seq=1000", c.lastSentSeq)
		}
	}
	if c.lastSentSeq >= 1000 {
		t.Fatalf("lastSentSeq = %d, must not adopt alien hub.seq=1000", c.lastSentSeq)
	}
	c.seqMu.Unlock()

	// First frame must be seq_reset (critical, before replay events).
	var sawReset, sawReplay bool
	deadline := time.After(2 * time.Second)
	for !sawReset || !sawReplay {
		select {
		case payload := <-c.sendCh:
			var f map[string]any
			if json.Unmarshal(payload, &f) != nil {
				continue
			}
			switch f["type"] {
			case "seq_reset":
				sawReset = true
			case "events":
				sawReplay = true
				evs, _ := f["events"].([]any)
				if len(evs) < 1 {
					t.Errorf("replay events frame empty: %s", payload)
				}
			}
		case <-deadline:
			t.Fatalf("timeout: sawReset=%v sawReplay=%v", sawReset, sawReplay)
		}
	}
	if !sawReset {
		t.Error("expected seq_reset control frame on epoch mismatch")
	}
}

// TestHandleHelloSeqSameProcess: when hub.seq is within this process's
// seq space, hello raises lastSentSeq and resumes after that point only.
func TestHandleHelloSeqSameProcess(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 16)

	c.seqAndReplay([]acp.Event{
		{"type": "chunk", "text": "a", "seq": uint64(1)},
		{"type": "chunk", "text": "b", "seq": uint64(2)},
		{"type": "chunk", "text": "c", "seq": uint64(3)},
	})
	c.seqMu.Lock()
	c.lastSentSeq = 1 // hub already has seq 1
	c.seqMu.Unlock()

	c.handleHelloSeq(1)

	c.seqMu.Lock()
	if c.lastSentSeq != 1 {
		// After resume, noteLastSentSeq may advance to 3 from replay of 2,3.
		if c.lastSentSeq < 1 {
			t.Fatalf("lastSentSeq = %d, want >= 1", c.lastSentSeq)
		}
	}
	c.seqMu.Unlock()

	// Should NOT emit seq_reset.
	deadline := time.After(time.Second)
	var texts []string
	for {
		select {
		case payload := <-c.sendCh:
			var f map[string]any
			if json.Unmarshal(payload, &f) != nil {
				continue
			}
			if f["type"] == "seq_reset" {
				t.Fatal("same-process resume must not emit seq_reset")
			}
			if f["type"] == "events" {
				// Collect chunk texts from the frame.
				raw, _ := json.Marshal(f["events"])
				var evs []map[string]any
				_ = json.Unmarshal(raw, &evs)
				for _, ev := range evs {
					if ev["type"] == "chunk" {
						if txt, ok := ev["text"].(string); ok {
							texts = append(texts, txt)
						}
					}
				}
				if len(texts) >= 2 {
					// seq 2,3 = b,c
					if texts[0] != "b" || texts[1] != "c" {
						t.Errorf("replay texts = %v, want [b c]", texts)
					}
					return
				}
			}
		case <-deadline:
			if len(texts) == 0 {
				t.Fatal("no replay events after same-process hello")
			}
			return
		}
	}
}

// TestDrainSendChOnSession: drainSendCh empties the uplink queue so a
// reconnect cannot re-send stale frames alongside hello-driven replay.
func TestDrainSendChOnSession(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 8)
	c.sendCh <- []byte(`{"v":1,"type":"events","stale":true}`)
	c.sendCh <- []byte(`{"v":1,"type":"ping"}`)
	if len(c.sendCh) != 2 {
		t.Fatalf("precondition: sendCh len = %d, want 2", len(c.sendCh))
	}
	c.drainSendCh()
	if len(c.sendCh) != 0 {
		t.Fatalf("after drainSendCh len = %d, want 0", len(c.sendCh))
	}
	// Second drain is a no-op.
	c.drainSendCh()
	if len(c.sendCh) != 0 {
		t.Fatalf("second drain left %d frames", len(c.sendCh))
	}
}

// TestIsAuthErrTight: isAuthErr must match a handshake 401 and the
// auth-specific phrases, but not a bare "token", and — the regression this
// exists for — not a WS close frame with an ambiguous or unrelated reason.
// The library's Error() text used to be matched for "StatusPolicyViolation",
// which every policy close renders, so a superseded link re-paired.
func TestIsAuthErrTight(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{&wsHandshakeError{status: 401, err: errors.New("bad handshake")}, true},
		{&wsHandshakeError{status: 403, err: errors.New("forbidden")}, false},
		{errNoToken, true},
		{fmt.Errorf("auth failed: bad credentials"), true},
		// Close frames: only an explicit "auth" reason is a credential
		// problem. The status text the library renders is NOT a signal.
		{websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "auth"}, true},
		{websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "liveness probe timeout"}, false},
		{websocket.CloseError{Code: websocket.StatusPolicyViolation}, false},
		{websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: "superseded"}, false},
		{fmt.Errorf("websocket: StatusPolicyViolation"), false},
		{fmt.Errorf("missing host token bucket config"), false},
		{fmt.Errorf("token expired quietly"), false},
		{fmt.Errorf("network timeout"), false},
	}
	for _, tc := range cases {
		if got := isAuthErr(tc.err); got != tc.want {
			t.Errorf("isAuthErr(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// TestClassifyClose: the close frame is the contract, and each meaning maps
// to one reconnect policy. A hub that predates the contract sends no reason;
// its code alone is ambiguous, so it must land in closeOther (old behavior),
// never in closeAuth.
func TestClassifyClose(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want closeKind
	}{
		{"nil", nil, closeOther},
		{"not a close frame", fmt.Errorf("hub ws dial: connection refused"), closeOther},
		{"auth", websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: closeReasonAuth}, closeAuth},
		{"superseded", websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: closeReasonSuperseded}, closeSuperseded},
		{"shutdown", websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: closeReasonShutdown}, closeShutdown},
		{"liveness", websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: closeReasonLiveness}, closeLiveness},
		// Old hub: no reason at all.
		{"old hub 1000 no reason", websocket.CloseError{Code: websocket.StatusNormalClosure}, closeOther},
		{"old hub 1008 no reason", websocket.CloseError{Code: websocket.StatusPolicyViolation}, closeOther},
		{"old hub 1011 no reason", websocket.CloseError{Code: websocket.StatusInternalError}, closeOther},
		// Unknown reason from a future hub: not interpretable, must not be
		// guessed at.
		{"unknown reason", websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "rate limited"}, closeOther},
		// The CloseError is normally wrapped by the read path.
		{"wrapped", fmt.Errorf("read: %w", websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: closeReasonSuperseded}), closeSuperseded},
		{"wrapped old hub", fmt.Errorf("read: %w", websocket.CloseError{Code: websocket.StatusPolicyViolation}), closeOther},
	}
	for _, tc := range cases {
		if got := classifyClose(tc.err); got != tc.want {
			t.Errorf("classifyClose(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestClassifyCloseQUIC: the QUIC transport has no equivalent of the WS
// close frame, so the hub encodes the same verdicts in the application error
// code (see errCode* in acp-hub's cmd/capri-hub/main.go). The codes with no
// host-side action — uplink idle, ping write, unreadable — and any code this
// build does not know must stay closeOther: the pre-existing "back off and
// redial" path, never a re-pair.
func TestClassifyCloseQUIC(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"application error 0", &quic.ApplicationError{ErrorCode: 0, ErrorMessage: "no stream"}},
		{"application error 0 empty", &quic.ApplicationError{ErrorCode: 0}},
		{"application error 1", &quic.ApplicationError{ErrorCode: 1, ErrorMessage: "internal error"}},
		{"ordinary drop: uplink idle", &quic.ApplicationError{ErrorCode: quicCodeUplinkIdle}},
		{"ordinary drop: ping write", &quic.ApplicationError{ErrorCode: quicCodePingWrite}},
		{"ordinary drop: unreadable", &quic.ApplicationError{ErrorCode: quicCodeUnreadable}},
		{"unknown future code", &quic.ApplicationError{ErrorCode: 99}},
		{"transport error", &quic.TransportError{ErrorCode: quic.NoError, ErrorMessage: "closed by peer"}},
	}
	for _, tc := range cases {
		if got := classifyClose(tc.err); got != closeOther {
			t.Errorf("classifyClose(%s) = %v, want closeOther", tc.name, got)
		}
		if isAuthErr(tc.err) {
			t.Errorf("isAuthErr(%s) = true; a QUIC close must never trigger a re-pair", tc.name)
		}
	}
}

// TestSupersededIsNotAuth is the P0 regression: a superseded close used to
// match isAuthErr through "StatusPolicyViolation" in the rendered error
// text, so the host wiped a working token and burned a pairing code. It must
// not re-pair, and it must redial at once.
func TestSupersededIsNotAuth(t *testing.T) {
	err := error(websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: closeReasonSuperseded})
	if isAuthErr(err) {
		t.Fatal("superseded close classified as auth: a working token would be cleared and re-paired")
	}
	if got := classifyClose(err); got != closeSuperseded {
		t.Fatalf("classifyClose = %v, want closeSuperseded", got)
	}
	// The pre-contract shape of the same event (old hub: 1000, no reason)
	// must not be auth either.
	if isAuthErr(websocket.CloseError{Code: websocket.StatusNormalClosure}) {
		t.Fatal("old-hub normal close classified as auth")
	}
}

// TestEventSeqTypes: eventSeq accepts uint64/float64/int/int64/json.Number.
func TestEventSeqTypes(t *testing.T) {
	if eventSeq(acp.Event{"seq": uint64(7)}) != 7 {
		t.Error("uint64")
	}
	if eventSeq(acp.Event{"seq": float64(8)}) != 8 {
		t.Error("float64")
	}
	if eventSeq(acp.Event{"seq": int(9)}) != 9 {
		t.Error("int")
	}
	if eventSeq(acp.Event{"seq": int64(10)}) != 10 {
		t.Error("int64")
	}
	if eventSeq(acp.Event{"seq": json.Number("11")}) != 11 {
		t.Error("json.Number")
	}
	if eventSeq(acp.Event{"seq": "nope"}) != 0 {
		t.Error("string should be 0")
	}
}

// TestSendReplayAfterChunksFrames is the P0 regression: a large replay
// backlog must be packed into frames of at most replayFrameBudget bytes
// (one giant frame would exceed the hub read limit and livelock the
// resume), with seqs contiguous across frames.
func TestSendReplayAfterChunksFrames(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 64)

	// 100 events × ~40KB ≈ 4MB backlog — far beyond one frame budget.
	const n = 100
	backlog := make([]acp.Event, 0, n)
	for i := 1; i <= n; i++ {
		backlog = append(backlog, acp.Event{
			"seq":  uint64(i),
			"type": "chunk",
			"text": strings.Repeat("x", 40<<10),
		})
	}
	c.seqAndReplay(backlog)

	if last := c.sendReplayAfter(0); last != n {
		t.Fatalf("sendReplayAfter last = %d, want %d", last, n)
	}

	var total, frames int
	var prevSeq float64
	for total < n {
		select {
		case payload := <-c.sendCh:
			frames++
			if len(payload) > replayFrameBudget+64<<10 {
				t.Fatalf("frame %d too large: %d bytes (budget %d)", frames, len(payload), replayFrameBudget)
			}
			var f struct {
				SeqStart uint64           `json:"seqStart"`
				Events   []map[string]any `json:"events"`
			}
			if json.Unmarshal(payload, &f) != nil {
				t.Fatalf("bad frame %d: %.60s", frames, payload)
			}
			for _, ev := range f.Events {
				s, _ := ev["seq"].(float64)
				if prevSeq > 0 && s != prevSeq+1 {
					t.Fatalf("seq discontinuity across replay frames: %v after %v", s, prevSeq)
				}
				prevSeq = s
				total++
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out after %d events in %d frames", total, frames)
		}
	}
	if frames < 2 {
		t.Fatalf("replay produced %d frame(s), want >1 (chunked)", frames)
	}
}

// TestEnqueueEventsDropsOversizedFrame: a single frame over maxFrameBytes
// must be dropped with a log, not enqueued — enqueuing it would exceed
// the hub read limit and kill the connection (livelocking the resume).
func TestEnqueueEventsDropsOversizedFrame(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)
	big := acp.Event{"seq": 1.0, "type": "chunk", "text": strings.Repeat("y", 9<<20)}
	c.enqueueEvents([]acp.Event{big})
	select {
	case p := <-c.sendCh:
		t.Fatalf("oversized frame enqueued (%d bytes)", len(p))
	case <-time.After(150 * time.Millisecond):
		// expected: dropped with a log
	}
}

// TestEnqueueReplaySurvivesFullQueue: replay frames (reconnect catch-up)
// ride the critical enqueue path — when the send queue is full, the
// enqueue blocks (up to 5s) instead of dropping, so a busy queue cannot
// lose the transcript the hub is waiting for after a resume.
func TestEnqueueReplaySurvivesFullQueue(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 1) // tiny queue
	// Fill the queue: a plain enqueueEvents would drop here.
	c.sendCh <- []byte("stale-frame")

	evs := make([]acp.Event, 0, 3)
	for i := 1; i <= 3; i++ {
		evs = append(evs, acp.Event{"seq": float64(i), "type": "chunk", "text": "x"})
	}
	replayDone := make(chan struct{})
	go func() {
		// The production replay path (sendReplayAfter) holds enqueueMu
		// across enqueueReplayLocked — mirror that here.
		c.enqueueMu.Lock()
		c.enqueueReplayLocked(replayItemsOf(evs))
		c.enqueueMu.Unlock()
		close(replayDone)
	}()

	// Consume the stale frame; the blocked critical send then completes.
	select {
	case p := <-c.sendCh:
		if string(p) != "stale-frame" {
			t.Fatalf("first frame = %.40s, want the stale filler", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stale frame never consumed")
	}
	select {
	case payload := <-c.sendCh:
		var f struct {
			Type     string `json:"type"`
			SeqStart uint64 `json:"seqStart"`
		}
		if json.Unmarshal(payload, &f) != nil {
			t.Fatalf("bad replay frame: %.60s", payload)
		}
		if f.Type != "events" || f.SeqStart != 1 {
			t.Fatalf("replay frame = type %q seqStart %d, want events/1", f.Type, f.SeqStart)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replay frame dropped while the queue was full")
	}
	select {
	case <-replayDone:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueueReplayLocked never returned")
	}
}

// TestRelayRejectsOversizedLocalResponse: a local API response over
// maxRelayResponseBytes must be answered with an explicit error instead of
// being silently truncated (an over-limit respond frame would exceed the
// hub's read limit and kill the whole connection).
func TestRelayRejectsOversizedLocalResponse(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"request","reqId":"r1","method":"POST","path":"/api/big","body":{}}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	big := strings.Repeat("z", maxRelayResponseBytes+10)
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, 200, map[string]any{"ok": true, "data": big})
	}))
	defer local.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", LocalBase: local.URL, StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.wsSession(ctx, bridge) }()

	select {
	case resp := <-fh.respondsCh:
		if resp["status"] != float64(502) && resp["status"] != 502 {
			t.Errorf("respond status = %v, want 502 (oversized local response)", resp["status"])
		}
		body, _ := json.Marshal(resp["body"])
		if !bytes.Contains(body, []byte("过大")) {
			t.Errorf("respond body = %s, want explicit oversize error", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no respond for oversized relay")
	}
}

// TestRelayResponseWriterSharesCeiling: the in-process relay (the production
// path — LocalBase is empty, so the hub request is served by c.local) must
// use the SAME ceiling as the loopback fallback below it. A second literal
// here is how it drifted to 16MB while the fallback moved to 28MB, which
// turned every 16–28MB detail=full history page into a 502 on a real hub.
func TestRelayResponseWriterSharesCeiling(t *testing.T) {
	chunk := bytes.Repeat([]byte("y"), 1<<20)
	w := &relayResponseWriter{hdr: http.Header{}}
	for i := 0; i < maxRelayResponseBytes>>20; i++ {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if w.truncated {
			t.Fatalf("truncated at %d MiB, want the full %d MiB through", i+1, maxRelayResponseBytes>>20)
		}
	}
	if w.buf.Len() != maxRelayResponseBytes {
		t.Fatalf("buffered %d bytes, want %d", w.buf.Len(), maxRelayResponseBytes)
	}
	if _, err := w.Write(chunk); err != nil {
		t.Fatalf("oversized write: %v", err)
	}
	if !w.truncated {
		t.Fatal("no truncation one MiB past the ceiling")
	}
	// Once truncated, the handler must still be able to finish writing.
	if n, err := w.Write(chunk); err != nil || n != len(chunk) {
		t.Fatalf("post-truncate write = (%d, %v), want swallowed", n, err)
	}
}

// drainSendCh parses events frames from the client's send queue into the fake hub.
func drainSendCh(ctx context.Context, c *Client, fh *fakeHub) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-c.sendCh:
			var frame struct {
				Type   string           `json:"type"`
				Events []map[string]any `json:"events"`
			}
			if json.Unmarshal(payload, &frame) != nil || frame.Type != "events" {
				continue
			}
			for _, ev := range frame.Events {
				fh.eventsCh <- ev
			}
		}
	}
}

func (f *fakeHub) countPairCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pairCalls
}

// TestRunSupersededDoesNotRepair drives the whole reconnect loop against a
// hub that closes with "superseded": the host must redial on the SAME
// token. Before the close contract, the rendered "StatusPolicyViolation"
// text matched isAuthErr and this path cleared the credential and consumed a
// pairing code.
func TestRunSupersededDoesNotRepair(t *testing.T) {
	var mu sync.Mutex
	pairCalls, wsCalls := 0, 0
	authHdrs := make(chan string, 4)

	closeConn := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pair", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		pairCalls++
		mu.Unlock()
		writeTestJSON(w, 200, map[string]any{"ok": true, "token": "tok-super"})
	})
	mux.HandleFunc("GET /ws/host", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		wsCalls++
		n := wsCalls
		mu.Unlock()
		select {
		case authHdrs <- r.Header.Get("Authorization"):
		default:
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		// First session: superseded by another connection (the credential
		// stays valid). Second and later: hold the link open.
		if n == 1 {
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"v":1,"type":"hello","subscribers":0}`))
			_ = conn.Close(websocket.StatusNormalClosure, closeReasonSuperseded)
			return
		}
		<-closeConn
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer close(closeConn)

	// Pair once up front so the credential lives in a state file: clearing
	// state is exactly what a misclassified auth failure would do, and the
	// file is how that shows up.
	stateFile := filepath.Join(t.TempDir(), "hub.json")
	c := NewClient(Config{URL: ts.URL, HostID: "h1", PairCode: "ABC123", StateFile: stateFile, DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, bridge)

	// The loop must come back with the SAME credential.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case hdr := <-authHdrs:
			if hdr != "Bearer tok-super" {
				t.Fatalf("redial used %q, want the original token", hdr)
			}
		case <-deadline:
			t.Fatal("host never redialed after a superseded close")
		}
		mu.Lock()
		gotWS := wsCalls
		mu.Unlock()
		if gotWS >= 2 {
			break
		}
	}
	mu.Lock()
	gotPair := pairCalls
	mu.Unlock()
	if gotPair != 1 {
		t.Errorf("pair calls = %d, want 1 (only the initial pairing; a superseded close must not re-pair)", gotPair)
	}
	if _, err := os.Stat(stateFile); err != nil {
		t.Errorf("state file removed by a superseded close: %v", err)
	}
}

// TestQUICSessionRoundTrip exercises the QUIC transport end to end:
// auth frame → hello (subscribers+seq) → events uplink → request downlink → respond.
func TestQUICSessionRoundTrip(t *testing.T) {
	// Multi-stream in-memory hub (see dualstream_test.go): control plane is
	// the first accepted stream, responds arrive on per-request streams.
	th := newTestQUICHub(t)
	th.serve(func(ctrl *quic.Stream) {
		// Push a relayed request down the control plane.
		_ = writeTestFrame(ctrl, map[string]any{
			"v": 1, "type": "request", "reqId": "qr1",
			"method": "POST", "path": "/api/prompt",
			"body": map[string]any{"blocks": []any{map[string]any{"type": "text", "text": "hi"}}},
		})
	})

	eventsCh := th.eventsCh

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, 200, map[string]any{"ok": true, "stopReason": "end_turn"})
	}))
	defer local.Close()

	// Point the client at the QUIC port itself (URL host:port = QUIC endpoint).
	c := NewClient(Config{
		URL:       fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:    "h1",
		Token:     "tok123",
		LocalBase: local.URL,
		// The in-test hub is self-signed; an https URL otherwise makes the
		// QUIC transport verify the certificate (see quicTLSClientConfig
		// and TestQUICTLSPolicy).
		QUICInsecure: true,
	})
	c.sendCh = make(chan []byte, 64)
	c.reqCh = make(chan reqFrame, 16)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge) // forwardLoop is a private method; test is in-package

	done := make(chan error, 1)
	go func() { _, err := c.quicSession(ctx, bridge); done <- err }()

	// Wait for forwarding enabled (hello.subscribers=1).
	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			select {
			case err := <-done:
				t.Fatalf("forwarding never enabled over QUIC (session err: %v)", err)
			case err := <-th.errCh:
				t.Fatalf("forwarding never enabled over QUIC (server err: %v)", err)
			default:
				t.Fatal("forwarding never enabled over QUIC")
			}
		case <-time.After(10 * time.Millisecond):
		}
	}

	bridge.Broadcast(acp.Event{"type": "chunk", "text": "over quic", "sessionId": "s1"})
	select {
	case ev := <-eventsCh:
		// host_status heartbeat may arrive first — keep reading for the chunk.
		deadline := time.After(5 * time.Second)
		for ev["text"] != "over quic" {
			select {
			case ev = <-eventsCh:
			case <-deadline:
				c.seqMu.Lock()
				n := c.nextSeq
				replayLen := len(c.replay)
				c.seqMu.Unlock()
				t.Fatalf("no chunk over QUIC, last=%v (nextSeq=%d replay=%d)", ev, n, replayLen)
			}
		}
		if ev["type"] != "chunk" {
			t.Fatalf("event over quic = %v", ev)
		}
		if ev["seq"] == nil {
			t.Errorf("event missing seq: %v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no events over QUIC")
	}

	// Relay round trip: respond for qr1 arrives on its per-request stream.
	select {
	case resp := <-th.respondCh:
		if resp.ReqID != "qr1" {
			t.Fatalf("respond reqId = %q, want qr1", resp.ReqID)
		}
		if resp.Status != 200 {
			t.Errorf("respond status = %d, want 200", resp.Status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no respond for qr1 over QUIC")
	}
	cancel()
	<-done
}

// TestQUICTLSPolicy pins the QUIC certificate-verification policy. The hub
// refuses to serve QUIC with a self-signed certificate once FE_TOKEN is set
// (its quicTLSConfig), so treating every hub as untrusted here would make
// that requirement meaningless: whoever answers on the hub's UDP port could
// impersonate it, collect this host's token and drive relayed requests,
// which execute agent operations on this machine.
func TestQUICTLSPolicy(t *testing.T) {
	cases := []struct {
		name       string
		url        string
		insecure   bool
		wantSkip   bool
		wantServer string
	}{
		{
			name:       "https verifies against the hub domain",
			url:        "https://hub.example.com",
			wantSkip:   false,
			wantServer: "hub.example.com",
		},
		{
			name:       "https with port still verifies the hostname",
			url:        "https://hub.example.com:8787/",
			wantSkip:   false,
			wantServer: "hub.example.com",
		},
		{
			name:     "plain http is a lab/localhost hub — nothing to verify",
			url:      "http://127.0.0.1:8787",
			wantSkip: true,
		},
		{
			name:     "explicit opt-out wins over https",
			url:      "https://hub.example.com",
			insecure: true,
			wantSkip: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(Config{URL: tc.url, QUICInsecure: tc.insecure})
			conf := c.quicTLSClientConfig("203.0.113.10")
			if conf.InsecureSkipVerify != tc.wantSkip {
				t.Errorf("InsecureSkipVerify = %v, want %v", conf.InsecureSkipVerify, tc.wantSkip)
			}
			if len(conf.NextProtos) != 1 || conf.NextProtos[0] != "capri-hub" {
				t.Errorf("NextProtos = %v, want [capri-hub] (hub ALPN)", conf.NextProtos)
			}
			if !tc.wantSkip && conf.ServerName != tc.wantServer {
				// HUB_QUIC_HOST may point at a raw IP to dodge a fake-ip
				// resolver; the certificate still names the domain.
				t.Errorf("ServerName = %q, want %q (hub domain, not the dial IP)", conf.ServerName, tc.wantServer)
			}
		})
	}
}

// TestSubscribersGenGate: an out-of-order subscriber count must not pause
// event upload. The hub writes these frames from one goroutine per host, so
// a browser refresh (0 then 1, microseconds apart) can arrive reversed —
// applying the stale 0 leaves the host silent while the reloaded page waits.
func TestSubscribersGenGate(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 8)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	// Newest first (gen 7 = "one browser"), then the stale gen 6 = "none".
	c.applySubscribers(1, 7, bridge, true)
	if !c.forwardingEnabled() {
		t.Fatal("count=1 gen=7 should enable forwarding")
	}
	c.applySubscribers(0, 6, bridge, true)
	if !c.forwardingEnabled() {
		t.Fatal("stale gen=6 count=0 must be ignored — host would go silent")
	}
	// A genuinely newer zero still applies.
	c.applySubscribers(0, 8, bridge, true)
	if c.forwardingEnabled() {
		t.Fatal("gen=8 count=0 should pause forwarding")
	}
	// gen==0 (hub too old to stamp) always applies.
	c.applySubscribers(1, 0, bridge, true)
	if !c.forwardingEnabled() {
		t.Fatal("unstamped count must still be applied (legacy hub)")
	}
}

// TestReplayRingCompaction: acked events are dropped for real — a plain
// reslice keeps the whole backing array reachable. Unacked events are
// not eligible, so a ring with no delivery ack grows past the high water.
func TestReplayRingCompaction(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	n := replayHighWater + 5
	for i := 1; i <= n; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "seq": uint64(i)}})
	}
	c.seqMu.Lock()
	if got := len(c.replay); got != n {
		c.seqMu.Unlock()
		t.Fatalf("unacked replay len = %d, want %d (nothing delivered yet)", got, n)
	}
	c.hubAckSeq = uint64(n)
	c.seqMu.Unlock()
	// One more event runs compaction. Everything up to n is acked.
	c.seqAndReplay([]acp.Event{{"type": "chunk", "seq": uint64(n + 1)}})
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if cap(c.replay) != replayHighWater {
		t.Errorf("cap = %d, want %d (fresh compacted array, not a reslice)", cap(c.replay), replayHighWater)
	}
	if got := len(c.replay); got != replayCap {
		t.Fatalf("acked replay len = %d, want %d", got, replayCap)
	}
	// The newest replayCap events, ending at n+1.
	wantFirst := uint64(n + 1 - replayCap + 1)
	if got := c.replay[0].seq; got != wantFirst {
		t.Errorf("first replay seq = %d, want %d", got, wantFirst)
	}
	if got := c.replay[len(c.replay)-1].seq; got != uint64(n+1) {
		t.Errorf("last replay seq = %d, want %d", got, n+1)
	}
}

// TestReplayRingStoresBytesNotMaps: after seqAndReplay the ring must not
// alias the caller's Event map — mutating it must not change what reconnect
// would re-send.
func TestReplayRingStoresBytesNotMaps(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	ev := acp.Event{"type": "error", "message": "orig", "seq": uint64(1)}
	c.seqAndReplay([]acp.Event{ev})
	ev["message"] = "mutated"
	c.seqMu.Lock()
	raw := append([]byte(nil), c.replay[0].raw...)
	c.seqMu.Unlock()
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if stored["message"] != "orig" {
		t.Fatalf("ring saw %v after caller mutated the Event map", stored["message"])
	}
}

// TestSeqResetOrderedBeforeReplay: seq_reset and the replay it authorizes
// must reach the hub back-to-back. If a live batch slips between them the
// hub zeroes its watermark, jumps to the live seq, then discards the whole
// replay as stale — and it is gone from the hub's gap-pull buffer too
// (seq_reset cleared it), so that transcript is lost for good.
func TestSeqResetOrderedBeforeReplay(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 256)
	// Buffer some events from "this process" (seq 1..3).
	c.seqAndReplay([]acp.Event{
		{"type": "chunk", "seq": uint64(1)},
		{"type": "chunk", "seq": uint64(2)},
		{"type": "chunk", "seq": uint64(3)},
	})

	// Hub claims seq 900 — higher than anything this process produced, so
	// handleHelloSeq must ask for a reset and replay from scratch.
	var live sync.WaitGroup
	live.Add(1)
	gate := make(chan struct{})
	go func() {
		defer live.Done()
		<-gate // race a live batch against the reset+replay
		c.enqueueEvents([]acp.Event{{"type": "chunk", "seq": uint64(4)}})
	}()
	close(gate)
	c.handleHelloSeq(900)
	live.Wait()

	// Drain the queue and assert seq_reset is immediately followed by the
	// replay frames, with no live frame wedged in between.
	var kinds []string
	var firstEventSeqAfterReset uint64
	sawReset := false
	for {
		select {
		case p := <-c.sendCh:
			var f struct {
				Type     string  `json:"type"`
				SeqStart uint64  `json:"seqStart"`
				Events   []any   `json:"events"`
				Ready    *bool   `json:"ready"`
				TS       float64 `json:"ts"`
			}
			if json.Unmarshal(p, &f) != nil {
				continue
			}
			kinds = append(kinds, f.Type)
			if f.Type == "seq_reset" {
				sawReset = true
				continue
			}
			if sawReset && f.Type == "events" && firstEventSeqAfterReset == 0 {
				firstEventSeqAfterReset = f.SeqStart
			}
		default:
			goto done
		}
	}
done:
	if !sawReset {
		t.Fatalf("no seq_reset frame was enqueued: %v", kinds)
	}
	if firstEventSeqAfterReset != 1 {
		t.Fatalf("first events frame after seq_reset had seqStart=%d, want 1 "+
			"(a live frame jumped the reset → hub drops the replay); frames: %v",
			firstEventSeqAfterReset, kinds)
	}
}

// TestPingDoesNotCarryAbsoluteState drives the real frame loop: the ping is
// the data-plane delivery ack only. Absolute state (subscriber count) rides
// its own confirmed `subscribers` frame, so a stale count inside a ping must
// not pause or resume the uplink.
func TestPingDoesNotCarryAbsoluteState(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"subscribers","count":0,"gen":9}`,
		// A ping claiming a watcher must be ignored: the frame type no
		// longer carries absolute state.
		`{"v":1,"type":"ping","ts":1,"subscribers":1,"subsGen":10}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.wsSession(ctx, bridge) }()

	// Let the frames land, then assert the ping did not flip the gate.
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case <-deadline:
			if c.forwardingEnabled() {
				t.Fatal("ping-carried subscriber count resumed forwarding")
			}
			return
		case <-time.After(20 * time.Millisecond):
			if c.forwardingEnabled() {
				t.Fatal("ping-carried subscriber count resumed forwarding")
			}
		}
	}
}

// TestRespondWrapsNonJSONBody: a local relay body that is not embeddable
// JSON (JSONL, plain text) must not silently drop the respond frame —
// the hub's Dispatch would wait forever. respond wraps it as a JSON
// string so the relay still answers.
func TestRespondWrapsNonJSONBody(t *testing.T) {
	c := NewClient(Config{URL: "http://hub", HostID: "h1", Token: "t", DisableQUIC: true})
	c.reqCh = make(chan reqFrame, 1)

	c.respond("r1", 200, json.RawMessage("{\"ok\":true}"))
	f := <-c.reqCh
	if !json.Valid(f.payload) || !bytes.Contains(f.payload, []byte(`"ok":true`)) {
		t.Fatalf("valid JSON body must pass through unwrapped: %s", f.payload)
	}
	if !f.final {
		t.Error("relay respond must be the request's final frame")
	}

	c.respond("r2", 200, json.RawMessage("line one\nline two\n"))
	f = <-c.reqCh
	var frame struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("wrapped respond must be valid JSON: %v (%s)", err, f.payload)
	}
	if frame.Body != "line one\nline two\n" {
		t.Fatalf("wrapped body = %q", frame.Body)
	}
}

// ── 上行会话过滤（viewing gate）─────────────────────────────────────

func newViewingClient() *Client {
	return NewClient(Config{HostID: "h1", HostName: "H1"})
}

// 没人看的会话：正文被去掉，type/seq/sessionId 留下（保序号连续）。
func TestTrimUnwatchedReplacesBody(t *testing.T) {
	c := newViewingClient()
	c.applyViewing(viewingView{Sessions: []string{"s1"}})

	watched := acp.Event{"type": "chunk", "sessionId": "s1", "text": "keep me", "seq": float64(7)}
	unwatched := acp.Event{"type": "chunk", "sessionId": "s2", "text": "drop me", "seq": float64(8)}
	c.trimUnwatched([]acp.Event{watched, unwatched})

	if watched["type"] != "chunk" || watched["text"] != "keep me" {
		t.Errorf("watched event was modified: %v", watched)
	}
	if unwatched["type"] != "trimmed" {
		t.Errorf("unwatched type = %v, want trimmed", unwatched["type"])
	}
	if _, ok := unwatched["text"]; ok {
		t.Errorf("unwatched body survived: %v", unwatched)
	}
	if unwatched["seq"] != float64(8) || unwatched["sessionId"] != "s2" {
		t.Errorf("placeholder lost its seq/session: %v", unwatched)
	}
}

// 终态与控制轨道永远上传：丢它们会让页面卡在「进行中」或错过权限卡。
func TestTrimUnwatchedKeepsControlRails(t *testing.T) {
	c := newViewingClient()
	c.applyViewing(viewingView{Sessions: []string{"s1"}})
	for _, typ := range []string{
		"done", "turn_completed", "cancelled", "error", "client_request",
		"client_request_resolved", "usage", "busy", "session_status",
		"queue_changed", "session_recap", "subagent_spawned", "sessions_changed",
	} {
		ev := acp.Event{"type": typ, "sessionId": "s2", "seq": float64(9)}
		c.trimUnwatched([]acp.Event{ev})
		if ev["type"] != typ {
			t.Errorf("%s was trimmed, but it decides UI state for an unwatched session", typ)
		}
	}
}

// 未声明会话的页面（或旧 hub 什么都不发）时一律不过滤。
func TestTrimUnwatchedDisabledWithoutAudience(t *testing.T) {
	c := newViewingClient()
	ev := acp.Event{"type": "chunk", "sessionId": "s2", "text": "keep", "seq": float64(1)}
	c.trimUnwatched([]acp.Event{ev})
	if ev["type"] != "chunk" {
		t.Errorf("filtering started before the hub said anything: %v", ev)
	}

	c.applyViewing(viewingView{All: true})
	ev = acp.Event{"type": "chunk", "sessionId": "s2", "text": "keep", "seq": float64(2)}
	c.trimUnwatched([]acp.Event{ev})
	if ev["type"] != "chunk" {
		t.Errorf("filtering ran with all=true: %v", ev)
	}
}

// 子代理子会话跟随父会话的观看状态（父页面的迷你滚动区吃这条流）。
func TestTrimUnwatchedFollowsSubagentChild(t *testing.T) {
	c := newViewingClient()
	c.applyViewing(viewingView{Sessions: []string{"parent"}})
	spawn := acp.Event{
		"type":      "subagent_spawned",
		"sessionId": "parent",
		"seq":       float64(1),
		"update":    map[string]any{"child_session_id": "child", "subagent_id": "sub-1"},
	}
	c.trimUnwatched([]acp.Event{spawn})

	ev := acp.Event{"type": "chunk", "sessionId": "child", "text": "child work", "seq": float64(2)}
	c.trimUnwatched([]acp.Event{ev})
	if ev["type"] != "chunk" {
		t.Errorf("watched parent's child session was trimmed: %v", ev)
	}

	// 父会话没人看时，子会话也要裁。
	c.applyViewing(viewingView{Sessions: []string{"other"}})
	ev = acp.Event{"type": "chunk", "sessionId": "child", "text": "child work", "seq": float64(3)}
	c.trimUnwatched([]acp.Event{ev})
	if ev["type"] != "trimmed" {
		t.Errorf("child session of an unwatched parent survived: %v", ev)
	}
}

// plan 携带完整 entries 数组，属于 PROTOCOL.md §6.4 的 bulk 集：没人看的
// 会话只留占位，被查看的会话原样上传。
func TestTrimUnwatchedPlanIsBulk(t *testing.T) {
	c := newViewingClient()
	c.applyViewing(viewingView{Sessions: []string{"s1"}})

	watched := acp.Event{"type": "plan", "sessionId": "s1", "entries": []any{"a"}, "seq": float64(1)}
	unwatched := acp.Event{"type": "plan", "sessionId": "s2", "entries": []any{"b"}, "seq": float64(2)}
	c.trimUnwatched([]acp.Event{watched, unwatched})
	if watched["type"] != "plan" || watched["entries"] == nil {
		t.Errorf("watched plan was modified: %v", watched)
	}
	if unwatched["type"] != "trimmed" {
		t.Errorf("unwatched plan type = %v, want trimmed", unwatched["type"])
	}
	if _, ok := unwatched["entries"]; ok {
		t.Errorf("unwatched plan body survived: %v", unwatched)
	}
}

// viewChildren 是有界 FIFO：超限时淘汰最早的关系，且表永远不超过 cap。
func TestNoteViewingChildFIFO(t *testing.T) {
	c := newViewingClient()
	for i := 0; i < viewingChildCap+2; i++ {
		c.noteViewingChild("child-"+strconv.Itoa(i), "", "parent")
		if n := len(c.viewChildren); n > viewingChildCap {
			t.Fatalf("viewChildren size %d exceeds cap %d at insert %d", n, viewingChildCap, i)
		}
	}
	if n := len(c.viewChildren); n != viewingChildCap {
		t.Fatalf("viewChildren size = %d, want %d", n, viewingChildCap)
	}
	if _, ok := c.viewChildren["child-0"]; ok {
		t.Error("oldest relation (child-0) was not evicted")
	}
	if _, ok := c.viewChildren["child-1"]; ok {
		t.Error("second-oldest relation (child-1) was not evicted")
	}
	if got := c.viewChildren["child-"+strconv.Itoa(viewingChildCap+1)]; got != "parent" {
		t.Errorf("newest relation = %q, want parent", got)
	}
}

// 切到没被观看的会话时，观看集合更新，正文立刻恢复上传。
func TestViewingSwitchResumesTrimmedSession(t *testing.T) {
	c := newViewingClient()
	c.applyViewing(viewingView{Sessions: []string{"s1"}})
	if !c.viewingFiltered("s2") {
		t.Fatal("s2 should be filtered while only s1 is watched")
	}
	c.applyViewing(viewingView{Sessions: []string{"s2"}})
	if c.viewingFiltered("s2") {
		t.Error("s2 should resume once a page displays it")
	}
}

// 乱序的 viewing 帧（旧 gen 后到）不得把新集合顶掉：否则宿主会裁掉用户
// 刚打开的会话。
func TestApplyViewingDropsStaleGen(t *testing.T) {
	c := newViewingClient()
	c.applyViewing(viewingView{Sessions: []string{"s1", "s2"}, Gen: 5})
	if c.viewingFiltered("s2") {
		t.Fatal("s2 should be watched")
	}
	// 迟到的旧帧：只声明 s1。必须忽略。
	c.applyViewing(viewingView{Sessions: []string{"s1"}, Gen: 4})
	if c.viewingFiltered("s2") {
		t.Error("stale viewing frame narrowed the set")
	}
	// 更新的帧照常生效。
	c.applyViewing(viewingView{Sessions: []string{"s1"}, Gen: 6})
	if !c.viewingFiltered("s2") {
		t.Error("newer viewing frame was ignored")
	}
}

// 端到端：hub 在 hello 里带回 viewing 集合、或补发一条独立的 viewing 帧
// 之后，宿主的上行帧里只有被查看会话保留正文，其余事件变成保序号的
// trimmed 占位；seq 仍然连续。
//
// 独立帧按 hub 发送的形状写：集合平铺在顶层、带 type:"viewing"；hello
// 里则嵌在 "viewing" 键下。两种形状各自有测试覆盖。
func TestStreamViewingTrimEndToEnd(t *testing.T) {
	cases := []struct {
		name    string
		frames  []string
		watched string
		trimmed string
	}{
		{
			name: "hello nested",
			frames: []string{
				`{"v":1,"type":"hello","service":"hub","subscribers":1,"viewing":{"all":false,"sessions":["s1"],"gen":1}}`,
			},
			watched: "s1",
			trimmed: "s2",
		},
		{
			name: "standalone flat",
			frames: []string{
				`{"v":1,"type":"hello","service":"hub","subscribers":1}`,
				`{"v":1,"type":"viewing","all":false,"sessions":["s2"],"gen":2}`,
			},
			watched: "s2",
			trimmed: "s1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fh := newFakeHub(t)
			fh.streamFrames = tc.frames
			ts := httptest.NewServer(fh.handler())
			defer ts.Close()

			c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
			if err := c.ensureToken(context.Background()); err != nil {
				t.Fatalf("ensureToken: %v", err)
			}
			c.sendCh = make(chan []byte, 64)
			bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go drainSendCh(ctx, c, fh)
			go c.forwardLoop(ctx, bridge)
			go func() { _ = c.wsSession(ctx, bridge) }()

			// 等到 viewing 集合生效（forwarding 由 hello.subscribers=1 打开）。
			deadline := time.After(3 * time.Second)
			for !c.viewingFiltered(tc.trimmed) {
				select {
				case <-deadline:
					t.Fatalf("viewing set (%s) never applied", tc.name)
				case <-time.After(20 * time.Millisecond):
				}
			}
			if c.viewingFiltered(tc.watched) {
				t.Fatalf("%s must be watched after %s", tc.watched, tc.name)
			}

			bridge.Broadcast(acp.Event{"type": "chunk", "sessionId": tc.watched, "text": "watched"})
			bridge.Broadcast(acp.Event{"type": "chunk", "sessionId": tc.trimmed, "text": "unwatched body"})

			got := map[string]map[string]any{}
			lastSeq := 0.0
			for len(got) < 2 {
				select {
				case ev := <-fh.eventsCh:
					if _, ok := ev["sessionId"].(string); !ok {
						continue // host_status 等控制帧
					}
					got[ev["sessionId"].(string)] = ev
					if seq, ok := ev["seq"].(float64); ok && seq <= lastSeq {
						t.Fatalf("seq went backwards: %v after %v", seq, lastSeq)
					} else if ok {
						lastSeq = seq
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("missing events, got %v", got)
				}
			}
			if got[tc.watched]["type"] != "chunk" || got[tc.watched]["text"] != "watched" {
				t.Errorf("watched session = %v", got[tc.watched])
			}
			if got[tc.trimmed]["type"] != "trimmed" {
				t.Errorf("unwatched session type = %v, want trimmed", got[tc.trimmed]["type"])
			}
			if _, ok := got[tc.trimmed]["text"]; ok {
				t.Errorf("unwatched session payload survived: %v", got[tc.trimmed])
			}
		})
	}
}

// ── 心跳档位（按 hello.probe）────────────────────────────────────
//
// probe 表示这条传输有套接字探针，host_status 只是注册表重申，可以拉长。
// 没有 probe（QUIC，或 hello 还没到）时帧本身仍是存活证据，必须用快心跳。
// 传输名字不参与判断：hello 里的字段才是契约。
func TestHeartbeatIntervalFollowsProbe(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	if got := c.heartbeatEvery(); got != heartbeatInterval {
		t.Fatalf("heartbeat before hello = %s, want %s", got, heartbeatInterval)
	}
	c.probe.Store(true)
	if got := c.heartbeatEvery(); got != probeHeartbeatInterval {
		t.Fatalf("probe heartbeatEvery() = %s, want %s", got, probeHeartbeatInterval)
	}
	c.probe.Store(false)
	if got := c.heartbeatEvery(); got != heartbeatInterval {
		t.Fatalf("non-probe heartbeatEvery() = %s, want %s", got, heartbeatInterval)
	}
}

// 版本门禁：缺少当前协议必填字段的 hello 必须让 session 失败，而不是带着
// 默认值继续跑。旧 hub 字段缺失时的兼容分支已经移除，两端版本不一致要
// 当场暴露，而不是变成难查的行为差异。
func TestHelloFromOutdatedHubFailsSession(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hello string
	}{
		{"no seq", `{"v":1,"type":"hello","service":"hub","subscribers":0,"deflate":true,"pingInterval":5,"idleTimeout":20}`},
		{"no deflate echo", `{"v":1,"type":"hello","service":"hub","subscribers":0,"seq":0,"pingInterval":5,"idleTimeout":20}`},
		{"no liveness fields", `{"v":1,"type":"hello","service":"hub","subscribers":0,"seq":0,"deflate":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fh := newFakeHub(t)
			fh.rawStream = true
			fh.streamFrames = []string{tc.hello}
			ts := httptest.NewServer(fh.handler())
			defer ts.Close()

			c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
			c.sendCh = make(chan []byte, 8)
			bridge := newStatusBridge()

			err := c.wsSession(context.Background(), bridge)
			if err == nil {
				t.Fatal("outdated hello must fail the session")
			}
			if !strings.Contains(err.Error(), "版本过旧") {
				t.Fatalf("error = %v, want a version-gate error", err)
			}
		})
	}
}

// ── 下行看门狗与 hello 广告的时间尺度 ─────────────────────────────
//
// 看门狗阈值不再写死：hub 在 hello 里说明它自己的判活尺度（MQTT Server
// Keep Alive 的思路），host 照此设置。这样两端不会各自硬编码、又慢慢漂开。

// hello 的 idleTimeout 必须成为本 session 的看门狗值；pingInterval 单独
// 给出，不能反过来影响看门狗。
func TestHelloIdleTimeoutSetsWatchdog(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hello string
		want  time.Duration
	}{
		{"advertised", `{"v":1,"type":"hello","service":"hub","subscribers":0,"pingInterval":5,"idleTimeout":12}`, 12 * time.Second},
		{"too small", `{"v":1,"type":"hello","service":"hub","subscribers":0,"pingInterval":5,"idleTimeout":1}`, defaultSessionIdleTimeout},
		{"too large", `{"v":1,"type":"hello","service":"hub","subscribers":0,"pingInterval":5,"idleTimeout":301}`, defaultSessionIdleTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fh := newFakeHub(t)
			fh.streamFrames = []string{tc.hello}
			ts := httptest.NewServer(fh.handler())
			defer ts.Close()

			c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
			c.sendCh = make(chan []byte, 64)
			bridge := newStatusBridge()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = c.wsSession(ctx, bridge) }()

			deadline := time.After(3 * time.Second)
			for c.idleTimeout() != tc.want {
				select {
				case <-deadline:
					t.Fatalf("idleTimeout() = %s, want %s", c.idleTimeout(), tc.want)
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	}
}

// 越界值必须被忽略并回落到本地默认，而不是被裁剪到边界：hub 报 1s 会让
// 每个 5s ping 间隔都触发重连，报 10000s 则等于关掉看门狗。
func TestApplyHubLivenessIgnoresOutOfRange(t *testing.T) {
	c := NewClient(Config{URL: "http://127.0.0.1:1", HostID: "h1", DisableQUIC: true})

	if got := c.idleTimeout(); got != defaultSessionIdleTimeout {
		t.Fatalf("initial idleTimeout = %s, want %s", got, defaultSessionIdleTimeout)
	}
	for _, bad := range []int{0, -1, minHelloIdleSec - 1, maxHelloIdleSec + 1, 10000} {
		c.applyHubLiveness(5, bad)
		if got := c.idleTimeout(); got != defaultSessionIdleTimeout {
			t.Errorf("idleTimeout=%d accepted, watchdog = %s", bad, got)
		}
	}
	// 边界本身是合法的。
	for _, ok := range []int{minHelloIdleSec, maxHelloIdleSec} {
		c.applyHubLiveness(5, ok)
		if got := c.idleTimeout(); got != time.Duration(ok)*time.Second {
			t.Errorf("idleTimeout=%d rejected, watchdog = %s", ok, got)
		}
		c.idleTimeoutNanos.Store(0)
	}
}

// 契约：hello 不带 idleTimeout 时，看门狗必须仍是 20s。这条断言是
// 「链路已死的尺度」在两端一致的锚点——hub 的 sweep 靠这个数字排在自己前面。
func TestDefaultWatchdogIsTwentySeconds(t *testing.T) {
	if defaultSessionIdleTimeout != 20*time.Second {
		t.Fatalf("defaultSessionIdleTimeout = %s, want 20s", defaultSessionIdleTimeout)
	}
	c := NewClient(Config{URL: "http://127.0.0.1:1", HostID: "h1", DisableQUIC: true})
	if got := c.idleTimeout(); got != defaultSessionIdleTimeout {
		t.Fatalf("fresh client idleTimeout = %s, want %s", got, defaultSessionIdleTimeout)
	}
}

// 新 session 不能继承上一个 session（可能是更慢的 hub）的看门狗值。
func TestRunSessionResetsIdleTimeout(t *testing.T) {
	fh := newFakeHub(t)
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	c.sendCh = make(chan []byte, 8)
	c.idleTimeoutNanos.Store(int64(99 * time.Second)) // 上一 session 留下的状态
	bridge := newStatusBridge()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.wsSession(ctx, bridge) }()

	deadline := time.After(3 * time.Second)
	for c.idleTimeout() != defaultSessionIdleTimeout {
		select {
		case <-deadline:
			t.Fatalf("new session kept idleTimeout %s", c.idleTimeout())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// hub 停发所有下行帧后，看门狗必须在阈值附近（而不是老常量的 75s）断开
// 这个 session。用缩短的阈值测：真实 20s 会让单测慢 20 倍。
func TestIdleWatchdogFiresWhenHubGoesSilent(t *testing.T) {
	const budget = 120 * time.Millisecond
	c := NewClient(Config{URL: "http://127.0.0.1:1", HostID: "h1", DisableQUIC: true})
	c.idleTimeoutNanos.Store(int64(budget))
	c.markRecv()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.idleWatchdog(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("watchdog returned nil, want a dead-link error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog never fired on a silent hub")
	}
}

// 反向保证：有下行帧时看门狗不能误伤。帧到达必须把计时推后。
func TestIdleWatchdogHoldsWhileFramesArrive(t *testing.T) {
	const budget = 120 * time.Millisecond
	c := NewClient(Config{URL: "http://127.0.0.1:1", HostID: "h1", DisableQUIC: true})
	c.idleTimeoutNanos.Store(int64(budget))
	c.markRecv()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.idleWatchdog(ctx) }()

	// 持续喂帧，总时长超过预算的若干倍。
	stop := time.After(4 * budget)
feed:
	for {
		select {
		case <-stop:
			break feed
		case <-done:
			t.Fatal("watchdog fired while downlink frames were arriving")
		case <-time.After(budget / 4):
			c.markRecv()
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not exit on ctx cancel")
	}
}

// End-to-end shape of the same guarantee: once the hub's hello lands, a hub
// that then sends NOTHING at all must tear the session down at the
// advertised budget, not at the old 75s constant. Uses the smallest legal
// advertisement (5s) so the test does not wait out a production value.
func TestSessionDiesWhenHubStopsSendingFrames(t *testing.T) {
	if testing.Short() {
		t.Skip("waits one advertised idleTimeout")
	}
	fh := newFakeHub(t)
	// hello only; the handler sends nothing afterwards.
	fh.streamFrames = []string{`{"v":1,"type":"hello","service":"hub","subscribers":0,"probe":true,"pingInterval":5,"idleTimeout":5}`}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	c.sendCh = make(chan []byte, 64)
	bridge := newStatusBridge()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	err := c.wsSession(ctx, bridge)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("wsSession returned nil after the hub went silent")
	}
	// Budget 5s ± a tick (budget/8) and scheduling slack; must be nowhere
	// near the old 75s.
	if elapsed < 4*time.Second || elapsed > 12*time.Second {
		t.Fatalf("session died after %s, want ~5s (the advertised idleTimeout)", elapsed)
	}
	if c.idleTimeout() != 5*time.Second {
		t.Fatalf("watchdog budget during session = %s, want 5s", c.idleTimeout())
	}
}

// TestClassifyCloseProtocolVersion: the hub's protocol-version close is its
// own kind, so the operator sees a version mismatch instead of a generic
// drop that looks like a network problem.
func TestClassifyCloseProtocolVersion(t *testing.T) {
	got := classifyClose(websocket.CloseError{
		Code:   websocket.StatusProtocolError,
		Reason: "protocol version",
	})
	if got != closeProtocol {
		t.Fatalf("classifyClose = %v, want closeProtocol", got)
	}
}
