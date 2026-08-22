package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
)

// fakeHub stands in for capri-hub in tests (WS transport).
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
		f.mu.Unlock()

		ctx := r.Context()
		for _, fr := range frames {
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
// {"v":1,"type":"host_status","ready":…} — no seq, not an events frame,
// not in the replay buffer — so it cannot collide with bridge.Broadcast seq.
func TestHostStatusIsControlFrame(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
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
		if _, ok := f["ready"]; !ok {
			t.Errorf("host_status missing ready: %s", payload)
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

// TestIsAuthErrTight: isAuthErr must match 401 / StatusPolicyViolation /
// "auth failed" / "no token", but not bare "token" alone.
func TestIsAuthErrTight(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("401 hub ws auth failed"), true},
		{fmt.Errorf("websocket: StatusPolicyViolation"), true},
		{fmt.Errorf("auth failed: bad credentials"), true},
		{fmt.Errorf("401: no token"), true},
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
	c.seqMu.Lock()
	for i := 1; i <= n; i++ {
		c.nextSeq++
		c.replay = append(c.replay, acp.Event{
			"seq":  float64(c.nextSeq),
			"type": "chunk",
			"text": strings.Repeat("x", 40<<10),
		})
	}
	c.seqMu.Unlock()

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
		c.enqueueReplay(evs)
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
		t.Fatal("enqueueReplay never returned")
	}
}

// TestRelayRejectsOversizedLocalResponse: a local API response over 16MB
// must be answered with an explicit error instead of being silently
// truncated (a 16MB+ respond frame would exceed the hub read limit and
// kill the whole connection).
func TestRelayRejectsOversizedLocalResponse(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"request","reqId":"r1","method":"POST","path":"/api/big","body":{}}`,
	}
	ts := httptest.NewServer(fh.handler())
	defer ts.Close()

	big := strings.Repeat("z", 16<<20+10)
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
	c.applySubscribers(1, 7, bridge)
	if !c.forwardingEnabled() {
		t.Fatal("count=1 gen=7 should enable forwarding")
	}
	c.applySubscribers(0, 6, bridge)
	if !c.forwardingEnabled() {
		t.Fatal("stale gen=6 count=0 must be ignored — host would go silent")
	}
	// A genuinely newer zero still applies.
	c.applySubscribers(0, 8, bridge)
	if c.forwardingEnabled() {
		t.Fatal("gen=8 count=0 should pause forwarding")
	}
	// gen==0 (hub too old to stamp) always applies.
	c.applySubscribers(1, 0, bridge)
	if !c.forwardingEnabled() {
		t.Fatal("unstamped count must still be applied (legacy hub)")
	}
}

// TestReplayRingCompaction: the ring must stay bounded AND drop its old
// events for real — a plain reslice keeps the whole backing array (and every
// Event map in it) reachable until the next reallocation.
func TestReplayRingCompaction(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	for i := 1; i <= replayHighWater+5; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "seq": uint64(i)}})
		c.seqMu.Lock()
		n := len(c.replay)
		c.seqMu.Unlock()
		if n > replayHighWater {
			t.Fatalf("replay len = %d after %d events, must stay <= %d", n, i, replayHighWater)
		}
	}
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if cap(c.replay) != replayHighWater {
		t.Errorf("cap = %d, want %d (fresh compacted array, not a reslice)", cap(c.replay), replayHighWater)
	}
	// Oldest retained seq after the compaction at replayHighWater+1.
	wantFirst := uint64(replayHighWater + 1 - replayCap + 1)
	if got := eventSeq(c.replay[0]); got != wantFirst {
		t.Errorf("first replay seq = %d, want %d", got, wantFirst)
	}
	if got := eventSeq(c.replay[len(c.replay)-1]); got != uint64(replayHighWater+5) {
		t.Errorf("last replay seq = %d, want %d", got, replayHighWater+5)
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

// TestPingReassertsSubscribers drives the real frame loop: after a stale
// `subscribers:0` has paused the uplink, the hub's next ping — which now
// carries the authoritative count — must resume it.
//
// This is the recovery path for a lost or reordered subscribers frame. Before
// the count was re-asserted on the ping, a host that ended up paused while a
// browser was actually watching stayed paused for the life of the connection:
// host_status heartbeats kept it "online" in the registry, so the user saw a
// connected-looking page that never updated again.
func TestPingReassertsSubscribers(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		// A browser IS connected, but this frame lost the race / was lost.
		`{"v":1,"type":"subscribers","count":0,"gen":9}`,
		// The periodic ping carries the truth.
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

	deadline := time.After(3 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("ping-carried subscriber count never resumed forwarding")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestStalePingSubscribersIgnored: a ping whose subsGen is older than the
// count we already applied must not undo it (same reordering hazard as the
// subscribers frames themselves).
func TestStalePingSubscribersIgnored(t *testing.T) {
	fh := newFakeHub(t)
	fh.streamFrames = []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"subscribers","count":1,"gen":20}`,
		// Older stamp: must be ignored, or the host goes silent.
		`{"v":1,"type":"ping","ts":1,"subscribers":0,"subsGen":19}`,
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

	deadline := time.After(3 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled after subscribers:1")
		case <-time.After(20 * time.Millisecond):
		}
	}
	// Give the stale ping time to (wrongly) take effect.
	time.Sleep(300 * time.Millisecond)
	if !c.forwardingEnabled() {
		t.Fatal("stale ping subsGen=19 disabled forwarding — ordering gate failed")
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
