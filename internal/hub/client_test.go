package hub

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
)

// fakeHub stands in for acp-hub in tests (WS transport).
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

func TestMergeChunkEvents(t *testing.T) {
	in := []acp.Event{
		{"type": "chunk", "text": "hel", "sessionId": "s1"},
		{"type": "chunk", "text": "lo", "sessionId": "s1"},
		{"type": "thought", "text": "hmm", "sessionId": "s1"},
		{"type": "thought", "text": "…", "sessionId": "s1"},
		{"type": "chunk", "text": "!", "sessionId": "s1"},
		{"type": "done", "sessionId": "s1"},
		{"type": "chunk", "text": "a", "sessionId": "s2"},
		{"type": "chunk", "text": "b", "sessionId": "s1"},
	}
	out := mergeChunkEvents(in)
	// hello | hmm… | ! | done | s2:a | s1:b
	if len(out) != 6 {
		t.Fatalf("merged len = %d, want 6: %v", len(out), out)
	}
	if out[0]["text"] != "hello" || out[0]["type"] != "chunk" {
		t.Errorf("out[0] = %v", out[0])
	}
	if out[1]["text"] != "hmm…" || out[1]["type"] != "thought" {
		t.Errorf("out[1] = %v", out[1])
	}
	if out[2]["text"] != "!" {
		t.Errorf("out[2] = %v", out[2])
	}
	if out[3]["type"] != "done" {
		t.Errorf("out[3] = %v", out[3])
	}
	if out[4]["text"] != "a" || out[4]["sessionId"] != "s2" {
		t.Errorf("out[4] = %v", out[4])
	}
	if out[5]["text"] != "b" || out[5]["sessionId"] != "s1" {
		t.Errorf("out[5] = %v", out[5])
	}
}

func TestMergeChunkEventsDifferentMessageID(t *testing.T) {
	in := []acp.Event{
		{"type": "chunk", "text": "a", "messageId": "m1"},
		{"type": "chunk", "text": "b", "messageId": "m2"},
	}
	out := mergeChunkEvents(in)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2 (different messageId)", len(out))
	}
}

// TestForwardLoopSeqAssignedAfterMerge is the P1 regression: seqs must be
// assigned AFTER chunk/thought merging, so wire and replay seqs are
// contiguous (no holes from merged-away events). Holes made the hub
// counter jump (spurious FE gap-pulls) and made reconnect replays
// duplicate text that a merged event already carried.
func TestForwardLoopSeqAssignedAfterMerge(t *testing.T) {
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
	go drainSendCh(ctx, c, fh)
	go c.forwardLoop(ctx, bridge)

	time.Sleep(80 * time.Millisecond) // let the subscription register
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "a", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "b", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "c", "sessionId": "s1"})
	bridge.Broadcast(acp.Event{"type": "done", "sessionId": "s1"})

	// Collect chunk/done events (tick splits are fine — the invariant is
	// seq contiguity and the merged text reassembling to "abc").
	var chunks []string
	var seqs []float64
	sawDone := false
	deadline := time.After(3 * time.Second)
	for !sawDone || strings.Join(chunks, "") != "abc" {
		select {
		case ev := <-fh.eventsCh:
			t, _ := ev["type"].(string)
			if t != "chunk" && t != "done" {
				continue
			}
			if s, ok := ev["seq"].(float64); ok {
				seqs = append(seqs, s)
			}
			if t == "chunk" {
				if txt, ok := ev["text"].(string); ok {
					chunks = append(chunks, txt)
				}
			} else if t == "done" {
				sawDone = true
			}
		case <-deadline:
			t.Fatalf("timed out: chunks=%v seqs=%v sawDone=%v", chunks, seqs, sawDone)
		}
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("seq hole in %v (want contiguous after merge)", seqs)
		}
	}
	if seqs[0] != 1 {
		t.Errorf("first seq = %v, want 1", seqs[0])
	}
	// Replay must hold exactly the merged events — nothing merged-away.
	c.seqMu.Lock()
	n := len(c.replay)
	c.seqMu.Unlock()
	if n != len(seqs) {
		t.Errorf("replay len = %d, want %d (merged events only)", n, len(seqs))
	}
}

// TestHostStatusCarriesSeq: host_status must get a real seq like any
// other event. A seq-less events frame made the hub renumber from 1,
// resetting its per-host counter and triggering a full replay (with FE
// re-emission) on the next reconnect.
func TestHostStatusCarriesSeq(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	c.enqueueHostStatus(bridge)
	select {
	case payload := <-c.sendCh:
		var f struct {
			Type     string           `json:"type"`
			SeqStart uint64           `json:"seqStart"`
			Events   []map[string]any `json:"events"`
		}
		if json.Unmarshal(payload, &f) != nil || f.Type != "events" {
			t.Fatalf("bad frame: %s", payload)
		}
		if f.SeqStart != 1 || len(f.Events) != 1 {
			t.Fatalf("frame = seqStart %d, %d events; want seqStart 1, 1 event", f.SeqStart, len(f.Events))
		}
		if s, _ := f.Events[0]["seq"].(float64); s != 1 {
			t.Errorf("host_status seq = %v, want 1", s)
		}
	case <-time.After(time.Second):
		t.Fatal("no host_status frame")
	}
	// And it lands in replay for resume.
	c.seqMu.Lock()
	n := len(c.replay)
	c.seqMu.Unlock()
	if n != 1 {
		t.Errorf("replay len = %d, want 1", n)
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
	// Self-signed TLS for the in-test QUIC server.
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-hub"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	qtls := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}

	ln, err := quic.ListenAddr("127.0.0.1:0", qtls, &quic.Config{KeepAlivePeriod: 10 * time.Second})
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	defer ln.Close()
	quicPort := ln.Addr().(*net.UDPAddr).Port

	eventsCh := make(chan map[string]any, 16)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		ctx := context.Background()
		// The host client opens the stream; the server accepts it.
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			serverErr <- err
			return
		}
		readFrame := func() ([]byte, error) {
			var lb [4]byte
			if _, err := io.ReadFull(stream, lb[:]); err != nil {
				return nil, err
			}
			buf := make([]byte, binary.BigEndian.Uint32(lb[:]))
			_, err := io.ReadFull(stream, buf)
			return buf, err
		}
		writeFrame := func(v any) error {
			b, _ := json.Marshal(v)
			var lb [4]byte
			binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
			if _, err := stream.Write(lb[:]); err != nil {
				return err
			}
			_, err := stream.Write(b)
			return err
		}
		// Auth frame first.
		data, err := readFrame()
		if err != nil {
			serverErr <- err
			return
		}
		var auth struct {
			Type  string `json:"type"`
			Token string `json:"token"`
		}
		if json.Unmarshal(data, &auth) != nil || auth.Type != "auth" || auth.Token != "tok123" {
			serverErr <- fmt.Errorf("bad auth: %s", data)
			return
		}
		if err := writeFrame(map[string]any{"v": 1, "type": "hello", "service": "hub", "subscribers": 1, "seq": 0}); err != nil {
			serverErr <- err
			return
		}
		// Push a relayed request.
		if err := writeFrame(map[string]any{
			"v": 1, "type": "request", "reqId": "qr1",
			"method": "POST", "path": "/api/prompt",
			"body": map[string]any{"blocks": []any{map[string]any{"type": "text", "text": "hi"}}},
		}); err != nil {
			serverErr <- err
			return
		}
		for {
			data, err := readFrame()
			if err != nil {
				serverErr <- fmt.Errorf("server read: %w", err)
				return
			}
			var frame struct {
				Type     string           `json:"type"`
				Events   []map[string]any `json:"events"`
				ReqID    string           `json:"reqId"`
				Status   int              `json:"status"`
				SeqStart float64          `json:"seqStart"`
			}
			if json.Unmarshal(data, &frame) != nil {
				continue
			}
			switch frame.Type {
			case "events":
				for _, ev := range frame.Events {
					ev["__seqStart"] = frame.SeqStart
					eventsCh <- ev
					if ev["text"] == "over quic" {
						serverErr <- nil
						return
					}
				}
			case "respond":
				// relay answer observed; keep reading until the chunk lands
			}
		}
	}()

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, 200, map[string]any{"ok": true, "stopReason": "end_turn"})
	}))
	defer local.Close()

	// Point the client at the QUIC port itself (URL host:port = QUIC endpoint).
	c := NewClient(Config{
		URL:       fmt.Sprintf("https://127.0.0.1:%d", quicPort),
		HostID:    "h1",
		Token:     "tok123",
		LocalBase: local.URL,
	})
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge) // forwardLoop is a private method; test is in-package

	done := make(chan error, 1)
	go func() { done <- c.quicSession(ctx, bridge) }()

	// Wait for forwarding enabled (hello.subscribers=1).
	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			select {
			case err := <-done:
				t.Fatalf("forwarding never enabled over QUIC (session err: %v)", err)
			case err := <-serverErr:
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

	// Relay round trip: respond arrives for qr1.
	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no respond for qr1 over QUIC")
	}
	cancel()
	<-done
}
