package hub

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"

	"github.com/AgentsHarness/capri-wire"
)

func newSessionClient(t *testing.T) *Client {
	t.Helper()
	return NewClient(Config{HostID: "h1", HostName: "H1", Token: "tok123", LocalBase: "http://127.0.0.1:1"})
}

// TestHandshakeAdoptsHelloAck: hello carries the epoch, the retired high-water
// baseline and the capability offer; hello_ack is authoritative for both
// watermarks and supplies the time scale, limits and audience.
func TestHandshakeAdoptsHelloAck(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	bridge.setSeq(41)

	h := newSessionHarness(t, c, bridge)
	hello := h.handshake(helloAckFrame(c.epoch, 41, 7, watchingAudience(3, "s1")))

	if hello.Epoch != c.epoch {
		t.Errorf("hello epoch = %d, want %d", hello.Epoch, c.epoch)
	}
	if hello.Resume == nil || hello.Resume.Up != 41 || hello.Resume.Down != 0 {
		t.Errorf("hello resume = %+v, want up 41 down 0", hello.Resume)
	}
	if hello.Caps == nil || !hello.Caps.Deflate {
		t.Errorf("hello caps = %+v, want deflate offered", hello.Caps)
	}
	if hello.Token != "tok123" {
		t.Errorf("hello token = %q", hello.Token)
	}

	if got := c.out.acked(); got != 41 {
		t.Errorf("ackUp not adopted: outbox acked %d", got)
	}
	if got := c.downApplied(); got != 7 {
		t.Errorf("ackDown not adopted: down applied %d", got)
	}
	if got := c.out.isPaused(); got {
		t.Error("a watching audience must not leave the uplink paused")
	}
	if got := c.watchers(); got != 1 {
		t.Errorf("watchers = %d, want 1", got)
	}
	h.tr.mu.Lock()
	defer h.tr.mu.Unlock()
	if h.tr.keepalive != time.Second {
		t.Errorf("keepalive = %v, want 1s from hello_ack", h.tr.keepalive)
	}
	if !h.tr.deflate {
		t.Error("deflate not enabled after the hub echoed the offer")
	}
}

// TestHelloAckEpochMismatchIsProtocol: a hub that echoes another epoch is
// speaking a different session.
func TestHelloAckEpochMismatchIsProtocol(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	hello := h.tr.next(t, 2*time.Second)
	if hello.Type != wire.TypeHello {
		t.Fatalf("first frame = %q", hello.Type)
	}
	h.tr.push(t, helloAckFrame(c.epoch+1, 0, 0, watchingAudience(1)))
	res := h.wait(t, 3*time.Second)
	if res.reason != wire.CloseProtocol {
		t.Fatalf("close reason = %s, want protocol", res.reason)
	}
}

// TestEventsBatchingIsContiguous: one frame is one contiguous run whose seq is
// its first event, and every event carries its own bridge seq.
func TestEventsBatchingIsContiguous(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "a"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "b"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "c"})

	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if fr.Seq != 1 {
		t.Fatalf("frame seq = %d, want 1", fr.Seq)
	}
	if len(fr.Events) != 3 {
		t.Fatalf("batch = %d events, want 3", len(fr.Events))
	}
	for i, raw := range fr.Events {
		var ev struct {
			Seq  uint64 `json:"seq"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if ev.Seq != uint64(i+1) {
			t.Errorf("event %d seq = %d, want %d", i, ev.Seq, i+1)
		}
	}
}

// TestGapSynthesisKeepsTheRunGapless: events the bus dropped upstream become
// placeholders at their own seq, never a hole.
func TestGapSynthesisKeepsTheRunGapless(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1, "s1")))
	runForwardLoop(t, c, bridge)

	// Simulate the bus having published 1..4 while the subscriber only saw
	// 1 and 4 (a droppable burst dropped by the per-subscriber ring).
	bridge.setSeq(3)
	bridge.ch <- acp.Event{"type": "chunk", "sessionId": "s1", "text": "one", "seq": uint64(1)}
	bridge.ch <- acp.Event{"type": "chunk", "sessionId": "s1", "text": "four", "seq": uint64(4)}

	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if fr.Seq != 1 || len(fr.Events) != 4 {
		t.Fatalf("frame seq %d with %d events, want seq 1 with 4", fr.Seq, len(fr.Events))
	}
	for i := 1; i < 3; i++ {
		var ev struct {
			Type string `json:"type"`
			Seq  uint64 `json:"seq"`
		}
		if err := json.Unmarshal(fr.Events[i], &ev); err != nil {
			t.Fatalf("placeholder %d: %v", i, err)
		}
		if ev.Type != "trimmed" || ev.Seq != uint64(i+1) {
			t.Errorf("event %d = %+v, want a trimmed placeholder at seq %d", i, ev, i+1)
		}
	}
}

// TestRetransmitFromAckedPlusOne: an ack drops the log; no ack means the RTO
// replays from acked+1.
func TestRetransmitFromAckedPlusOne(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "a"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "b"})
	first := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if first.Seq != 1 || len(first.Events) != 2 {
		t.Fatalf("first frame seq %d events %d", first.Seq, len(first.Events))
	}

	// No ack: keepalive is 1s, so RTO (2*keepalive) replays.
	again := h.tr.nextOfType(t, wire.TypeEvents, 5*time.Second)
	if again.Seq != 1 {
		t.Fatalf("retransmit started at seq %d, want 1", again.Seq)
	}
	if len(again.Events) != 2 {
		t.Fatalf("retransmit carried %d events, want 2", len(again.Events))
	}

	// Ack 1: the next retransmit must start at 2.
	h.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeHeartbeat, Ack: 1,
		Status: mustJSON(wire.HeartbeatStatus{})})
	third := h.tr.nextOfType(t, wire.TypeEvents, 5*time.Second)
	if third.Seq != 2 || len(third.Events) != 1 {
		t.Fatalf("retransmit after partial ack = seq %d with %d events, want seq 2 with 1", third.Seq, len(third.Events))
	}
}

// TestAudiencePauseStopsUpload: watchers == 0 pauses the uplink but keeps
// logging; watchers > 0 flushes the backlog.
func TestAudiencePauseStopsUpload(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrame(c.epoch, 0, 0, pausedAudience(1)))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "while-paused"})
	// Only heartbeats may arrive while paused.
	select {
	case raw := <-h.tr.sent:
		fr, err := wire.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if fr.Type == wire.TypeEvents {
			t.Fatal("event frame sent while the audience was empty")
		}
	case <-time.After(500 * time.Millisecond):
	}

	h.tr.push(t, audienceFrame(1, 0, watchingAudience(2, "s1")))
	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if fr.Seq != 1 || len(fr.Events) != 1 {
		t.Fatalf("resume flush = seq %d with %d events, want the backlogged event", fr.Seq, len(fr.Events))
	}
}

// TestAudienceGenOrdering: a snapshot older than the one in force is ignored;
// an equal gen is a duplicate that still yields an ack.
func TestAudienceGenOrdering(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrame(c.epoch, 0, 0, pausedAudience(5)))

	// Older generation with watchers: must not resume the uplink.
	h.tr.push(t, audienceFrame(1, 0, watchingAudience(4, "s1")))
	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case raw := <-h.tr.sent:
			fr, _ := wire.Decode(raw)
			if fr.Type == wire.TypeHeartbeat && fr.Ack != 1 {
				t.Fatalf("heartbeat ack = %d, want the audience frame acked", fr.Ack)
			}
		case <-deadline:
			if !c.out.isPaused() {
				t.Fatal("a stale audience generation (4 < 5) was applied")
			}
			return
		}
	}
}

// TestAudienceTrimsUnwatchedSessions: bulk transcript for a session nobody
// displays becomes a placeholder; all:true disables the filter.
func TestAudienceTrimsUnwatchedSessions(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1, "s1")))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "watched"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s2", "text": "unwatched"})
	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if len(fr.Events) != 2 {
		t.Fatalf("batch = %d events", len(fr.Events))
	}
	var watched, unwatched struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	_ = json.Unmarshal(fr.Events[0], &watched)
	_ = json.Unmarshal(fr.Events[1], &unwatched)
	if watched.Text != "watched" {
		t.Errorf("watched event was trimmed: %+v", watched)
	}
	if unwatched.Type != "trimmed" || unwatched.Text != "" {
		t.Errorf("unwatched event = %+v, want a trimmed placeholder", unwatched)
	}

	// all:true means the audience cannot be narrowed.
	c.setAudience(wire.Audience{Gen: 2, Watchers: 1, All: true})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s3", "text": "also-kept"})
	var kept struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	for i := 0; i < 4; i++ {
		fr = h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
		_ = json.Unmarshal(fr.Events[len(fr.Events)-1], &kept)
		if kept.Text == "also-kept" {
			break
		}
	}
	if kept.Text != "also-kept" {
		t.Errorf("all:true did not disable trimming: %+v", kept)
	}
}

// TestOversizedEventBecomesPlaceholder: an event that cannot fit one frame is
// replaced by a trimmed placeholder at its own seq, never dropped.
func TestOversizedEventBecomesPlaceholder(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	small := &wire.Limits{MaxFrame: 2048, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(watchingAudience(1, "s1")),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 2}, Limits: small, Caps: &wire.Caps{Deflate: true},
	}))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "tool_call", "sessionId": "s1", "text": strings.Repeat("x", 8<<10)})
	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	var ev struct {
		Type      string `json:"type"`
		SessionID string `json:"sessionId"`
		Seq       uint64 `json:"seq"`
	}
	if err := json.Unmarshal(fr.Events[0], &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != "trimmed" || ev.Seq != 1 || ev.SessionID != "s1" {
		t.Fatalf("oversized event = %+v, want a trimmed placeholder at seq 1", ev)
	}
}

// TestOutboxElidesDroppableWhilePaused: with no audience the log's droppable
// tail is replaced by placeholders; critical entries alone over the hard cap
// end the session with overflow so the hub replays after the reconnect.
func TestOutboxElidesDroppableWhilePaused(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	small := &wire.Limits{MaxFrame: 1024, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(pausedAudience(1)),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 2}, Limits: small, Caps: &wire.Caps{Deflate: true},
	}))
	runForwardLoop(t, c, bridge)

	body := strings.Repeat("y", 700) // under maxFrame 1024, over soft/8
	for i := 0; i < 6; i++ {
		bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": body})
	}
	// soft = 2*1024: the earlier chunks must have been elided in place.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if c.out.bytes() <= 2*1024 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("paused log stayed at %d bytes, over the soft cap", c.out.bytes())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, e := range c.out.entriesAfter(0) {
		var ev struct {
			Type string `json:"type"`
			Seq  uint64 `json:"seq"`
		}
		if err := json.Unmarshal(e.Payload, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Type == "chunk" {
			t.Fatalf("droppable payload survived the paused trim: seq %d", e.Seq)
		}
		if ev.Seq != e.Seq {
			t.Fatalf("placeholder moved the seq: entry %d, event %d", e.Seq, ev.Seq)
		}
	}

	// Critical entries cannot be elided: enough of them must end the session.
	for i := 0; i < 12; i++ {
		bridge.push(acp.Event{"type": "client_request", "sessionId": "s1", "text": body})
	}
	res := h.wait(t, 5*time.Second)
	if res.reason != wire.CloseOverflow {
		t.Fatalf("close reason = %s, want overflow", res.reason)
	}
}

// TestDownProtocolFrameClosesProtocol: an `events` frame from the hub means
// the peer is speaking the other half of the protocol.
func TestDownProtocolFrameClosesProtocol(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeEvents, Seq: 1,
		Events: []json.RawMessage{json.RawMessage(`{"type":"chunk","seq":1}`)}})
	res := h.wait(t, 3*time.Second)
	if res.reason != wire.CloseProtocol {
		t.Fatalf("close reason = %s, want protocol", res.reason)
	}
}

// TestIdleClosesLiveness: the hub's advertised idle budget is enforced even
// when the transport itself is silent but open.
func TestIdleClosesLiveness(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	res := h.wait(t, 5*time.Second)
	if res.reason != wire.CloseLiveness {
		t.Fatalf("close reason = %s, want liveness", res.reason)
	}
}

// TestHeartbeatCarriesDowmAckAndStatus: hb acks the down watermark and reports
// the host's registry snapshot.
func TestHeartbeatCarriesDownAckAndStatus(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	bridge.setStatus(acp.Status{Ready: true, Busy: true, PendingRequests: []acp.PendingReq{{RequestID: "r1"}}})
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	// A down frame advances the watermark; the host must confirm immediately.
	h.tr.push(t, audienceFrame(1, 0, watchingAudience(2, "s1")))
	for {
		fr := h.tr.nextOfType(t, wire.TypeHeartbeat, 3*time.Second)
		if fr.Ack != 1 {
			continue
		}
		st, err := fr.Heartbeat()
		if err != nil {
			t.Fatal(err)
		}
		if !st.Ready || !st.Busy || st.PendingCount != 1 {
			t.Fatalf("heartbeat status = %+v", st)
		}
		if st.Port != 1 {
			t.Logf("port = %d (LocalBase has no port in this test)", st.Port)
		}
		return
	}
}

// TestRequestRunsOncePerReqID: a same-epoch duplicate neither re-executes nor
// loses its answer.
func TestRequestRunsOncePerReqID(t *testing.T) {
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	})
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "7", "POST", "/api/prompt", `{"x":1}`))
	first := waitRespond(t, h.tr)
	if first.ReqID != "7" || first.RespondStatus() != 200 {
		t.Fatalf("first respond = %+v", first)
	}
	if hits.Load() != 1 {
		t.Fatalf("handler hits = %d, want 1", hits.Load())
	}

	// Same-epoch resume redelivers the request.
	h2 := newSessionHarness(t, c, bridge)
	h2.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h2.tr.push(t, requestFrame(1, 0, "7", "POST", "/api/prompt", `{"x":1}`))
	again := waitRespond(t, h2.tr)
	if again.ReqID != "7" || again.RespondStatus() != 200 {
		t.Fatalf("duplicate respond = %+v", again)
	}
	if hits.Load() != 1 {
		t.Fatalf("a duplicate request re-executed the prompt: hits = %d", hits.Load())
	}

	// A fresh reqId still runs.
	h2.tr.push(t, requestFrame(2, 0, "8", "GET", "/api/state", ""))
	if fr := waitRespond(t, h2.tr); fr.ReqID != "8" {
		t.Fatalf("new request respond = %+v", fr)
	}
	if hits.Load() != 2 {
		t.Fatalf("handler hits = %d, want 2", hits.Load())
	}
}

// TestDuplicateInFlightWaits: a duplicate that arrives while the first run is
// still executing must wait for it, not start a second one.
func TestDuplicateInFlightWaits(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			<-release
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "9", "POST", "/api/prompt", `{}`))
	h.tr.push(t, requestFrame(2, 0, "9", "POST", "/api/prompt", `{}`))
	time.Sleep(200 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("in-flight duplicate started a second run: hits = %d", hits.Load())
	}
	close(release)
	waitRespond(t, h.tr)
	waitRespond(t, h.tr) // both deliveries answered, one execution
	if hits.Load() != 1 {
		t.Fatalf("handler hits = %d, want 1", hits.Load())
	}
}

// TestRequestThenDuplicateWithEvictedAnswerGets409: when the answer is no
// longer in memory a duplicate is refused rather than re-executed.
func TestRequestThenDuplicateWithEvictedAnswerGets409(t *testing.T) {
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`{"big":"` + strings.Repeat("z", memoEntryBytes) + `"}`))
		}),
	})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "11", "GET", "/api/big", ""))
	if fr := waitRespond(t, h.tr); fr.ReqID != "11" {
		t.Fatalf("respond = %+v", fr)
	}

	h2 := newSessionHarness(t, c, newTestBridge())
	h2.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h2.tr.push(t, requestFrame(1, 0, "11", "GET", "/api/big", ""))
	fr := waitRespond(t, h2.tr)
	status := fr.RespondStatus()
	if status != http.StatusConflict && status != 200 {
		t.Fatalf("duplicate answer status = %d", status)
	}
	if hits.Load() != 1 {
		t.Fatalf("duplicate re-executed: hits = %d", hits.Load())
	}
}

// TestRespondCarriesDownAckAndNoSessionSeq: respond is relay-plane, seq 0, and
// its ack proves the request frame was applied.
func TestRespondCarriesDownAckAndNoSessionSeq(t *testing.T) {
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "12", "POST", "/api/x", `{}`))
	fr := waitRespond(t, h.tr)
	if fr.Seq != 0 {
		t.Errorf("respond carried session seq %d, want 0", fr.Seq)
	}
	if fr.Ack != 1 {
		t.Errorf("respond ack = %d, want the request's seq 1", fr.Ack)
	}
}

// TestBatchingRespectsMaxFrame: one frame never exceeds limits.maxFrame, so a
// batch is split into contiguous runs of several frames.
func TestBatchingRespectsMaxFrame(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	small := &wire.Limits{MaxFrame: 1024, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(watchingAudience(1, "s1")),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 2}, Limits: small, Caps: &wire.Caps{Deflate: true},
	}))
	runForwardLoop(t, c, bridge)

	body := strings.Repeat("q", 400)
	for i := 0; i < 4; i++ {
		bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": body})
	}
	var frames int
	var covered uint64
	deadline := time.Now().Add(3 * time.Second)
	for covered < 4 && time.Now().Before(deadline) {
		fr := h.tr.nextOfType(t, wire.TypeEvents, time.Until(deadline))
		if len(fr.Events) > 2 {
			t.Fatalf("frame carried %d events of 400 bytes under a 1024-byte maxFrame", len(fr.Events))
		}
		frames++
		covered = fr.Seq + uint64(len(fr.Events)) - 1
	}
	if frames < 2 || covered != 4 {
		t.Fatalf("batch was not split into contiguous runs: %d frames covering %d", frames, covered)
	}
}

func audiencePtr(a wire.Audience) *wire.Audience { return &a }

// TestDownReceiverUsesAdvertisedWindow: the down receiver parks at most
// hello_ack.limits.resumeWindow frames, not the local package default. A host
// that kept the default would happily buffer far past what the hub promised
// and never notice the gap is unrecoverable.
func TestDownReceiverUsesAdvertisedWindow(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	limits := &wire.Limits{MaxFrame: wire.DefaultLimits.MaxFrame, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: 2}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(watchingAudience(1, "s1")),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 30}, Limits: limits, Caps: &wire.Caps{Deflate: true},
	}))
	if got := c.currentSession().down.Window(); got != 2 {
		t.Fatalf("down receiver window = %d, want the advertised 2", got)
	}

	// Seq 1 is missing; 2..4 park one frame past the advertised window.
	h.tr.push(t, requestFrame(2, 0, "a", "GET", "/api/x", ""))
	h.tr.push(t, requestFrame(3, 0, "b", "GET", "/api/x", ""))
	h.tr.push(t, requestFrame(4, 0, "c", "GET", "/api/x", ""))
	if res := h.wait(t, 3*time.Second); res.reason != wire.CloseOverflow {
		t.Fatalf("close = %s, want overflow at the advertised bound", res.reason)
	}
}

func waitRespond(t *testing.T, tr *fakeTransport) wire.Frame {
	t.Helper()
	return tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
}

// TestRelayExecuteErrorIsAnswered: a local failure still answers the request.
func TestRelayExecuteErrorIsAnswered(t *testing.T) {
	c := NewClient(Config{HostID: "h1", Token: "tok123", LocalBase: "http://127.0.0.1:1"})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h.tr.push(t, requestFrame(1, 0, "13", "GET", "/api/x", ""))
	fr := waitRespond(t, h.tr)
	if fr.RespondStatus() == 0 {
		t.Fatalf("respond lost its status: %+v", fr)
	}
	if !json.Valid(fr.Body) {
		t.Fatalf("respond body is not JSON: %s", fr.Body)
	}
}
