package hub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"

	"github.com/AgentsHarness/capri-hub/wire"
)

// This file is the Stage-4 fault matrix at the host end of the session: the
// failure modes v2 must answer, driven through the real
// session loop over the in-process fake transport (deterministic) plus Run
// over a real WebSocket where the reconnect policy matters.

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within 5s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func eventSeqs(fr wire.Frame) []uint64 {
	out := make([]uint64, len(fr.Events))
	for i, raw := range fr.Events {
		var ev struct {
			Seq uint64 `json:"seq"`
		}
		_ = unmarshal(raw, &ev)
		out[i] = ev.Seq
	}
	return out
}

func unmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

// ── same-epoch resume ──────────────────────────────────────────────────

// TestSameEpochResumeReplaysUnackedTail drops the link under a client that
// still holds unacked events, reconnects with the SAME epoch, and asserts the
// hub's ack decides the replay start. No new Client, outbox or epoch is
// created: c.runSession is the reconnect seam, which is exactly what Run uses
// between attempts.
func TestSameEpochResumeReplaysUnackedTail(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()

	first := newSessionHarness(t, c, bridge)
	hello1 := first.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "a"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "b"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "c"})
	fr := first.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if fr.Seq != 1 || len(fr.Events) != 3 {
		t.Fatalf("first send = seq %d with %d events, want 1 with 3", fr.Seq, len(fr.Events))
	}
	// The hub applied event 1 and the ack reached the host before the drop;
	// events 2 and 3 stay unacked.
	first.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeHeartbeat, Ack: 1,
		Status: mustJSON(wire.HeartbeatStatus{})})
	waitUntil(t, "the hub ack", func() bool { return c.out.acked() == 1 })

	// The link dies with the rest of the run unacked. The outbox and epoch
	// survive.
	first.cancel()
	if res := first.wait(t, 3*time.Second); res.reason != wire.CloseNormal && res.reason != wire.CloseLiveness {
		t.Fatalf("first session close = %s", res.reason)
	}

	// The hub applies only event 1 before the drop and says so in ackUp.
	second := newSessionHarness(t, c, bridge)
	hello2 := second.handshake(helloAckFrameLiveness(c.epoch, 1, 0, watchingAudience(1, "s1"), 1, 30))
	if hello1.Epoch != hello2.Epoch || hello2.Epoch != c.epoch {
		t.Fatalf("resume changed the epoch: %d → %d", hello1.Epoch, hello2.Epoch)
	}
	if hello2.Resume.Up != 1 {
		t.Fatalf("resume.up = %d, want the hub's ack 1", hello2.Resume.Up)
	}

	replay := second.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if replay.Seq != 2 {
		t.Fatalf("replay started at seq %d, want 2 (acked+1)", replay.Seq)
	}
	seqs := eventSeqs(replay)
	if len(seqs) != 2 || seqs[0] != 2 || seqs[1] != 3 {
		t.Fatalf("replay = %v, want exactly the unacked tail [2 3]", seqs)
	}
}

// TestNewEpochRebasesSequenceSpace restarts the host process (new Client, new
// epoch): the outbox rebases at the event bus's high-water mark instead of
// replaying the retired epoch's log.
func TestNewEpochRebasesSequenceSpace(t *testing.T) {
	bridge := newTestBridge()
	old := newSessionClient(t)
	first := newSessionHarness(t, old, bridge)
	first.handshake(helloAckFrameLiveness(old.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	stopOldForward := runForwardLoop(t, old, bridge)
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "old-epoch"})
	if fr := first.tr.nextOfType(t, wire.TypeEvents, 3*time.Second); fr.Seq != 1 {
		t.Fatalf("old epoch first seq = %d", fr.Seq)
	}
	first.cancel()
	first.wait(t, 3*time.Second)
	stopOldForward()

	// The agent bus kept running while the process was down: its high-water
	// mark is now 10. Those events are history the FE rebuilds from the host.
	bridge.setSeq(10)

	fresh := NewClient(Config{HostID: "h1", HostName: "H1", Token: "tok123", LocalBase: "http://127.0.0.1:1"})
	if fresh.epoch == old.epoch {
		t.Fatal("a restarted process reused the retired epoch")
	}
	runForwardLoop(t, fresh, bridge)
	second := newSessionHarness(t, fresh, bridge)
	hello := second.handshake(helloAckFrameLiveness(fresh.epoch, 10, 0, watchingAudience(1, "s1"), 1, 30))
	if hello.Resume.Up != 10 {
		t.Fatalf("new epoch hello.resume.up = %d, want the bus high-water 10", hello.Resume.Up)
	}
	if n := fresh.out.len(); n != 0 {
		t.Fatalf("new epoch inherited %d unacked events from the retired log", n)
	}

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "new-epoch"})
	fr := second.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if fr.Seq != 11 {
		t.Fatalf("new epoch first live seq = %d, want baseline+1 = 11", fr.Seq)
	}
}

// ── idle / blackhole ───────────────────────────────────────────────────

// TestBlackholeClosesLiveness: frames stop in both directions; the host must
// give up on its own idle timer even though the transport stays open, and a
// busy link must never be killed by the same timer.
func TestBlackholeClosesLiveness(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 2))
	runForwardLoop(t, c, bridge)

	// The host keeps writing; the hub answers nothing. The receive clock is
	// the only liveness signal, as the protocol specifies.
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "into-the-void"})
	res := h.wait(t, 5*time.Second)
	if res.reason != wire.CloseLiveness {
		t.Fatalf("blackhole close = %s, want liveness", res.reason)
	}
}

// TestBusyLinkIsNotKilled: a frame every 500ms keeps the session alive well
// past the 2s idle budget.
func TestBusyLinkIsNotKilled(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 2))

	hb, err := wire.NewHeartbeat(0, wire.HeartbeatStatus{Ready: true})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		h.tr.push(t, hb)
		time.Sleep(400 * time.Millisecond)
	}
	if !h.alive() {
		t.Fatal("a link exchanging frames was closed by the idle timer")
	}
	// Once the frames stop, the same timer fires.
	if res := h.wait(t, 5*time.Second); res.reason != wire.CloseLiveness {
		t.Fatalf("close after silence = %s, want liveness", res.reason)
	}
}

// TestRunReconnectsAfterBlackholedLink: Run must notice a link that stopped
// answering, reconnect with the same epoch, and then stay connected instead of
// entering a flap loop.
func TestRunReconnectsAfterBlackholedLink(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1, "s1"))
	hub.live = wire.Liveness{Keepalive: 1, Idle: 2}
	hub.blackholeFirst = true
	c := NewClient(Config{URL: hub.url(), HostID: "h1", HostName: "H1", Token: "tok123", DisableQUIC: true})
	runClient(t, c, newTestBridge())

	deadline := time.Now().Add(15 * time.Second)
	for hub.connCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("host never reconnected after the blackhole (conns=%d, last error %q)",
				hub.connCount(), c.State().LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitConnectedWithin(t, c, 10*time.Second)

	// No flap loop: the replacement link stays up across several keepalive
	// intervals, and the reconnect reused the credential (no re-pair).
	time.Sleep(3500 * time.Millisecond)
	if n := hub.connCount(); n != 2 {
		t.Fatalf("host flapped after the reconnect: %d connections", n)
	}
	if !c.connected.Load() {
		t.Fatal("host is not connected after the successful reconnect")
	}
	hub.mu.Lock()
	pairs := hub.pairs
	hub.mu.Unlock()
	if pairs != 0 {
		t.Fatalf("a liveness reconnect re-paired (%d pair calls)", pairs)
	}
}

// ── audience ───────────────────────────────────────────────────────────

// TestAudienceResumeFromAckNotBeginning: watchers 1 after a pause resumes at
// the ack watermark, not from the start of the log.
func TestAudienceResumeFromAckNotBeginning(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	runForwardLoop(t, c, bridge)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "a"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "b"})
	if fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second); fr.Seq != 1 || len(fr.Events) != 2 {
		t.Fatalf("first frame = seq %d, %d events", fr.Seq, len(fr.Events))
	}
	h.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeHeartbeat, Ack: 2,
		Status: mustJSON(wire.HeartbeatStatus{})})
	waitUntil(t, "the hub ack", func() bool { return c.out.acked() == 2 })

	h.tr.push(t, audienceFrame(1, 2, pausedAudience(2)))
	waitUntil(t, "the paused audience", func() bool { return c.out.isPaused() })

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "c"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "d"})
	waitUntil(t, "the paused backlog", func() bool { return c.out.next() == 4 })

	h.tr.push(t, audienceFrame(2, 2, watchingAudience(3, "s1")))
	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	if fr.Seq != 3 {
		t.Fatalf("resume sent seq %d, want 3 (acked+1), not the beginning", fr.Seq)
	}
	if seqs := eventSeqs(fr); len(seqs) != 2 || seqs[0] != 3 || seqs[1] != 4 {
		t.Fatalf("resume carried %v, want [3 4]", seqs)
	}
}

// TestAudienceNarrowedTrimKeepsSeqGapless: a narrowed viewing set turns
// unwatched bulk events into trimmed placeholders in place; the run stays
// contiguous and the hub's ack advances over it.
func TestAudienceNarrowedTrimKeepsSeqGapless(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	runForwardLoop(t, c, bridge)

	c.setAudience(wire.Audience{Gen: 2, Watchers: 1, Sessions: []string{"s1"}})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "watched-1"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s2", "text": "unwatched-1"})
	bridge.push(acp.Event{"type": "thought", "sessionId": "s2", "text": "unwatched-2"})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "watched-2"})

	var seqs []uint64
	var kinds []string
	deadline := time.Now().Add(3 * time.Second)
	for len(seqs) < 4 && time.Now().Before(deadline) {
		fr := h.tr.nextOfType(t, wire.TypeEvents, time.Until(deadline))
		for _, raw := range fr.Events {
			var ev struct {
				Type string `json:"type"`
				Seq  uint64 `json:"seq"`
			}
			if err := unmarshal(raw, &ev); err != nil {
				t.Fatalf("event: %v", err)
			}
			seqs = append(seqs, ev.Seq)
			kinds = append(kinds, ev.Type)
		}
	}
	if len(seqs) != 4 {
		t.Fatalf("got %d events, want 4", len(seqs))
	}
	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("seq hole in the placeholder run: %v", seqs)
		}
	}
	if kinds[0] != "chunk" || kinds[3] != "chunk" {
		t.Fatalf("watched events lost their payload: %v", kinds)
	}
	if kinds[1] != "trimmed" || kinds[2] != "trimmed" {
		t.Fatalf("unwatched bulk events were not trimmed: %v", kinds)
	}

	h.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeHeartbeat, Ack: 4,
		Status: mustJSON(wire.HeartbeatStatus{})})
	waitUntil(t, "the hub watermark over the trimmed run", func() bool { return c.out.len() == 0 })
}

// TestAudienceSwitchRecoversTranscript: switching the viewed session stops
// trimming that session's transcript, while the previously viewed one becomes
// the trimmed one.
func TestAudienceSwitchRecoversTranscript(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 30))
	runForwardLoop(t, c, bridge)

	c.setAudience(wire.Audience{Gen: 2, Watchers: 1, Sessions: []string{"s1"}})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s2", "text": "before-switch"})
	if fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second); !trimmedEvent(t, fr.Events[0]) {
		t.Fatalf("unwatched session was not trimmed before the switch: %s", fr.Events[0])
	}

	c.setAudience(wire.Audience{Gen: 3, Watchers: 1, Sessions: []string{"s2"}})
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s2", "text": "after-switch"})
	fr := h.tr.nextOfType(t, wire.TypeEvents, 3*time.Second)
	var ev struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := unmarshal(fr.Events[len(fr.Events)-1], &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != "chunk" || ev.Text != "after-switch" {
		t.Fatalf("switching the viewed session did not recover the transcript: %+v", ev)
	}
}

func trimmedEvent(t *testing.T, raw []byte) bool {
	t.Helper()
	var ev struct {
		Type string `json:"type"`
	}
	if err := unmarshal(raw, &ev); err != nil {
		t.Fatalf("event: %v", err)
	}
	return ev.Type == "trimmed"
}

// ── request dedupe ─────────────────────────────────────────────────────

// TestRequestReplayAfterDropRunsOnce: a request whose delivery was replayed
// after a same-epoch drop must not run the local handler twice, including
// while the first execution is still in flight.
func TestRequestReplayAfterDropRunsOnce(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			<-release
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"ran":1}`))
		}),
	})

	first := newSessionHarness(t, c, newTestBridge())
	first.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	first.tr.push(t, requestFrame(1, 0, "7", "POST", "/api/prompt", `{"x":1}`))
	waitUntil(t, "the first execution", func() bool { return hits.Load() == 1 })

	// The link dies before the answer. The execution is deliberately not bound
	// to the session, so the duplicate must wait for it instead of restarting.
	first.cancel()
	first.wait(t, 3*time.Second)

	second := newSessionHarness(t, c, newTestBridge())
	second.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	second.tr.push(t, requestFrame(1, 0, "7", "POST", "/api/prompt", `{"x":1}`))

	// Let the run finish; the single answer is delivered on the live link and
	// the replayed request itself stays a no-op.
	close(release)
	fr := waitRespond(t, second.tr)
	if fr.ReqID != "7" || fr.RespondStatus() != 200 {
		t.Fatalf("duplicate answer = %+v", fr)
	}
	var body struct {
		Ran int `json:"ran"`
	}
	if err := unmarshal(fr.Body, &body); err != nil || body.Ran != 1 {
		t.Fatalf("duplicate answer body = %s", fr.Body)
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times: a replayed request re-executed the prompt", hits.Load())
	}
}

// TestDuplicateOfFinishedRequestGetsCachedAnswer: after a drop, an answer that
// never reached the hub is replayed from the answer log, and the duplicate
// request itself does not re-execute.
func TestDuplicateOfFinishedRequestGetsCachedAnswer(t *testing.T) {
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"token":"answer-42"}`))
		}),
	})
	first := newSessionHarness(t, c, newTestBridge())
	first.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	first.tr.push(t, requestFrame(1, 0, "21", "GET", "/api/state", ""))
	firstResp := waitRespond(t, first.tr)
	if hits.Load() != 1 {
		t.Fatalf("handler hits = %d, want 1", hits.Load())
	}
	first.cancel()
	first.wait(t, 3*time.Second)

	// The hub never applied the answer and replays the request; its table
	// still lists the request, so the answer resumes from chunk 1.
	ack := helloAckFrame(c.epoch, 0, 0, watchingAudience(1))
	ack.RelayAcks = []wire.RelayAck{{ReqID: "21"}}
	second := newSessionHarness(t, c, newTestBridge())
	second.handshake(ack)
	second.tr.push(t, requestFrame(1, 0, "21", "GET", "/api/state", ""))
	again := waitRespond(t, second.tr)
	if string(again.Body) != string(firstResp.Body) {
		t.Fatalf("replayed answer = %s, want %s", again.Body, firstResp.Body)
	}
	if hits.Load() != 1 {
		t.Fatalf("duplicate re-executed: hits = %d", hits.Load())
	}
}

// ── outbox overflow ────────────────────────────────────────────────────

// TestOutboxElisionKeepsSeqAndWatermark: droppable payloads become in-place
// placeholders; the frame run stays contiguous and the hub's ack drains the
// whole log.
func TestOutboxElisionKeepsSeqAndWatermark(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	small := &wire.Limits{MaxFrame: 1024, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(pausedAudience(1)),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 30}, Limits: small, Caps: testCaps(),
	}))
	runForwardLoop(t, c, bridge)

	body := strings.Repeat("y", 700)
	for i := 0; i < 6; i++ {
		bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": body})
	}
	waitUntil(t, "the paused log", func() bool { return c.out.next() == 6 })
	waitUntil(t, "the paused trim", func() bool { return c.out.bytes() <= 2*1024 })
	for i, e := range c.out.entriesAfter(0) {
		if e.Seq != uint64(i+1) {
			t.Fatalf("entry %d moved to seq %d", i, e.Seq)
		}
		if !trimmedEvent(t, e.Payload) {
			t.Fatalf("entry %d kept its droppable payload", e.Seq)
		}
	}

	h.tr.push(t, audienceFrame(1, 0, watchingAudience(2, "s1")))
	var seqs []uint64
	deadline := time.Now().Add(3 * time.Second)
	for len(seqs) < 6 && time.Now().Before(deadline) {
		fr := h.tr.nextOfType(t, wire.TypeEvents, time.Until(deadline))
		seqs = append(seqs, eventSeqs(fr)...)
	}
	if len(seqs) != 6 {
		t.Fatalf("sent %v, want the six contiguous placeholders", seqs)
	}
	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("placeholder run has a hole: %v", seqs)
		}
	}

	h.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeHeartbeat, Ack: 6,
		Status: mustJSON(wire.HeartbeatStatus{})})
	waitUntil(t, "the watermark over the placeholders", func() bool { return c.out.len() == 0 })
}

// TestOutboxCriticalOverflowReplaysAfterReconnect: critical events over the
// hard cap end the session with overflow; Run reconnects with the same epoch
// and replays the retained log from seq 1.
func TestOutboxCriticalOverflowReplaysAfterReconnect(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1, "s1"))
	hub.noAutoAck = true
	hub.limits = &wire.Limits{MaxFrame: 1024, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	c := NewClient(Config{URL: hub.url(), HostID: "h1", HostName: "H1", Token: "tok123", DisableQUIC: true})
	bridge := newTestBridge()
	runClient(t, c, bridge)
	waitConnected(t, c)

	body := strings.Repeat("y", 700)
	for i := 0; i < 14; i++ {
		bridge.push(acp.Event{"type": "client_request", "sessionId": "s1", "text": body})
	}

	deadline := time.Now().Add(20 * time.Second)
	for hub.connCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("overflow did not trigger a reconnect (conns=%d, last error %q)",
				hub.connCount(), c.State().LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The replacement session replays the retained critical log from its head.
	for {
		if _, ok := hub.frameOnConnAfter(2, 1, 0); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the overflowed log was not replayed after the reconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ── compression ────────────────────────────────────────────────────────

// TestCorruptDownlinkClosesUnreadable: a transport that cannot decode a frame
// reports it as unreadable; the session must close with that verdict (the hub
// then knows to replay from its watermark), not as an ordinary disconnect.
func TestCorruptDownlinkClosesUnreadable(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h.tr.failRecv(fmt.Errorf("%w: inflate: corrupt deflate stream", wire.ErrUnreadable))
	res := h.wait(t, 3*time.Second)
	if res.reason != wire.CloseUnreadable {
		t.Fatalf("corrupt frame close = %s, want unreadable", res.reason)
	}
}

// TestOverflowWhileDisconnectedKeepsCriticalEvents: once the log is over the
// hard cap (critical tail alone), the session is gone but the process keeps
// publishing. A flush must not abandon the rest of its batch: a turn terminal
// that lands behind the overflow point has to stay in the log (the hub replays
// it after the reconnect) instead of being silently turned into a placeholder.
func TestOverflowWhileDisconnectedKeepsCriticalEvents(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	small := &wire.Limits{MaxFrame: 1024, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(pausedAudience(1)),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 30}, Limits: small, Caps: testCaps(),
	}))
	runForwardLoop(t, c, bridge)

	body := strings.Repeat("y", 700)
	for i := 0; i < 12; i++ {
		bridge.push(acp.Event{"type": "client_request", "sessionId": "s1", "text": body})
	}
	res := h.wait(t, 5*time.Second)
	if res.reason != wire.CloseOverflow {
		t.Fatalf("close reason = %s, want overflow", res.reason)
	}
	next := c.out.next()

	// No session now. A batch of droppable events followed by one terminal
	// event: every one of them must be logged at its own seq.
	for i := 0; i < 10; i++ {
		bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": body})
	}
	bridge.push(acp.Event{"type": "done", "sessionId": "s1"})
	wantLast := next + 11
	waitUntil(t, "the batch to be logged", func() bool { return c.out.next() == wantLast })

	var found bool
	for _, e := range c.out.entriesAfter(0) {
		if e.Seq != wantLast {
			continue
		}
		var ev struct {
			Type string `json:"type"`
		}
		if err := unmarshal(e.Payload, &ev); err != nil {
			t.Fatal(err)
		}
		found = ev.Type == "done"
	}
	if !found {
		t.Fatalf("the terminal event at seq %d was dropped after the overflow; a later append would replace it with a placeholder", wantLast)
	}
}

// TestWSTextAndBinaryUplink: below the compression floor a frame rides as a
// WS text message; above it, as one deflated binary message the hub decodes
// back to the same events.
func TestWSTextAndBinaryUplink(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1, "s1"))
	bridge := newTestBridge()
	c := NewClient(Config{URL: hub.url(), HostID: "h1", HostName: "H1", Token: "tok123", DisableQUIC: true})
	runClient(t, c, bridge)
	waitConnected(t, c)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "small"})
	hub.waitEvent(t, "small", 5*time.Second)
	hub.mu.Lock()
	afterSmall := hub.binary
	hub.mu.Unlock()
	if afterSmall != 0 {
		t.Fatalf("a small uplink frame was compressed (%d binary frames)", afterSmall)
	}

	big := strings.Repeat("compressible ", 400)
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": big})
	hub.waitEvent(t, big, 5*time.Second)
	hub.mu.Lock()
	afterBig := hub.binary
	hub.mu.Unlock()
	if afterBig == 0 {
		t.Fatal("a large uplink frame was not sent as a deflated binary message")
	}
}

// TestRelayAnswerFollowsTheLiveSession: a long execution started on a link
// that then drops must deliver its answer on the replacement link. The hub has
// already acked the request frame, so it will not resend it; routing the
// answer to the dead session would strand the browser for the relay deadline.
func TestRelayAnswerFollowsTheLiveSession(t *testing.T) {
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

	first := newSessionHarness(t, c, newTestBridge())
	first.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	first.tr.push(t, requestFrame(1, 0, "31", "POST", "/api/prompt", `{}`))
	waitUntil(t, "the execution to start", func() bool { return hits.Load() == 1 })

	// The link dies while the run is in flight; the hub already acked the
	// request frame, so the replacement carries no duplicate.
	first.cancel()
	first.wait(t, 3*time.Second)

	second := newSessionHarness(t, c, newTestBridge())
	second.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	close(release)
	fr := waitRespond(t, second.tr)
	if fr.ReqID != "31" || fr.RespondStatus() != 200 {
		t.Fatalf("answer on the live session = %+v", fr)
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times", hits.Load())
	}
}
