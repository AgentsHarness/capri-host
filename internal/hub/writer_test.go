package hub

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"

	"github.com/AgentsHarness/capri-hub/wire"
)

// incompressibleString builds a JSON-friendly payload deflate cannot fold into
// one unit: base64 of random bytes leaves ~6 bits of entropy per character.
func incompressibleString(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// blockFirstFragment parks the transport inside its first unit write and
// returns the entered/release channels: the writer is then mid-frame while the
// session loop keeps running.
func blockFirstFragment(tr *fakeTransport) (entered <-chan struct{}, release chan<- struct{}) {
	in := make(chan struct{})
	out := make(chan struct{})
	var once sync.Once
	tr.setFragmentHook(func() {
		once.Do(func() {
			close(in)
			<-out
		})
	})
	return in, out
}

// drainEvents reassembles records until one complete events frame arrives,
// reporting how many control frames were interleaved between its units and how
// many unit records the frame spanned.
func drainEvents(t *testing.T, tr *fakeTransport, timeout time.Duration) (wire.Frame, int, int) {
	t.Helper()
	var asm wire.Reassembler
	interleaved, units := 0, 0
	deadline := time.Now().Add(timeout)
	for {
		rec := tr.nextRecord(t, time.Until(deadline))
		if !rec.fragment {
			fr, err := wire.Decode(rec.payload)
			if err != nil {
				t.Fatalf("control record: %v", err)
			}
			if fr.Type != wire.TypeHeartbeat {
				t.Fatalf("only hb may interleave with a frame, got %q", fr.Type)
			}
			interleaved++
			continue
		}
		units++
		frame, compressed, complete, err := asm.Accept(rec.payload, wire.DefaultLimits.MaxFrame)
		if err != nil {
			t.Fatalf("unit: %v", err)
		}
		if !complete {
			continue
		}
		if compressed {
			if frame, err = inflatePayload(frame); err != nil {
				t.Fatalf("inflate: %v", err)
			}
		}
		fr, err := wire.Decode(frame)
		if err != nil {
			t.Fatalf("events frame: %v", err)
		}
		return fr, interleaved, units
	}
}

// TestLargeFrameSplitsAndInterleavesHeartbeat: a frame above UnitSize travels
// as several units, and a control hb written while it is open lands between
// them instead of behind the whole frame.
func TestLargeFrameSplitsAndInterleavesHeartbeat(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 30, 120))
	runForwardLoop(t, c, bridge)

	entered, release := blockFirstFragment(h.tr)
	body := incompressibleString(t, 96<<10)
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": body})

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never started the fragmented frame")
	}

	// A status change makes the session loop post an hb while the writer is
	// still parked on the first unit.
	bridge.setStatus(acp.Status{Ready: true, Busy: true})
	c.notifyStatus()
	waitUntil(t, "a heartbeat queued behind the open frame", func() bool {
		s := c.currentSession()
		if s == nil || s.w == nil {
			return false
		}
		s.w.ctrlMu.Lock()
		defer s.w.ctrlMu.Unlock()
		return s.w.ctrl != nil
	})
	close(release)

	fr, interleaved, units := drainEvents(t, h.tr, 5*time.Second)
	if fr.Type != wire.TypeEvents || fr.Seq != 1 {
		t.Fatalf("reassembled frame = %s seq %d", fr.Type, fr.Seq)
	}
	if units < 2 {
		t.Fatalf("frame spanned %d unit records, want a fragmented frame", units)
	}
	if interleaved != 1 {
		t.Fatalf("interleaved control frames = %d, want exactly the pending hb", interleaved)
	}
	var ev struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(fr.Events[0], &ev); err != nil || ev.Text != body {
		t.Fatalf("reassembled payload lost data (err %v, %d bytes)", err, len(ev.Text))
	}
}

// TestWriterDoesNotBlockSessionLoop: while the writer is parked inside a unit,
// the session loop must still apply an inbound ack.
func TestWriterDoesNotBlockSessionLoop(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 30, 120))
	runForwardLoop(t, c, bridge)

	entered, release := blockFirstFragment(h.tr)
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": incompressibleString(t, 96<<10)})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never started the fragmented frame")
	}

	h.tr.push(t, wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeHeartbeat, Ack: 1,
		Status: mustJSON(wire.HeartbeatStatus{})})
	waitUntil(t, "the ack applied while the writer was blocked", func() bool {
		return c.out.acked() == 1
	})
	if s := c.currentSession(); s == nil || !s.w.Busy() {
		t.Fatal("the writer should still be parked mid-frame")
	}
	close(release)
}

// TestRTODeferredWhileWriterBusy: the RTO must not queue a retransmit behind a
// frame the writer is still draining, and must resume once the frame is out.
func TestRTODeferredWhileWriterBusy(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1, "s1"), 1, 60))
	runForwardLoop(t, c, bridge)

	entered, release := blockFirstFragment(h.tr)
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": incompressibleString(t, 96<<10)})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never started the fragmented frame")
	}

	// RTO is 2*keepalive = 2s; wait past it while the writer is still open.
	time.Sleep(2500 * time.Millisecond)
	s := c.currentSession()
	if s == nil {
		t.Fatal("the session ended while the writer was blocked")
	}
	if s.w.retransmit.Load() {
		t.Fatal("the RTO fired while the writer was busy")
	}
	close(release)

	if fr, _, _ := drainEvents(t, h.tr, 5*time.Second); fr.Seq != 1 {
		t.Fatalf("first frame seq = %d", fr.Seq)
	}
	// No ack ever arrives, so the RTO must replay from the ack watermark.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		fr, ok := h.tr.tryFrame(time.Until(deadline))
		if !ok {
			break
		}
		if fr.Type == wire.TypeEvents && fr.Seq == 1 {
			return
		}
	}
	t.Fatal("no retransmit after the writer finished its frame")
}

// TestRetransmitReplaysOnlyTheOldestBatch: with several frames unacked, one
// RTO replays the first batch and stops; the rest waits for the next RTO or an
// ack, so a repair never buries the link under the whole log.
func TestRetransmitReplaysOnlyTheOldestBatch(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	h := newSessionHarness(t, c, bridge)
	small := &wire.Limits{MaxFrame: 1024, MaxRelayBody: wire.DefaultLimits.MaxRelayBody, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(watchingAudience(1, "s1")),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 30}, Limits: small, Caps: testCaps(),
	}))
	runForwardLoop(t, c, bridge)

	// ~600-byte events: one per frame under maxFrame 1024, and the pair stays
	// under the outbox soft cap so nothing is elided.
	body := strings.Repeat("q", 555)
	for i := 0; i < 2; i++ {
		bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": body})
	}
	sent := map[uint64]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for len(sent) < 2 && time.Now().Before(deadline) {
		fr := h.tr.nextOfType(t, wire.TypeEvents, time.Until(deadline))
		sent[fr.Seq] = true
	}
	if len(sent) != 2 {
		t.Fatalf("first flush covered %d frames, want 2", len(sent))
	}

	// No ack: the RTO may replay the oldest batch, never the whole log.
	replay := h.tr.nextOfType(t, wire.TypeEvents, 5*time.Second)
	if replay.Seq != 1 {
		t.Fatalf("retransmit started at seq %d, want 1", replay.Seq)
	}
	if fr, ok := h.tr.tryFrame(700 * time.Millisecond); ok && fr.Type == wire.TypeEvents {
		t.Fatalf("one RTO replayed beyond the oldest batch: seq %d", fr.Seq)
	}
}

// TestBusyHostHeartbeatsAtBusyCadence: an active turn sends hb every
// wire.BusyKeepalive instead of the negotiated keepalive.
func TestBusyHostHeartbeatsAtBusyCadence(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	bridge.setStatus(acp.Status{Ready: true, Busy: true})
	h := newSessionHarness(t, c, bridge)
	start := time.Now()
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1), 5, 60))

	for i := 0; i < 2; i++ {
		fr := h.tr.nextOfType(t, wire.TypeHeartbeat, 4*time.Second)
		st, err := fr.Heartbeat()
		if err != nil {
			t.Fatal(err)
		}
		if !st.Busy {
			t.Fatalf("heartbeat %d lost the busy flag: %+v", i, st)
		}
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("two busy heartbeats took %v, want about 2s at the 1s cadence", took)
	}
}

// waitForProbe scans uplink frames until the host probes a silent hub.
func waitForProbe(t *testing.T, tr *fakeTransport, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		fr, ok := tr.tryFrame(time.Until(deadline))
		if !ok {
			t.Fatal("host never sent a liveness probe")
		}
		if fr.Type == wire.TypeHeartbeat && fr.Probe != 0 {
			return fr.Probe
		}
	}
}

// TestProbeAfterSilenceIsAnswered: silence past ProbeAfter produces hb{probe},
// and the matching probeAck keeps the session alive.
func TestProbeAfterSilenceIsAnswered(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1), 5, 60))

	probe := waitForProbe(t, h.tr, wire.ProbeAfter+5*time.Second)
	ack, err := wire.NewHeartbeatProbe(0, probe, wire.HeartbeatStatus{}, true)
	if err != nil {
		t.Fatal(err)
	}
	h.tr.push(t, ack)

	time.Sleep(wire.ProbeTimeout + 500*time.Millisecond)
	if !h.alive() {
		t.Fatal("a probe ack did not keep the session alive")
	}
}

// TestMissingProbeAckClosesLiveness: a probe with no answer inside
// ProbeTimeout ends the session with liveness.
func TestMissingProbeAckClosesLiveness(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1), 5, 60))

	waitForProbe(t, h.tr, wire.ProbeAfter+5*time.Second)
	res := h.wait(t, wire.ProbeTimeout+3*time.Second)
	if res.reason != wire.CloseLiveness {
		t.Fatalf("close reason = %s, want liveness", res.reason)
	}
}

// TestRespondProbeAckAnswersImmediately: an hb carrying probe is answered with
// probeAck on the control slot.
func TestRespondProbeAckAnswersImmediately(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1), 60, 120))

	probe, err := wire.NewHeartbeatProbe(0, 4242, wire.HeartbeatStatus{Ready: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	h.tr.push(t, probe)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fr, ok := h.tr.tryFrame(time.Until(deadline))
		if !ok {
			break
		}
		if fr.Type == wire.TypeHeartbeat && fr.ProbeAck == 4242 {
			return
		}
	}
	t.Fatal("host did not echo the probe nonce")
}
