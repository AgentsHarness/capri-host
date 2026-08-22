package hub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// TestEnqueueEventsTryLockDoesNotBlock: while a replay holds the ordering
// lock (each critical frame briefly blocking on a full queue), a live
// enqueue must DROP (and flag catch-up) instead of waiting — blocking
// forwardLoop there lets bridge.Broadcast drop droppable events before
// they ever reach the replay ring, where no repair path can find them.
func TestEnqueueEventsTryLockDoesNotBlock(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)

	c.enqueueMu.Lock() // stand in for the in-progress replay
	defer c.enqueueMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.enqueueEvents([]acp.Event{
			{"type": "chunk", "text": "a", "seq": uint64(1)},
			{"type": "chunk", "text": "b", "seq": uint64(2)},
		})
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("enqueueEvents blocked behind the replay's ordering lock")
	}
	if !c.needCatchUp.Load() {
		t.Fatal("dropped live batch must set needCatchUp for the heartbeat repair")
	}
	select {
	case p := <-c.sendCh:
		t.Fatalf("frame enqueued while the ordering lock was held: %.40s", p)
	default:
	}
}

// TestRepairDroppedEventsFromHubAck: the heartbeat repair anchors at the
// hub's delivery watermark (ping "seq" ack) when the session has one, not
// at the enqueue watermark — re-sending only what the hub might be missing.
func TestRepairDroppedEventsFromHubAck(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 16)

	evs := make([]acp.Event, 0, 6)
	for i := 1; i <= 6; i++ {
		evs = append(evs, acp.Event{"type": "chunk", "text": "x", "seq": uint64(i)})
	}
	c.seqAndReplay(evs)
	c.seqMu.Lock()
	c.hubAckSeq = 3   // hub confirmed delivery through seq 3
	c.lastSentSeq = 6 // seq 4..6 were enqueued; 5,6 dropped on a full queue
	c.seqMu.Unlock()
	c.setBrowserSubscribers(1)
	c.needCatchUp.Store(true)

	c.repairDroppedEvents()

	first := uint64(0)
	seen := 0
	deadline := time.After(time.Second)
	for seen < 3 {
		select {
		case payload := <-c.sendCh:
			var f struct {
				Type     string           `json:"type"`
				SeqStart uint64           `json:"seqStart"`
				Events   []map[string]any `json:"events"`
			}
			if json.Unmarshal(payload, &f) != nil || f.Type != "events" {
				continue
			}
			if f.SeqStart < first || (first == 0 && f.SeqStart != 4) {
				t.Fatalf("repair frame seqStart = %d, want to start at the ack watermark 4 (first=%d)", f.SeqStart, first)
			}
			first = f.SeqStart
			seen += len(f.Events)
		case <-deadline:
			t.Fatalf("repair re-sent only %d events, want 3 (seq 4..6)", seen)
		}
	}
	if seen != 3 {
		t.Fatalf("repair re-sent %d events, want exactly 3 (seq 4..6)", seen)
	}
}

// TestRepairDroppedEventsFallsBackToLastSentSeq: without an ack this
// session (old hub / hello not yet processed) the repair falls back to the
// enqueue watermark — same anchor the repair used before ping acks existed.
func TestRepairDroppedEventsFallsBackToLastSentSeq(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 16)

	evs := make([]acp.Event, 0, 8)
	for i := 1; i <= 8; i++ {
		evs = append(evs, acp.Event{"type": "chunk", "text": "x", "seq": uint64(i)})
	}
	c.seqAndReplay(evs)
	c.seqMu.Lock()
	c.lastSentSeq = 6 // seq 7,8 dropped on a full queue
	c.seqMu.Unlock()
	c.setBrowserSubscribers(1)
	c.needCatchUp.Store(true)

	c.repairDroppedEvents()

	seen := 0
	deadline := time.After(time.Second)
	for seen < 2 {
		select {
		case payload := <-c.sendCh:
			var f struct {
				Type   string           `json:"type"`
				Events []map[string]any `json:"events"`
			}
			if json.Unmarshal(payload, &f) != nil || f.Type != "events" {
				continue
			}
			for _, ev := range f.Events {
				seen++
				if s, ok := ev["seq"].(float64); ok && (s != 7 && s != 8) {
					t.Fatalf("fallback repair re-sent seq %v, want only 7..8", s)
				}
			}
		case <-deadline:
			t.Fatalf("fallback repair re-sent only %d events, want 2 (seq 7..8)", seen)
		}
	}
	if seen != 2 {
		t.Fatalf("fallback repair re-sent %d events, want exactly 2", seen)
	}
}

// TestNoteHubAckFiltersAlienSeq: an ack naming a seq this process never
// produced (a ping in flight when a seq_reset landed still carries the
// previous epoch's high watermark) must not anchor repairs past the events
// the current epoch still needs to send. Lower acks never regress the
// watermark.
func TestNoteHubAckFiltersAlienSeq(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.seqAndReplay([]acp.Event{
		{"type": "chunk", "seq": uint64(1)},
		{"type": "chunk", "seq": uint64(2)},
		{"type": "chunk", "seq": uint64(3)},
		{"type": "chunk", "seq": uint64(4)},
		{"type": "chunk", "seq": uint64(5)},
	})

	c.noteHubAck(4)
	c.seqMu.Lock()
	if c.hubAckSeq != 4 {
		t.Fatalf("hubAckSeq = %d, want 4", c.hubAckSeq)
	}
	c.seqMu.Unlock()

	c.noteHubAck(99) // previous epoch's watermark: must be ignored
	c.seqMu.Lock()
	if c.hubAckSeq != 4 {
		t.Fatalf("alien ack moved hubAckSeq to %d, want it to stay 4", c.hubAckSeq)
	}
	c.seqMu.Unlock()

	c.noteHubAck(2) // stale ping: watermark must not regress
	c.seqMu.Lock()
	if c.hubAckSeq != 4 {
		t.Fatalf("stale ack regressed hubAckSeq to %d, want 4", c.hubAckSeq)
	}
	c.seqMu.Unlock()
}

// TestPingSeqAckRecorded drives readLoop with canned downlink frames: a
// ping carrying "seq" arms the delivery watermark, and a later ping with a
// pre-reset (alien) high seq does not move it.
func TestPingSeqAckRecorded(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 128)
	c.reqCh = make(chan reqFrame, 8)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	evs := make([]acp.Event, 0, 50)
	for i := 1; i <= 50; i++ {
		evs = append(evs, acp.Event{"type": "log", "seq": uint64(i)})
	}
	c.seqAndReplay(evs) // nextSeq = 50

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := [][]byte{
		[]byte(`{"v":1,"type":"ping","ts":1,"subscribers":1,"subsGen":1,"seq":42}`),
		[]byte(`{"v":1,"type":"ping","ts":2,"subscribers":1,"subsGen":2,"seq":99}`),
	}
	i := 0
	recv := func() ([]byte, error) {
		if i < len(frames) {
			f := frames[i]
			i++
			return f, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	go c.readLoop(ctx, recv, bridge)

	deadline := time.After(2 * time.Second)
	for {
		c.seqMu.Lock()
		got := c.hubAckSeq
		c.seqMu.Unlock()
		if got == 42 {
			return
		}
		if got > 42 {
			t.Fatalf("hubAckSeq = %d, want 42 (alien seq 99 must be filtered)", got)
		}
		select {
		case <-deadline:
			t.Fatalf("ping seq ack never recorded (hubAckSeq = %d, want 42)", got)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestQuicNegativeCache: quicFailMax consecutive ESTABLISHMENT failures arm
// the cooldown (pickTransport → "ws" with no dial attempt); an established
// session resets the failure count; the cooldown lapses on its own so a
// recovered UDP path is picked back up.
func TestQuicNegativeCache(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok"})

	if got := c.pickTransport(); got != "quic" {
		t.Fatalf("fresh client transport = %q, want quic", got)
	}
	c.noteQuicOutcome(false)
	c.noteQuicOutcome(false)
	if got := c.pickTransport(); got != "quic" {
		t.Fatalf("transport after 2 failures = %q, want quic (below quicFailMax)", got)
	}
	c.noteQuicOutcome(false)
	if got := c.pickTransport(); got != "ws" {
		t.Fatalf("transport after %d failures = %q, want ws (cooldown)", quicFailMax, got)
	}

	// Established session resets the count but not the armed cooldown.
	c.noteQuicOutcome(true)
	if got := c.pickTransport(); got != "ws" {
		t.Fatal("established session must not cancel an armed cooldown")
	}

	// Cooldown lapse: QUIC again, and the reset counter needs a full
	// quicFailMax failures to re-arm.
	c.quicCooldownUntil = time.Now().Add(-time.Second)
	if got := c.pickTransport(); got != "quic" {
		t.Fatalf("transport after cooldown lapse = %q, want quic", got)
	}
	c.noteQuicOutcome(false)
	c.noteQuicOutcome(false)
	if got := c.pickTransport(); got != "quic" {
		t.Fatalf("transport after 2 post-reset failures = %q, want quic", got)
	}
	c.noteQuicOutcome(false)
	if got := c.pickTransport(); got != "ws" {
		t.Fatalf("transport after re-accumulated failures = %q, want ws", got)
	}

	// DisableQUIC always wins.
	c.quicCooldownUntil = time.Time{}
	c.cfg.DisableQUIC = true
	if got := c.pickTransport(); got != "ws" {
		t.Fatalf("DisableQUIC transport = %q, want ws", got)
	}
}
