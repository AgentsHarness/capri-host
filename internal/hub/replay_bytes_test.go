package hub

import (
	"context"
	"strings"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

func setReplayByteCap(t *testing.T, cap int) {
	t.Helper()
	prev := replayByteCap
	replayByteCap = cap
	t.Cleanup(func() { replayByteCap = prev })
}

func ringLenAndBytes(c *Client) (int, int) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	return len(c.replay), c.replayBytes
}

// A live session whose unacked tail outgrows the byte budget must end the
// session (hello replays from the watermark) — never silently drop events
// that the hub has not confirmed.
func TestReplayByteCapSaturatesConnectedSession(t *testing.T) {
	setReplayByteCap(t, 1000)

	c := NewClient(Config{HostID: "h1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.bindSession(ctx, cancel)
	c.noteConnected(TransportWS)

	fed := 0
	for i := 1; i <= 20; i++ {
		c.seqAndReplay([]acp.Event{{
			"type": "done", "text": strings.Repeat("x", 300), "seq": uint64(i),
		}})
		fed++
		if ctx.Err() != nil {
			break
		}
	}
	if ctx.Err() == nil {
		t.Fatal("session was not cancelled after the unacked bytes passed replayByteCap")
	}
	if !c.replaySaturated.Load() {
		t.Fatal("replaySaturated not set")
	}
	n, bytes := ringLenAndBytes(c)
	if n != fed {
		t.Fatalf("ring holds %d events, want all %d unacked events (no silent trim while connected)", n, fed)
	}
	if bytes <= replayByteCap {
		t.Fatalf("ring bytes = %d, expected it to exceed the %d cap that triggered the reset", bytes, replayByteCap)
	}
}

// Disconnected: nothing can be acked, so the ring keeps the newest unacked
// events that fit the byte budget and drops the oldest with a log.
func TestReplayByteCapTrimsDisconnectedRing(t *testing.T) {
	setReplayByteCap(t, 2000)

	c := NewClient(Config{HostID: "h1"})
	for i := 1; i <= 40; i++ {
		c.seqAndReplay([]acp.Event{{
			"type": "chunk", "text": strings.Repeat("x", 300), "seq": uint64(i),
		}})
	}
	n, bytes := ringLenAndBytes(c)
	if bytes > replayByteCap {
		t.Fatalf("ring bytes = %d, want <= %d", bytes, replayByteCap)
	}
	if n == 0 || n >= 40 {
		t.Fatalf("ring holds %d of 40 events, want a trimmed tail", n)
	}
	// The counter must match the slice exactly.
	c.seqMu.Lock()
	sum := replayRingBytes(c.replay)
	last := c.replay[len(c.replay)-1].seq
	c.seqMu.Unlock()
	if sum != bytes {
		t.Fatalf("replayBytes = %d, actual ring bytes = %d", bytes, sum)
	}
	if last != 40 {
		t.Fatalf("newest kept seq = %d, want 40", last)
	}
}

// The byte budget must also bound ACKED events. The unacked tail can be
// empty (everything delivered) while the ring still holds replayCap events;
// without the acked-prefix trim, 1000 × 8MB is 8GB of pure history.
func TestReplayByteCapTrimsAckedPrefix(t *testing.T) {
	prev := replayByteCap
	t.Cleanup(func() { replayByteCap = prev })

	// Feed under a generous cap so nothing trims while the events are still
	// unacked; lower it afterwards and ack everything.
	replayByteCap = 1 << 30
	c := NewClient(Config{HostID: "h1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.bindSession(ctx, cancel)
	c.noteConnected(TransportWS)

	text := strings.Repeat("x", 100_000)
	const n = 50
	for i := 1; i <= n; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": text, "seq": uint64(i)}})
	}
	c.seqMu.Lock()
	c.hubAckSeq = n
	c.seqMu.Unlock()

	replayByteCap = 4 << 20
	// One live event triggers compaction; it is the only unacked event.
	c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "tail", "seq": uint64(n + 1)}})

	if ctx.Err() != nil {
		t.Fatal("trimming already-acked events must not fail the session")
	}
	c.seqMu.Lock()
	ring := append([]replayItem(nil), c.replay...)
	bytes := c.replayBytes
	ack := c.hubAckSeq
	c.seqMu.Unlock()

	if bytes > replayByteCap {
		t.Fatalf("replayBytes = %d, want <= %d (acked events must be bounded too)", bytes, replayByteCap)
	}
	if bytes != replayRingBytes(ring) {
		t.Fatalf("replayBytes = %d, actual ring bytes = %d", bytes, replayRingBytes(ring))
	}
	if ack != n {
		t.Fatalf("hubAckSeq = %d, want %d unchanged", ack, n)
	}
	if len(ring) == 0 || ring[len(ring)-1].seq != n+1 {
		t.Fatalf("unacked tail lost: last kept seq = %v", ring[len(ring)-1].seq)
	}
	if len(ring) >= n+1 {
		t.Fatalf("ring holds %d events, want the acked prefix trimmed", len(ring))
	}
	if len(ring) > replayCap {
		t.Fatalf("ring holds %d events, over replayCap %d", len(ring), replayCap)
	}
	// What is kept must be the newest contiguous suffix, not a sparse set.
	if ring[0].seq <= 1 {
		t.Fatalf("oldest kept seq = %d, want the oldest acked events dropped", ring[0].seq)
	}
	for i, it := range ring {
		if want := ring[0].seq + uint64(i); it.seq != want {
			t.Fatalf("kept ring is not contiguous at %d: seq %d, want %d", i, it.seq, want)
		}
	}
}

// If the budget cannot fit even one acked event beside the unacked tail, the
// ring keeps the unacked part alone — no panic, no dropped unacked event.
func TestReplayByteCapTooSmallForAckedEvents(t *testing.T) {
	prev := replayByteCap
	t.Cleanup(func() { replayByteCap = prev })

	replayByteCap = 1 << 30
	c := NewClient(Config{HostID: "h1"})
	text := strings.Repeat("x", 100_000)
	for i := 1; i <= 5; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": text, "seq": uint64(i)}})
	}
	c.seqMu.Lock()
	c.hubAckSeq = 5
	c.seqMu.Unlock()

	replayByteCap = 50 // smaller than one acked event
	c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "t", "seq": uint64(6)}})

	c.seqMu.Lock()
	ring := append([]replayItem(nil), c.replay...)
	bytes := c.replayBytes
	c.seqMu.Unlock()
	if len(ring) != 1 || ring[0].seq != 6 {
		t.Fatalf("ring = %d events (first seq %v), want only the unacked seq 6", len(ring), ring[0].seq)
	}
	if bytes != replayRingBytes(ring) || bytes > replayByteCap {
		t.Fatalf("replayBytes = %d, want %d and <= %d", bytes, replayRingBytes(ring), replayByteCap)
	}
}

// Paused: critical events bypass the 128-event count cap, so they need the
// byte gate too. Over the budget the oldest are dropped (droppable first,
// then critical) and the trim marker advances so a resume still resets.
func TestPausedReplayByteCapTrimsCriticalEvents(t *testing.T) {
	setReplayByteCap(t, 2000)

	c := NewClient(Config{HostID: "h1"})
	c.setBrowserSubscribers(0)
	for i := 1; i <= 20; i++ {
		// "done" is a critical event: not in the droppable set.
		c.seqAndReplay([]acp.Event{{
			"type": "done", "text": strings.Repeat("x", 300), "seq": uint64(i),
		}})
	}
	c.seqMu.Lock()
	n := len(c.replay)
	bytes := c.replayBytes
	sum := replayRingBytes(c.replay)
	trimmed := c.pausedTrimmedThrough
	var newest uint64
	if n > 0 {
		newest = c.replay[n-1].seq
	}
	c.seqMu.Unlock()

	if n == 0 || n >= 20 {
		t.Fatalf("ring holds %d of 20 critical events, want a trimmed but non-empty ring", n)
	}
	if bytes > replayByteCap || bytes != sum {
		t.Fatalf("replayBytes = %d (actual %d), want <= %d", bytes, sum, replayByteCap)
	}
	if trimmed == 0 {
		t.Fatal("pausedTrimmedThrough not updated; the resume would replay into a hole")
	}
	if newest != 20 {
		t.Fatalf("newest kept seq = %d, want 20", newest)
	}
	// The marker must sit just below the oldest kept event.
	c.seqMu.Lock()
	oldest := c.replay[0].seq
	c.seqMu.Unlock()
	if trimmed != oldest-1 {
		t.Fatalf("pausedTrimmedThrough = %d, want %d (one below the oldest kept seq %d)", trimmed, oldest-1, oldest)
	}
}
