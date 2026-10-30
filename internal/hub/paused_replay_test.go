package hub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// A host with no browser subscribers can never be acked, so its unacked tail
// must not be judged against replayHardCap: that ended the session every
// 8000 events and replayed the whole ring into a hub whose fan-out dropped
// all of it (a reconnect loop). While paused the ring keeps a small tail and
// the session stays up.
func TestPausedForwardingTrimsReplayInsteadOfFailingSession(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 4096)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.bindSession(ctx, cancel)
	c.noteConnected(TransportWS)
	c.setBrowserSubscribers(0)

	const total = pausedReplayCap + 100
	for i := 1; i <= total; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "x", "seq": uint64(i)}})
	}

	if ctx.Err() != nil {
		t.Fatalf("session cancelled while paused after %d events: %v", total, ctx.Err())
	}
	if c.replaySaturated.Load() {
		t.Fatal("replaySaturated set while paused; an empty audience must not end the session")
	}

	c.seqMu.Lock()
	ring := len(c.replay)
	var oldest, newest uint64
	if ring > 0 {
		oldest, newest = c.replay[0].seq, c.replay[ring-1].seq
	}
	c.seqMu.Unlock()
	if ring > pausedReplayCap {
		t.Fatalf("replay ring = %d events, want <= %d", ring, pausedReplayCap)
	}
	if newest != total {
		t.Fatalf("newest buffered seq = %d, want %d", newest, total)
	}
	if want := uint64(total - pausedReplayCap + 1); oldest != want {
		t.Fatalf("oldest buffered seq = %d, want %d (a tail, not the head)", oldest, want)
	}

	// 0→1: the resume replays exactly the tail that was kept.
	c.applySubscribers(1, 1, newStatusBridge(), true)

	var replayed []uint64
	deadline := time.After(2 * time.Second)
collect:
	for {
		select {
		case payload := <-c.sendCh:
			var f struct {
				Type   string      `json:"type"`
				Events []acp.Event `json:"events"`
			}
			if json.Unmarshal(payload, &f) != nil || f.Type != "events" {
				continue
			}
			for _, ev := range f.Events {
				replayed = append(replayed, eventSeq(ev))
			}
			if len(replayed) > 0 && replayed[len(replayed)-1] == total {
				break collect
			}
		case <-deadline:
			t.Fatal("timed out waiting for the resume replay")
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("session cancelled during the resume replay: %v", ctx.Err())
	}
	if len(replayed) != pausedReplayCap {
		t.Fatalf("resume replayed %d events, want the %d-event tail", len(replayed), pausedReplayCap)
	}
	if replayed[0] != oldest {
		t.Fatalf("resume started at seq %d, want %d", replayed[0], oldest)
	}
}

// Paused compaction must never drop a critical event: a client_request (or
// done/error) has no history to be rebuilt from, and a permission prompt the
// user never sees is a turn that hangs. Only chunk-like payloads are capped.
func TestPausedTrimKeepsCriticalEvents(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.setBrowserSubscribers(0)

	for i := 1; i <= 200; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "x", "seq": uint64(i)}})
	}
	c.seqAndReplay([]acp.Event{{"type": "client_request", "sessionId": "s1", "requestId": "r1", "seq": uint64(201)}})
	for i := 202; i <= 251; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "x", "seq": uint64(i)}})
	}

	c.seqMu.Lock()
	ring := append([]replayItem(nil), c.replay...)
	trimmedThrough := c.pausedTrimmedThrough
	c.seqMu.Unlock()

	var haveRequest bool
	droppable := 0
	for _, it := range ring {
		if it.typ == "client_request" {
			haveRequest = true
		}
		if it.droppable() {
			droppable++
		}
	}
	if !haveRequest {
		t.Fatalf("client_request was trimmed from the ring (seqs %d..%d)", ring[0].seq, ring[len(ring)-1].seq)
	}
	if droppable != pausedReplayCap {
		t.Fatalf("droppable events in ring = %d, want %d", droppable, pausedReplayCap)
	}
	// 200 chunks trimmed down to 73..200; the request stays; 50 more chunks
	// push the oldest 50 droppable out.
	if ring[0].seq != 123 {
		t.Fatalf("oldest kept seq = %d, want 123", ring[0].seq)
	}
	if trimmedThrough != 122 {
		t.Fatalf("pausedTrimmedThrough = %d, want 122", trimmedThrough)
	}
}

// A resume whose ring starts above the hub watermark (1050 vs ack 1000)
// cannot be repaired by replaying: 1001..1049 are gone, so the hub would park
// 1050 in its out-of-order set forever. The resume must send seq_reset (in
// the same ordering critical section) before replaying what is still held.
func TestPausedTrimResumeResetsEpoch(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 256)
	c.setBrowserSubscribers(0)

	c.seqMu.Lock()
	for seq := uint64(1050); seq <= 1060; seq++ {
		raw, err := json.Marshal(acp.Event{"type": "chunk", "seq": seq})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		c.replay = append(c.replay, replayItem{seq: seq, typ: "chunk", raw: raw})
	}
	c.nextSeq = 1060
	c.lastSentSeq = 1000
	c.hubAckSeq = 1000
	c.pausedTrimmedThrough = 1049
	c.seqMu.Unlock()

	c.applySubscribers(1, 1, newStatusBridge(), true)

	frames := drainFrames(t, c)
	if len(frames) < 2 {
		t.Fatalf("frames = %+v, want seq_reset then a replay", frames)
	}
	if frames[0].Type != "seq_reset" {
		t.Fatalf("first frame = %q, want seq_reset (a replay before the reset would race the hub watermark)", frames[0].Type)
	}
	if frames[1].Type != "events" {
		t.Fatalf("second frame = %q, want events", frames[1].Type)
	}
	var replayed []uint64
	for _, ev := range frames[1].Events {
		replayed = append(replayed, eventSeq(ev))
	}
	if len(replayed) != 11 || replayed[0] != 1050 || replayed[10] != 1060 {
		t.Fatalf("replayed seqs = %v, want 1050..1060", replayed)
	}

	c.seqMu.Lock()
	ack, last, trimmed := c.hubAckSeq, c.lastSentSeq, c.pausedTrimmedThrough
	c.seqMu.Unlock()
	if ack != 0 {
		t.Fatalf("hubAckSeq = %d, want 0 (the hub's epoch was reset)", ack)
	}
	if last != 1060 {
		t.Fatalf("lastSentSeq = %d, want 1060 (end of the replayed ring)", last)
	}
	if trimmed != 0 {
		t.Fatalf("pausedTrimmedThrough = %d, want 0 after the reset", trimmed)
	}
}

// The same reset is required on a reconnect hello: the hub's watermark is
// still above what the ring can reach.
func TestPausedTrimHelloResetsEpoch(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 256)
	c.setBrowserSubscribers(1)

	c.seqMu.Lock()
	for seq := uint64(1050); seq <= 1055; seq++ {
		raw, err := json.Marshal(acp.Event{"type": "chunk", "seq": seq})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		c.replay = append(c.replay, replayItem{seq: seq, typ: "chunk", raw: raw})
	}
	c.nextSeq = 1055
	c.lastSentSeq = 1000
	c.hubAckSeq = 1000
	c.pausedTrimmedThrough = 1049
	c.seqMu.Unlock()

	if err := c.handleHelloSeq(1000); err != nil {
		t.Fatalf("handleHelloSeq: %v", err)
	}
	frames := drainFrames(t, c)
	if len(frames) < 2 || frames[0].Type != "seq_reset" || frames[1].Type != "events" {
		t.Fatalf("frames = %+v, want seq_reset then events", frames)
	}
	if got := eventSeq(frames[1].Events[0]); got != 1050 {
		t.Fatalf("first replayed seq = %d, want 1050", got)
	}
}

// Without a trim the 0→1 resume stays a plain replay: no seq_reset, so the
// hub keeps its buffer and the FE does not need to rebuild.
func TestPausedResumeWithoutTrimDoesNotReset(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 64)
	c.setBrowserSubscribers(0)

	for i := 1; i <= 20; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "x", "seq": uint64(i)}})
	}
	c.applySubscribers(1, 1, newStatusBridge(), true)

	frames := drainFrames(t, c)
	total := 0
	for _, f := range frames {
		if f.Type == "seq_reset" {
			t.Fatal("0→1 resume without a trim must not reset the hub epoch")
		}
		if f.Type == "events" {
			total += len(f.Events)
		}
	}
	if total != 20 {
		t.Fatalf("replayed %d events, want 20", total)
	}
}

// drainFrames reads every frame already queued on sendCh.
func drainFrames(t *testing.T, c *Client) []struct {
	Type   string      `json:"type"`
	Events []acp.Event `json:"events"`
} {
	t.Helper()
	var out []struct {
		Type   string      `json:"type"`
		Events []acp.Event `json:"events"`
	}
	for {
		select {
		case payload := <-c.sendCh:
			var f struct {
				Type   string      `json:"type"`
				Events []acp.Event `json:"events"`
			}
			if err := json.Unmarshal(payload, &f); err != nil {
				t.Fatalf("frame is not JSON: %s", payload)
			}
			out = append(out, f)
		default:
			return out
		}
	}
}

// fwdKnown=false means the hub has not spoken yet — that is not "paused".
// The ring must keep its normal size, or a reconnect would lose the backlog
// before the count ever arrives.
func TestUnknownAudienceKeepsReplayRing(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	const n = pausedReplayCap * 3
	evs := make([]acp.Event, 0, n)
	for i := 1; i <= n; i++ {
		evs = append(evs, acp.Event{"type": "chunk", "text": "x", "seq": uint64(i)})
	}
	c.seqAndReplay(evs)

	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if len(c.replay) != n {
		t.Fatalf("replay ring = %d events, want all %d before the hub reports a count", len(c.replay), n)
	}
}

// A hello on a reconnect while paused must not push the whole ring (nobody
// would receive it); it records the hub watermark so the 0→1 resume sends
// only the segment after it.
func TestPausedHelloSeqSkipsReplay(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.bindSession(ctx, cancel)
	c.noteConnected(TransportWS)
	c.setBrowserSubscribers(0)

	for i := 1; i <= 20; i++ {
		c.seqAndReplay([]acp.Event{{"type": "chunk", "text": "x", "seq": uint64(i)}})
	}
	if err := c.handleHelloSeq(5); err != nil {
		t.Fatalf("handleHelloSeq: %v", err)
	}
	select {
	case payload := <-c.sendCh:
		t.Fatalf("paused hello replayed a frame: %s", payload)
	default:
	}
	c.seqMu.Lock()
	last, ack := c.lastSentSeq, c.hubAckSeq
	c.seqMu.Unlock()
	if last != 5 || ack != 5 {
		t.Fatalf("lastSentSeq/hubAckSeq = %d/%d, want 5/5", last, ack)
	}
}
