package acp

import (
	"fmt"
	"testing"
	"time"
)

// A droppable event that a slow subscriber cannot receive must still consume
// its seq: a hole in the per-host numbering is what the hub's cumulative
// watermark and the FE's ordered delivery both read as "lost event". The body
// is replaced, the number is not.
func TestSlowSubscriberGetsPlaceholderNotHole(t *testing.T) {
	b := NewBridge(GrokConfig{})
	ch, unsub := b.Subscribe()
	defer unsub()

	// Saturate the consumer channel so the pump has to choose between
	// dropping a droppable event and substituting a placeholder.
	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}
	const dropped = 8
	for i := 0; i < dropped; i++ {
		b.Broadcast(Event{"type": "chunk", "text": "body nobody will see", "sessionId": "s1"})
	}

	// The invariant is the SEQUENCE, not which bodies survived: every seq the
	// bus assigned must surface exactly once, in order, whatever the pump
	// chose to do with the payload.
	var got []uint64
	deadline := time.After(5 * time.Second)
	for len(got) < dropped {
		select {
		case ev := <-ch:
			if ev["type"] == "filler" {
				continue
			}
			s, ok := ev["seq"].(uint64)
			if !ok || s == 0 {
				t.Fatalf("delivered event without a seq: %v", ev)
			}
			if ev["type"] == "trimmed" && ev["text"] != nil {
				t.Fatalf("trimmed placeholder kept the body: %v", ev)
			}
			got = append(got, s)
		case <-deadline:
			t.Fatalf("only %d of %d droppable events surfaced: a dropped event left a hole", len(got), dropped)
		}
	}
	for i, s := range got {
		if s != uint64(i+1) {
			t.Fatalf("delivered seqs = %v, want 1..%d with no hole", got, dropped)
		}
	}
}

// trimPlaceholder itself: number and session kept, body gone.
func TestTrimPlaceholderKeepsSeqAndSession(t *testing.T) {
	in := Event{"type": "chunk", "text": "body", "seq": uint64(7), "sessionId": "s1", "toolCallId": "t"}
	out := trimPlaceholder(in)
	if out["type"] != "trimmed" {
		t.Fatalf("type = %v, want trimmed", out["type"])
	}
	if out["seq"] != uint64(7) || out["sessionId"] != "s1" {
		t.Fatalf("placeholder lost identity: %v", out)
	}
	if out["text"] != nil || out["toolCallId"] != nil {
		t.Fatalf("placeholder kept the payload: %v", out)
	}
}

// The placeholder is a COPY: other subscribers and the hub-client batch hold
// the same Event map, and rewriting it in place would delete their payload.
func TestPlaceholderDoesNotMutateSharedEvent(t *testing.T) {
	b := NewBridge(GrokConfig{})
	slow, slowUnsub := b.Subscribe()
	defer slowUnsub()
	_, fastUnsub := b.Subscribe()
	defer fastUnsub()

	for i := 0; i < cap(slow); i++ {
		slow <- Event{"type": "filler"}
	}
	ev := Event{"type": "chunk", "text": "shared body", "sessionId": "s1"}
	b.Broadcast(ev)

	// The publisher's own map is untouched, so the hub batch still carries
	// the body.
	if ev["text"] != "shared body" || ev["type"] != "chunk" {
		t.Fatalf("shared event was rewritten in place: %v", ev)
	}
}

// Critical events are never replaced by placeholders, however full the queue.
func TestCriticalNeverBecomesPlaceholder(t *testing.T) {
	b := NewBridge(GrokConfig{})
	ch, unsub := b.Subscribe()
	defer unsub()

	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}
	b.Broadcast(Event{"type": "done", "stopReason": "end_turn"})

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev["type"] == "trimmed" {
				t.Fatalf("critical event was replaced by a placeholder: %v", ev)
			}
			if ev["type"] == "done" {
				return
			}
		case <-deadline:
			t.Fatal("critical event never arrived")
		}
	}
}

// The ring-full path (droppable backlog at subQueueCap) must also keep the
// number: the publish path assigned it before the drop decision.
func TestDroppableBacklogCapKeepsSeq(t *testing.T) {
	b := NewBridge(GrokConfig{})
	ch, unsub := b.Subscribe()
	defer unsub()

	// No reader at all: fill the consumer channel, then the droppable ring.
	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}
	const n = subQueueCap + 16
	for i := 0; i < n; i++ {
		b.Broadcast(Event{"type": "chunk", "text": fmt.Sprintf("c%d", i)})
	}

	var lastSeq uint64
	var sawTrimmed bool
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev["type"] == "trimmed" {
				sawTrimmed = true
			}
			if s, ok := ev["seq"].(uint64); ok && s > lastSeq {
				lastSeq = s
			}
			if lastSeq >= n {
				if !sawTrimmed {
					t.Fatal("expected at least one placeholder under backlog cap")
				}
				return
			}
		case <-deadline:
			t.Fatalf("seq stopped at %d of %d; a dropped droppable event left a hole", lastSeq, n)
		}
	}
}
