package acp

import (
	"testing"
	"time"
)

// T5: Broadcast 分级丢弃——可丢事件（chunk/thought）满则丢，关键事件
// （终态/client_request 等）必须送达即使 channel 已满。
func TestBroadcastGradedDrop(t *testing.T) {
	b := NewBridge(GrokConfig{}) // no agent needed for Broadcast
	ch, unsub := b.Subscribe()
	defer unsub()

	// Fill the subscriber channel without draining.
	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}

	// Droppable event: must not block, must be dropped.
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Broadcast(Event{"type": "chunk", "text": "x"})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast of droppable event blocked on full channel")
	}
	select {
	case ev := <-ch:
		if ev["type"] != "filler" {
			t.Fatalf("droppable event was unexpectedly delivered: %v", ev)
		}
	default:
		t.Fatal("channel unexpectedly drained")
	}

	// Critical event: must be delivered (blocking until we drain).
	go b.Broadcast(Event{"type": "done"})
	// Wait for it to land, then read it back.
	deadline := time.After(6 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev["type"] == "done" {
				if ev["seq"] == nil || ev["seq"].(uint64) == 0 {
					t.Fatalf("critical event missing seq: %v", ev)
				}
				return
			}
			continue // filler frames drained while waiting
		case <-deadline:
			t.Fatal("critical event was dropped instead of blocking")
		}
	}
}

func TestDroppableEventType(t *testing.T) {
	for _, s := range []string{"chunk", "thought", "user_chunk", "gen_rate", "log"} {
		if !droppableEventType(s) {
			t.Errorf("%q should be droppable", s)
		}
	}
	for _, s := range []string{"done", "turn_completed", "client_request", "error", "sessions_changed"} {
		if droppableEventType(s) {
			t.Errorf("%q must be critical", s)
		}
	}
	if droppableEventType(42) || droppableEventType(nil) {
		t.Error("non-string types must be critical")
	}
}
