package hub

import (
	"context"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// newBudgetClient builds a client with the byte budget already snapshotted
// from a test-sized cap (NewClient reads the package var).
func newBudgetClient(t *testing.T, cap int) *Client {
	t.Helper()
	prevCap, prevWait := sendByteCap, sendCriticalWait
	sendByteCap = cap
	sendCriticalWait = 30 * time.Millisecond
	t.Cleanup(func() {
		sendByteCap = prevCap
		sendCriticalWait = prevWait
	})
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 8)
	return c
}

// A live frame that does not fit the byte budget fails immediately (never
// blocks forwardLoop), flags catch-up, and leaves no reservation behind.
func TestSendBudgetLiveDrop(t *testing.T) {
	c := newBudgetClient(t, 100)

	if c.enqueuePayload(make([]byte, 200), []acp.Event{{"type": "chunk", "seq": uint64(1)}}, false) {
		t.Fatal("live frame over the byte budget was enqueued")
	}
	if !c.needCatchUp.Load() {
		t.Fatal("needCatchUp not set for a budget drop")
	}
	if used := c.sendBudgetUsed(); used != 0 {
		t.Fatalf("send budget leaked %d bytes after a failed live enqueue", used)
	}
	if len(c.sendCh) != 0 {
		t.Fatalf("sendCh has %d frames after a failed enqueue", len(c.sendCh))
	}

	// A frame that fits reserves exactly its size.
	payload := make([]byte, 50)
	if !c.enqueuePayload(payload, []acp.Event{{"type": "chunk", "seq": uint64(2)}}, false) {
		t.Fatal("frame within budget rejected")
	}
	if used := c.sendBudgetUsed(); used != len(payload) {
		t.Fatalf("budget used = %d, want %d", used, len(payload))
	}
}

// The writer releases each payload after the write attempt, so a drained
// queue leaves the budget at zero.
func TestSendBudgetReleasedByWriteLoop(t *testing.T) {
	c := newBudgetClient(t, 1024)
	payload := []byte("hello")
	if !c.enqueueFrame(payload, false) {
		t.Fatal("small frame rejected")
	}
	if used := c.sendBudgetUsed(); used != len(payload) {
		t.Fatalf("budget used = %d, want %d", used, len(payload))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	written := make(chan []byte, 1)
	go func() {
		_ = c.writeLoop(ctx, func(p []byte) error {
			written <- p
			return nil
		})
	}()
	select {
	case p := <-written:
		if string(p) != "hello" {
			t.Fatalf("writer got %q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not write the frame")
	}
	deadline := time.After(2 * time.Second)
	for c.sendBudgetUsed() != 0 {
		select {
		case <-deadline:
			t.Fatalf("budget not released after the write, used = %d", c.sendBudgetUsed())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// A critical frame waits for budget, and when none frees up it fails through
// the existing critical-timeout path (catch-up set, no reservation leaked).
func TestSendBudgetCriticalTimesOut(t *testing.T) {
	c := newBudgetClient(t, 100)
	if !c.enqueueFrame(make([]byte, 100), false) {
		t.Fatal("filling frame rejected")
	}
	if used := c.sendBudgetUsed(); used != 100 {
		t.Fatalf("budget used = %d, want 100", used)
	}

	start := time.Now()
	if c.enqueuePayload(make([]byte, 10), []acp.Event{{"type": "chunk", "seq": uint64(1)}}, true) {
		t.Fatal("critical frame enqueued without byte budget")
	}
	if d := time.Since(start); d < sendCriticalWait {
		t.Fatalf("critical frame failed after %v, want at least the %v budget wait", d, sendCriticalWait)
	}
	if !c.needCatchUp.Load() {
		t.Fatal("needCatchUp not set after the critical budget timeout")
	}
	if used := c.sendBudgetUsed(); used != 100 {
		t.Fatalf("budget used = %d, want the original 100 (no leak, no double release)", used)
	}

	// Freeing the slot lets a later attempt through.
	c.drainSendCh()
	if used := c.sendBudgetUsed(); used != 0 {
		t.Fatalf("budget used = %d after drain, want 0", used)
	}
}

// Draining a stale queue releases every reserved payload.
func TestSendBudgetDrainReleases(t *testing.T) {
	c := newBudgetClient(t, 1024)
	if !c.enqueueFrame(make([]byte, 10), false) {
		t.Fatal("first frame rejected")
	}
	if !c.enqueueFrame(make([]byte, 20), false) {
		t.Fatal("second frame rejected")
	}
	if used := c.sendBudgetUsed(); used != 30 {
		t.Fatalf("budget used = %d, want 30", used)
	}
	c.drainSendCh()
	if used := c.sendBudgetUsed(); used != 0 {
		t.Fatalf("budget used = %d after drain, want 0", used)
	}
	if n := len(c.sendCh); n != 0 {
		t.Fatalf("sendCh still holds %d frames", n)
	}
}

// sendQueuePressure must react to bytes, not just frame count: a few large
// frames can fill the budget while the channel still looks empty.
func TestSendQueuePressureSeesBytes(t *testing.T) {
	c := newBudgetClient(t, 100)
	if c.sendQueuePressure() {
		t.Fatal("empty queue reported pressure")
	}
	if !c.enqueueFrame(make([]byte, 60), false) {
		t.Fatal("frame rejected")
	}
	if !c.sendQueuePressure() {
		t.Fatal("60% of the byte budget did not report pressure")
	}
}
