package hub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// readLoop must never park behind a full ctrlCh: while the writer is stuck on
// one large frame, pong/state_ack/host_status pile up, and blocking there
// stops the socket from being drained at all. The bounded wait drops instead
// — safe, because every one of those frames is re-sent or re-requested — and
// the drop is counted.
func TestControlEnqueueBoundedWaitKeepsWriteLoopRunning(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 8)
	c.ctrlCh = make(chan []byte, 32)
	for i := 0; i < cap(c.ctrlCh); i++ {
		c.ctrlCh <- []byte(fmt.Sprintf(`{"v":1,"type":"pong","i":%d}`, i))
	}

	start := time.Now()
	if c.enqueueControl([]byte(`{"v":1,"type":"pong"}`)) {
		t.Fatal("enqueueControl reported success with a full ctrlCh")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("enqueueControl blocked for %v, want about %v", d, ctrlEnqueueWait)
	}
	if got := c.ctrlDropped.Load(); got != 1 {
		t.Fatalf("ctrlDropped = %d, want 1", got)
	}

	// The writer still consumes what was already queued — the drop did not
	// wedge the queue.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drained := make(chan struct{})
	go func() {
		n := 0
		_ = c.writeLoop(ctx, func([]byte) error {
			n++
			if n == cap(c.ctrlCh) {
				close(drained)
			}
			return nil
		})
	}()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("writeLoop did not drain the control queue")
	}
}
