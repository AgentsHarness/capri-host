package hub

import (
	"strconv"
	"testing"

	"github.com/AgentsHarness/capri-hub/wire"
)

// TestRelayOutboxConfirmedChunksReturnByteBudget: the byte counter must drop
// with every chunk the hub confirms, including the non-final ones. Otherwise
// each multi-chunk answer permanently leaks its acknowledged prefix, and after
// enough traffic the log refuses every answer with a 502.
func TestRelayOutboxConfirmedChunksReturnByteBudget(t *testing.T) {
	old := relayOutboxMaxBytes
	relayOutboxMaxBytes = 64
	t.Cleanup(func() { relayOutboxMaxBytes = old })

	o := newRelayOutbox()
	chunks := [][]byte{
		[]byte("aaaaaaaaaaaaaaaa"),
		[]byte("bbbbbbbbbbbbbbbb"),
		[]byte("cccccccccccccccc"),
	}
	for i := 0; i < 5; i++ {
		id := "r" + strconv.Itoa(i)
		if err := o.add(id, 7, chunks, 3); err != nil {
			t.Fatalf("answer %d refused after %d bytes of confirmed answers: %v", i, i*len(chunks[0]), err)
		}
		// The hub confirms chunks one by one, then marks the request done —
		// the same sequence its hb and hello_ack tables produce.
		o.reconcile([]wire.RelayAck{{ReqID: id, Chunk: 1}}, false)
		o.reconcile([]wire.RelayAck{{ReqID: id, Chunk: 2}}, false)
		o.reconcile([]wire.RelayAck{{ReqID: id, Chunk: 3, Done: true}}, false)
	}
	if o.bytes != 0 {
		t.Fatalf("byte counter = %d after every answer was confirmed, want 0", o.bytes)
	}
}
