package hub

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// TestReplayLiveInterleaveNoLoss is a regression test for the reconnect
// ordering hazard:
//
// After a disconnect with a large buffered backlog, the hello-driven
// replay (sendReplayAfter → enqueueReplayLocked) enqueues frames in chunks
// (replayFrameBudget), while live forwarding (forwardLoop) re-arms at
// the same moment. If a live frame — whose seqs are HIGHER than the
// backlog's — lands between replay frames, the hub's stale-seq gate
// (RegisterEvent: s <= last seen ⇒ skip) drops every replay event that
// arrives afterwards. Those events are absent from the hub's gap-pull
// buffer too (they were never accepted), so the transcript is
// permanently lost with no recovery path.
//
// The test consumes the frames exactly as the hub would and applies a
// mirror of the hub's seq gate, asserting that no event is dropped and
// that frame seq ranges arrive in strictly increasing order.
func TestReplayLiveInterleaveNoLoss(t *testing.T) {
	const attempts = 25
	for attempt := 0; attempt < attempts; attempt++ {
		c := NewClient(Config{URL: "http://hub.invalid", HostID: "h1", Token: "t", DisableQUIC: true})
		c.sendCh = make(chan []byte, 4096)
		c.setBrowserSubscribers(1) // browser online → live forwarding re-arms

		// Simulated disconnect backlog: 2400 events × ~4KB ≈ 10MB,
		// chunked into ~10 replay frames at the 1MB frame budget.
		big := strings.Repeat("x", 4000)
		backlog := make([]acp.Event, 0, 2400)
		for i := 1; i <= 2400; i++ {
			backlog = append(backlog, acp.Event{"type": "chunk", "text": big, "seq": uint64(i)})
		}
		c.seqAndReplay(backlog)

		// Hub side: consume frames in arrival order, applying the hub's
		// RegisterEvent seq gate (s <= last ⇒ skip fan-out/buffer).
		var (
			mu       sync.Mutex
			ranges   []string
			hubLast  uint64
			accepted int
			dropped  int
		)
		consumed := make(chan struct{})
		go func() {
			defer close(consumed)
			for payload := range c.sendCh {
				var fr struct {
					Events []acp.Event `json:"events"`
				}
				if json.Unmarshal(payload, &fr) != nil || len(fr.Events) == 0 {
					continue
				}
				f0, f1 := eventSeq(fr.Events[0]), eventSeq(fr.Events[len(fr.Events)-1])
				mu.Lock()
				ranges = append(ranges, fmt.Sprintf("[%d..%d]", f0, f1))
				for _, ev := range fr.Events {
					if s := eventSeq(ev); s > 0 {
						if s > hubLast {
							hubLast = s
							accepted++
						} else {
							dropped++ // mirror of hub RegisterEvent's stale-seq skip
						}
					}
				}
				mu.Unlock()
			}
		}()

		// Hello-driven replay (readLoop path) racing live forwarding
		// (forwardLoop path): live batches land while replay is still
		// chunking frames.
		stopLive := make(chan struct{})
		var liveWG sync.WaitGroup
		liveWG.Add(1)
		go func() {
			defer liveWG.Done()
			seq := uint64(2401)
			for {
				select {
				case <-stopLive:
					return
				case <-time.After(2 * time.Millisecond):
				}
				live := make([]acp.Event, 0, 16)
				for i := 0; i < 16; i++ {
					live = append(live, acp.Event{"type": "chunk", "text": "live", "seq": seq})
					seq++
				}
				c.enqueueEvents(live)
			}
		}()
		c.sendReplayAfter(0)
		time.Sleep(80 * time.Millisecond)
		close(stopLive)
		liveWG.Wait()
		close(c.sendCh)
		<-consumed

		mu.Lock()
		gotDropped := dropped
		gotAccepted := accepted
		gotRanges := append([]string(nil), ranges...)
		mu.Unlock()

		if gotDropped > 0 {
			t.Fatalf("attempt %d: hub seq gate dropped %d events (accepted %d); frame arrival order: %v",
				attempt, gotDropped, gotAccepted, gotRanges)
		}
	}
}
