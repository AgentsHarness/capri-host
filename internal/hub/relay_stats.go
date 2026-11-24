package hub

import (
	"sync/atomic"
	"time"

	"github.com/AgentsHarness/capri-host/internal/hubstate"
)

// relayLatencyBuckets mirror the hub's coarse debug buckets. They describe
// local handler latency (request frame applied to answer ready), not network
// time between host and hub.
const relayLatencyBucketCount = 15

var relayLatencyBuckets = [relayLatencyBucketCount]uint64{
	10, 25, 50, 100, 250,
	500, 1000, 2000, 5000, 10000,
	30000, 60000, 300000, 900000, 2700000,
}

type relayStats struct {
	inflight  atomic.Int64
	requests  atomic.Uint64
	completed atomic.Uint64
	errors    atomic.Uint64
	totalMs   atomic.Uint64
	maxMs     atomic.Uint64
	buckets   [relayLatencyBucketCount]atomic.Uint64
	overflow  atomic.Uint64
}

func (s *relayStats) begin() {
	s.inflight.Add(1)
}

// end records one finished local relay execution. ok is false for transport
// failures and 5xx handler outcomes; a normal 4xx HTTP answer is a completed
// request.
func (s *relayStats) end(d time.Duration, ok bool) {
	s.inflight.Add(-1)
	s.requests.Add(1)
	if ok {
		s.completed.Add(1)
	} else {
		s.errors.Add(1)
	}
	ms := uint64(d.Milliseconds())
	if d < 0 {
		ms = 0
	}
	s.totalMs.Add(ms)
	for {
		cur := s.maxMs.Load()
		if ms <= cur || s.maxMs.CompareAndSwap(cur, ms) {
			break
		}
	}
	for i, upper := range relayLatencyBuckets {
		if ms <= upper {
			s.buckets[i].Add(1)
			return
		}
	}
	s.overflow.Add(1)
}

func (s *relayStats) snapshot() hubstate.RelayStat {
	requests := s.requests.Load()
	out := hubstate.RelayStat{
		Requests:  requests,
		Completed: s.completed.Load(),
		Errors:    s.errors.Load(),
		Inflight:  s.inflight.Load(),
		MaxMs:     s.maxMs.Load(),
	}
	if requests > 0 {
		out.AvgMs = s.totalMs.Load() / requests
	}
	percentile := func(p uint64) uint64 {
		if requests == 0 {
			return 0
		}
		target := (requests*p + 99) / 100
		var cumulative uint64
		for i, upper := range relayLatencyBuckets {
			cumulative += s.buckets[i].Load()
			if target <= cumulative {
				return upper
			}
		}
		return out.MaxMs
	}
	out.P50Ms = percentile(50)
	out.P95Ms = percentile(95)
	out.P99Ms = percentile(99)
	return out
}
