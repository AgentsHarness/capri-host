package hub

import (
	"testing"
	"time"
)

func TestHostRelayStatsSnapshot(t *testing.T) {
	var s relayStats
	s.begin()
	s.end(5*time.Millisecond, true)
	s.begin()
	s.end(100*time.Millisecond, true)
	s.begin()
	s.end(5*time.Second, false)

	got := s.snapshot()
	if got.Requests != 3 || got.Completed != 2 || got.Errors != 1 || got.Inflight != 0 {
		t.Fatalf("counts = %+v, want 3/2/1/0", got)
	}
	if got.AvgMs != 1701 || got.MaxMs != 5000 {
		t.Fatalf("latency = avg %d max %d, want 1701/5000", got.AvgMs, got.MaxMs)
	}
	if got.P50Ms != 100 || got.P95Ms != 5000 || got.P99Ms != 5000 {
		t.Fatalf("percentiles = %+v, want 100/5000/5000", got)
	}
}
