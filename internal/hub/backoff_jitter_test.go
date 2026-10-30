package hub

import (
	"testing"
	"time"
)

// Jitter must stay inside [0.8d, 1.2d] for every input the reconnect loop
// can produce, and must leave a zero delay alone.
func TestBackoffJitterRange(t *testing.T) {
	for _, d := range []time.Duration{time.Second, 3 * time.Second, reconnectBackoffMax} {
		lo, hi := d*8/10, d*12/10
		var seen = map[time.Duration]bool{}
		for i := 0; i < 200; i++ {
			got := backoffJitter(d)
			if got < lo || got > hi {
				t.Fatalf("backoffJitter(%v) = %v, want within [%v, %v]", d, got, lo, hi)
			}
			seen[got] = true
		}
		if len(seen) < 2 {
			t.Fatalf("backoffJitter(%v) returned a single value %v; jitter is not applied", d, seen)
		}
	}
	if got := backoffJitter(0); got != 0 {
		t.Fatalf("backoffJitter(0) = %v, want 0", got)
	}
}

// nextBackoff stays a pure doubling function: jitter is applied to the wait,
// not to the sequence, so escalation (and its tests) do not change.
func TestNextBackoffUnchanged(t *testing.T) {
	cases := []struct{ in, want time.Duration }{
		{time.Second, 2 * time.Second},
		{2 * time.Second, 4 * time.Second},
		{5 * time.Second, reconnectBackoffMax},
		{reconnectBackoffMax, reconnectBackoffMax},
	}
	for _, tc := range cases {
		if got := nextBackoff(tc.in); got != tc.want {
			t.Errorf("nextBackoff(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
