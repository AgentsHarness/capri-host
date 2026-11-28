package hub

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/AgentsHarness/capri-hub/wire"
)

// TestOutboxEnforceLockedElidesMixedLog: the one-pass walk must skip critical
// entries without losing its place and still elide every droppable entry.
func TestOutboxEnforceLockedElidesMixedLog(t *testing.T) {
	o := newOutbox()
	o.setLimits(1 << 10) // soft 2 KiB, hard 8 KiB
	chunk := []byte(`{"type":"chunk","sessionId":"s1","text":"` + strings.Repeat("x", 300) + `"}`)
	critical := []byte(`{"type":"client_request","sessionId":"s1","text":"` + strings.Repeat("y", 300) + `"}`)
	for seq := uint64(1); seq <= 18; seq++ {
		payload := chunk
		if seq%2 == 1 {
			payload = critical
		}
		if err := o.snd.AppendAt(seq, payload); err != nil {
			t.Fatalf("append seq %d: %v", seq, err)
		}
	}
	if err := o.enforceLocked(); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	for _, e := range o.snd.Entries() {
		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(e.Payload, &head); err != nil {
			t.Fatalf("decode seq %d: %v", e.Seq, err)
		}
		if e.Seq%2 == 1 && head.Type != "client_request" {
			t.Fatalf("critical seq %d was elided to %q", e.Seq, head.Type)
		}
		if e.Seq%2 == 0 && head.Type != "trimmed" {
			t.Fatalf("droppable seq %d survived as %q", e.Seq, head.Type)
		}
	}
	want := 9 * len(critical)
	for seq := uint64(2); seq <= 18; seq += 2 {
		want += len(wire.TrimmedEvent("s1", seq))
	}
	if got := o.snd.Bytes(); got != want {
		t.Fatalf("bytes = %d, want %d", got, want)
	}
}

// TestOutboxGapFillCap: a bus jump beyond the fill cap is refused instead of
// synthesizing an unbounded run of placeholders; a gap at the cap still fills.
// The refusal must also request an epoch re-anchor so the log cannot stay
// stuck forever.
func TestOutboxGapFillCap(t *testing.T) {
	o := newOutbox()
	if err := o.appendEvent([]byte(`{"type":"chunk","sessionId":"s1","seq":257,"text":"x"}`)); !errors.Is(err, errOutboxGap) {
		t.Fatalf("append past a %d-seq gap: %v, want errOutboxGap", maxGapFill, err)
	}
	if n := o.len(); n != 0 {
		t.Fatalf("refused append logged %d entries", n)
	}
	if !o.gapResetPending() {
		t.Fatal("oversize gap did not request an epoch re-anchor")
	}

	fill := newOutbox()
	if err := fill.appendEvent([]byte(`{"type":"chunk","sessionId":"s1","seq":256,"text":"x"}`)); err != nil {
		t.Fatalf("append at the cap: %v", err)
	}
	if n := fill.len(); n != maxGapFill {
		t.Fatalf("log = %d entries, want %d (placeholders plus the event)", n, maxGapFill)
	}
}

// TestOutboxGapReanchors: after an oversize gap, prepareEpoch must start a
// fresh sender at the bus high-water mark and accept the next live event.
func TestOutboxGapReanchors(t *testing.T) {
	o := newOutbox()
	if got, reanchored := o.prepareEpoch(func() uint64 { return 10 }); got != 10 || reanchored {
		t.Fatalf("initial baseline = %d, reanchored=%v; want 10, false", got, reanchored)
	}
	if err := o.appendEvent([]byte(`{"type":"chunk","sessionId":"s1","seq":300,"text":"x"}`)); !errors.Is(err, errOutboxGap) {
		t.Fatalf("oversize gap: %v, want errOutboxGap", err)
	}
	if got, reanchored := o.prepareEpoch(func() uint64 { return 500 }); got != 500 || !reanchored {
		t.Fatalf("re-anchor baseline = %d, reanchored=%v; want 500, true", got, reanchored)
	}
	if o.gapResetPending() {
		t.Fatal("re-anchor left the gap pending")
	}
	if err := o.appendEvent([]byte(`{"type":"chunk","sessionId":"s1","seq":501,"text":"ok"}`)); err != nil {
		t.Fatalf("append after re-anchor: %v", err)
	}
	if got := o.next(); got != 501 {
		t.Fatalf("next seq = %d, want 501", got)
	}
}

// BenchmarkOutboxEnforceLockedDroppable measures the producer-path compaction
// that runs when a burst of small events pushes the uplink log over its soft
// cap. Every entry is droppable, so the old per-event Sender.Elide scanned the
// whole log once per replacement (O(n²)); the indexed walk should stay linear.
func BenchmarkOutboxEnforceLockedDroppable(b *testing.B) {
	const entries = 12000
	payload := []byte(`{"type":"chunk","sessionId":"s1","text":"` + strings.Repeat("x", 220) + `"}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		o := newOutbox()
		o.setLimits(1 << 20) // soft 2 MiB, hard 8 MiB
		for seq := uint64(1); seq <= entries; seq++ {
			if err := o.snd.AppendAt(seq, payload); err != nil {
				b.Fatalf("append seq %d: %v", seq, err)
			}
		}
		if o.snd.Bytes() <= o.soft {
			b.Fatalf("benchmark log %d bytes is not over the soft cap %d", o.snd.Bytes(), o.soft)
		}
		b.StartTimer()

		if err := o.enforceLocked(); err != nil {
			b.Fatalf("enforce: %v", err)
		}
		if o.snd.Bytes() > o.soft {
			b.Fatalf("log stayed at %d bytes, over the soft cap %d", o.snd.Bytes(), o.soft)
		}
	}
}
