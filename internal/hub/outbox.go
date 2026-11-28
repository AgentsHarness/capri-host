package hub

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-hub/wire"
)

// errOutboxOverflow means the critical entries alone no longer fit the uplink
// log's hard cap. The session closes with wire.CloseOverflow and the hub
// replays after the reconnect; growing without a bound would pin the events
// nobody can deliver.
var errOutboxOverflow = errors.New("hub: uplink log over the hard cap")

// errOutboxGap means one event's seq jumped past the fill cap. The outbox
// requests a fresh epoch at the bus high-water mark; the session closes with
// overflow so the hub retires the old sequence space instead of replaying a
// permanently stuck log.
var errOutboxGap = errors.New("hub: event seq gap over the fill cap")

// outbox is the host's uplink log: one wire.Sender entry per event, keyed by
// the seq the bridge event bus assigned. It is the only repair path — a
// reconnect retransmits from the hub's ack — so nothing is ever dropped
// silently: a discarded payload becomes wire.TrimmedEvent at the same seq.
//
// The producer (forwardLoop) and the session loop share it under mu. Entries
// are per event, not per frame: an `events` frame is a contiguous run taken
// from this log at send time, which is what lets the hub ack per event and
// lets a single event be elided without opening a hole.
type outbox struct {
	mu       sync.Mutex
	snd      *wire.Sender
	anchored bool
	paused   bool
	maxFrame int
	soft     int
	hard     int

	// gapPending is set when the event bus dropped a run too large to fill
	// with placeholders. The next handshake starts a new epoch from the bus
	// high-water mark, re-anchoring this log instead of leaving it stuck.
	gapPending bool
	// gapLogAt/gapLogCount throttle the per-event gap diagnostics; a permanent
	// gap would otherwise write one log line for every event the bus emits.
	gapLogAt    time.Time
	gapLogCount int
	// compactAt is the byte size due for the next full compaction walk. A log
	// over the soft cap that consists mostly of critical entries must not be
	// walked on every append: that turns the producer path quadratic.
	compactAt int
}

func newOutbox() *outbox {
	o := &outbox{snd: wire.NewSender(0)}
	o.setLimitsLocked(wire.DefaultLimits.MaxFrame)
	return o
}

// prepareEpoch anchors a fresh sequence space at the event bus's current
// high-water mark and returns it as hello.resume.up. The hub seeds its up
// receiver with that baseline, so the first live event is baseline+1 and
// everything below is history the FE rebuilds from the host.
//
// A same-epoch reconnect keeps the log and returns its ack watermark instead;
// the hub's ackUp is authoritative there. reanchored is true when the caller
// must mint a new epoch for the returned baseline: either the first handshake
// or a bus gap too large to fill.
func (o *outbox) prepareEpoch(lastSeq func() uint64) (base uint64, reanchored bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.anchored && !o.gapPending {
		return o.snd.Acked(), false
	}
	base = lastSeq()
	// Nothing already appended can be above base: every entry's seq was
	// assigned by the same bus the high-water mark is read from.
	reanchored = o.gapPending
	if reanchored {
		log.Printf("[hub-client] 按新 epoch 重置上行序号基线为 %d", base)
	}
	o.snd = wire.NewSender(base)
	o.anchored = true
	o.gapPending = false
	o.compactAt = o.soft
	return base, reanchored
}

// appendEvent logs one event under its own seq. A seq at or below the log's
// head is a duplicate (the bus never reuses one); a gap means the event bus
// dropped something upstream, so the missing seqs are logged as placeholders
// and the run stays contiguous.
func (o *outbox) appendEvent(raw []byte) error {
	var head struct {
		Seq uint64 `json:"seq"`
	}
	if err := json.Unmarshal(raw, &head); err != nil || head.Seq == 0 {
		return nil // the bus stamps every event; one without a seq is not ours
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gapPending {
		return errOutboxGap
	}
	next := o.snd.Next()
	if head.Seq <= next {
		return nil
	}
	if head.Seq-next > maxGapFill {
		o.gapPending = true
		o.noteGapLocked(next, head.Seq)
		return errOutboxGap
	}
	if len(raw)+frameEnvelopeOverhead > o.maxFrame {
		// One event that cannot fit a frame becomes a placeholder at its own
		// seq: dropping it would open the hole the FE cannot fill.
		var meta struct {
			SessionID string `json:"sessionId"`
		}
		_ = json.Unmarshal(raw, &meta)
		log.Printf("[hub-client] 单条事件 %d 字节超过单帧上限 %d，改为占位 seq=%d", len(raw), o.maxFrame, head.Seq)
		raw = wire.TrimmedEvent(meta.SessionID, head.Seq)
	}
	for seq := next + 1; seq < head.Seq; seq++ {
		if err := o.snd.AppendAt(seq, wire.TrimmedEvent("", seq)); err != nil {
			return err
		}
	}
	if err := o.snd.AppendAt(head.Seq, raw); err != nil {
		return err
	}
	return o.enforceLocked()
}

// noteGapLocked records one oversize gap. The first gap is logged at once;
// repeats inside the window are summarized, so a permanently stuck log cannot
// emit hundreds of thousands of identical lines.
func (o *outbox) noteGapLocked(next, head uint64) {
	o.gapLogCount++
	now := time.Now()
	if !o.gapLogAt.IsZero() && now.Sub(o.gapLogAt) < gapLogWindow {
		return
	}
	if o.gapLogAt.IsZero() {
		log.Printf("[hub-client] 事件 seq 从 %d 跳到 %d，超过 %d 的空缺填充上限；将以新 epoch 重连", next, head, maxGapFill)
	} else {
		log.Printf("[hub-client] 事件 seq 仍停在 %d（最新 %d），过去 %.0fs 有 %d 条事件超过空缺填充上限", next, head, now.Sub(o.gapLogAt).Seconds(), o.gapLogCount)
	}
	o.gapLogAt = now
	o.gapLogCount = 0
}

// gapResetPending reports whether the log needs a fresh epoch baseline before
// the next handshake.
func (o *outbox) gapResetPending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.gapPending
}

// enforceLocked keeps the log inside its byte budget: droppable transcript
// payloads become placeholders in place (the seq is kept), critical entries
// are retained, and a log that critical entries alone push past the hard cap
// ends the session so the hub replays after a reconnect.
//
// The overflow error is returned even while no session is running; the caller
// then has nothing to close, and the critical tail stays queued. That is
// deliberate: dropping an unacked critical event would open a hole the FE can
// never fill, and critical events (prompts, turn terminals) are rare enough
// that the cap is a link-health signal, not a memory ceiling.
func (o *outbox) enforceLocked() error {
	bytes := o.snd.Bytes()
	if bytes > o.hard {
		return errOutboxOverflow
	}
	if bytes <= o.soft {
		o.compactAt = o.soft
		return nil
	}
	// The log has not grown enough since the last full walk; a repeat scan
	// cannot shrink it. Skip until the size threshold advances.
	if bytes < o.compactAt {
		return nil
	}
	// Walk the log once. ElideFrom resumes after the previous replacement, so
	// compacting n entries costs one scan instead of one head scan per entry.
	for i, n := 0, o.snd.Len(); i < n; i++ {
		e, ok := o.snd.EntryAt(i)
		if !ok {
			break
		}
		var head struct {
			Type      string `json:"type"`
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(e.Payload, &head) != nil {
			continue
		}
		if !acp.IsDroppableEventType(head.Type) {
			continue
		}
		if _, err := o.snd.ElideFrom(i, e.Seq, wire.TrimmedEvent(head.SessionID, e.Seq)); err != nil {
			continue
		}
	}
	if bytes := o.snd.Bytes(); bytes > o.soft {
		o.compactAt = bytes + o.maxFrame
	} else {
		o.compactAt = o.soft
	}
	if o.snd.Bytes() > o.hard {
		return errOutboxOverflow
	}
	return nil
}

// ack applies the hub's watermark.
func (o *outbox) ack(n uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snd.Ack(n)
}

// entriesAfter returns the unacked entries above seq, in order.
func (o *outbox) entriesAfter(seq uint64) []wire.Entry {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []wire.Entry
	for _, e := range o.snd.Entries() {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out
}

func (o *outbox) setPaused(paused bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.paused = paused
}

func (o *outbox) isPaused() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.paused
}

// setLimits adopts hello_ack limits.maxFrame and derives the log's byte
// budget: twice the frame ceiling while delivering, eight times as the hard
// cap that ends the session.
func (o *outbox) setLimits(maxFrame int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.setLimitsLocked(maxFrame)
}

func (o *outbox) setLimitsLocked(maxFrame int) {
	if maxFrame < 1<<10 {
		maxFrame = wire.DefaultLimits.MaxFrame
	}
	o.maxFrame = maxFrame
	o.soft = inflightSoftFrames * maxFrame
	o.hard = inflightHardFrames * maxFrame
	o.compactAt = o.soft
}

// frameEnvelopeOverhead is the slack one event needs for the frame envelope
// (type, seq, ack, brackets) when checking it against maxFrame.
const frameEnvelopeOverhead = 64

// maxGapFill bounds the trimmed placeholders one append may synthesize for a
// bus gap. A larger jump means the event bus lost a whole run: the outbox
// requests a fresh epoch instead of walking an unbounded jump.
const maxGapFill = 256

// gapLogWindow bounds how often the gap diagnostic may reach the log.
const gapLogWindow = 30 * time.Second

// inflightSoftFrames / inflightHardFrames scale hello_ack limits.maxFrame into
// the outbox budget. A tunnel that cannot move two frames' worth of log is
// not delivering; a critical-only log eight frames deep cannot drain in any
// useful time.
const (
	inflightSoftFrames = 2
	inflightHardFrames = 8
)

func (o *outbox) next() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.snd.Next()
}

func (o *outbox) acked() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.snd.Acked()
}

func (o *outbox) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.snd.Len()
}

func (o *outbox) bytes() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.snd.Bytes()
}

func (o *outbox) frameLimit() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.maxFrame
}
