package hub

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-wire"
)

// errOutboxOverflow means the critical entries alone no longer fit the uplink
// log's hard cap. The session closes with wire.CloseOverflow and the hub
// replays after the reconnect; growing without a bound would pin the events
// nobody can deliver.
var errOutboxOverflow = errors.New("hub: uplink log over the hard cap")

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
// the hub's ackUp is authoritative there.
func (o *outbox) prepareEpoch(lastSeq func() uint64) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.anchored {
		return o.snd.Acked()
	}
	base := lastSeq()
	// Nothing already appended can be above base: every entry's seq was
	// assigned by the same bus the high-water mark is read from.
	o.snd = wire.NewSender(base)
	o.anchored = true
	return base
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
	next := o.snd.Next()
	if head.Seq <= next {
		return nil
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
	if o.snd.Bytes() <= o.soft {
		return nil
	}
	for _, e := range o.snd.Entries() {
		if o.snd.Bytes() <= o.soft {
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
		if err := o.snd.Elide(e.Seq, wire.TrimmedEvent(head.SessionID, e.Seq)); err != nil {
			continue
		}
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
}

// frameEnvelopeOverhead is the slack one event needs for the frame envelope
// (type, seq, ack, brackets) when checking it against maxFrame.
const frameEnvelopeOverhead = 64

// inflightSoftFrames / inflightHardFrames scale hello_ack limits.maxFrame into
// the outbox budget. A tunnel that cannot move two frames' worth of log is
// not delivering; a critical-only log eight frames deep cannot fit the peer's
// resume window in any useful time.
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
