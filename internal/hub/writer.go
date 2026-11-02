package hub

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/AgentsHarness/capri-hub/wire"
)

// writer is the session's single uplink goroutine. It owns every byte the
// session writes after the handshake — heartbeats, relay frames and the
// reliable events log — so the session loop never blocks on a socket write: a
// 32MiB frame cannot delay an ack or a heartbeat.
//
// Work is picked by priority: a pending control frame (hb) first, then a
// retransmit request, then relay and events alternating so neither starves. A
// control frame is also written between two fragments of an open logical
// frame, which is what keeps liveness responsive while a big frame drains.
type writer struct {
	s  *session
	tr Transport

	deflate atomic.Bool
	// ready gates every data plane until hello_ack was adopted: only the
	// handshake itself may reach the hub before that.
	ready atomic.Bool
	// busy is true while a logical frame is being written. The session skips
	// its RTO while it is set: a retransmit would queue behind the very write
	// the timer is meant to repair.
	busy atomic.Bool
	// sent is how far the uplink log has been handed to this transport. It
	// moves only after a complete logical frame reached the transport.
	sent    atomic.Uint64
	frameID atomic.Uint32
	failed  atomic.Bool

	wake chan struct{}
	// progress is signalled after a complete events frame was written; the
	// session resets its RTO on it.
	progress chan struct{}
	// done closes when run returned, so the session can wait for the writer
	// before the next session starts touching the shared answer log.
	done chan struct{}
	// retransmit asks for one replay of the oldest unacked batch.
	retransmit atomic.Bool

	ctrlMu sync.Mutex
	ctrl   []byte

	// relayTurn alternates relay answers with events so neither starves. It is
	// only used on the synchronous (WebSocket) relay path.
	relayTurn bool
	// relay is the asynchronous relay lane, set when the transport has its own
	// relay plane (QUIC request streams). Relay chunk writes then run off this
	// goroutine, so a 256KiB chunk can never delay hb, probes or events.
	relay *relayLane
}

func newWriter(s *session, tr Transport) *writer {
	w := &writer{
		s:        s,
		tr:       tr,
		wake:     make(chan struct{}, 1),
		progress: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	if rs, ok := tr.(relaySender); ok {
		w.relay = newRelayLane(w, rs)
	}
	return w
}

// run drains queued work and parks until the session signals more. It stops at
// the first step after the session ended, so a dying writer cannot keep
// advancing the shared answer log while the replacement session resumes it.
func (w *writer) run() {
	defer close(w.done)
	for {
		select {
		case <-w.s.ctx.Done():
			return
		case <-w.s.stopCh:
			return
		default:
		}
		if w.failed.Load() {
			return
		}
		if w.step() {
			continue
		}
		select {
		case <-w.s.ctx.Done():
			return
		case <-w.s.stopCh:
			return
		case <-w.wake:
		}
	}
}

// step does one unit of work and reports whether it did any, so a backlog
// drains without waiting for a wakeup.
func (w *writer) step() bool {
	if !w.ready.Load() {
		return false
	}
	if w.flushControl() {
		return true
	}
	if w.retransmit.Swap(false) {
		did := false
		// One events batch and one relay chunk: a repair must never turn
		// into a burst of the whole unacked tail.
		if w.sendRetransmit() {
			did = true
		}
		if w.relay != nil {
			// The lane owns relay writes on this transport; it retries the
			// oldest unacked chunk on its own goroutine.
			if !w.failed.Load() {
				w.relay.requestRepair()
			}
		} else if !w.failed.Load() && w.sendRelayRepair() {
			did = true
		}
		if did {
			return true
		}
	}
	if w.relay != nil {
		// Relay writes run on the lane; this loop only streams events (and
		// control), so a chunk write never holds up either plane.
		if w.eventsPending() {
			w.relayTurn = true
			return w.sendEvents()
		}
		return false
	}
	relay, hasRelay := w.s.c.relayOutbox.next()
	events := w.eventsPending()
	if hasRelay && (w.relayTurn || !events) {
		return w.sendRelay(relay)
	}
	if events {
		w.relayTurn = true
		return w.sendEvents()
	}
	if hasRelay {
		return w.sendRelay(relay)
	}
	return false
}

func (w *writer) eventsPending() bool {
	if w.s.up.isPaused() {
		return false
	}
	return w.sent.Load() < w.s.up.next()
}

// sendEvents writes the next contiguous run above the sent watermark as one
// events frame.
func (w *writer) sendEvents() bool {
	entries := w.s.up.entriesAfter(w.sent.Load())
	if len(entries) == 0 {
		return false
	}
	payload, _, last, err := w.buildEvents(entries)
	if err != nil {
		w.fail(wire.CloseUnreadable, err)
		return true
	}
	if err := w.writeFrame(payload); err != nil {
		log.Printf("[hub-client] 上行写入失败: %v", err)
		w.fail(reasonOfError(err), err)
		return true
	}
	w.sent.Store(last)
	select {
	case w.progress <- struct{}{}:
	default:
	}
	return true
}

// sendRetransmit replays exactly one batch from the ack watermark. The next
// RTO starts at the same place until the hub acks that batch, so a lost batch
// is retried on its own instead of burying the link under the whole log. The
// send cursor only moves forward: an entry written here need not be sent again
// by the normal path.
func (w *writer) sendRetransmit() bool {
	if w.s.up.isPaused() {
		return false
	}
	entries := w.s.up.entriesAfter(w.s.up.acked())
	if len(entries) == 0 {
		return false
	}
	payload, _, last, err := w.buildEvents(entries)
	if err != nil {
		w.fail(wire.CloseUnreadable, err)
		return true
	}
	if err := w.writeFrame(payload); err != nil {
		log.Printf("[hub-client] 上行重传失败: %v", err)
		w.fail(reasonOfError(err), err)
		return true
	}
	if cur := w.sent.Load(); last > cur {
		w.sent.Store(last)
	}
	select {
	case w.progress <- struct{}{}:
	default:
	}
	return true
}

// sendRelay writes one queued answer chunk. A transport with a relay plane
// (QUIC request streams) keeps that framing untouched; WebSocket multiplexes
// the chunk on the one socket as a normal frame.
func (w *writer) sendRelay(r relaySend) bool {
	w.relayTurn = false
	w.busy.Store(true)
	var err error
	if rs, ok := w.tr.(relaySender); ok {
		err = rs.SendRelay(r.reqID, r.payload, r.final)
	} else {
		err = w.writeFrame(r.payload)
	}
	w.busy.Store(false)
	if err != nil {
		log.Printf("[hub-client] 中转响应写入失败 reqId=%s: %v", r.reqID, err)
		w.fail(reasonOfError(err), err)
		return true
	}
	w.s.c.relayOutbox.markSent(r.reqID, r.seq)
	return true
}

// sendRelayRepair resends the oldest unacked chunk of the oldest answer. It is
// the RTO path, so it never replays a whole answer.
func (w *writer) sendRelayRepair() bool {
	r, ok := w.s.c.relayOutbox.repair()
	if !ok {
		return false
	}
	return w.sendRelay(r)
}

// writeFrame sends one complete logical frame: text when it is small and
// uncompressed, otherwise transport units. A frame that compressed on its own
// still travels as a unit so the receiver can tell it from a text JSON frame,
// and a unit-bearing frame is split so a control frame can interleave.
func (w *writer) writeFrame(payload []byte) error {
	w.busy.Store(true)
	defer w.busy.Store(false)

	compressed := false
	if w.deflate.Load() {
		if zp, ok := deflatePayload(payload); ok {
			payload, compressed = zp, true
		}
	}
	if !compressed && len(payload) <= wire.UnitSize {
		return w.tr.Send(payload)
	}
	units, err := wire.SplitFrame(payload, compressed, w.frameID.Add(1), wire.UnitSize)
	if err != nil {
		return err
	}
	for i, unit := range units {
		if i > 0 {
			// A complete unit passes straight through on the receiving side,
			// so a heartbeat overtakes the rest of the frame.
			w.flushControl()
		}
		if err := w.tr.SendFragment(unit); err != nil {
			return err
		}
	}
	return nil
}

// buildEvents packs a contiguous prefix of entries into one frame under the
// hub's maxFrame and reports how many entries it consumed. The frame's seq is
// its first event, so the hub accepts the run one event at a time and its
// watermark keeps per-event meaning.
func (w *writer) buildEvents(entries []wire.Entry) ([]byte, int, uint64, error) {
	limit := w.s.up.frameLimit()
	raws := make([]json.RawMessage, 0, len(entries))
	size := 64
	var last uint64
	for _, e := range entries {
		if len(raws) > 0 && size+len(e.Payload)+1 > limit {
			break
		}
		raws = append(raws, e.Payload)
		size += len(e.Payload) + 1
		last = e.Seq
	}
	if len(raws) == 0 {
		return nil, 0, 0, errors.New("events frame would be empty")
	}
	f := wire.Frame{
		V:      wire.ProtocolVersion,
		Type:   wire.TypeEvents,
		Seq:    entries[0].Seq,
		Ack:    w.s.downAck.Load(),
		Events: raws,
	}
	payload, err := wire.Encode(f)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(payload) > limit {
		// A single event that does not fit must have been clamped by the
		// outbox; anything else here is a bug worth failing loudly on.
		return nil, 0, 0, fmt.Errorf("events frame %d bytes over maxFrame %d", len(payload), limit)
	}
	return payload, len(raws), last, nil
}

// flushControl writes the latest pending control frame, if any. It is called
// both by the step loop and between fragments, and never blocks on anything
// but the transport's own write.
func (w *writer) flushControl() bool {
	w.ctrlMu.Lock()
	payload := w.ctrl
	w.ctrl = nil
	w.ctrlMu.Unlock()
	if payload == nil {
		return false
	}
	if err := w.tr.Send(payload); err != nil {
		log.Printf("[hub-client] 心跳写入失败: %v", err)
		w.fail(reasonOfError(err), err)
	}
	return true
}

// postControl queues a control frame. Latest wins: an hb is a snapshot, so an
// older queued one carries nothing a newer one does not.
func (w *writer) postControl(payload []byte) {
	w.ctrlMu.Lock()
	w.ctrl = payload
	w.ctrlMu.Unlock()
	w.pump()
}

// pump wakes the writer after new work was queued, and the relay lane when
// this transport writes relay chunks off the writer.
func (w *writer) pump() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
	if w.relay != nil {
		w.relay.kick()
	}
}

// requestRetransmit asks the writer to replay the oldest unacked batch.
func (w *writer) requestRetransmit() {
	w.retransmit.Store(true)
	w.pump()
}

func (w *writer) setDeflate(v bool) { w.deflate.Store(v) }

// Busy reports whether a logical frame — or, on a transport with a relay
// plane, a relay chunk — is currently being written. The session skips the RTO
// while it is set: an active write does not need (and would be duplicated by)
// a retransmit.
func (w *writer) Busy() bool {
	if w.busy.Load() {
		return true
	}
	return w.relay != nil && w.relay.writing.Load()
}

func (w *writer) sentThrough() uint64 { return w.sent.Load() }

// setSent seeds the watermark before the writer starts (hello_ack adoption).
func (w *writer) setSent(n uint64) { w.sent.Store(n) }

// raiseSent keeps the watermark at or above n when the hub acks past what was
// marked sent (an ack can only cover frames already written, but reconciling
// here keeps a stale watermark from resending them).
func (w *writer) raiseSent(n uint64) {
	for {
		cur := w.sent.Load()
		if n <= cur || w.sent.CompareAndSwap(cur, n) {
			return
		}
	}
}

// fail records the write error and ends the session once.
func (w *writer) fail(reason wire.CloseReason, err error) {
	if w.failed.Swap(true) {
		return
	}
	w.s.stop(reason, err)
}
