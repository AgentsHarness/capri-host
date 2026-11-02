package hub

import (
	"errors"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-hub/wire"
)

// relayOutboxMaxBytes bounds the memory held by completed answers the hub has
// not confirmed. A var so tests can shrink it. Frames carry base64 chunks, so
// allow the 4/3 encoding expansion for two maximum answers; this matches the
// hub's per-host parked-chunk budget.
var relayOutboxMaxBytes = 2 * (wire.DefaultLimits.MaxRelayBody*4/3 + relayChunkSize)

var (
	// errRelayOutboxFull reports that an answer did not fit the byte budget.
	errRelayOutboxFull = errors.New("hub: relay answer log over its byte cap")
	// errRelayOutboxStale reports an answer for a hub incarnation this log no
	// longer serves (a restarted hub reuses reqIds).
	errRelayOutboxStale = errors.New("hub: relay answer belongs to another hub incarnation")
	// errRelayOutboxEmpty reports an answer with no encoded chunk.
	errRelayOutboxEmpty = errors.New("hub: relay answer without a chunk")
)

// relayChunk is one encoded respond frame, keyed by its per-request ChunkSeq.
type relayChunk struct {
	seq     uint64
	payload []byte
}

// relayAnswer is one completed answer waiting for the hub's chunk acks. It
// outlives the session that produced it: the browser request was already acked
// by the hub, so this log is its only route to an answer.
//
// acked is the highest contiguous chunk the hub confirmed, sent is the writing
// cursor for the current session, and final is the last ChunkSeq. Chunks at or
// below acked are dropped, so chunks[0] is always acked+1. inflight is the one
// chunk an asynchronous relay lane is writing right now; a request never has
// two chunk writes outstanding.
type relayAnswer struct {
	reqID    string
	chunks   []relayChunk
	acked    uint64
	sent     uint64
	inflight uint64
	final    uint64
	at       time.Time
	aborted  bool
}

func (a *relayAnswer) size() int {
	n := 0
	for _, c := range a.chunks {
		n += len(c.payload)
	}
	return n
}

// payload returns the chunk at seq, if it is still held.
func (a *relayAnswer) payload(seq uint64) ([]byte, bool) {
	if seq <= a.acked || seq > a.final {
		return nil, false
	}
	i := seq - a.acked - 1
	if i >= uint64(len(a.chunks)) {
		return nil, false
	}
	c := a.chunks[i]
	if c.seq != seq {
		return nil, false // the slice and the watermark disagree
	}
	return c.payload, true
}

// advance records n as the hub's highest contiguous chunk and drops everything
// it covers. The writing cursor only moves forward.
func (a *relayAnswer) advance(n uint64) {
	if n > a.final {
		n = a.final
	}
	if n <= a.acked {
		return
	}
	drop := int(n - a.acked)
	if drop > len(a.chunks) {
		drop = len(a.chunks)
	}
	a.chunks = a.chunks[drop:]
	a.acked = n
	if a.sent < n {
		a.sent = n
	}
	if a.inflight != 0 && a.inflight <= n {
		a.inflight = 0
	}
}

// relaySend is one chunk the writer should put on the wire.
type relaySend struct {
	reqID   string
	seq     uint64
	payload []byte
	final   bool
}

// relayDrop reports an answer the outbox released because the hub confirmed it,
// discarded it, or no longer holds its request. The caller settles the memo
// tombstone so a late duplicate cannot re-execute.
type relayDrop struct {
	reqID   string
	aborted bool
}

// relayOutbox is the host's durable relay answer log. It is one per Client, so
// answers survive reconnects, and it is keyed by reqID inside one hubBoot: a
// restarted hub has a fresh reqId space and reset() drops the old answers.
type relayOutbox struct {
	mu      sync.Mutex
	order   []string
	answers map[string]*relayAnswer
	bytes   int
	boot    uint64
}

func newRelayOutbox() *relayOutbox {
	return &relayOutbox{answers: make(map[string]*relayAnswer)}
}

// reset drops every answer and adopts a new hub incarnation.
func (o *relayOutbox) reset(boot uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.answers = make(map[string]*relayAnswer)
	o.order = nil
	o.bytes = 0
	o.boot = boot
}

// add appends one complete answer, replacing any answer it supersedes. It
// refuses to exceed the byte budget and refuses an answer for another hub
// incarnation.
func (o *relayOutbox) add(reqID string, hubBoot uint64, chunks [][]byte, final uint64) error {
	if len(chunks) == 0 || final == 0 || uint64(len(chunks)) < final {
		return errRelayOutboxEmpty
	}
	size := 0
	for _, c := range chunks {
		size += len(c)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if hubBoot != 0 && o.boot != 0 && hubBoot != o.boot {
		return errRelayOutboxStale
	}
	if o.boot == 0 {
		o.boot = hubBoot
	}
	if o.bytes+size > relayOutboxMaxBytes {
		return errRelayOutboxFull
	}
	if old, ok := o.answers[reqID]; ok {
		o.bytes -= old.size()
	} else {
		o.order = append(o.order, reqID)
	}
	a := &relayAnswer{reqID: reqID, acked: 0, final: final, at: time.Now()}
	for i, c := range chunks {
		a.chunks = append(a.chunks, relayChunk{seq: uint64(i + 1), payload: c})
	}
	o.answers[reqID] = a
	o.bytes += size
	return nil
}

// addError appends a terminal error answer regardless of the budget: it is a
// few dozen bytes, and dropping it would leave the browser waiting out the
// relay deadline for a request that already failed.
func (o *relayOutbox) addError(reqID string, hubBoot uint64, payload []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if hubBoot != 0 && o.boot != 0 && hubBoot != o.boot {
		return errRelayOutboxStale
	}
	if o.boot == 0 {
		o.boot = hubBoot
	}
	if old, ok := o.answers[reqID]; ok {
		o.bytes -= old.size()
	} else {
		o.order = append(o.order, reqID)
	}
	o.answers[reqID] = &relayAnswer{
		reqID:  reqID,
		chunks: []relayChunk{{seq: 1, payload: payload}},
		final:  1,
		at:     time.Now(),
	}
	o.bytes += len(payload)
	return nil
}

// next returns the next chunk to stream: the oldest answer first, in chunk
// order. It never returns a chunk at or below the ack watermark, and never one
// an asynchronous lane already holds in flight.
func (o *relayOutbox) next() (relaySend, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range o.order {
		a := o.answers[id]
		if a == nil || a.aborted || a.inflight != 0 || a.sent >= a.final {
			continue
		}
		if a.sent < a.acked {
			a.sent = a.acked
		}
		seq := a.sent + 1
		payload, ok := a.payload(seq)
		if !ok {
			continue
		}
		return relaySend{reqID: id, seq: seq, payload: payload, final: seq == a.final}, true
	}
	return relaySend{}, false
}

// repair returns the oldest unacked chunk of the oldest answer. This is the
// RTO path: one chunk, never the whole answer, so a repair cannot bury the
// link it is meant to fix.
func (o *relayOutbox) repair() (relaySend, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range o.order {
		a := o.answers[id]
		if a == nil || a.aborted || a.inflight != 0 || a.acked >= a.final {
			continue
		}
		if a.sent < a.acked {
			a.sent = a.acked
		}
		seq := a.acked + 1
		payload, ok := a.payload(seq)
		if !ok {
			continue
		}
		if a.sent < seq {
			a.sent = seq
		}
		return relaySend{reqID: id, seq: seq, payload: payload, final: seq == a.final}, true
	}
	return relaySend{}, false
}

// reserveNext is the asynchronous lane's next(): it hands out the next chunk
// and marks it in flight, so neither a second call nor an RTO repair can pick
// the same chunk while its write is outstanding.
func (o *relayOutbox) reserveNext() (relaySend, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range o.order {
		a := o.answers[id]
		if a == nil || a.aborted || a.inflight != 0 || a.sent >= a.final {
			continue
		}
		if a.sent < a.acked {
			a.sent = a.acked
		}
		seq := a.sent + 1
		payload, ok := a.payload(seq)
		if !ok {
			continue
		}
		a.inflight = seq
		return relaySend{reqID: id, seq: seq, payload: payload, final: seq == a.final}, true
	}
	return relaySend{}, false
}

// reserveRepair is the asynchronous lane's repair(): the oldest unacked chunk,
// unless that request already has a write outstanding.
func (o *relayOutbox) reserveRepair() (relaySend, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range o.order {
		a := o.answers[id]
		if a == nil || a.aborted || a.inflight != 0 || a.acked >= a.final {
			continue
		}
		seq := a.acked + 1
		payload, ok := a.payload(seq)
		if !ok {
			continue
		}
		a.inflight = seq
		return relaySend{reqID: id, seq: seq, payload: payload, final: seq == a.final}, true
	}
	return relaySend{}, false
}

// completeSend records that an asynchronous write of chunk seq finished: the
// in-flight slot is released and the cursor advances.
func (o *relayOutbox) completeSend(reqID string, seq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	a := o.answers[reqID]
	if a == nil {
		return
	}
	if a.inflight == seq {
		a.inflight = 0
	}
	if seq > a.sent {
		a.sent = seq
	}
}

// releaseSend drops an in-flight marker after a failed write. The session is
// closing anyway; this only keeps the bookkeeping honest.
func (o *relayOutbox) releaseSend(reqID string, seq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if a := o.answers[reqID]; a != nil && a.inflight == seq {
		a.inflight = 0
	}
}

// markSent records that chunk seq of reqID reached the transport. It is the
// synchronous (WebSocket) path's completion; a lane marker for the same chunk,
// if any, is cleared too.
func (o *relayOutbox) markSent(reqID string, seq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	a := o.answers[reqID]
	if a == nil {
		return
	}
	if a.inflight == seq {
		a.inflight = 0
	}
	if seq > a.sent {
		a.sent = seq
	}
}

// pending reports whether any answer still lacks a hub confirmation.
func (o *relayOutbox) pending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, a := range o.answers {
		if !a.aborted && a.acked < a.final {
			return true
		}
	}
	return false
}

// queued counts the answers still waiting for confirmation, for diagnostics.
func (o *relayOutbox) queued() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, a := range o.answers {
		if !a.aborted && a.acked < a.final {
			n++
		}
	}
	return n
}

// bootID is the hub incarnation this log serves, 0 before the first adoption.
func (o *relayOutbox) bootID() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.boot
}

// reconcile applies the hub's relay ack table. Every answer in the table moves
// its ack watermark and drops the chunks the hub already holds; Done or Abort
// releases it outright. A handshake table (hello_ack) additionally releases
// answers the hub no longer lists — it never held them or already resolved
// them — and starts the writing cursor at the ack watermark, because the dead
// link's cursor proves nothing.
func (o *relayOutbox) reconcile(acks []wire.RelayAck, handshake bool) []relayDrop {
	table := make(map[string]wire.RelayAck, len(acks))
	for _, a := range acks {
		table[a.ReqID] = a
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var drops []relayDrop
	snapshot := append([]string(nil), o.order...)
	for _, id := range snapshot {
		a := o.answers[id]
		if a == nil {
			continue
		}
		ack, ok := table[id]
		switch {
		case !ok:
			if !handshake {
				continue
			}
			o.releaseLocked(id)
			drops = append(drops, relayDrop{reqID: id})
			continue
		case ack.Abort:
			o.releaseLocked(id)
			drops = append(drops, relayDrop{reqID: id, aborted: true})
			continue
		case ack.Done:
			o.releaseLocked(id)
			drops = append(drops, relayDrop{reqID: id})
			continue
		default:
			a.advance(ack.Chunk)
		}
		if a.acked >= a.final {
			o.releaseLocked(id)
			drops = append(drops, relayDrop{reqID: id})
			continue
		}
		if handshake {
			a.sent = a.acked
		}
	}
	o.compactLocked()
	return drops
}

func (o *relayOutbox) releaseLocked(reqID string) {
	if a := o.answers[reqID]; a != nil {
		o.bytes -= a.size()
		delete(o.answers, reqID)
	}
}

func (o *relayOutbox) compactLocked() {
	kept := o.order[:0]
	for _, id := range o.order {
		if o.answers[id] != nil {
			kept = append(kept, id)
		}
	}
	o.order = kept
}
