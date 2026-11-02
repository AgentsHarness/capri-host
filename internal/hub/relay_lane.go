package hub

import (
	"log"
	"sync/atomic"
)

// relayLane writes relay chunks for transports with an independent relay plane
// (QUIC request streams). It exists so a 256KiB chunk write can never delay the
// control stream: the session writer keeps sending hb, probes and events while
// a chunk is in flight.
//
// The lane owns the asynchronous half of the outbox protocol: reserve (which
// marks the chunk in flight), write, complete. A request never has two writes
// outstanding, so per-request chunk order is preserved and an RTO repair can
// never queue a chunk that is already being written.
type relayLane struct {
	w  *writer
	rs relaySender

	wake chan struct{}
	done chan struct{}
	// repair asks for the oldest unacked chunk instead of the next unsent one.
	repair atomic.Bool
	// writing is true while a chunk write is parked in the transport. The
	// session treats it like writer.Busy(): an answer that is actively being
	// written does not need an RTO repair, and a repair would duplicate it.
	writing atomic.Bool
}

func newRelayLane(w *writer, rs relaySender) *relayLane {
	return &relayLane{
		w:    w,
		rs:   rs,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

// kick wakes the lane after the answer log changed.
func (l *relayLane) kick() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// requestRepair schedules one oldest-unacked-chunk write on the lane.
func (l *relayLane) requestRepair() {
	l.repair.Store(true)
	l.kick()
}

// run streams chunks until the session ends. It stops at the first step after
// the session ended, like the writer, so a dying lane cannot advance the
// shared answer log while the replacement session resumes it.
func (l *relayLane) run() {
	defer close(l.done)
	for {
		select {
		case <-l.w.s.ctx.Done():
			return
		case <-l.w.s.stopCh:
			return
		default:
		}
		if l.w.failed.Load() {
			return
		}
		if !l.w.ready.Load() {
			l.wait()
			continue
		}
		if l.repair.Swap(false) {
			if r, ok := l.w.s.c.relayOutbox.reserveRepair(); ok {
				if !l.write(r) {
					return
				}
				continue
			}
		}
		if r, ok := l.w.s.c.relayOutbox.reserveNext(); ok {
			if !l.write(r) {
				return
			}
			continue
		}
		l.wait()
	}
}

func (l *relayLane) wait() {
	select {
	case <-l.w.s.ctx.Done():
	case <-l.w.s.stopCh:
	case <-l.wake:
	}
}

// write puts one chunk on its request stream. A failure ends the session with
// the same verdict mapping the writer uses.
func (l *relayLane) write(r relaySend) bool {
	l.writing.Store(true)
	err := l.rs.SendRelay(r.reqID, r.payload, r.final)
	l.writing.Store(false)
	if err != nil {
		log.Printf("[hub-client] 中转响应写入失败 reqId=%s: %v", r.reqID, err)
		l.w.s.c.relayOutbox.releaseSend(r.reqID, r.seq)
		l.w.fail(reasonOfError(err), err)
		return false
	}
	l.w.s.c.relayOutbox.completeSend(r.reqID, r.seq)
	return true
}
