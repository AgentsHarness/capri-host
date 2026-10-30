package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AgentsHarness/capri-hub/wire"
)

// Transport is one attached hub link. Recv blocks on the transport's own
// reader and is called from the session's single reader goroutine; Send and
// SetKeepalive may be called from the session loop and from the relay writer,
// so an implementation must serialize its writes. Close may be called from any
// goroutine and must unblock Recv.
type Transport interface {
	Send([]byte) error
	Recv() ([]byte, error)
	Close(wire.CloseReason) error
	SetKeepalive(time.Duration)
}

// deflateSetter is the optional capability a transport exposes when its wire
// can carry raw-deflate frames. It is applied only after hello_ack echoed
// hello.caps.deflate.
type deflateSetter interface{ SetDeflate(bool) }

// relaySender is the optional plane a transport exposes for `respond` frames.
// QUIC carries each answer on its own stream; WebSocket multiplexes and does
// not implement this.
type relaySender interface {
	SendRelay(reqID string, payload []byte, final bool) error
}

// peerClose is the peer's verdict on why the session ended. Run switches its
// reconnect policy on that reason, never on a string.
type peerClose struct{ reason wire.CloseReason }

func (e *peerClose) Error() string { return "hub closed the session: " + e.reason.String() }

func closeReasonOf(err error) (wire.CloseReason, bool) {
	var pc *peerClose
	if errors.As(err, &pc) {
		return pc.reason, true
	}
	return 0, false
}

// closeReasonFor maps a wire decode error onto the close the peer must see.
func closeReasonFor(err error) wire.CloseReason {
	if errors.Is(err, wire.ErrProtocol) {
		return wire.CloseProtocol
	}
	return wire.CloseUnreadable
}

// transportCloseReason maps a transport Recv error onto the session verdict.
// A transport that could not decode a frame reports it wrapped in
// wire.ErrUnreadable (corrupt deflate, wrong framing); carrying that verdict
// into the close is what tells the hub the log must be replayed from its
// watermark instead of looking like an ordinary disconnect. Anything else is
// a plain link drop.
func transportCloseReason(err error) wire.CloseReason {
	switch {
	case errors.Is(err, wire.ErrProtocol):
		return wire.CloseProtocol
	case errors.Is(err, wire.ErrUnreadable):
		return wire.CloseUnreadable
	default:
		return wire.CloseNormal
	}
}

// session is one live host link. Its loop owns every timer and the uplink log
// cursor; the relay writer and the bridge producer reach it through the
// outbox mutex, the wake channel and the respond queue.
type session struct {
	c      *Client
	bridge bridgeSource
	tr     Transport

	epoch uint64
	// hubBoot is the hub process behind this link, adopted from hello_ack.
	// Relay answers may only be sent to the hub that asked for them: a hub
	// restart resets its request ids.
	hubBoot uint64

	up     *outbox
	down   *wire.Receiver
	parked map[uint64]wire.Frame

	lastGen uint64

	keepalive time.Duration
	idle      time.Duration
	maxRelay  int

	// Loop-owned link state: how far the uplink log has been handed to THIS
	// transport, and the hub's last heartbeat status.
	sentThrough uint64
	lastStatus  wire.HeartbeatStatus
	ackProgress bool

	relayOut chan relayFrame

	ctx      context.Context
	stopOnce sync.Once
	stopCh   chan struct{}
	stopWhy  atomic.Uint32
	stopMu   sync.Mutex
	stopErr  error
}

func (s *session) stop(reason wire.CloseReason, err error) {
	s.stopOnce.Do(func() {
		s.stopWhy.Store(uint32(reason))
		s.stopMu.Lock()
		s.stopErr = err
		s.stopMu.Unlock()
		close(s.stopCh)
	})
}

func (s *session) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// result is the (reason, error) the session ended with.
func (s *session) result() (wire.CloseReason, error) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	return s.stopReason(), s.stopErr
}

func (s *session) stopReason() wire.CloseReason {
	if r := wire.CloseReason(s.stopWhy.Load()); r.Valid() {
		return r
	}
	return wire.CloseNormal
}

func (s *session) rto() time.Duration { return 2 * s.keepalive }

// runSession owns one transport from hello to close. It is the only place a
// session is created, so the Client's "current session" pointer and the relay
// plane always agree on which link is live.
func (c *Client) runSession(ctx context.Context, bridge bridgeSource, tr Transport) (wire.CloseReason, error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := &session{
		c:         c,
		bridge:    bridge,
		tr:        tr,
		epoch:     c.epoch,
		up:        c.out,
		parked:    make(map[uint64]wire.Frame),
		keepalive: time.Duration(wire.DefaultLiveness.Keepalive) * time.Second,
		idle:      time.Duration(wire.DefaultLiveness.Idle) * time.Second,
		maxRelay:  wire.DefaultLimits.MaxRelayBody,
		relayOut:  make(chan relayFrame, 64),
		ctx:       sctx,
		stopCh:    make(chan struct{}),
	}
	s.down = wire.NewReceiver(c.downApplied())

	c.setSession(s)
	defer c.setSession(nil)
	defer c.noteDisconnected()

	reader, stopReader := readFrames(tr)
	defer stopReader()

	go s.relayWriter()

	reason, err := s.run(reader)
	if s.stopped() {
		reason, err = s.result()
	}
	// Stop the relay writer first, then close the transport: the close is
	// what unblocks a write still parked in the adapter.
	cancel()
	if cerr := tr.Close(reason); cerr != nil {
		log.Printf("[hub-client] close(%s): %v", reason, cerr)
	}
	return reason, err
}

// run is the one session loop: handshake, then read frames, apply them, flush
// the uplink log, send heartbeats, retransmit and enforce idle. One timer is
// re-armed for whichever deadline comes next — keepalive, RTO, idle.
func (s *session) run(reader <-chan readResult) (wire.CloseReason, error) {
	if err := s.sendHello(); err != nil {
		return wire.CloseNormal, err
	}

	timer := time.NewTimer(s.idle)
	defer timer.Stop()

	select {
	case r, ok := <-reader:
		if !ok {
			return wire.CloseNormal, errors.New("hub closed the connection during the handshake")
		}
		if r.err != nil {
			return transportCloseReason(r.err), r.err
		}
		if reason, err := s.adoptHelloAck(r.data); err != nil {
			log.Printf("[hub-client] 握手失败: %v", err)
			return reason, err
		}
	case <-timer.C:
		return wire.CloseLiveness, errors.New("hub 在握手窗口内没有回应 hello_ack")
	case <-s.ctx.Done():
		return wire.CloseNormal, s.ctx.Err()
	}

	if reason, err := s.flush(); err != nil {
		return reason, err
	}

	now := time.Now()
	lastRecv := now
	nextHB := now.Add(s.keepalive)
	rtoAt := now.Add(s.rto())

	for {
		next := nextHB
		if d := lastRecv.Add(s.idle); d.Before(next) {
			next = d
		}
		if s.hasPending() && rtoAt.Before(next) {
			next = rtoAt
		}
		resetTimer(timer, time.Until(next))

		select {
		case <-s.stopCh:
			return s.result()

		case <-s.ctx.Done():
			return wire.CloseNormal, s.ctx.Err()

		case r, ok := <-reader:
			if !ok {
				return wire.CloseNormal, errors.New("hub closed the connection")
			}
			if r.err != nil {
				return transportCloseReason(r.err), r.err
			}
			lastRecv = time.Now()
			s.ackProgress = false
			if reason, err := s.handleFrame(r.data); err != nil {
				return reason, err
			}
			if s.ackProgress {
				// Ack progress resets the RTO and confirms immediately: a
				// frame already in flight must not be retransmitted on the
				// old schedule.
				rtoAt = time.Now().Add(s.rto())
				s.sendHeartbeat()
			}
			if reason, err := s.flush(); err != nil {
				return reason, err
			}

		case <-s.c.wake:
			if reason, err := s.flush(); err != nil {
				return reason, err
			}

		case <-s.c.statusCh:
			if st := s.status(); st != s.lastStatus {
				s.sendHeartbeat()
			}

		case <-timer.C:
			now := time.Now()
			if !now.Before(nextHB) {
				s.sendHeartbeat()
				nextHB = now.Add(s.keepalive)
			}
			if s.hasPending() && !now.Before(rtoAt) {
				if reason, err := s.retransmit(); err != nil {
					return reason, err
				}
				rtoAt = now.Add(s.rto())
			}
			if !now.Before(lastRecv.Add(s.idle)) {
				log.Printf("[hub-client] hub 静默超过 %.0fs，判定链路已死", s.idle.Seconds())
				return wire.CloseLiveness, fmt.Errorf("no frame from the hub for %s", s.idle)
			}
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if d < 0 {
		d = 0
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func (s *session) sendHello() error {
	hello := wire.NewHello(wire.Hello{
		Epoch: s.epoch,
		Token: s.c.tokenLocked(),
		Resume: &wire.Resume{
			Up:   s.up.prepareEpoch(s.bridge.LastSeq),
			Down: s.c.downApplied(),
		},
		Caps: &wire.Caps{Deflate: true},
	})
	payload, err := wire.Encode(hello)
	if err != nil {
		return err
	}
	return s.tr.Send(payload)
}

// adoptHelloAck takes the hub's authoritative watermarks, time scale, limits
// and audience, then arms the transport for this session.
func (s *session) adoptHelloAck(data []byte) (wire.CloseReason, error) {
	f, err := wire.Decode(data)
	if err != nil {
		return closeReasonFor(err), err
	}
	if f.Type != wire.TypeHelloAck {
		return wire.CloseProtocol, fmt.Errorf("expected hello_ack, got %q", f.Type)
	}
	ack, err := f.HelloAck()
	if err != nil {
		return wire.CloseProtocol, err
	}
	if ack.Epoch != s.epoch {
		return wire.CloseProtocol, fmt.Errorf("hub echoed epoch %d, this host is %d", ack.Epoch, s.epoch)
	}

	// ackUp is authoritative: it drops everything the hub already applied and
	// leaves the rest queued for replay. ackDown seeds the down receiver over
	// the hub's own sequence space, which a hub restart resets; the parked-run
	// bound is the window the hub advertised, not a local constant.
	s.up.ack(ack.AckUp)
	window := wire.DefaultLimits.ResumeWindow
	if lim := ack.Limits; lim != nil {
		window = lim.ResumeWindow
	}
	s.down = wire.NewReceiverWindow(ack.AckDown, window)
	s.c.setDownApplied(ack.AckDown)
	s.sentThrough = s.up.acked()

	// A different hubBoot is a restarted hub: its request-id counter starts
	// over, so nothing remembered under the old boot may answer (or suppress)
	// a request from this one.
	if ack.HubBoot != s.hubBoot {
		s.c.memo.pruneHub(ack.HubBoot)
		s.hubBoot = ack.HubBoot
	}

	if l := ack.Liveness; l != nil {
		s.keepalive = time.Duration(l.Keepalive) * time.Second
		s.idle = time.Duration(l.Idle) * time.Second
	}
	if lim := ack.Limits; lim != nil {
		s.maxRelay = lim.MaxRelayBody
		s.up.setLimits(lim.MaxFrame)
	}
	s.tr.SetKeepalive(s.keepalive)
	if ds, ok := s.tr.(deflateSetter); ok {
		ds.SetDeflate(ack.Caps != nil && ack.Caps.Deflate)
	}
	if ack.Audience != nil {
		s.applyAudience(*ack.Audience)
	}
	s.lastStatus = s.status()
	s.sendHeartbeat()

	// Answers that finished while this host had no session go out now: the
	// hub acked their requests and will never replay them, so this link is
	// their only route to the browser.
	s.c.flushRelayAnswers(s)

	log.Printf("[hub-client] hub 会话已就绪（epoch %d，ackUp %d，ackDown %d，audience gen %d，watchers %d）",
		s.epoch, ack.AckUp, ack.AckDown, s.lastGen, s.c.watchers())
	return 0, nil
}

// handleFrame applies one downlink frame.
func (s *session) handleFrame(data []byte) (wire.CloseReason, error) {
	f, err := wire.Decode(data)
	if err != nil {
		log.Printf("[hub-client] 无法解析下行帧: %v", err)
		return closeReasonFor(err), err
	}
	if f.Ack > s.up.acked() {
		s.up.ack(f.Ack)
		if s.up.acked() > s.sentThrough {
			s.sentThrough = s.up.acked()
		}
		s.ackProgress = true
	}
	switch f.Type {
	case wire.TypeAudience:
		return s.acceptDown(f, data, func(pf wire.Frame) (wire.CloseReason, error) {
			s.applyAudience(pf.AudienceState())
			return 0, nil
		})
	case wire.TypeRequest:
		return s.acceptDown(f, data, func(pf wire.Frame) (wire.CloseReason, error) {
			s.onRequest(pf)
			return 0, nil
		})
	case wire.TypeHeartbeat:
		// The echoed status is informational; the ack was applied above.
		return 0, nil
	default:
		return wire.CloseProtocol, fmt.Errorf("hub sent %q on the down link", f.Type)
	}
}

// acceptDown feeds one reliable downframe through the receiver and applies
// whatever became contiguous. A frame that arrives ahead of a gap is parked
// with its decoded form; the receiver hands back payloads in seq order, so the
// contiguous seqs are prev+1, prev+2, …
func (s *session) acceptDown(f wire.Frame, raw []byte, apply func(wire.Frame) (wire.CloseReason, error)) (wire.CloseReason, error) {
	prev := s.down.Acked()
	s.parked[f.Seq] = f
	ready, ack, err := s.down.Accept(f.Seq, raw)
	switch {
	case errors.Is(err, wire.ErrOverflow):
		log.Printf("[hub-client] 下行乱序窗口溢出（seq %d）", f.Seq)
		return wire.CloseOverflow, err
	case err != nil:
		return wire.CloseUnreadable, err
	}
	for i := range ready {
		seq := prev + 1 + uint64(i)
		pf, ok := s.parked[seq]
		delete(s.parked, seq)
		if !ok {
			// The contiguous frame is the one that just arrived; it can only
			// be missing here if the receiver advanced differently.
			continue
		}
		if reason, err := apply(pf); err != nil {
			return reason, err
		}
	}
	if f.Seq <= ack {
		delete(s.parked, f.Seq)
	}
	s.c.setDownApplied(ack)
	return 0, nil
}

func (s *session) applyAudience(a wire.Audience) {
	if a.Gen < s.lastGen {
		return // a snapshot older than the one already in force
	}
	s.lastGen = a.Gen
	s.up.setPaused(a.Watchers == 0)
	s.c.setAudience(a)
}

func (s *session) onRequest(f wire.Frame) {
	go s.c.serveRelay(s, f)
}

func (s *session) status() wire.HeartbeatStatus {
	st := s.bridge.Snapshot()
	return wire.HeartbeatStatus{
		Ready:        st.Ready,
		Busy:         st.Busy,
		Booting:      st.Booting,
		PendingCount: len(st.PendingRequests),
		Port:         s.c.listenPort(),
	}
}

func (s *session) sendHeartbeat() {
	st := s.status()
	f, err := wire.NewHeartbeat(s.down.Acked(), st)
	if err != nil {
		return
	}
	payload, err := wire.Encode(f)
	if err != nil {
		return
	}
	s.lastStatus = st
	if err := s.tr.Send(payload); err != nil {
		s.stop(reasonOfError(err), err)
	}
}

// hasPending reports whether the log holds events the hub has not acked yet —
// the retransmit timer's only trigger. A paused audience keeps them queued
// without a timer: nothing is expected to be acked while nobody watches.
func (s *session) hasPending() bool {
	return !s.up.isPaused() && s.up.next() > s.up.acked()
}

// flush hands every logged event above sentThrough to the transport as
// `events` frames of contiguous runs. A paused audience keeps logging but
// sends nothing; the backlog goes out on the first flush after it resumes.
func (s *session) flush() (wire.CloseReason, error) {
	if s.up.isPaused() {
		return 0, nil
	}
	entries := s.up.entriesAfter(s.sentThrough)
	for len(entries) > 0 {
		payload, consumed, last, err := s.buildEvents(entries)
		if err != nil {
			return wire.CloseUnreadable, err
		}
		if err := s.tr.Send(payload); err != nil {
			log.Printf("[hub-client] 上行写入失败: %v", err)
			return reasonOfError(err), err
		}
		s.sentThrough = last
		entries = entries[consumed:]
	}
	return 0, nil
}

// buildEvents packs a contiguous prefix of entries into one frame under the
// hub's maxFrame and reports how many entries it consumed. The frame's seq is
// its first event, so the hub accepts the run one event at a time and its
// watermark keeps per-event meaning.
func (s *session) buildEvents(entries []wire.Entry) ([]byte, int, uint64, error) {
	limit := s.up.frameLimit()
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
		Ack:    s.down.Acked(),
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

// retransmit replays the unacked log from the ack watermark, re-batching the
// contiguous run. The payloads are the ones the hub already saw (minus the
// acked prefix), so the receiver treats them as duplicates where they overlap.
func (s *session) retransmit() (wire.CloseReason, error) {
	s.sentThrough = s.up.acked()
	return s.flush()
}

// ── relay plane ───────────────────────────────────────────────────────

// relayFrame is one queued respond frame: the encoded wire frame, the request
// it answers, and whether it closes the request's stream.
type relayFrame struct {
	reqID   string
	payload []byte
	final   bool
}

// relayWriter drains the respond queue onto the transport. It runs beside the
// session loop so a large answer (up to limits.maxRelayBody) can never delay a
// heartbeat or an ack.
func (s *session) relayWriter() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.stopCh:
			return
		case f := <-s.relayOut:
			var err error
			if rs, ok := s.tr.(relaySender); ok {
				err = rs.SendRelay(f.reqID, f.payload, f.final)
			} else {
				err = s.tr.Send(f.payload)
			}
			if err != nil {
				log.Printf("[hub-client] 中转响应写入失败: %v", err)
				s.stop(reasonOfError(err), err)
				return
			}
		}
	}
}

// sendRelay queues one respond frame. It blocks while the queue is full — a
// dropped answer would leave the browser waiting out the relay deadline — but
// never past the end of the session.
func (s *session) sendRelay(f relayFrame) {
	select {
	case s.relayOut <- f:
	case <-s.ctx.Done():
		log.Printf("[hub-client] 中转响应随会话结束未发出 reqId=%s", f.reqID)
	case <-s.stopCh:
		log.Printf("[hub-client] 中转响应随会话结束未发出 reqId=%s", f.reqID)
	}
}

// ── handshake plumbing ────────────────────────────────────────────────

type readResult struct {
	data []byte
	err  error
}

// readFrames runs the transport's blocking Recv on one goroutine and hands
// each frame to the session loop. stop must be called before the transport is
// closed so a reader parked on its send cannot outlive the session.
func readFrames(t Transport) (<-chan readResult, func()) {
	out := make(chan readResult, 8)
	quit := make(chan struct{})
	go func() {
		defer close(out)
		for {
			b, err := t.Recv()
			select {
			case out <- readResult{data: b, err: err}:
			case <-quit:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var once sync.Once
	return out, func() { once.Do(func() { close(quit) }) }
}

// reasonOfError maps a transport error onto the close the peer's verdict names,
// or wire.CloseNormal when the error says nothing about the protocol (a dial
// drop, a local socket error).
func reasonOfError(err error) wire.CloseReason {
	if r, ok := closeReasonOf(err); ok {
		return r
	}
	return wire.CloseNormal
}
