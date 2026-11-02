package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AgentsHarness/capri-hub/wire"
)

// Transport is one attached hub link. Recv blocks on the transport's own
// reader and is called from the session's single reader goroutine. Send and
// SendFragment are called from the session's writer goroutine only, so an
// implementation may serialize its writes with a plain lock. Close may be
// called from any goroutine and must unblock Recv.
//
// Send writes one complete logical frame (text JSON on WebSocket, one complete
// transport unit on QUIC); SendFragment writes one encoded wire transport unit
// as a binary record. A transport never compresses on its own: the session's
// writer owns deflate, and a transport only carries the flag it was given.
type Transport interface {
	Send([]byte) error
	SendFragment([]byte) error
	Recv() ([]byte, error)
	Close(wire.CloseReason) error
	SetKeepalive(time.Duration)
	// SetWriteTimeout bounds one unit write; zero restores the default.
	SetWriteTimeout(time.Duration)
	// SetMaxFrame bounds one reassembled logical frame.
	SetMaxFrame(int)
	// LastActivity is the last time the transport read anything, including a
	// partial unit. The session takes max(received frame, this) as its
	// liveness clock.
	LastActivity() time.Time
}

// deflateSetter is the optional capability a transport exposes when its wire
// can carry raw-deflate frames outside the unit path. Only the QUIC relay
// streams still compress on the transport: the session writer compresses the
// control stream itself.
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

// session is one live host link. Its loop owns every timer and the downlink
// receiver; the uplink writer and the bridge producer reach it through the
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

	// w is the single uplink goroutine: every socket write after the
	// handshake is posted to it, never performed by the session loop.
	w *writer
	// downAck mirrors down.Acked() for the writer, which builds events frames
	// on another goroutine. Bumped by the session loop after every accept.
	downAck atomic.Uint64
	// probeSeq mints liveness probe nonces.
	probeSeq atomic.Uint64

	// Loop-owned link state: the hub's last heartbeat status and whether the
	// last frame advanced the uplink watermark.
	lastStatus  wire.HeartbeatStatus
	ackProgress bool

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
		ctx:       sctx,
		stopCh:    make(chan struct{}),
	}
	s.down = wire.NewReceiver(c.downApplied())
	s.w = newWriter(s, tr)

	c.setSession(s)
	defer c.setSession(nil)
	defer c.noteDisconnected()

	reader, stopReader := readFrames(tr)
	defer stopReader()

	// The writer runs for the whole session, not just after the handshake:
	// hello_ack adopts the link and immediately flushes answers that finished
	// while disconnected, which must not wait behind a full relay queue.
	go s.w.run()
	if s.w.relay != nil {
		go s.w.relay.run()
	}

	reason, err := s.run(reader)
	if s.stopped() {
		reason, err = s.result()
	}
	// Stop the writers first, then close the transport: the close is what
	// unblocks a write still parked in the adapter. Waiting for both keeps a
	// dying link's cursors out of the answer log the next session resumes.
	cancel()
	if cerr := tr.Close(reason); cerr != nil {
		log.Printf("[hub-client] close(%s): %v", reason, cerr)
	}
	<-s.w.done
	if s.w.relay != nil {
		<-s.w.relay.done
	}
	return reason, err
}

// run is the one session loop: handshake, then read frames, apply them, hand
// uplink work to the writer, and enforce the keepalive, RTO, probe and idle
// deadlines. It never writes to the transport after the handshake, so a slow
// socket can never stall it.
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
	case <-s.stopCh:
		return s.result()
	case <-timer.C:
		return wire.CloseLiveness, errors.New("hub 在握手窗口内没有回应 hello_ack")
	case <-s.ctx.Done():
		return wire.CloseNormal, s.ctx.Err()
	}

	s.flush()

	now := time.Now()
	lastRecv := now
	nextHB := now.Add(s.hbInterval(s.lastStatus.Busy))
	rtoAt := now.Add(s.rto())
	// probeNonce is the outstanding liveness probe; any received frame clears
	// it, so only genuine silence escalates.
	var probeNonce uint64
	var probeSentAt time.Time

	for {
		next := nextHB
		if d := lastRecv.Add(s.idle); d.Before(next) {
			next = d
		}
		if s.hasPending() && !s.w.Busy() && rtoAt.Before(next) {
			next = rtoAt
		}
		if d := s.probeDeadline(lastRecv, probeNonce, probeSentAt); d.Before(next) {
			next = d
		}
		resetTimer(timer, time.Until(next))

		select {
		case <-s.stopCh:
			return s.result()

		case <-s.ctx.Done():
			return wire.CloseNormal, s.ctx.Err()

		case <-s.w.progress:
			// A complete events frame reached the transport; the old RTO was
			// for a frame that is no longer the last one written.
			rtoAt = time.Now().Add(s.rto())

		case r, ok := <-reader:
			if !ok {
				return wire.CloseNormal, errors.New("hub closed the connection")
			}
			if r.err != nil {
				return transportCloseReason(r.err), r.err
			}
			lastRecv = time.Now()
			probeNonce = 0
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
			s.flush()

		case <-s.c.wake:
			s.flush()

		case <-s.c.statusCh:
			if st := s.status(); st != s.lastStatus {
				s.sendHeartbeat()
				nextHB = time.Now().Add(s.hbInterval(st.Busy))
			}

		case <-timer.C:
			now := time.Now()
			if !now.Before(nextHB) {
				s.sendHeartbeat()
				nextHB = now.Add(s.hbInterval(s.lastStatus.Busy))
			}
			if s.hasPending() && !s.w.Busy() && !now.Before(rtoAt) {
				s.retransmit()
				rtoAt = now.Add(s.rto())
			}
			switch {
			case probeNonce != 0 && !now.Before(probeSentAt.Add(wire.ProbeTimeout)):
				log.Printf("[hub-client] 心跳探针 %.0fs 无回应，判定链路已死", wire.ProbeTimeout.Seconds())
				return wire.CloseLiveness, fmt.Errorf("no response to the liveness probe within %s", wire.ProbeTimeout)
			case probeNonce == 0 && !now.Before(lastRecv.Add(wire.ProbeAfter)):
				probeNonce = s.probeSeq.Add(1)
				probeSentAt = now
				s.sendProbe(probeNonce)
			}
			// A transport that is reading partial units is alive even when no
			// whole frame arrived: take the later of the two clocks.
			liveFrom := lastRecv
			if a := s.tr.LastActivity(); a.After(liveFrom) {
				liveFrom = a
			}
			if !now.Before(liveFrom.Add(s.idle)) {
				log.Printf("[hub-client] hub 静默超过 %.0fs，判定链路已死", s.idle.Seconds())
				return wire.CloseLiveness, fmt.Errorf("no frame from the hub for %s", s.idle)
			}
		}
	}
}

// hbInterval is the heartbeat cadence: the negotiated keepalive, tightened to
// wire.BusyKeepalive while a turn is active so a long tool call neither looks
// like an idle link nor leaves the peer's watermark stale.
func (s *session) hbInterval(busy bool) time.Duration {
	d := s.keepalive
	if busy && wire.BusyKeepalive < d {
		d = wire.BusyKeepalive
	}
	return d
}

// probeDeadline is the next liveness-probe event: when to probe after
// silence, or when an unanswered probe gives up.
func (s *session) probeDeadline(lastRecv time.Time, nonce uint64, sentAt time.Time) time.Time {
	if nonce != 0 {
		return sentAt.Add(wire.ProbeTimeout)
	}
	return lastRecv.Add(wire.ProbeAfter)
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
		Caps: &wire.Caps{Deflate: true, TransportUnits: true, RelayAck: true},
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
	// Transport-unit framing and the relay ack table are the whole point of
	// this protocol revision: a hub that did not echo them would see unit
	// records where it expects JSON, and could never confirm a relay answer.
	if ack.Caps == nil || !ack.Caps.TransportUnits || !ack.Caps.RelayAck {
		return wire.CloseProtocol, errors.New("hub did not echo caps.transportUnits and caps.relayAck")
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
	s.downAck.Store(ack.AckDown)
	s.w.setSent(s.up.acked())

	// A different hubBoot is a restarted hub: its request-id counter starts
	// over, so nothing remembered under the old boot may answer (or suppress)
	// a request from this one. The answer log belongs to the old incarnation
	// for the same reason. The comparison is against the answer log's boot, not
	// the session's: a fresh session starts at 0 and would otherwise look like
	// a restart on every reconnect.
	s.c.memo.pruneHub(ack.HubBoot)
	if s.c.relayOutbox.bootID() != ack.HubBoot {
		s.c.relayOutbox.reset(ack.HubBoot)
	}
	s.hubBoot = ack.HubBoot
	// The handshake table is authoritative for this incarnation: an answer it
	// no longer lists has been resolved or was never held, and a listed chunk
	// watermark is where this session resumes.
	s.c.applyRelayAcks(ack.HubBoot, ack.RelayAcks, true)

	if l := ack.Liveness; l != nil {
		s.keepalive = time.Duration(l.Keepalive) * time.Second
		s.idle = time.Duration(l.Idle) * time.Second
	}
	if lim := ack.Limits; lim != nil {
		s.maxRelay = lim.MaxRelayBody
		s.up.setLimits(lim.MaxFrame)
	}
	s.tr.SetKeepalive(s.keepalive)
	// A unit write must not outlive three quarters of the idle budget by more
	// than the peer allows: a stalled socket surfaces as a liveness close
	// instead of pinning the writer forever.
	s.tr.SetWriteTimeout(s.idle * 3 / 4)
	if lim := ack.Limits; lim != nil {
		s.tr.SetMaxFrame(lim.MaxFrame)
	}
	if ds, ok := s.tr.(deflateSetter); ok {
		ds.SetDeflate(ack.Caps.Deflate)
	}
	s.w.setDeflate(ack.Caps.Deflate)
	// Every writer input is configured: from here the writer may put data on
	// the wire. The first post below is also what wakes it.
	s.w.ready.Store(true)
	if ack.Audience != nil {
		s.applyAudience(*ack.Audience)
	}
	s.lastStatus = s.status()
	s.sendHeartbeat()
	// Answers reconciled above (and any answer that finished while this host
	// had no session) go out now; the hub acked their requests and will never
	// replay them, so this link is their only route to the browser.
	s.w.pump()

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
		s.w.raiseSent(f.Ack)
		s.ackProgress = true
	}
	switch f.Type {
	case wire.TypeAudience:
		if f.Seq <= s.down.Acked() {
			// The hub may replace an unacked audience frame's payload in place
			// with a newer absolute snapshot. The seq is then a duplicate for
			// the receiver, but the generation is not: apply it so a state
			// change can never be mistaken for a retransmission.
			s.applyAudience(f.AudienceState())
			return 0, nil
		}
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
		// The echoed status is informational; the ack was applied above. A
		// probe is answered from the control slot so it is not delayed by a
		// large frame the writer is still draining.
		if len(f.RelayAcks) > 0 {
			// A mid-session table is a delta-free snapshot, but a request the
			// hub has not listed yet is not evidence of loss: only Done and
			// Abort release an answer here.
			s.c.applyRelayAcks(s.hubBoot, f.RelayAcks, false)
			s.w.pump()
		}
		if f.Probe != 0 {
			s.sendProbeAck(f.Probe)
		}
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
	s.downAck.Store(ack)
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
	f, err := wire.NewHeartbeat(s.downAck.Load(), st)
	if err != nil {
		return
	}
	payload, err := wire.Encode(f)
	if err != nil {
		return
	}
	s.lastStatus = st
	s.w.postControl(payload)
}

// sendProbe asks the hub to echo nonce. It rides the control slot, so it goes
// out before any queued events or relay frames.
func (s *session) sendProbe(nonce uint64) {
	f, err := wire.NewHeartbeatProbe(s.downAck.Load(), nonce, s.status(), false)
	if err != nil {
		return
	}
	payload, err := wire.Encode(f)
	if err != nil {
		return
	}
	s.w.postControl(payload)
}

// sendProbeAck answers a hub probe immediately, from the control slot.
func (s *session) sendProbeAck(nonce uint64) {
	f, err := wire.NewHeartbeatProbe(s.downAck.Load(), nonce, s.status(), true)
	if err != nil {
		return
	}
	payload, err := wire.Encode(f)
	if err != nil {
		return
	}
	s.w.postControl(payload)
}

// hasPending reports whether anything is still waiting for the hub: the events
// log above the ack watermark, or a relay answer chunk the hub has not
// confirmed. Either one arms the retransmit timer. A paused audience keeps
// events queued without a timer: nothing is expected to be acked while nobody
// watches, but relay answers are outside the audience.
func (s *session) hasPending() bool {
	if s.c.relayOutbox.pending() {
		return true
	}
	return !s.up.isPaused() && s.up.next() > s.up.acked()
}

// flush tells the writer there may be new uplink events. The session loop
// never touches the transport itself: a paused audience keeps logging but the
// writer sends nothing, and the backlog goes out on the first flush after it
// resumes.
func (s *session) flush() {
	s.w.pump()
}

// retransmit asks the writer to replay the oldest unacked batch and the oldest
// unacked relay chunk. The writer owns the send path, so the session loop only
// schedules the request.
func (s *session) retransmit() {
	s.w.requestRetransmit()
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
