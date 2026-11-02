package hub

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/AgentsHarness/capri-hub/wire"
)

// quicDialTimeout bounds the QUIC handshake before falling back to WebSocket.
// 3s was too tight for a lossy mobile / intercontinental path; WS fallback is
// still immediate on a real failure.
const quicDialTimeout = 8 * time.Second

// quicWriteTimeout and quicStreamOpenTimeout keep a stalled peer from pinning
// the writer or the relay plane forever. The write timeout is replaced by the
// session's own deadline (three quarters of idle) once hello_ack arrives.
const (
	quicWriteTimeout      = 30 * time.Second
	quicStreamOpenTimeout = 5 * time.Second
	quicDownlinkLimit     = 32 << 20
)

// quicKeepAlive is the QUIC transport keepalive. It is what refreshes a
// client-side NAT binding; the protocol's heartbeat is a separate signal.
const quicKeepAlive = 5 * time.Second

// quicTransport adapts one host QUIC connection. The control stream carries
// the session in both directions as transport units; request streams carry the
// host's `respond` frames only, still as length-prefixed complete frames, and
// the hub never writes on them.
type quicTransport struct {
	conn *quic.Conn
	ctrl *quic.Stream

	// deflate gates compression on the relay streams, the only plane that
	// still compresses in the transport. The control stream is the session
	// writer's job.
	deflate atomic.Bool
	// wmu serializes control-stream writes: the writer and the relay fallback
	// share it.
	wmu sync.Mutex

	writeTimeout atomic.Int64 // nanoseconds; 0 = quicWriteTimeout
	maxFrame     atomic.Int64
	lastActivity atomic.Int64 // unix nanoseconds of the last successful read
	// asm reassembles fragmented control-stream units. Recv is the only
	// reader, so no lock.
	asm wire.Reassembler

	mu       sync.Mutex
	streams  map[string]*quic.Stream
	stopOnce sync.Once
	stop     chan struct{}
}

func newQUICTransport(conn *quic.Conn, ctrl *quic.Stream) *quicTransport {
	t := &quicTransport{
		conn:    conn,
		ctrl:    ctrl,
		streams: make(map[string]*quic.Stream),
		stop:    make(chan struct{}),
	}
	t.lastActivity.Store(time.Now().UnixNano())
	return t
}

// SetDeflate gates relay-stream compression; the control stream carries the
// flag on each unit instead.
func (t *quicTransport) SetDeflate(v bool) { t.deflate.Store(v) }

// SetKeepalive is a no-op: QUIC's keepalive is a per-connection transport
// option (quicKeepAlive), not a per-stream one.
func (t *quicTransport) SetKeepalive(time.Duration) {}

// Send writes one complete logical frame as a single complete transport unit.
// A complete unit carries no frame id, so it can never collide with the open
// fragmented frame the writer may have on the same stream.
func (t *quicTransport) Send(payload []byte) error {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.writeRecord(t.ctrl, wire.EncodeUnit(wire.Unit{Total: uint32(len(payload)), Data: payload}))
}

// SendFragment writes one encoded transport unit.
func (t *quicTransport) SendFragment(unit []byte) error {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.writeRecord(t.ctrl, unit)
}

// Recv returns the next complete frame from the control stream, reassembling
// fragmented units and inflating a compressed frame.
func (t *quicTransport) Recv() ([]byte, error) {
	for {
		raw, err := readQUICRecord(t.ctrl)
		if err != nil {
			return nil, wrapQUICClose(err)
		}
		t.lastActivity.Store(time.Now().UnixNano())
		frame, compressed, complete, err := t.asm.Accept(raw, int(t.maxFrame.Load()))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", wire.ErrUnreadable, err)
		}
		if !complete {
			continue
		}
		if compressed {
			out, err := inflatePayload(frame)
			if err != nil {
				return nil, fmt.Errorf("%w: inflate: %v", wire.ErrUnreadable, err)
			}
			frame = out
		}
		return frame, nil
	}
}

// SetWriteTimeout bounds one unit write.
func (t *quicTransport) SetWriteTimeout(d time.Duration) { t.writeTimeout.Store(int64(d)) }

// SetMaxFrame bounds one reassembled downlink frame.
func (t *quicTransport) SetMaxFrame(n int) { t.maxFrame.Store(int64(n)) }

func (t *quicTransport) unitTimeout() time.Duration {
	if d := time.Duration(t.writeTimeout.Load()); d > 0 {
		return d
	}
	return quicWriteTimeout
}

// LastActivity is the last successful read, partial unit included.
func (t *quicTransport) LastActivity() time.Time {
	ns := t.lastActivity.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// SendRelay writes one respond frame on the request's own stream, opening and
// priming it on first use and closing it after the final frame. Relay streams
// keep the whole-frame framing: a respond frame is not unit-fragmented. A
// stream limit or open failure degrades to the control stream: correctness
// first, head-of-line separation best effort.
func (t *quicTransport) SendRelay(reqID string, payload []byte, final bool) error {
	s := t.streamFor(reqID)
	defer func() {
		if final {
			t.finish(reqID)
		}
	}()
	if s == nil {
		return t.Send(payload)
	}
	return t.writeRelayFrame(s, payload)
}

func (t *quicTransport) streamFor(reqID string) *quic.Stream {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.streams[reqID]; ok {
		return s
	}
	if len(t.streams) >= reqStreamMax {
		log.Printf("[hub-client] 在途请求流已达上限 %d，该请求响应走控制流", reqStreamMax)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), quicStreamOpenTimeout)
	defer cancel()
	s, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		log.Printf("[hub-client] 每请求流创建失败（%v），该请求响应走控制流", err)
		return nil
	}
	t.streams[reqID] = s
	return s
}

func (t *quicTransport) finish(reqID string) {
	t.mu.Lock()
	s := t.streams[reqID]
	delete(t.streams, reqID)
	t.mu.Unlock()
	if s != nil {
		_ = s.Close()
	}
}

// writeRecord writes one transport unit behind the 4-byte length prefix the
// control stream uses. The prefix only sizes the record now: compression is a
// unit flag.
func (t *quicTransport) writeRecord(s *quic.Stream, unit []byte) error {
	if err := s.SetWriteDeadline(time.Now().Add(t.unitTimeout())); err != nil {
		return err
	}
	defer s.SetWriteDeadline(time.Time{})
	prefix := frameLengthPrefix(len(unit), false)
	if _, err := s.Write(prefix[:]); err != nil {
		return err
	}
	_, err := s.Write(unit)
	return err
}

// writeRelayFrame writes one complete respond frame on a request stream,
// compressing it when deflate was negotiated and the gain is real.
func (t *quicTransport) writeRelayFrame(s *quic.Stream, payload []byte) error {
	if err := s.SetWriteDeadline(time.Now().Add(t.unitTimeout())); err != nil {
		return err
	}
	defer s.SetWriteDeadline(time.Time{})
	compressed := false
	if t.deflate.Load() {
		if zp, ok := deflatePayload(payload); ok {
			payload, compressed = zp, true
		}
	}
	prefix := frameLengthPrefix(len(payload), compressed)
	if _, err := s.Write(prefix[:]); err != nil {
		return err
	}
	_, err := s.Write(payload)
	return err
}

// Close maps the session verdict onto the shared QUIC application error code.
func (t *quicTransport) Close(r wire.CloseReason) error {
	t.stopOnce.Do(func() { close(t.stop) })
	t.mu.Lock()
	for id, s := range t.streams {
		_ = s.Close()
		delete(t.streams, id)
	}
	t.mu.Unlock()
	_ = t.ctrl.Close()
	return t.conn.CloseWithError(quic.ApplicationErrorCode(r.QUICCode()), r.String())
}

// wrapQUICClose turns the peer's application error code into the shared
// reason enum.
func wrapQUICClose(err error) error {
	var ae *quic.ApplicationError
	if errors.As(err, &ae) {
		if r, ok := wire.CloseReasonFromQUIC(uint64(ae.ErrorCode)); ok {
			return &peerClose{reason: r}
		}
	}
	return err
}

// readQUICRecord reads one length-prefixed control-stream record. The deflate
// bit in the prefix is a relay-plane legacy and is masked off: a control
// record is always a transport unit.
func readQUICRecord(s *quic.Stream) ([]byte, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(s, prefix[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	size := n &^ deflateFlagQUIC
	if size > quicDownlinkLimit {
		return nil, fmt.Errorf("%w: frame %d bytes", wire.ErrUnreadable, size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(s, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// reqStreamMax caps how many per-request QUIC streams may be open at once.
// Past the cap responds fall back to the control stream. A var so tests can
// shrink it.
var reqStreamMax = 64

// quicSession dials the hub over QUIC (UDP) and runs the session on one
// control stream. It reports whether the session was ESTABLISHED (dial and
// stream open succeeded — the session itself owns the outcome) so Run's
// negative cache can tell "UDP path broken" from "session ran and dropped".
func (c *Client) quicSession(ctx context.Context, bridge bridgeSource) (bool, wire.CloseReason, error) {
	if _, err := c.hostToken(); err != nil {
		return false, wire.CloseAuth, err
	}
	host, port := c.quicAddr()
	dialCtx, cancel := context.WithTimeout(ctx, quicDialTimeout)
	defer cancel()
	// 0-RTT is NOT enabled: quic-go's early data replays the transport's
	// 0-RTT write path wholesale, so a replayed hello/events frame could
	// reach the hub twice.
	dial := quic.DialAddr
	if c.quicDial != nil {
		// Test seam (T6 connection migration): run the session over a
		// caller-controlled UDP socket.
		dial = c.quicDial
	}
	conn, err := dial(dialCtx, net.JoinHostPort(host, port), c.quicTLSClientConfig(host),
		&quic.Config{KeepAlivePeriod: quicKeepAlive})
	if err != nil {
		return false, 0, fmt.Errorf("hub quic dial %s: %w", host, err)
	}

	sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
	ctrl, err := conn.OpenStreamSync(sctx)
	scancel()
	if err != nil {
		_ = conn.CloseWithError(quic.ApplicationErrorCode(wire.CloseNormal.QUICCode()), "no control stream")
		return false, 0, fmt.Errorf("hub quic open control stream: %w", err)
	}

	log.Printf("[hub-client] 已连接 hub（quic %s:%s）", host, port)
	c.noteConnected(TransportQUIC)
	tr := newQUICTransport(conn, ctrl)
	reason, err := c.runSession(ctx, bridge, tr)
	return true, reason, err
}
