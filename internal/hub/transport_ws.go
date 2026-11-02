package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/AgentsHarness/capri-hub/wire"
)

// wsDialTimeout bounds one WebSocket dial (DNS + TCP + TLS + upgrade). 6s
// covers the first three SYN retransmits (1+2+4) plus a TLS handshake on a
// high-RTT path; a genuinely slow path simply retries.
const wsDialTimeout = 6 * time.Second

// wsWriteTimeout caps one uplink message before hello_ack supplied the
// session's own deadline (three quarters of idle). 15s is far above any live
// 32KiB unit write and short enough that a half-open peer cannot pin the
// writer.
const wsWriteTimeout = 15 * time.Second

// wsDownlinkLimit bounds one downlink message: either a text JSON frame or one
// transport unit, so it only has to cover the largest single frame a hub may
// send inline.
const wsDownlinkLimit = 16 << 20

// wsTransport adapts one /ws/host WebSocket. A text message is one complete
// JSON frame; a binary message is one wire transport unit. Compression is the
// session writer's job, and a binary unit carries its own flag, so the
// transport never looks at deflate.
type wsTransport struct {
	conn *websocket.Conn
	// wmu serializes writes: coder/websocket forbids concurrent ones. Only
	// the session writer writes after the handshake, but the handshake itself
	// goes through the same lock.
	wmu       sync.Mutex
	stop      chan struct{}
	closeOnce sync.Once

	writeTimeout atomic.Int64 // nanoseconds; 0 = wsWriteTimeout
	maxFrame     atomic.Int64
	lastActivity atomic.Int64 // unix nanoseconds of the last successful read
	// asm reassembles fragmented downlink frames. Recv is the only reader, so
	// no lock.
	asm wire.Reassembler
}

func newWSTransport(conn *websocket.Conn) *wsTransport {
	t := &wsTransport{conn: conn, stop: make(chan struct{})}
	t.lastActivity.Store(time.Now().UnixNano())
	return t
}

// Send writes one complete logical frame as a text message. The writer only
// calls it for uncompressed frames that fit one unit.
func (t *wsTransport) Send(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.unitTimeout())
	defer cancel()
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.conn.Write(ctx, websocket.MessageText, payload)
}

// SendFragment writes one encoded transport unit as a binary message.
func (t *wsTransport) SendFragment(unit []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.unitTimeout())
	defer cancel()
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.conn.Write(ctx, websocket.MessageBinary, unit)
}

// Recv returns the next complete frame, reassembling fragmented binary units.
func (t *wsTransport) Recv() ([]byte, error) {
	for {
		typ, data, err := t.conn.Read(context.Background())
		if err != nil {
			return nil, wrapWSClose(err)
		}
		t.lastActivity.Store(time.Now().UnixNano())
		if typ != websocket.MessageBinary {
			return data, nil
		}
		frame, compressed, complete, err := t.asm.Accept(data, int(t.maxFrame.Load()))
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

// SetWriteTimeout bounds one message write.
func (t *wsTransport) SetWriteTimeout(d time.Duration) { t.writeTimeout.Store(int64(d)) }

// SetMaxFrame bounds one reassembled downlink frame.
func (t *wsTransport) SetMaxFrame(n int) { t.maxFrame.Store(int64(n)) }

func (t *wsTransport) unitTimeout() time.Duration {
	if d := time.Duration(t.writeTimeout.Load()); d > 0 {
		return d
	}
	return wsWriteTimeout
}

// LastActivity is the last successful read, partial unit included.
func (t *wsTransport) LastActivity() time.Time {
	ns := t.lastActivity.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// SetKeepalive runs WS control pings at the negotiated cadence. The ping is
// not a protocol signal: it keeps NAT bindings alive and surfaces half-open
// sockets. Idle is enforced by the session's own received-frame clock.
func (t *wsTransport) SetKeepalive(d time.Duration) {
	if d <= 0 {
		d = time.Duration(wire.DefaultLiveness.Keepalive) * time.Second
	}
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-t.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), d)
				err := t.conn.Ping(ctx)
				cancel()
				if err != nil {
					return // the session's next read or write ends it
				}
			}
		}
	}()
}

// Close maps the session verdict onto the one WS close table.
func (t *wsTransport) Close(r wire.CloseReason) error {
	t.closeOnce.Do(func() { close(t.stop) })
	code, reason := r.WS()
	return t.conn.Close(websocket.StatusCode(code), reason)
}

// wrapWSClose turns the peer's close frame into the shared reason enum so the
// reconnect policy never has to look at a string.
func wrapWSClose(err error) error {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		if r, ok := wire.CloseReasonFromWS(int(ce.Code), ce.Reason); ok {
			return &peerClose{reason: r}
		}
	}
	return err
}

// wsSession dials the hub WebSocket and runs the session over it. The bearer
// token authenticates the upgrade; hello carries the epoch and the same token.
func (c *Client) wsSession(ctx context.Context, bridge bridgeSource) (wire.CloseReason, error) {
	tok, err := c.hostToken()
	if err != nil {
		return wire.CloseAuth, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, wsDialTimeout)
	defer cancel()
	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + tok}},
		// 与 c.httpc 同一份代理判定，但独立实例：库会把
		// HTTPClient.Timeout 当成拨号超时，复用 50s 分钟的 httpc 会让
		// wsDialTimeout 失效。
		HTTPClient: &http.Client{Transport: c.httpc.Transport},
	}
	conn, resp, err := websocket.Dial(dialCtx, c.wsURL(), opts)
	if err != nil {
		// Classify the handshake by its HTTP status, not by the text the
		// library happens to include: a 401 is a rejected credential (the
		// reconnect loop re-pairs), anything else is a transport problem.
		if resp != nil {
			return 0, &wsHandshakeError{status: resp.StatusCode, err: err}
		}
		return 0, fmt.Errorf("hub ws dial: %w", err)
	}
	conn.SetReadLimit(wsDownlinkLimit)

	log.Printf("[hub-client] 已连接 hub（ws）")
	c.noteConnected(TransportWS)
	tr := newWSTransport(conn)
	reason, err := c.runSession(ctx, bridge, tr)
	return reason, err
}
