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

// wsWriteTimeout caps one uplink write so a half-open peer cannot pin the
// session loop.
const wsWriteTimeout = 30 * time.Second

// wsDownlinkLimit bounds one downlink message. A relayed request carries a
// body the hub caps well below this; 16MB is all the headroom needed.
const wsDownlinkLimit = 16 << 20

// wsTransport adapts one /ws/host WebSocket. A text message is one plain JSON
// frame; a binary message is one raw-deflate stream of an uplink frame, allowed
// only after hello_ack echoed hello.caps.deflate.
type wsTransport struct {
	conn *websocket.Conn
	// deflate is set once the capability was negotiated; the handshake frame
	// is always text.
	deflate atomic.Bool
	// wmu serializes writes: coder/websocket forbids concurrent ones, and the
	// groomer (session loop) and the relay plane both send.
	wmu       sync.Mutex
	stop      chan struct{}
	closeOnce sync.Once
}

func newWSTransport(conn *websocket.Conn) *wsTransport {
	return &wsTransport{conn: conn, stop: make(chan struct{})}
}

func (t *wsTransport) Send(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), wsWriteTimeout)
	defer cancel()
	if t.deflate.Load() {
		if zp, ok := deflatePayload(payload); ok {
			t.wmu.Lock()
			defer t.wmu.Unlock()
			return t.conn.Write(ctx, websocket.MessageBinary, zp)
		}
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.conn.Write(ctx, websocket.MessageText, payload)
}

func (t *wsTransport) Recv() ([]byte, error) {
	typ, data, err := t.conn.Read(context.Background())
	if err != nil {
		return nil, wrapWSClose(err)
	}
	if typ == websocket.MessageBinary {
		// The hub never compresses its downlink; a binary message is a peer
		// speaking something else.
		return nil, fmt.Errorf("%w: binary downlink frame", wire.ErrUnreadable)
	}
	return data, nil
}

func (t *wsTransport) SetDeflate(v bool) { t.deflate.Store(v) }

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
