package hub

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/quic-go/quic-go"
)

// ── migratable UDP socket ──────────────────────────────────────────────

// migPacket is one datagram read off either underlying socket.
type migPacket struct {
	data []byte
	addr net.Addr
}

// migratableConn wraps two UDP sockets and can switch which one the QUIC
// client sends from — simulating a NAT rebind / Wi-Fi↔cellular switch
// without tearing the connection down (QUIC connection migration, T6).
// Reads drain BOTH sockets, so packets the hub still sends to the old
// address during the switchover are not lost (a real client would lose
// them; tolerating them here only makes the test less flaky, the migration
// itself is what carries the traffic afterwards).
type migratableConn struct {
	t       *testing.T
	orig    *net.UDPConn // original local address
	rebound *net.UDPConn // post-migration local address

	mu       sync.Mutex
	current  *net.UDPConn
	packets  chan migPacket
	closed   chan struct{}
	closeOne sync.Once
}

func newMigratableConn(t *testing.T) *migratableConn {
	t.Helper()
	listen := func() *net.UDPConn {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatalf("listen udp: %v", err)
		}
		return c
	}
	m := &migratableConn{
		t:       t,
		orig:    listen(),
		rebound: listen(),
		packets: make(chan migPacket, 256),
		closed:  make(chan struct{}),
	}
	m.current = m.orig
	pump := func(c *net.UDPConn) {
		for {
			buf := make([]byte, 64<<10)
			n, addr, err := c.ReadFromUDP(buf)
			if err != nil {
				return // socket closed
			}
			select {
			case m.packets <- migPacket{data: buf[:n], addr: addr}:
			case <-m.closed:
				return
			}
		}
	}
	go pump(m.orig)
	go pump(m.rebound)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// migrate rebinds the client's source address: subsequent packets leave
// from the second socket (new local port), like a NAT rebinding or a
// Wi-Fi→cellular switch. The old socket keeps receiving.
func (m *migratableConn) migrate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = m.rebound
}

func (m *migratableConn) localAddr() net.Addr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.LocalAddr()
}

func (m *migratableConn) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case pkt := <-m.packets:
		n := copy(p, pkt.data)
		return n, pkt.addr, nil
	case <-m.closed:
		return 0, nil, net.ErrClosed
	}
}

func (m *migratableConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	m.mu.Lock()
	c := m.current
	m.mu.Unlock()
	return c.WriteToUDP(p, addr.(*net.UDPAddr))
}

func (m *migratableConn) LocalAddr() net.Addr { return m.localAddr() }

func (m *migratableConn) Close() error {
	m.closeOne.Do(func() {
		close(m.closed)
		_ = m.orig.Close()
		_ = m.rebound.Close()
	})
	return nil
}

// Best-effort deadlines: the QUIC transport only uses them to unblock
// reads on shutdown, which the closed channel already handles.
func (m *migratableConn) SetDeadline(time.Time) error      { return nil }
func (m *migratableConn) SetReadDeadline(time.Time) error  { return nil }
func (m *migratableConn) SetWriteDeadline(time.Time) error { return nil }

// ── test ──────────────────────────────────────────────────────────────

// TestQUICConnectionMigration (T6) verifies that the host↔hub QUIC
// connection survives a client address change (NAT rebind / network
// switch): after the local UDP socket rebinds to a new port, events still
// flow host→hub on the same stream and hub→host subscriber frames still
// take effect — no reconnect, no stream reset.
func TestQUICConnectionMigration(t *testing.T) {
	th := newTestQUICHub(t)
	ctrlCh := make(chan *quic.Stream, 1)
	th.serve(func(ctrl *quic.Stream) { ctrlCh <- ctrl })

	mconn := newMigratableConn(t)

	c := NewClient(Config{
		URL:          fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:       "h1",
		Token:        "tok123",
		LocalBase:    "http://127.0.0.1:1", // unused: no relay requests pushed
		QUICInsecure: true,
	})
	c.sendCh = make(chan []byte, 64)
	c.reqCh = make(chan reqFrame, 16)
	c.quicDial = func(ctx context.Context, addr string, tlsConf *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		tr := &quic.Transport{Conn: mconn}
		return tr.Dial(ctx, raddr, tlsConf, cfg)
	}
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)
	done := make(chan error, 1)
	go func() { _, err := c.quicSession(ctx, bridge); done <- err }()
	sessionAlive := func() bool {
		select {
		case err := <-done:
			t.Logf("session ended early: %v", err)
			return false
		default:
			return true
		}
	}

	// Stage 1 — pre-migration: session up, events flow.
	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled over QUIC")
		case <-time.After(10 * time.Millisecond):
		}
	}
	waitEvent := func(text string) {
		t.Helper()
		for {
			select {
			case ev := <-th.eventsCh:
				if evText, _ := ev["text"].(string); evText == text {
					return
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("event %q never reached the hub (session alive: %v)", text, sessionAlive())
			}
		}
	}
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "before-migration", "sessionId": "s1"})
	waitEvent("before-migration")

	// Stage 2 — rebind the local socket (NAT/Wi-Fi switch).
	before := mconn.localAddr().String()
	mconn.migrate()
	after := mconn.localAddr().String()
	if before == after {
		t.Fatalf("migration did not change the local address: %s", after)
	}

	// Stage 3 — uplink after migration: same connection, same control
	// stream (id 0), new source port.
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "after-migration", "sessionId": "s1"})
	waitEvent("after-migration")
	ids := th.acceptedIDs()
	if len(ids) == 0 || ids[0] != 0 {
		t.Fatalf("control stream not stream 0: %v", ids)
	}
	if !sessionAlive() {
		t.Fatal("session died across the address change — connection migration failed")
	}

	// Stage 4 — downlink after migration: a subscriber count pushed by the
	// hub must still steer the host (pause, then resume), proving
	// hub→host traffic follows the migrated path too.
	var ctrl *quic.Stream
	select {
	case ctrl = <-ctrlCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no control stream captured")
	}
	if err := writeTestFrame(ctrl, map[string]any{"v": 1, "type": "subscribers", "count": 0, "gen": 1}); err != nil {
		t.Fatalf("write subscribers: %v", err)
	}
	paused := time.After(5 * time.Second)
	for c.forwardingEnabled() {
		select {
		case <-paused:
			t.Fatal("subscribers:0 after migration never paused forwarding — downlink broken")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := writeTestFrame(ctrl, map[string]any{"v": 1, "type": "subscribers", "count": 1, "gen": 2}); err != nil {
		t.Fatalf("write subscribers: %v", err)
	}
	resumed := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-resumed:
			t.Fatal("subscribers:1 after migration never resumed forwarding — downlink broken")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Stage 5 — events still flow in both directions after all that.
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "post-resume", "sessionId": "s1"})
	waitEvent("post-resume")
	if !sessionAlive() {
		t.Fatal("session died after migration + downlink round trips")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not shut down after ctx cancel")
	}
}
