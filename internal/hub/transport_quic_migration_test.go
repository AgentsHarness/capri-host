package hub

import (
	"context"
	"crypto/tls"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/quic-go/quic-go"
	"net"
	"sync"
	"testing"
	"time"
)

// ── connection migration (T6) ─────────────────────────────────────────

// migratableConn wraps two UDP sockets and can switch which one the QUIC
// client sends from — simulating a NAT rebind / Wi-Fi↔cellular switch without
// tearing the connection down.
type migratableConn struct {
	t       *testing.T
	orig    *net.UDPConn
	rebound *net.UDPConn

	mu       sync.Mutex
	current  *net.UDPConn
	packets  chan migPacket
	closed   chan struct{}
	closeOne sync.Once
}

type migPacket struct {
	data []byte
	addr net.Addr
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
				return
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

func (m *migratableConn) migrate() {
	m.mu.Lock()
	m.current = m.rebound
	m.mu.Unlock()
}

func (m *migratableConn) LocalAddr() net.Addr {
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

func (m *migratableConn) Close() error {
	m.closeOne.Do(func() {
		close(m.closed)
		_ = m.orig.Close()
		_ = m.rebound.Close()
	})
	return nil
}

func (m *migratableConn) SetDeadline(time.Time) error      { return nil }
func (m *migratableConn) SetReadDeadline(time.Time) error  { return nil }
func (m *migratableConn) SetWriteDeadline(time.Time) error { return nil }

// TestQUICConnectionMigration: after the local UDP socket rebinds, the same
// session keeps carrying events in both directions.
func TestQUICConnectionMigration(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1, "s1"))
	hub.serve()
	mconn := newMigratableConn(t)
	c := newQUICClient(t, hub)
	c.quicDial = func(ctx context.Context, addr string, tlsConf *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		tr := &quic.Transport{Conn: mconn}
		return tr.Dial(ctx, raddr, tlsConf, cfg)
	}
	bridge := newTestBridge()
	runForwardLoop(t, c, bridge)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := make(chan sessionResult, 1)
	go func() {
		_, reason, err := c.quicSession(ctx, bridge)
		res <- sessionResult{reason: reason, err: err}
	}()
	ctrl := hub.control(t)
	_ = ctrl

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "before-migration"})
	hub.waitEvent(t, 5*time.Second)

	before := mconn.LocalAddr().String()
	mconn.migrate()
	if mconn.LocalAddr().String() == before {
		t.Fatal("migration did not change the local address")
	}

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "after-migration"})
	hub.waitEvent(t, 5*time.Second)

	// Downlink still steers the host on the migrated path.
	if err := hub.writeControl(ctrl, audienceFrame(pausedAudience(2))); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !c.out.isPaused() {
		if time.Now().After(deadline) {
			t.Fatal("downlink broke across the address change")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case r := <-res:
		t.Fatalf("session died across the address change: %v / %v", r.reason, r.err)
	default:
	}
	cancel()
}
