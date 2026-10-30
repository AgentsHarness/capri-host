package hub

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/AgentsHarness/capri-host/internal/acp"

	"github.com/AgentsHarness/capri-wire"
)

// quicTestHub is a v2 hub over QUIC: the control stream carries the session,
// every further stream is a per-request respond plane.
type quicTestHub struct {
	t       *testing.T
	ln      *quic.Listener
	certDER []byte
	aud     wire.Audience

	eventsCh    chan wire.Frame
	respondCh   chan quicRespond
	errCh       chan error
	ctrlStreams chan *quic.Stream
	readyCh     chan struct{}

	mu        sync.Mutex
	ctrlMu    sync.Mutex
	conns     int
	helloOf   *wire.Hello
	events    []wire.Frame
	responds  []quicRespond
	binary    int
	noAutoAck bool

	streams map[quic.StreamID]*quic.Stream
}

type quicRespond struct {
	frame  wire.Frame
	stream quic.StreamID
}

func newQUICTestHub(t *testing.T, aud wire.Audience) *quicTestHub {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-hub"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("gen cert: %v", err)
	}
	qtls := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{"capri-hub"},
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", qtls, &quic.Config{})
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return &quicTestHub{
		t:           t,
		ln:          ln,
		certDER:     der,
		aud:         aud,
		eventsCh:    make(chan wire.Frame, 32),
		respondCh:   make(chan quicRespond, 32),
		errCh:       make(chan error, 4),
		ctrlStreams: make(chan *quic.Stream, 4),
		readyCh:     make(chan struct{}),
		streams:     make(map[quic.StreamID]*quic.Stream),
	}
}

func (h *quicTestHub) spkiPin() [32]byte {
	h.t.Helper()
	cert, err := x509.ParseCertificate(h.certDER)
	if err != nil {
		h.t.Fatalf("parse test cert: %v", err)
	}
	return sha256.Sum256(cert.RawSubjectPublicKeyInfo)
}

func (h *quicTestHub) port() int { return h.ln.Addr().(*net.UDPAddr).Port }

func (h *quicTestHub) url() string {
	return fmt.Sprintf("https://127.0.0.1:%d", h.port())
}

// serve accepts one connection and runs its control stream.
func (h *quicTestHub) serve() {
	go func() {
		conn, err := h.ln.Accept(context.Background())
		if err != nil {
			h.errCh <- err
			return
		}
		h.mu.Lock()
		h.conns++
		h.mu.Unlock()

		ctrl, err := conn.AcceptStream(context.Background())
		if err != nil {
			h.errCh <- err
			return
		}
		if ctrl.StreamID() != 0 {
			h.errCh <- fmt.Errorf("first stream id = %d, want 0", ctrl.StreamID())
			return
		}
		h.mu.Lock()
		h.streams[ctrl.StreamID()] = ctrl
		h.mu.Unlock()
		h.ctrlStreams <- ctrl

		fr, err := readQUICTestFrame(h, ctrl)
		if err != nil {
			h.errCh <- err
			return
		}
		hello, err := fr.Hello()
		if err != nil {
			h.errCh <- err
			return
		}
		h.mu.Lock()
		h.helloOf = &hello
		aud := h.aud
		h.mu.Unlock()
		ack := helloAckFrame(hello.Epoch, 0, 0, aud)
		if err := h.writeControl(ctrl, ack); err != nil {
			h.errCh <- err
			return
		}
		close(h.readyCh)

		go h.acceptRequestStreams(conn)

		for {
			f, err := readQUICTestFrame(h, ctrl)
			if err != nil {
				return
			}
			switch f.Type {
			case wire.TypeEvents:
				last := f.Seq + uint64(len(f.Events)) - 1
				h.mu.Lock()
				h.events = append(h.events, f)
				noAck := h.noAutoAck
				h.mu.Unlock()
				h.eventsCh <- f
				if !noAck {
					hb, _ := wire.NewHeartbeat(last, wire.HeartbeatStatus{Ready: true})
					_ = h.writeControl(ctrl, hb)
				}
			case wire.TypeHeartbeat:
				// nothing to do; the ack is relay-plane bookkeeping here
			case wire.TypeRespond:
				h.recordRespond(quicRespond{frame: f, stream: ctrl.StreamID()})
			}
		}
	}()
}

func (h *quicTestHub) acceptRequestStreams(conn *quic.Conn) {
	for {
		s, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		h.mu.Lock()
		h.streams[s.StreamID()] = s
		h.mu.Unlock()
		go func(s *quic.Stream) {
			for {
				f, err := readQUICTestFrame(h, s)
				if err != nil {
					return
				}
				if f.Type == wire.TypeRespond {
					h.recordRespond(quicRespond{frame: f, stream: s.StreamID()})
				}
			}
		}(s)
	}
}

// writeControl serializes control-stream writes: quic-go streams are not
// safe for concurrent writers, and the session's heartbeats share the stream
// with whatever the test pushes.
func (h *quicTestHub) writeControl(s *quic.Stream, f wire.Frame) error {
	h.ctrlMu.Lock()
	defer h.ctrlMu.Unlock()
	return writeQUICTestFrame(s, f, false)
}

func (h *quicTestHub) recordRespond(r quicRespond) {
	h.mu.Lock()
	h.responds = append(h.responds, r)
	h.mu.Unlock()
	h.respondCh <- r
}

func (h *quicTestHub) control(t *testing.T) *quic.Stream {
	t.Helper()
	select {
	case s := <-h.ctrlStreams:
		select {
		case <-h.readyCh:
		case <-time.After(5 * time.Second):
			t.Fatal("handshake never completed")
		}
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("host never opened the control stream")
	}
	return nil
}

func (h *quicTestHub) waitEvent(t *testing.T, timeout time.Duration) wire.Frame {
	t.Helper()
	select {
	case f := <-h.eventsCh:
		return f
	case <-time.After(timeout):
		t.Fatal("no event frame reached the hub")
	}
	return wire.Frame{}
}

func (h *quicTestHub) waitRespond(t *testing.T, timeout time.Duration) quicRespond {
	t.Helper()
	select {
	case r := <-h.respondCh:
		return r
	case <-time.After(timeout):
		t.Fatal("no respond reached the hub")
	}
	return quicRespond{}
}

func readQUICTestFrame(h *quicTestHub, s *quic.Stream) (wire.Frame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(s, prefix[:]); err != nil {
		return wire.Frame{}, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	compressed := n&deflateFlagQUIC != 0
	size := n &^ deflateFlagQUIC
	if size > 32<<20 {
		return wire.Frame{}, fmt.Errorf("frame too large: %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(s, buf); err != nil {
		return wire.Frame{}, err
	}
	if compressed {
		h.mu.Lock()
		h.binary++
		h.mu.Unlock()
		out, err := inflatePayload(buf)
		if err != nil {
			return wire.Frame{}, err
		}
		buf = out
	}
	return wire.Decode(buf)
}

func writeQUICTestFrame(s *quic.Stream, f wire.Frame, compress bool) error {
	b, err := wire.Encode(f)
	if err != nil {
		return err
	}
	if compress {
		if zp, ok := deflatePayload(b); ok {
			b = zp
		} else {
			compress = false
		}
	}
	prefix := frameLengthPrefix(len(b), compress)
	if _, err := s.Write(prefix[:]); err != nil {
		return err
	}
	_, err = s.Write(b)
	return err
}

func newQUICClient(t *testing.T, hub *quicTestHub) *Client {
	t.Helper()
	return NewClient(Config{
		URL: hub.url(), HostID: "h1", HostName: "H1", Token: "tok123",
		QUICInsecure: true, LocalBase: "http://127.0.0.1:1",
	})
}

// TestQUICSessionRoundTrip: hello on the control stream, events up, audience
// down.
func TestQUICSessionRoundTrip(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1, "s1"))
	hub.serve()
	c := newQUICClient(t, hub)
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
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "over-quic"})
	fr := hub.waitEvent(t, 5*time.Second)
	if fr.Seq != 1 {
		t.Fatalf("first events frame seq = %d", fr.Seq)
	}

	hub.mu.Lock()
	hello := hub.helloOf
	hub.mu.Unlock()
	if hello == nil || hello.Epoch == 0 || hello.Token != "tok123" {
		t.Fatalf("hello = %+v", hello)
	}

	// Downlink: an audience snapshot pauses and resumes the uplink.
	if err := hub.writeControl(ctrl, audienceFrame(1, 0, pausedAudience(2))); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !c.out.isPaused() {
		if time.Now().After(deadline) {
			t.Fatal("audience over QUIC never paused the uplink")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case r := <-res:
		if r.reason.Valid() && r.reason != wire.CloseNormal {
			t.Fatalf("close reason = %s", r.reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end on cancel")
	}
}

// TestQUICPerRequestStreams: each relay answer rides its own stream so a large
// body cannot head-of-line block the control plane.
func TestQUICPerRequestStreams(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1))
	hub.serve()
	c := newQUICClient(t, hub)
	c.local = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _, _ = c.quicSession(ctx, newTestBridge()) }()

	ctrl := hub.control(t)
	for i, reqID := range []string{"51", "52"} {
		if err := hub.writeControl(ctrl, requestFrame(uint64(i+1), 0, reqID, "POST", "/api/x", `{}`)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[quic.StreamID]bool{}
	for i := 0; i < 2; i++ {
		r := hub.waitRespond(t, 5*time.Second)
		if r.stream == 0 {
			t.Fatalf("respond for %s rode the control stream", r.frame.ReqID)
		}
		seen[r.stream] = true
	}
	if len(seen) != 2 {
		t.Fatalf("both answers shared one stream: %v", seen)
	}
}

// TestQUICCloseMapping: the hub's application error code becomes the reconnect
// policy's verdict.
func TestQUICCloseMapping(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1))
	hub.serve()
	c := newQUICClient(t, hub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := make(chan sessionResult, 1)
	go func() {
		_, reason, err := c.quicSession(ctx, newTestBridge())
		res <- sessionResult{reason: reason, err: err}
	}()
	ctrl := hub.control(t)
	_ = ctrl

	// Close the connection the way the hub does for a superseded host.
	if err := hub.ln.Close(); err != nil {
		t.Logf("close listener: %v", err)
	}
	select {
	case r := <-res:
		if r.err == nil {
			t.Log("session ended without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end when the hub went away")
	}

	// The mapping itself is the wire table's.
	if got, ok := wire.CloseReasonFromQUIC(wire.CloseAuth.QUICCode()); !ok || got != wire.CloseAuth {
		t.Fatalf("auth code did not map back to auth: %v %v", got, ok)
	}
}

// TestQUICCompressedUplink: negotiated deflate flags the length prefix.
func TestQUICCompressedUplink(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1, "s1"))
	hub.serve()
	c := newQUICClient(t, hub)
	bridge := newTestBridge()
	runForwardLoop(t, c, bridge)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _, _ = c.quicSession(ctx, bridge) }()
	hub.control(t)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": strings.Repeat("quic-compress ", 200)})
	hub.waitEvent(t, 5*time.Second)
	hub.mu.Lock()
	binary := hub.binary
	hub.mu.Unlock()
	if binary == 0 {
		t.Fatal("large QUIC frame was not compressed")
	}
}

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
	if err := hub.writeControl(ctrl, audienceFrame(1, 0, pausedAudience(2))); err != nil {
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

// ── SPKI pinning ──────────────────────────────────────────────────────

func TestParseSPKIPin(t *testing.T) {
	sum := sha256.Sum256([]byte("spki-pin-test-vector"))
	hexPin := hex.EncodeToString(sum[:])
	b64Pin := base64.StdEncoding.EncodeToString(sum[:])
	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "hex", in: hexPin},
		{name: "hex uppercase", in: strings.ToUpper(hexPin)},
		{name: "hex with sha256/ prefix", in: "sha256/" + hexPin},
		{name: "base64 padded", in: b64Pin},
		{name: "base64 with sha256// prefix", in: "sha256//" + b64Pin},
		{name: "surrounding whitespace", in: "  " + hexPin + "\n"},
		{name: "empty", in: "", wantErr: true},
		{name: "garbage", in: "!!", wantErr: true},
		{name: "hex too short", in: hexPin[:62], wantErr: true},
		{name: "prefix alone", in: "sha256//", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSPKIPin(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parse(%q) = %x, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse(%q): %v", tc.in, err)
			}
			if got != sum {
				t.Fatalf("parse(%q) = %x, want %x", tc.in, got, sum)
			}
		})
	}
}

func TestQUICTLSPinPolicy(t *testing.T) {
	pin := hex.EncodeToString(make([]byte, 32))

	t.Run("pin replaces CA verification", func(t *testing.T) {
		c := NewClient(Config{URL: "https://hub.example.com", QUICPin: pin})
		conf := c.quicTLSClientConfig("203.0.113.10")
		if !conf.InsecureSkipVerify {
			t.Error("InsecureSkipVerify = false; the pin path must skip the system CA chain")
		}
		if conf.VerifyPeerCertificate == nil {
			t.Fatal("VerifyPeerCertificate = nil; the pin must be enforced in its place")
		}
		if conf.ServerName != "" {
			t.Errorf("ServerName = %q; hostname verification is meaningless under a pin", conf.ServerName)
		}
		if len(conf.NextProtos) != 1 || conf.NextProtos[0] != "capri-hub" {
			t.Errorf("NextProtos = %v, want [capri-hub]", conf.NextProtos)
		}
	})
	t.Run("pin wins over insecure and http policy", func(t *testing.T) {
		for _, url := range []string{"http://10.0.0.5:8787", "https://hub.example.com"} {
			c := NewClient(Config{URL: url, QUICInsecure: true, QUICPin: pin})
			conf := c.quicTLSClientConfig("h")
			if conf.VerifyPeerCertificate == nil {
				t.Errorf("URL %s: pin not enforced when QUICInsecure also set", url)
			}
		}
	})
	t.Run("no pin keeps the CA policy", func(t *testing.T) {
		c := NewClient(Config{URL: "https://hub.example.com"})
		conf := c.quicTLSClientConfig("203.0.113.10")
		if conf.VerifyPeerCertificate != nil {
			t.Error("VerifyPeerCertificate set without a pin")
		}
		if conf.InsecureSkipVerify {
			t.Error("InsecureSkipVerify set without a pin over https")
		}
	})
	t.Run("malformed pin fails closed", func(t *testing.T) {
		c := NewClient(Config{URL: "https://hub.example.com", QUICPin: "definitely-not-a-fingerprint"})
		conf := c.quicTLSClientConfig("hub.example.com")
		if conf.VerifyPeerCertificate == nil {
			t.Fatal("malformed pin produced no verifier — verification would be silently disabled")
		}
		if err := conf.VerifyPeerCertificate(nil, nil); err == nil {
			t.Fatal("malformed pin verifier accepted a handshake; must always fail")
		}
	})
}

// TestQUICTLSPinHandshake: the matching fingerprint connects, a different one
// fails the handshake itself.
func TestQUICTLSPinHandshake(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1))
	hub.serve()
	pin := hub.spkiPin()
	local := httptest.NewServer(nil)
	defer local.Close()

	c := NewClient(Config{
		URL: hub.url(), HostID: "h1", Token: "tok123", LocalBase: local.URL,
		QUICPin: hex.EncodeToString(pin[:]),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := c.quicSession(ctx, newTestBridge())
		done <- err
	}()
	hub.control(t)
	cancel()
	<-done

	bad := pin
	bad[0] ^= 0xFF
	c2 := NewClient(Config{
		URL: hub.url(), HostID: "h1", Token: "tok123", LocalBase: local.URL,
		QUICPin: hex.EncodeToString(bad[:]),
	})
	bctx, bcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer bcancel()
	if _, _, err := c2.quicSession(bctx, newTestBridge()); err == nil {
		t.Fatal("session established against a mismatched SPKI pin")
	} else if !strings.Contains(err.Error(), "SPKI") {
		t.Errorf("error does not mention the pin mismatch: %v", err)
	}
}

func TestPinSPKIVerifyUnits(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1))
	pin := hub.spkiPin()
	verify := pinSPKIVerify(pin)

	if err := verify([][]byte{hub.certDER}, nil); err != nil {
		t.Fatalf("matching SPKI rejected: %v", err)
	}
	other := newQUICTestHub(t, watchingAudience(1))
	if err := verify([][]byte{other.certDER}, nil); err == nil {
		t.Fatal("different certificate SPKI accepted")
	}
	if err := verify(nil, nil); err == nil {
		t.Fatal("empty certificate chain accepted")
	}
}

// injectCorruptControl writes a length prefix flagged as deflate followed by
// bytes that are not a deflate stream: the host's control-stream decoder must
// report ErrUnreadable.
func (h *quicTestHub) injectCorruptControl(ctrl *quic.Stream) error {
	h.ctrlMu.Lock()
	defer h.ctrlMu.Unlock()
	prefix := frameLengthPrefix(6, true)
	if _, err := ctrl.Write(prefix[:]); err != nil {
		return err
	}
	_, err := ctrl.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	return err
}

// TestQUICCorruptDownlinkClosesUnreadable: a corrupt deflate stream from the
// hub ends the host session with `unreadable` (so the reconnect replays from
// the hub's watermark), never as an ordinary disconnect.
func TestQUICCorruptDownlinkClosesUnreadable(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1, "s1"))
	hub.serve()
	c := newQUICClient(t, hub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := make(chan sessionResult, 1)
	go func() {
		_, reason, err := c.quicSession(ctx, newTestBridge())
		res <- sessionResult{reason: reason, err: err}
	}()
	ctrl := hub.control(t)
	if err := hub.injectCorruptControl(ctrl); err != nil {
		t.Fatalf("inject: %v", err)
	}

	select {
	case r := <-res:
		if r.reason != wire.CloseUnreadable {
			t.Fatalf("corrupt downlink close = %s, want unreadable", r.reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end on the corrupt frame")
	}
}

// ── compression: bare and compressed uplinks ──────────────────────────

// TestQUICBareAndCompressedFrames: a small frame stays bare (no flag bit, no
// deflate), a large one is flagged; both decode to the same bytes at the hub.
func TestQUICBareAndCompressedFrames(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1, "s1"))
	hub.serve()
	c := newQUICClient(t, hub)
	bridge := newTestBridge()
	runForwardLoop(t, c, bridge)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _, _ = c.quicSession(ctx, bridge) }()
	hub.control(t)

	small := "tiny"
	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": small})
	first := hub.waitEvent(t, 5*time.Second)
	var ev struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(first.Events[0], &ev); err != nil || ev.Text != small {
		t.Fatalf("bare frame event = %s (%v)", first.Events[0], err)
	}
	hub.mu.Lock()
	compressedAfterSmall := hub.binary
	hub.mu.Unlock()
	if compressedAfterSmall != 0 {
		t.Fatalf("a %d-byte event was compressed (binary=%d)", len(small), compressedAfterSmall)
	}

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": strings.Repeat("compressible ", 400)})
	hub.waitEvent(t, 5*time.Second)
	hub.mu.Lock()
	compressedAfterBig := hub.binary
	hub.mu.Unlock()
	if compressedAfterBig == 0 {
		t.Fatal("a large frame was not compressed after deflate was negotiated")
	}
}
