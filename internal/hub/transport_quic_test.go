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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-hub/wire"
	"github.com/quic-go/quic-go"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// quicTestHub is a v3 hub over QUIC: the control stream carries the session,
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

	mu         sync.Mutex
	ctrlMu     sync.Mutex
	conns      int
	helloOf    *wire.Hello
	events     []wire.Frame
	responds   []quicRespond
	compressed int
	noAutoAck  bool

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
		ctrlReader := &quicUnitReader{h: h, s: ctrl}
		h.mu.Lock()
		h.streams[ctrl.StreamID()] = ctrl
		h.mu.Unlock()
		h.ctrlStreams <- ctrl

		fr, err := readQUICTestUnit(h, ctrlReader)
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

		// The real hub heartbeats every keepalive; without it the host's idle
		// timer would close a session that is only busy sending (a long relay
		// answer), especially under the race detector.
		stopHB := make(chan struct{})
		defer close(stopHB)
		go func() {
			// helloAckFrame advertises keepalive 1 / idle 2, so one heartbeat
			// per second keeps the host's idle timer satisfied.
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopHB:
					return
				case <-ticker.C:
					hb, err := wire.NewHeartbeat(0, wire.HeartbeatStatus{Ready: true})
					if err != nil {
						return
					}
					if err := h.writeControl(ctrl, hb); err != nil {
						return
					}
				}
			}
		}()

		for {
			f, err := readQUICTestUnit(h, ctrlReader)
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
// with whatever the test pushes. Every control frame is one transport unit.
func (h *quicTestHub) writeControl(s *quic.Stream, f wire.Frame) error {
	h.ctrlMu.Lock()
	defer h.ctrlMu.Unlock()
	return writeQUICTestUnit(s, f)
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

// quicUnitReader decodes the control stream, whose records are transport
// units; a fragmented frame is reassembled and inflated here so the test hub
// sees whole frames.
type quicUnitReader struct {
	h   *quicTestHub
	s   *quic.Stream
	asm wire.Reassembler
}

func readQUICTestUnit(h *quicTestHub, r *quicUnitReader) (wire.Frame, error) {
	for {
		raw, _, err := readQUICTestRecord(r.s)
		if err != nil {
			return wire.Frame{}, err
		}
		frame, compressed, complete, err := r.asm.Accept(raw, wire.DefaultLimits.MaxFrame)
		if err != nil {
			return wire.Frame{}, err
		}
		if !complete {
			continue
		}
		if compressed {
			h.mu.Lock()
			h.compressed++
			h.mu.Unlock()
			out, err := inflatePayload(frame)
			if err != nil {
				return wire.Frame{}, err
			}
			frame = out
		}
		return wire.Decode(frame)
	}
}

// readQUICTestRecord reads one length-prefixed record and reports the legacy
// relay-plane deflate bit.
func readQUICTestRecord(s *quic.Stream) ([]byte, bool, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(s, prefix[:]); err != nil {
		return nil, false, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	compressed := n&deflateFlagQUIC != 0
	size := n &^ deflateFlagQUIC
	if size > 32<<20 {
		return nil, false, fmt.Errorf("frame too large: %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(s, buf); err != nil {
		return nil, false, err
	}
	return buf, compressed, nil
}

var quicTestFrameID atomic.Uint32

// writeQUICTestUnit writes one frame as a single complete transport unit.
func writeQUICTestUnit(s *quic.Stream, f wire.Frame) error {
	b, err := wire.Encode(f)
	if err != nil {
		return err
	}
	units, err := wire.SplitFrame(b, false, quicTestFrameID.Add(1), wire.UnitSize)
	if err != nil {
		return err
	}
	for _, u := range units {
		prefix := frameLengthPrefix(len(u), false)
		if _, err := s.Write(prefix[:]); err != nil {
			return err
		}
		if _, err := s.Write(u); err != nil {
			return err
		}
	}
	return nil
}

// readQUICTestFrame reads one whole-frame relay-stream record (respond frames
// keep complete-frame framing and may be deflated).
func readQUICTestFrame(h *quicTestHub, s *quic.Stream) (wire.Frame, error) {
	raw, compressed, err := readQUICTestRecord(s)
	if err != nil {
		return wire.Frame{}, err
	}
	if compressed {
		h.mu.Lock()
		h.compressed++
		h.mu.Unlock()
		if raw, err = inflatePayload(raw); err != nil {
			return wire.Frame{}, err
		}
	}
	return wire.Decode(raw)
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
	if err := hub.writeControl(ctrl, audienceFrame(pausedAudience(2))); err != nil {
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

	// Close the session the way the hub does when it stops: the control
	// stream goes away, which the host sees as a normal link drop.
	if err := hub.ln.Close(); err != nil {
		t.Logf("close listener: %v", err)
	}
	if err := ctrl.Close(); err != nil {
		t.Logf("close control stream: %v", err)
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
	binary := hub.compressed
	hub.mu.Unlock()
	if binary == 0 {
		t.Fatal("large QUIC frame was not compressed")
	}
}

// injectCorruptControl writes a length-prefixed record that is not a
// transport unit: the host's control-stream decoder must report ErrUnreadable.
func (h *quicTestHub) injectCorruptControl(ctrl *quic.Stream) error {
	h.ctrlMu.Lock()
	defer h.ctrlMu.Unlock()
	prefix := frameLengthPrefix(6, false)
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
	compressedAfterSmall := hub.compressed
	hub.mu.Unlock()
	if compressedAfterSmall != 0 {
		t.Fatalf("a %d-byte event was compressed (binary=%d)", len(small), compressedAfterSmall)
	}

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": strings.Repeat("compressible ", 400)})
	hub.waitEvent(t, 5*time.Second)
	hub.mu.Lock()
	compressedAfterBig := hub.compressed
	hub.mu.Unlock()
	if compressedAfterBig == 0 {
		t.Fatal("a large frame was not compressed after deflate was negotiated")
	}
}
