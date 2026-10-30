package hub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/coder/websocket"

	"github.com/AgentsHarness/capri-wire"
)

// ── fake transport ────────────────────────────────────────────────────

// fakeTransport is one in-memory link: frames the host writes arrive on sent,
// frames pushed on in are what the host reads.
type fakeTransport struct {
	in   chan []byte
	sent chan []byte

	closeOnce sync.Once
	closed    chan struct{}

	mu          sync.Mutex
	deflate     bool
	keepalive   time.Duration
	keepaliveCh chan time.Duration
	// recvErr delivers an injected Recv error, the way a transport reports a
	// frame it could not decode. A channel so a Recv already parked on the
	// input wakes up.
	recvErr chan error
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		in:          make(chan []byte, 64),
		sent:        make(chan []byte, 64),
		closed:      make(chan struct{}),
		keepaliveCh: make(chan time.Duration, 4),
		recvErr:     make(chan error, 1),
	}
}

func (f *fakeTransport) Send(payload []byte) error {
	select {
	case f.sent <- payload:
		return nil
	case <-f.closed:
		return io.EOF
	}
}

func (f *fakeTransport) Recv() ([]byte, error) {
	select {
	case err := <-f.recvErr:
		return nil, err
	case b := <-f.in:
		return b, nil
	case <-f.closed:
		return nil, io.EOF
	}
}

// failRecv makes the next Recv return err, the way a transport reports a
// frame it could not decode (a corrupt deflate stream, a wrong framing).
func (f *fakeTransport) failRecv(err error) {
	f.recvErr <- err
}

func (f *fakeTransport) Close(wire.CloseReason) error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeTransport) SetKeepalive(d time.Duration) {
	f.mu.Lock()
	f.keepalive = d
	f.mu.Unlock()
	select {
	case f.keepaliveCh <- d:
	default:
	}
}

func (f *fakeTransport) SetDeflate(v bool) {
	f.mu.Lock()
	f.deflate = v
	f.mu.Unlock()
}

// next reads one frame the host wrote.
func (f *fakeTransport) next(t *testing.T, timeout time.Duration) wire.Frame {
	t.Helper()
	select {
	case b := <-f.sent:
		fr, err := wire.Decode(b)
		if err != nil {
			t.Fatalf("host wrote an invalid frame: %v (%s)", err, b)
		}
		return fr
	case <-time.After(timeout):
		t.Fatal("host wrote no frame in time")
	}
	return wire.Frame{}
}

// nextOfType skips heartbeats until a frame of the wanted type arrives.
func (f *fakeTransport) nextOfType(t *testing.T, typ string, timeout time.Duration) wire.Frame {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatalf("host never wrote a %q frame", typ)
		}
		if fr := f.next(t, left); fr.Type == typ {
			return fr
		}
	}
}

// push delivers one frame to the host.
func (f *fakeTransport) push(t *testing.T, fr wire.Frame) {
	t.Helper()
	b, err := wire.Encode(fr)
	if err != nil {
		t.Fatalf("encode %s: %v", fr.Type, err)
	}
	select {
	case f.in <- b:
	case <-time.After(2 * time.Second):
		t.Fatalf("host is not reading (%s)", fr.Type)
	}
}

// ── fake bridge ───────────────────────────────────────────────────────

// testBridge is the hub client's view of *acp.Bridge.
type testBridge struct {
	mu     sync.Mutex
	ch     chan acp.Event
	status acp.Status
	seq    uint64
}

func newTestBridge() *testBridge {
	return &testBridge{ch: make(chan acp.Event, 64)}
}

func (b *testBridge) Subscribe() (chan acp.Event, func()) {
	return b.ch, func() {}
}

func (b *testBridge) Snapshot() acp.Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.status
}

func (b *testBridge) LastSeq() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seq
}

func (b *testBridge) setStatus(st acp.Status) {
	b.mu.Lock()
	b.status = st
	b.mu.Unlock()
}

// setSeq moves the bus high-water mark without publishing an event (the
// history a new epoch declares as its baseline).
func (b *testBridge) setSeq(n uint64) {
	b.mu.Lock()
	if n > b.seq {
		b.seq = n
	}
	b.mu.Unlock()
}

// push publishes one event with the next bus seq.
func (b *testBridge) push(ev acp.Event) uint64 {
	b.mu.Lock()
	b.seq++
	seq := b.seq
	b.mu.Unlock()
	ev["seq"] = seq
	b.ch <- ev
	return seq
}

// startTestServer serves h on a loopback port and returns its base URL.
func startTestServer(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// ── frame helpers ─────────────────────────────────────────────────────

// testLimits keeps frames small so budget tests stay readable.
func testLimits() *wire.Limits {
	l := wire.DefaultLimits
	return &l
}

// testHubBoot is the hub incarnation every harness hello_ack names.
const testHubBoot = 7

func helloAckFrame(epoch, ackUp, ackDown uint64, aud wire.Audience) wire.Frame {
	return helloAckFrameLiveness(epoch, ackUp, ackDown, aud, 1, 2)
}

// helloAckFrameLiveness is helloAckFrame with an explicit time scale: tests
// that wait out an RTO need an idle budget above it.
func helloAckFrameLiveness(epoch, ackUp, ackDown uint64, aud wire.Audience, keepalive, idle int) wire.Frame {
	return wire.NewHelloAck(wire.HelloAck{
		Epoch:    epoch,
		HubBoot:  testHubBoot,
		AckUp:    ackUp,
		AckDown:  ackDown,
		Audience: &aud,
		Liveness: &wire.Liveness{Keepalive: keepalive, Idle: idle},
		Limits:   testLimits(),
		Caps:     &wire.Caps{Deflate: true},
	})
}

func watchingAudience(gen uint64, sessions ...string) wire.Audience {
	return wire.Audience{Gen: gen, Watchers: 1, Sessions: sessions}
}

func pausedAudience(gen uint64) wire.Audience {
	return wire.Audience{Gen: gen, Watchers: 0}
}

func audienceFrame(seq, ack uint64, a wire.Audience) wire.Frame {
	return wire.NewAudience(seq, ack, a)
}

func requestFrame(seq, ack uint64, reqID, method, path string, body string) wire.Frame {
	f := wire.Frame{V: wire.ProtocolVersion, Type: wire.TypeRequest, Seq: seq, Ack: ack, ReqID: reqID, Method: method, Path: path}
	if body != "" {
		f.Body = json.RawMessage(body)
	}
	return f
}

// websocketCloseCode maps a protocol reason onto the WS close code the hub
// would send.
func websocketCloseCode(r wire.CloseReason) websocket.StatusCode {
	code, _ := r.WS()
	return websocket.StatusCode(code)
}

// ── session harness ───────────────────────────────────────────────────

type sessionResult struct {
	reason wire.CloseReason
	err    error
}

type sessionHarness struct {
	t      *testing.T
	tr     *fakeTransport
	c      *Client
	res    chan sessionResult
	ctx    context.Context
	cancel context.CancelFunc
}

// newSessionHarness starts one session over a fake transport. The caller must
// complete the handshake with handshake.
func newSessionHarness(t *testing.T, c *Client, bridge bridgeSource) *sessionHarness {
	t.Helper()
	h := &sessionHarness{t: t, tr: newFakeTransport(), c: c, res: make(chan sessionResult, 1)}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	go func() {
		reason, err := c.runSession(h.ctx, bridge, h.tr)
		h.res <- sessionResult{reason: reason, err: err}
	}()
	t.Cleanup(h.cancel)
	return h
}

// handshake answers hello with ack and returns the hello the host sent.
func (h *sessionHarness) handshake(ack wire.Frame) wire.Hello {
	h.t.Helper()
	hello := h.tr.next(h.t, 2*time.Second)
	if hello.Type != wire.TypeHello {
		h.t.Fatalf("first frame = %q, want hello", hello.Type)
	}
	hd, err := hello.Hello()
	if err != nil {
		h.t.Fatal(err)
	}
	h.tr.push(h.t, ack)
	// Adoption sends a heartbeat immediately.
	hb := h.tr.nextOfType(h.t, wire.TypeHeartbeat, 2*time.Second)
	if _, err := hb.Heartbeat(); err != nil {
		h.t.Fatalf("heartbeat: %v", err)
	}
	return hd
}

// adopt delivers a hand-built hello_ack and waits for the heartbeat the host
// sends on adoption, so callers know the baseline, limits and audience are in
// force before they publish events.
func (h *sessionHarness) adopt(ack wire.Frame) {
	h.t.Helper()
	if hello := h.tr.next(h.t, 2*time.Second); hello.Type != wire.TypeHello {
		h.t.Fatalf("first frame = %q, want hello", hello.Type)
	}
	h.tr.push(h.t, ack)
	if hb := h.tr.nextOfType(h.t, wire.TypeHeartbeat, 2*time.Second); hb.Type != wire.TypeHeartbeat {
		h.t.Fatalf("expected the adoption heartbeat, got %q", hb.Type)
	}
}

func (h *sessionHarness) wait(t *testing.T, timeout time.Duration) sessionResult {
	t.Helper()
	select {
	case r := <-h.res:
		return r
	case <-time.After(timeout):
		t.Fatal("session did not end in time")
	}
	return sessionResult{}
}

func (h *sessionHarness) alive() bool {
	select {
	case r := <-h.res:
		h.res <- r
		return false
	default:
		return true
	}
}

// runForwardLoop starts the bridge→outbox pipeline for the duration of the
// test.
func runForwardLoop(t *testing.T, c *Client, bridge bridgeSource) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go c.forwardLoop(ctx, bridge)
	t.Cleanup(cancel)
	return cancel
}
