package hub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/coder/websocket"

	"github.com/AgentsHarness/capri-hub/wire"
)

// ── fake transport ────────────────────────────────────────────────────

// sentRecord is one uplink message as the transport wrote it: a complete
// logical frame (Send) or one encoded transport unit (SendFragment).
type sentRecord struct {
	fragment bool
	payload  []byte
}

// fakeTransport is one in-memory link: records the host writes arrive on sent,
// frames pushed on in are what the host reads. Unit records are kept raw so a
// test can assert on interleaving; next reassembles them.
type fakeTransport struct {
	in   chan []byte
	sent chan sentRecord

	closeOnce sync.Once
	closed    chan struct{}

	mu           sync.Mutex
	deflate      bool
	keepalive    time.Duration
	keepaliveCh  chan time.Duration
	writeTimeout time.Duration
	maxFrame     int
	// fragmentHook runs after each unit is queued, the way a slow socket
	// stalls a writer mid-frame.
	fragmentHook func()
	// recvErr delivers an injected Recv error, the way a transport reports a
	// frame it could not decode. A channel so a Recv already parked on the
	// input wakes up.
	recvErr chan error

	lastActivity atomic.Int64
	asm          wire.Reassembler
}

func newFakeTransport() *fakeTransport {
	f := &fakeTransport{
		in:          make(chan []byte, 64),
		sent:        make(chan sentRecord, 256),
		closed:      make(chan struct{}),
		keepaliveCh: make(chan time.Duration, 4),
		recvErr:     make(chan error, 1),
		maxFrame:    wire.DefaultLimits.MaxFrame,
	}
	f.lastActivity.Store(time.Now().UnixNano())
	return f
}

func (f *fakeTransport) Send(payload []byte) error {
	return f.write(sentRecord{payload: payload})
}

func (f *fakeTransport) SendFragment(unit []byte) error {
	if err := f.write(sentRecord{fragment: true, payload: unit}); err != nil {
		return err
	}
	f.mu.Lock()
	hook := f.fragmentHook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (f *fakeTransport) write(rec sentRecord) error {
	select {
	case f.sent <- rec:
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
		f.lastActivity.Store(time.Now().UnixNano())
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

func (f *fakeTransport) SetWriteTimeout(d time.Duration) {
	f.mu.Lock()
	f.writeTimeout = d
	f.mu.Unlock()
}

func (f *fakeTransport) SetMaxFrame(n int) {
	f.mu.Lock()
	f.maxFrame = n
	f.mu.Unlock()
}

func (f *fakeTransport) LastActivity() time.Time {
	ns := f.lastActivity.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// setFragmentHook installs a callback that runs after each unit write.
func (f *fakeTransport) setFragmentHook(hook func()) {
	f.mu.Lock()
	f.fragmentHook = hook
	f.mu.Unlock()
}

// nextRecord returns the next raw uplink record (a frame or a unit).
func (f *fakeTransport) nextRecord(t *testing.T, timeout time.Duration) sentRecord {
	t.Helper()
	select {
	case rec := <-f.sent:
		return rec
	case <-time.After(timeout):
		t.Fatal("host wrote nothing in time")
	}
	return sentRecord{}
}

// tryFrame returns the next reassembled frame, or ok=false on timeout.
func (f *fakeTransport) tryFrame(timeout time.Duration) (wire.Frame, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case rec := <-f.sent:
			if !rec.fragment {
				fr, err := wire.Decode(rec.payload)
				return fr, err == nil
			}
			f.mu.Lock()
			limit := f.maxFrame
			f.mu.Unlock()
			frame, compressed, complete, err := f.asm.Accept(rec.payload, limit)
			if err != nil {
				return wire.Frame{}, false
			}
			if !complete {
				continue
			}
			if compressed {
				if frame, err = inflatePayload(frame); err != nil {
					return wire.Frame{}, false
				}
			}
			fr, err := wire.Decode(frame)
			return fr, err == nil
		case <-f.closed:
			return wire.Frame{}, false
		case <-timer.C:
			return wire.Frame{}, false
		}
	}
}

// next reads one frame the host wrote, reassembling units.
func (f *fakeTransport) next(t *testing.T, timeout time.Duration) wire.Frame {
	t.Helper()
	fr, ok := f.tryFrame(timeout)
	if !ok {
		t.Fatal("host wrote no decodable frame in time")
	}
	return fr
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

// testCaps is the capability echo every fixture uses: the host requires
// transport units, and the relay-ack bit is carried ahead of its phase.
func testCaps() *wire.Caps {
	return &wire.Caps{Deflate: true, TransportUnits: true, RelayAck: true}
}

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
		Caps:     testCaps(),
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
	ft := newFakeTransport()
	return newSessionHarnessOn(t, c, bridge, ft, ft)
}

// newSessionHarnessOn starts a session over tr, using fake for the frame-level
// assertions. A transport that wraps a fakeTransport lets a test drive a
// special plane (relay streams) while still reading control frames.
func newSessionHarnessOn(t *testing.T, c *Client, bridge bridgeSource, tr Transport, fake *fakeTransport) *sessionHarness {
	t.Helper()
	h := &sessionHarness{t: t, tr: fake, c: c, res: make(chan sessionResult, 1)}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	go func() {
		reason, err := c.runSession(h.ctx, bridge, tr)
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
