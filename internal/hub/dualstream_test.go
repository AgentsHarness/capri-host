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
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/quic-go/quic-go"
)

// ── in-memory multi-stream QUIC hub ────────────────────────────────────

// testQUICHub is an in-memory QUIC hub implementing the multi-stream
// contract: the FIRST accepted stream is the control plane (auth, hello,
// relayed `request` downlink, events uplink); every further accepted stream
// is a request stream. Frames are self-describing and dispatched by type
// regardless of which stream they arrive on — no stream carries a request's
// identity.
type testQUICHub struct {
	t       *testing.T
	ln      *quic.Listener
	certDER []byte // leaf certificate (tests compute SPKI pins from it)

	eventsCh  chan map[string]any
	respondCh chan testHubRespond
	errCh     chan error

	mu         sync.Mutex
	accepted   []quic.StreamID          // in accept order
	firstFrame map[quic.StreamID]string // first frame type seen per stream
	closed     map[quic.StreamID]bool   // peer closed its send side (EOF)
}

// testHubRespond is one observed type:"respond" frame.
type testHubRespond struct {
	ReqID  string
	Status int
	Stream quic.StreamID
}

func newTestQUICHub(t *testing.T) *testQUICHub {
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
	return &testQUICHub{
		t:          t,
		ln:         ln,
		certDER:    der,
		eventsCh:   make(chan map[string]any, 32),
		respondCh:  make(chan testHubRespond, 32),
		errCh:      make(chan error, 4),
		firstFrame: make(map[quic.StreamID]string),
		closed:     make(map[quic.StreamID]bool),
	}
}

// spkiPin returns the sha256 of the hub certificate's SPKI — a valid
// HUB_QUIC_PIN value for this hub.
func (h *testQUICHub) spkiPin() [32]byte {
	h.t.Helper()
	cert, err := x509.ParseCertificate(h.certDER)
	if err != nil {
		h.t.Fatalf("parse test cert: %v", err)
	}
	return sha256.Sum256(cert.RawSubjectPublicKeyInfo)
}

// port is the hub's UDP port.
func (h *testQUICHub) port() int {
	return h.ln.Addr().(*net.UDPAddr).Port
}

// serve accepts one connection; its first stream is the control plane
// (auth → hello), onReady may push relayed request frames down it, then all
// further streams are served as request streams.
func (h *testQUICHub) serve(onReady func(ctrl *quic.Stream)) {
	go func() {
		conn, err := h.ln.Accept(context.Background())
		if err != nil {
			h.errCh <- err
			return
		}
		ctrl, err := conn.AcceptStream(context.Background())
		if err != nil {
			h.errCh <- err
			return
		}
		if ctrl.StreamID() != 0 {
			h.errCh <- fmt.Errorf("first accepted stream = %d, want 0", ctrl.StreamID())
			return
		}
		h.record(ctrl)
		data, err := readTestFrame(ctrl)
		if err != nil {
			h.errCh <- err
			return
		}
		var auth struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(data, &auth) != nil || auth.Type != "auth" {
			h.errCh <- fmt.Errorf("bad auth frame: %s", data)
			return
		}
		if err := writeTestFrame(ctrl, map[string]any{
			"v": 1, "type": "hello", "service": "hub", "subscribers": 1, "seq": 0,
		}); err != nil {
			h.errCh <- err
			return
		}
		if onReady != nil {
			onReady(ctrl)
		}
		go h.serveStream(ctrl)
		for {
			s, err := conn.AcceptStream(context.Background())
			if err != nil {
				return // connection closed (test teardown)
			}
			h.record(s)
			go h.serveStream(s)
		}
	}()
}

// serveStream reads frames off one accepted stream and dispatches by type.
func (h *testQUICHub) serveStream(s *quic.Stream) {
	for {
		data, err := readTestFrame(s)
		if err != nil {
			h.markClosed(s.StreamID())
			_ = s.Close()
			return
		}
		var frame struct {
			Type   string           `json:"type"`
			Events []map[string]any `json:"events"`
			ReqID  string           `json:"reqId"`
			Status int              `json:"status"`
		}
		if json.Unmarshal(data, &frame) != nil {
			continue
		}
		h.noteFrame(s.StreamID(), frame.Type)
		switch frame.Type {
		case "events":
			for _, ev := range frame.Events {
				ev["__stream"] = int64(s.StreamID())
				h.eventsCh <- ev
			}
		case "respond":
			h.respondCh <- testHubRespond{ReqID: frame.ReqID, Status: frame.Status, Stream: s.StreamID()}
		}
	}
}

func (h *testQUICHub) record(s *quic.Stream) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.accepted = append(h.accepted, s.StreamID())
}

// acceptedIDs snapshots accepted stream IDs in order.
func (h *testQUICHub) acceptedIDs() []quic.StreamID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]quic.StreamID(nil), h.accepted...)
}

func (h *testQUICHub) noteFrame(id quic.StreamID, typ string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.firstFrame[id]; !ok {
		h.firstFrame[id] = typ
	}
}

// firstFrameOf returns the first frame type observed on id ("" when none).
func (h *testQUICHub) firstFrameOf(id quic.StreamID) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.firstFrame[id]
}

func (h *testQUICHub) markClosed(id quic.StreamID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed[id] = true
}

// sawClosed reports whether the peer finished (EOF) the stream's send side.
func (h *testQUICHub) sawClosed(id quic.StreamID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed[id]
}

// waitForClosed blocks until the stream's send side was closed by the peer.
func (h *testQUICHub) waitForClosed(id quic.StreamID, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.sawClosed(id) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func readTestFrame(s *quic.Stream) ([]byte, error) {
	var lb [4]byte
	if _, err := io.ReadFull(s, lb[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lb[:]) &^ deflateFlagQUIC
	buf := make([]byte, n)
	_, err := io.ReadFull(s, buf)
	return buf, err
}

func writeTestFrame(s *quic.Stream, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	if _, err := s.Write(lb[:]); err != nil {
		return err
	}
	_, err = s.Write(b)
	return err
}

// ── tests ──────────────────────────────────────────────────────────────

// TestQUICDualStreamPlaneSeparation verifies the T1 stream layout: the
// session opens the control plane (stream 0: auth, events) plus a shared
// request stream (stream 4, primed with a no-op pong), and a relay request
// arriving on the CONTROL plane is answered on its own per-request stream —
// never on the control plane, never on the shared one.
func TestQUICDualStreamPlaneSeparation(t *testing.T) {
	th := newTestQUICHub(t)
	th.serve(func(ctrl *quic.Stream) {
		// Relay requests ride the control plane downlink (hub contract).
		_ = writeTestFrame(ctrl, map[string]any{
			"v": 1, "type": "request", "reqId": "ds1",
			"method": "GET", "path": "/api/state",
		})
	})

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, 200, map[string]any{"ok": true})
	}))
	defer local.Close()

	c := NewClient(Config{
		URL:          fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:       "h1",
		Token:        "tok123",
		LocalBase:    local.URL,
		QUICInsecure: true,
	})
	c.sendCh = make(chan []byte, 64)
	c.reqCh = make(chan reqFrame, 16)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)
	done := make(chan error, 1)
	go func() { done <- c.quicSession(ctx, bridge) }()

	// Wait for forwarding enabled (hello.subscribers=1 on the control plane).
	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled over QUIC")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// The respond must land on a per-request stream: neither the control
	// plane (first accepted) nor the shared request stream (second).
	var resp testHubRespond
	select {
	case resp = <-th.respondCh:
	case <-time.After(5 * time.Second):
		select {
		case err := <-th.errCh:
			t.Fatalf("no respond (server err: %v)", err)
		default:
			t.Fatal("no respond observed")
		}
	}
	ids := th.acceptedIDs()
	if len(ids) < 3 {
		t.Fatalf("accepted %v, want control + shared + per-request", ids)
	}
	if resp.Stream == ids[0] {
		t.Errorf("respond arrived on control stream %d; want a per-request stream", resp.Stream)
	}
	if resp.Stream == ids[1] {
		t.Errorf("respond arrived on shared request stream %d; want a per-request stream", resp.Stream)
	}
	if resp.ReqID != "ds1" {
		t.Errorf("respond reqId = %q, want ds1", resp.ReqID)
	}
	// The per-request stream's very first frame is the no-op pong prime
	// (a QUIC stream only becomes visible once it carries a frame).
	if got := th.firstFrameOf(resp.Stream); got != "pong" {
		t.Errorf("first frame on per-request stream = %q, want pong (activation prime)", got)
	}
	// Final respond ⇒ host closes the stream.
	if !th.waitForClosed(resp.Stream, 5*time.Second) {
		t.Errorf("per-request stream %d not closed after final respond", resp.Stream)
	}

	// Events still ride the control plane.
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "plane a", "sessionId": "s1"})
	select {
	case ev := <-th.eventsCh:
		if ev["__stream"] != int64(ids[0]) {
			t.Errorf("events arrived on stream %v; want control plane %d", ev["__stream"], ids[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no events observed")
	}
	cancel()
	<-done
}

// TestQUICPerRequestStreams verifies T1's per-request streams end to end:
// two overlapping relay requests get two DIFFERENT dedicated streams, each
// primed with a pong, each closed after its respond — while the control
// plane keeps carrying events.
func TestQUICPerRequestStreams(t *testing.T) {
	releaseSlow := make(chan struct{})
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-releaseSlow
		}
		writeTestJSON(w, 200, map[string]any{"ok": true, "path": r.URL.Path})
	}))
	defer local.Close()

	th := newTestQUICHub(t)
	th.serve(func(ctrl *quic.Stream) {
		_ = writeTestFrame(ctrl, map[string]any{
			"v": 1, "type": "request", "reqId": "r1",
			"method": "GET", "path": "/slow",
		})
		_ = writeTestFrame(ctrl, map[string]any{
			"v": 1, "type": "request", "reqId": "r2",
			"method": "GET", "path": "/fast",
		})
	})

	c := NewClient(Config{
		URL:          fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:       "h1",
		Token:        "tok123",
		LocalBase:    local.URL,
		QUICInsecure: true,
	})
	c.sendCh = make(chan []byte, 64)
	c.reqCh = make(chan reqFrame, 16)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)
	done := make(chan error, 1)
	go func() { done <- c.quicSession(ctx, bridge) }()

	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled over QUIC")
		case <-time.After(10 * time.Millisecond):
		}
	}

	waitRespond := func(reqID string) testHubRespond {
		t.Helper()
		for {
			select {
			case r := <-th.respondCh:
				if r.ReqID == reqID {
					return r
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("no respond for %s", reqID)
			}
		}
	}

	// r2 (fast) answers first: it must open the first per-request stream.
	// r1 is still executing locally (blocked), so ordering is deterministic.
	fast := waitRespond("r2")
	ids := th.acceptedIDs()
	if len(ids) < 3 {
		t.Fatalf("accepted %v, want control + shared + per-request", ids)
	}
	if fast.Stream == ids[0] || fast.Stream == ids[1] {
		t.Fatalf("r2 respond on stream %d (accepted %v); want a dedicated per-request stream", fast.Stream, ids)
	}
	if got := th.firstFrameOf(fast.Stream); got != "pong" {
		t.Errorf("first frame on r2's stream = %q, want pong prime", got)
	}

	// Now let r1 finish: its respond must ride a DIFFERENT stream.
	close(releaseSlow)
	slow := waitRespond("r1")
	if slow.Stream == fast.Stream {
		t.Errorf("r1 and r2 responds shared stream %d; want one stream per request", fast.Stream)
	}
	if slow.Stream == ids[0] || slow.Stream == ids[1] {
		t.Errorf("r1 respond on stream %d (accepted %v); want a dedicated per-request stream", slow.Stream, ids)
	}
	if got := th.firstFrameOf(slow.Stream); got != "pong" {
		t.Errorf("first frame on r1's stream = %q, want pong prime", got)
	}

	// Both per-request streams are closed after their final respond.
	if !th.waitForClosed(fast.Stream, 5*time.Second) {
		t.Errorf("r2's stream %d not closed after final respond", fast.Stream)
	}
	if !th.waitForClosed(slow.Stream, 5*time.Second) {
		t.Errorf("r1's stream %d not closed after final respond", slow.Stream)
	}

	// Control plane still carries events throughout.
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "per-req", "sessionId": "s1"})
	select {
	case ev := <-th.eventsCh:
		if ev["__stream"] != int64(ids[0]) {
			t.Errorf("events arrived on stream %v; want control plane %d", ev["__stream"], ids[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no events observed")
	}
	cancel()
	<-done
}

// TestQUICReqPlaneFallback verifies the degradation path: once the
// per-request stream cap is hit, further responds ride the SHARED request
// stream, and a request's final respond closes its dedicated stream. The
// plane is driven directly (no full client session) so the cap is reached
// deterministically.
func TestQUICReqPlaneFallback(t *testing.T) {
	oldMax := reqStreamMax
	reqStreamMax = 2
	defer func() { reqStreamMax = oldMax }()

	th := newTestQUICHub(t)
	th.serve(nil)

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, 200, map[string]any{"ok": true})
	}))
	defer local.Close()

	c := NewClient(Config{
		URL:          fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:       "h1",
		Token:        "tok123",
		LocalBase:    local.URL,
		QUICInsecure: true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, fmt.Sprintf("127.0.0.1:%d", th.port()), &tls.Config{
		NextProtos:         []string{"capri-hub"},
		InsecureSkipVerify: true,
	}, &quic.Config{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "")

	shared, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open shared: %v", err)
	}
	// The hub's serve loop expects an auth frame on the FIRST accepted
	// stream before it dispatches anything — that stream is the shared one
	// in this raw-conn setup.
	auth, _ := json.Marshal(map[string]any{"v": 1, "type": "auth", "token": "tok123"})
	if err := c.quicSendFrame(shared, auth); err != nil {
		t.Fatalf("auth on shared stream: %v", err)
	}
	plane := &quicReqPlane{
		conn:    conn,
		ctx:     ctx,
		shared:  shared,
		writeFn: c.quicSendFrame,
		streams: make(map[string]*quic.Stream),
	}
	defer plane.closeAll()

	send := func(reqID string, final bool) {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{
			"v": 1, "type": "respond", "reqId": reqID, "status": 200,
			"body": map[string]any{"ok": true},
		})
		if err := plane.send(reqID, payload, final); err != nil {
			t.Fatalf("plane.send(%s): %v", reqID, err)
		}
	}

	// Two non-final responds open two dedicated streams (cap = 2).
	send("a", false)
	send("b", false)
	// Per-stream reader goroutines dispatch independently, so responds may
	// arrive out of send order — stash non-matching ones instead of
	// dropping them.
	var pending []testHubRespond
	waitRespond := func(reqID string) testHubRespond {
		t.Helper()
		for i, r := range pending {
			if r.ReqID == reqID {
				pending = append(pending[:i], pending[i+1:]...)
				return r
			}
		}
		for {
			select {
			case r := <-th.respondCh:
				if r.ReqID == reqID {
					return r
				}
				pending = append(pending, r)
			case <-time.After(5 * time.Second):
				t.Fatalf("no respond observed for %s", reqID)
			}
		}
	}
	respA := waitRespond("a")
	respB := waitRespond("b")
	if respA.Stream == respB.Stream {
		t.Fatalf("a and b shared stream %d; want dedicated streams", respA.Stream)
	}
	ids := th.acceptedIDs()
	// shared is the first accepted stream here (no control plane in this
	// raw-conn setup): dedicated streams are the 2nd and 3rd.
	if respA.Stream == ids[0] || respB.Stream == ids[0] {
		t.Fatalf("dedicated responds landed on the shared stream %d (accepted %v)", ids[0], ids)
	}

	// Cap reached: c's respond must ride the shared stream (ids[0]).
	send("c", true)
	respC := waitRespond("c")
	if respC.Stream != ids[0] {
		t.Fatalf("over-cap respond on stream %d; want shared fallback %d (accepted %v)", respC.Stream, ids[0], ids)
	}

	// A final respond closes the dedicated stream.
	send("a", true)
	if !th.waitForClosed(respA.Stream, 5*time.Second) {
		t.Errorf("a's dedicated stream %d not closed after final respond", respA.Stream)
	}
	// Freed capacity: d gets a dedicated stream again.
	send("d", true)
	respD := waitRespond("d")
	if respD.Stream == ids[0] {
		t.Fatal("d still on shared stream after a slot freed; want a dedicated stream")
	}
}
