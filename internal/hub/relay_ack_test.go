package hub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"

	"github.com/AgentsHarness/capri-hub/wire"
)

// relayAckFrame builds an hb carrying the hub's relay ack table.
func relayAckFrame(acks ...wire.RelayAck) wire.Frame {
	return wire.Frame{
		V:         wire.ProtocolVersion,
		Type:      wire.TypeHeartbeat,
		Status:    mustJSON(wire.HeartbeatStatus{Ready: true}),
		RelayAcks: acks,
	}
}

// collectChunks reads respond frames for reqID until every sequence in
// [first,last] arrived, returning them by ChunkSeq (sequences outside the range
// are kept too, so a test can see an unexpected resend).
func collectChunks(t *testing.T, tr *fakeTransport, reqID string, first, last uint64, timeout time.Duration) map[uint64]wire.Frame {
	t.Helper()
	got := make(map[uint64]wire.Frame)
	deadline := time.Now().Add(timeout)
	for {
		missing := false
		for seq := first; seq <= last; seq++ {
			if _, ok := got[seq]; !ok {
				missing = true
				break
			}
		}
		if !missing {
			return got
		}
		fr, ok := tr.tryFrame(time.Until(deadline))
		if !ok {
			t.Fatalf("received chunks %v of %d..%d in time", chunkSeqs(got), first, last)
		}
		if fr.Type != wire.TypeRespond || fr.ReqID != reqID {
			continue
		}
		got[fr.ChunkSeq] = fr
	}
}

func chunkSeqs(m map[uint64]wire.Frame) []uint64 {
	out := make([]uint64, 0, len(m))
	for seq := range m {
		out = append(out, seq)
	}
	return out
}

// decodeChunk returns one respond frame's payload bytes.
func decodeChunk(t *testing.T, f wire.Frame) []byte {
	t.Helper()
	if f.Encoding != "base64" {
		t.Fatalf("chunk %d encoding = %q, want base64", f.ChunkSeq, f.Encoding)
	}
	b, err := base64.StdEncoding.DecodeString(f.Chunk)
	if err != nil {
		t.Fatalf("chunk %d is not base64: %v", f.ChunkSeq, err)
	}
	return b
}

// TestRelayAnswerResumesAtHubAckWatermark: chunked answers carry 1..N ChunkSeq,
// and a reconnect whose hello_ack table says "chunk N applied" resumes at N+1
// instead of replaying the whole answer.
func TestRelayAnswerResumesAtHubAckWatermark(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	s := c.currentSession()

	body := json.RawMessage(`{"rows":"` + strings.Repeat("a", 2<<20) + `"}`)
	total := uint64((len(body) + relayChunkSize - 1) / relayChunkSize)
	if total < 8 {
		t.Fatalf("fixture spans %d chunks, want at least 8", total)
	}
	s.respond("r1", 200, body)

	first := h.tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
	if first.ReqID != "r1" || first.ChunkSeq != 1 || first.RespondStatus() != 200 {
		t.Fatalf("first chunk = reqId %q seq %d status %d", first.ReqID, first.ChunkSeq, first.RespondStatus())
	}
	if !first.More {
		t.Fatal("the first chunk of a multi-chunk answer said more=false")
	}

	// The link dies after chunk 1; the hub confirms exactly that chunk.
	h.cancel()
	h.wait(t, 3*time.Second)

	h2 := newSessionHarness(t, c, newTestBridge())
	h2.handshake(helloAckBootAcks(c.epoch, testHubBoot, 0, 0, wire.RelayAck{ReqID: "r1", Chunk: 1}))

	got := collectChunks(t, h2.tr, "r1", 2, total, 10*time.Second)
	if _, ok := got[1]; ok {
		t.Fatal("the resumed session resent an acknowledged chunk")
	}
	rebuilt := decodeChunk(t, first)
	for seq := uint64(2); seq <= total; seq++ {
		f := got[seq]
		if f.RespondStatus() != 200 {
			t.Fatalf("chunk %d status = %d", seq, f.RespondStatus())
		}
		if want := seq < total; f.More != want {
			t.Fatalf("chunk %d more = %v, want %v", seq, f.More, want)
		}
		rebuilt = append(rebuilt, decodeChunk(t, f)...)
	}
	if !bytes.Equal(rebuilt, body) {
		t.Fatalf("reassembled %d bytes, want the original %d", len(rebuilt), len(body))
	}
}

// TestRelayDoneAckReleasesAnswerAndDuplicateDoesNotRerun: the hub confirming
// the whole answer releases it, and a duplicate that arrives after a gap is
// ignored instead of re-executing the prompt.
func TestRelayDoneAckReleasesAnswerAndDuplicateDoesNotRerun(t *testing.T) {
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`{"rows":"` + strings.Repeat("b", 2<<20) + `"}`))
		}),
	})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h.tr.push(t, requestFrame(1, 0, "77", "GET", "/api/big", ""))

	// Read the whole answer; the last frame is the terminal chunk.
	var total uint64
	for {
		f := waitRespond(t, h.tr)
		if f.ReqID != "77" {
			continue
		}
		total = f.ChunkSeq
		if !f.More {
			break
		}
	}
	if total < 2 {
		t.Fatalf("answer arrived as %d chunk(s), want a chunked answer", total)
	}

	h.tr.push(t, relayAckFrame(wire.RelayAck{ReqID: "77", Chunk: total}))
	waitUntil(t, "the answer to be released", func() bool { return !c.relayOutbox.pending() })

	// Same-epoch resume: the table no longer lists the request, and the
	// duplicate must not run the handler again.
	h2 := newSessionHarness(t, c, newTestBridge())
	h2.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	h2.tr.push(t, requestFrame(1, 0, "77", "GET", "/api/big", ""))
	if fr, ok := h2.tr.tryFrame(300 * time.Millisecond); ok && fr.Type == wire.TypeRespond {
		t.Fatalf("a resolved duplicate was answered again: %+v", fr)
	}
	if hits.Load() != 1 {
		t.Fatalf("handler hits = %d, want 1", hits.Load())
	}
}

// blockingRelayTransport is a fake link with a relay plane whose chunk writes
// park until the test releases them.
type blockingRelayTransport struct {
	*fakeTransport
	started chan relayAttempt
	release chan struct{}
}

type relayAttempt struct {
	reqID string
	final bool
}

func newBlockingRelayTransport() *blockingRelayTransport {
	return &blockingRelayTransport{
		fakeTransport: newFakeTransport(),
		started:       make(chan relayAttempt, 8),
		release:       make(chan struct{}),
	}
}

func (b *blockingRelayTransport) SendRelay(reqID string, payload []byte, final bool) error {
	select {
	case b.started <- relayAttempt{reqID: reqID, final: final}:
	default:
	}
	select {
	case <-b.release:
		return nil
	case <-b.closed:
		// Closing the transport unblocks a parked write, like the real ones.
		return io.EOF
	}
}

// TestRelayLaneKeepsControlResponsive: a relay chunk write on its own plane
// must not delay hb/control. The lane parks inside SendRelay while the writer
// keeps the session alive, and the chunk completes once released.
func TestRelayLaneKeepsControlResponsive(t *testing.T) {
	c := newSessionClient(t)
	bridge := newTestBridge()
	bt := newBlockingRelayTransport()
	h := newSessionHarnessOn(t, c, bridge, bt, bt.fakeTransport)
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1), 5, 60))

	// Two chunks: the lane streams them one at a time.
	body := json.RawMessage(`{"rows":"` + strings.Repeat("q", relayChunkSize) + `"}`)
	c.currentSession().respond("r1", 200, body)

	select {
	case a := <-bt.started:
		if a.reqID != "r1" {
			t.Fatalf("the lane wrote reqId %q", a.reqID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay lane never started writing")
	}

	// One write per request may be outstanding: the outbox must not hand the
	// same chunk to another caller, and an RTO repair must not either.
	if r, ok := c.relayOutbox.reserveNext(); ok {
		t.Fatalf("reserved duplicated chunk seq %d while one was in flight", r.seq)
	}
	if r, ok := c.relayOutbox.reserveRepair(); ok {
		t.Fatalf("repair reserved chunk seq %d while one was in flight", r.seq)
	}

	// While the chunk is parked, a status change must still reach the hub on
	// the control plane.
	bridge.setStatus(acp.Status{Ready: true, Busy: true})
	c.notifyStatus()
	deadline := time.Now().Add(2 * time.Second)
	for {
		fr, ok := h.tr.tryFrame(time.Until(deadline))
		if !ok {
			t.Fatal("no control frame while a relay chunk was in flight")
		}
		if fr.Type == wire.TypeHeartbeat {
			break
		}
	}
	if !h.alive() {
		t.Fatal("the session died while a relay chunk was in flight")
	}

	// Release: the lane completes chunk 1, streams chunk 2, and the answer is
	// fully handed to the transport (the hub's ack is still outstanding).
	close(bt.release)
	waitUntil(t, "the answer to be fully written", func() bool {
		c.relayOutbox.mu.Lock()
		defer c.relayOutbox.mu.Unlock()
		a := c.relayOutbox.answers["r1"]
		return a != nil && a.sent == a.final && a.inflight == 0
	})
	if !h.alive() {
		t.Fatal("the session did not survive the relay write")
	}
}

// TestRelayRTOReplaysOnlyTheOldestChunk: with a whole answer unacked, the
// retransmit timer repairs the oldest chunk only, never the whole answer.
func TestRelayRTOReplaysOnlyTheOldestChunk(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrameLiveness(c.epoch, 0, 0, watchingAudience(1), 1, 30))
	s := c.currentSession()

	body := json.RawMessage(`{"rows":"` + strings.Repeat("d", 2<<20) + `"}`)
	total := uint64((len(body) + relayChunkSize - 1) / relayChunkSize)
	s.respond("r1", 200, body)

	// Consume the initial burst.
	for seq := uint64(1); seq <= total; seq++ {
		f := h.tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
		if f.ChunkSeq != seq {
			t.Fatalf("chunk %d arrived as seq %d", seq, f.ChunkSeq)
		}
	}

	// No ack: RTO is 2s. The repair is one chunk, starting at the ack
	// watermark; the streamed tail must not be replayed with it.
	replay := h.tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
	if replay.ChunkSeq != 1 {
		t.Fatalf("RTO replayed chunk %d, want the oldest unacked chunk 1", replay.ChunkSeq)
	}
	if fr, ok := h.tr.tryFrame(700 * time.Millisecond); ok && fr.Type == wire.TypeRespond {
		t.Fatalf("one RTO replayed beyond the oldest chunk: seq %d", fr.ChunkSeq)
	}
}

// TestRelayAbortReleasesAnswerAndBlocksRerun: an abort acks and discards the
// answer, and a later delivery of the same request must not re-execute it.
func TestRelayAbortReleasesAnswerAndBlocksRerun(t *testing.T) {
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "88", "GET", "/api/x", ""))
	if fr := waitRespond(t, h.tr); fr.RespondStatus() != 200 {
		t.Fatalf("answer status = %d", fr.RespondStatus())
	}
	h.tr.push(t, relayAckFrame(wire.RelayAck{ReqID: "88", Abort: true}))
	waitUntil(t, "the aborted answer to be dropped", func() bool { return !c.relayOutbox.pending() })
	if !c.memo.aborted(testHubBoot, "88") {
		t.Fatal("the abort did not settle the memo entry")
	}

	h.tr.push(t, requestFrame(2, 0, "88", "GET", "/api/x", ""))
	time.Sleep(300 * time.Millisecond)
	if hits.Load() != 1 {
		t.Fatalf("an aborted request re-executed: hits = %d", hits.Load())
	}
	if fr, ok := h.tr.tryFrame(100 * time.Millisecond); ok && fr.Type == wire.TypeRespond {
		t.Fatalf("an aborted request was answered: %+v", fr)
	}
}

// TestRelayAbortBeforeExecutionSkipsHandler: an abort that arrives before the
// request it names suppresses the execution entirely — the hub already
// returned 503 to the browser.
func TestRelayAbortBeforeExecutionSkipsHandler(t *testing.T) {
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, relayAckFrame(wire.RelayAck{ReqID: "99", Abort: true}))
	waitUntil(t, "the abort tombstone", func() bool { return c.memo.aborted(testHubBoot, "99") })

	h.tr.push(t, requestFrame(1, 0, "99", "POST", "/api/prompt", `{}`))
	time.Sleep(300 * time.Millisecond)
	if hits.Load() != 0 {
		t.Fatalf("a request aborted before delivery ran: hits = %d", hits.Load())
	}
	if fr, ok := h.tr.tryFrame(100 * time.Millisecond); ok && fr.Type == wire.TypeRespond {
		t.Fatalf("an aborted request was answered: %+v", fr)
	}
}

// TestRelayOutboxCapAnswers502: an answer that cannot fit the byte budget
// fails only its own request with a small 502; the session stays up and keeps
// serving.
func TestRelayOutboxCapAnswers502(t *testing.T) {
	old := relayOutboxMaxBytes
	relayOutboxMaxBytes = 1024
	t.Cleanup(func() { relayOutboxMaxBytes = old })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/big", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"rows":"` + strings.Repeat("c", 4<<10) + `"}`))
	})
	mux.HandleFunc("GET /api/small", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	c := NewClient(Config{HostID: "h1", Token: "tok123", Local: mux})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "5", "GET", "/api/big", ""))
	fr := waitRespond(t, h.tr)
	if fr.RespondStatus() != http.StatusBadGateway {
		t.Fatalf("overflow answer status = %d, want 502", fr.RespondStatus())
	}
	if fr.ChunkSeq != 1 || fr.More {
		t.Fatalf("overflow answer chunkSeq %d more %v, want one terminal chunk", fr.ChunkSeq, fr.More)
	}
	var body struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(fr.Body, &body); err != nil || body.OK || body.Error == "" {
		t.Fatalf("overflow answer body = %s (%v)", fr.Body, err)
	}
	if !h.alive() {
		t.Fatal("an overflowed answer killed the session")
	}

	// The link still serves: a small answer fits the same budget.
	h.tr.push(t, requestFrame(2, 0, "6", "GET", "/api/small", ""))
	if fr := waitRespond(t, h.tr); fr.RespondStatus() != 200 {
		t.Fatalf("the session stopped answering after an overflow: status %d", fr.RespondStatus())
	}
}

// TestHubBootChangeClearsRelayOutbox: a restarted hub has a fresh reqId space,
// so answers and tombstones of the previous incarnation must not survive.
func TestHubBootChangeClearsRelayOutbox(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			<-release
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	})

	first := newSessionHarness(t, c, newTestBridge())
	first.handshake(helloAckBoot(c.epoch, 41, 0, 0))
	first.tr.push(t, requestFrame(1, 0, "1", "POST", "/api/prompt", `{}`))
	waitUntil(t, "the execution to start", func() bool { return hits.Load() == 1 })

	// The link dies; the answer finishes with no session and waits in the
	// outbox for the hub that asked.
	first.cancel()
	first.wait(t, 3*time.Second)
	close(release)
	waitUntil(t, "the queued answer", func() bool { return c.relayOutbox.pending() })

	second := newSessionHarness(t, c, newTestBridge())
	second.handshake(helloAckBoot(c.epoch, 42, 0, 0))
	if n := c.relayOutbox.queued(); n != 0 {
		t.Fatalf("%d answers of the old hub incarnation survived the restart", n)
	}
	if n := memoEntriesFor(c.memo, 41); n != 0 {
		t.Fatalf("%d memo tombstones of the old hub incarnations survived", n)
	}

	// The same reqId under the new hub is a new request.
	second.tr.push(t, requestFrame(1, 0, "1", "POST", "/api/prompt", `{}`))
	if fr := waitRespond(t, second.tr); fr.RespondStatus() != 200 {
		t.Fatalf("new incarnation answer = %+v", fr)
	}
	if hits.Load() != 2 {
		t.Fatalf("handler hits = %d, want 2 (the restarted hub's request is new)", hits.Load())
	}
}
