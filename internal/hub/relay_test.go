package hub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-hub/wire"
)

// TestRespondChunksLargeBody: a body over relayChunkSize goes out as several
// base64 frames whose chunks concatenate back to the exact original bytes,
// with more on every frame but the last and final (the QUIC stream close) only
// there.
func TestRespondChunksLargeBody(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	s := c.currentSession()

	body := json.RawMessage(`{"text":"` + strings.Repeat("a", 2<<20+512<<10) + `"}`)
	s.respond("r1", 200, body)

	wantFrames := (len(body) + relayChunkSize - 1) / relayChunkSize
	if wantFrames < 3 {
		t.Fatalf("test body spans %d chunks, want at least 3", wantFrames)
	}
	var got []byte
	for i := 0; i < wantFrames; i++ {
		f := h.tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
		if f.ReqID != "r1" || f.RespondStatus() != 200 {
			t.Fatalf("frame %d header = %+v", i, f)
		}
		if f.Seq != 0 {
			t.Fatalf("respond carried session seq %d", f.Seq)
		}
		if f.ChunkSeq != uint64(i+1) {
			t.Fatalf("frame %d chunkSeq = %d, want %d", i, f.ChunkSeq, i+1)
		}
		if f.Body != nil {
			t.Fatalf("chunk frame %d also carried a body", i)
		}
		if f.Encoding != "base64" {
			t.Fatalf("frame %d encoding = %q", i, f.Encoding)
		}
		dec, err := base64.StdEncoding.DecodeString(f.Chunk)
		if err != nil {
			t.Fatalf("frame %d chunk is not base64: %v", i, err)
		}
		if len(dec) > relayChunkSize {
			t.Fatalf("frame %d carries %d bytes, over relayChunkSize", i, len(dec))
		}
		if want := i < wantFrames-1; f.More != want {
			t.Fatalf("frame %d more = %v, want %v", i, f.More, want)
		}
		got = append(got, dec...)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("reassembled %d bytes, want the original %d", len(got), len(body))
	}
}

// TestRespondSmallBodyStaysSingleFrame: small answers keep the single-body
// shape.
func TestRespondSmallBodyStaysSingleFrame(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	s := c.currentSession()

	boundary := json.RawMessage(`{"x":"` + strings.Repeat("b", relayChunkSize-8) + `"}`)
	if len(boundary) != relayChunkSize {
		t.Fatalf("boundary body = %d bytes, want %d", len(boundary), relayChunkSize)
	}
	s.respond("r2", 201, boundary)
	s.respond("r3", 202, json.RawMessage(`{"ok":true}`))

	for i, wantStatus := range []int{201, 202} {
		f := h.tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
		if f.RespondStatus() != wantStatus {
			t.Fatalf("frame %d status = %d, want %d", i, f.RespondStatus(), wantStatus)
		}
		if f.Chunk != "" || f.Encoding != "" || f.More {
			t.Fatalf("frame %d used the chunk shape for a small body", i)
		}
		if f.ChunkSeq != 1 {
			t.Fatalf("frame %d chunkSeq = %d, want an inline answer as chunk 1", i, f.ChunkSeq)
		}
	}
}

// TestRespondWrapsNonJSONBody: a non-embeddable body is wrapped as a JSON
// string so the hub's pending request is never stranded.
func TestRespondWrapsNonJSONBody(t *testing.T) {
	c := newSessionClient(t)
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))
	c.currentSession().respond("r4", 200, json.RawMessage("plain text body"))

	f := h.tr.nextOfType(t, wire.TypeRespond, 5*time.Second)
	var s string
	if err := json.Unmarshal(f.Body, &s); err != nil || s != "plain text body" {
		t.Fatalf("body = %s (%v)", f.Body, err)
	}
}

// TestRelayInProcess: the in-process path injects the access token, answers a
// panic as a 500, and refuses to relay a body over limits.maxRelayBody.
func TestRelayInProcess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/echo", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"ok":false,"error":"auth header lost: ` + got + `"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		_, _ = w.Write(buf)
	})
	mux.HandleFunc("GET /api/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	mux.HandleFunc("GET /api/big", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 4<<20))
	})

	c := NewClient(Config{
		HostID: "h1", Token: "tok123", AccessToken: "tok", Local: mux,
	})
	h := newSessionHarness(t, c, newTestBridge())
	limits := &wire.Limits{MaxFrame: wire.DefaultLimits.MaxFrame, MaxRelayBody: 1 << 20, ResumeWindow: wire.ResumeWindow}
	h.adopt(wire.NewHelloAck(wire.HelloAck{
		Epoch: c.epoch, HubBoot: testHubBoot, Audience: audiencePtr(watchingAudience(1)),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 30}, Limits: limits, Caps: testCaps(),
	}))

	h.tr.push(t, requestFrame(1, 0, "1", "POST", "/api/echo", `{"hello":"relay"}`))
	got := waitRespond(t, h.tr)
	if got.RespondStatus() != 200 || !bytes.Contains(got.Body, []byte("relay")) {
		t.Fatalf("echo answer = %d %s", got.RespondStatus(), got.Body)
	}

	h.tr.push(t, requestFrame(2, 0, "2", "GET", "/api/panic", ""))
	if got := waitRespond(t, h.tr); got.RespondStatus() != 500 {
		t.Fatalf("panic answer = %d", got.RespondStatus())
	}

	h.tr.push(t, requestFrame(3, 0, "3", "GET", "/api/big", ""))
	if got := waitRespond(t, h.tr); got.RespondStatus() != 502 {
		t.Fatalf("oversized answer = %d %s", got.RespondStatus(), got.Body)
	}
}

// TestRelayHTTPFallback: without an in-process handler the request runs
// against LocalBase with the access token attached.
func TestRelayHTTPFallback(t *testing.T) {
	srv := newHTTPStub(t)
	c := NewClient(Config{HostID: "h1", Token: "tok123", LocalBase: srv.url, AccessToken: "tok"})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	h.tr.push(t, requestFrame(1, 0, "31", "GET", "/api/state", ""))
	got := waitRespond(t, h.tr)
	if got.RespondStatus() != 200 {
		t.Fatalf("status = %d body %s", got.RespondStatus(), got.Body)
	}
	if !bytes.Contains(got.Body, []byte(`"auth":"Bearer tok"`)) {
		t.Fatalf("access token not forwarded: %s", got.Body)
	}
}

// httpStub is a one-endpoint local API.
type httpStub struct {
	url string
}

func newHTTPStub(t *testing.T) *httpStub {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"auth":"` + r.Header.Get("Authorization") + `"}`))
	})
	return &httpStub{url: startTestServer(t, h)}
}

// memoEntriesFor counts the memo entries scoped to one hub incarnation.
func memoEntriesFor(m *relayMemo, hubBoot uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for key := range m.entries {
		if key.hubBoot == hubBoot {
			n++
		}
	}
	return n
}

// countingHandler answers every request with the running call number, so a
// test can tell a re-execution from a resend.
func countingHandler(calls *atomic.Int32, body func(n int32) string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/count", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body(calls.Add(1))))
	})
	return mux
}

func helloAckBoot(epoch, hubBoot, ackUp, ackDown uint64) wire.Frame {
	return helloAckBootAcks(epoch, hubBoot, ackUp, ackDown)
}

// helloAckBootAcks is helloAckBoot with the hub's relay ack table: a resumed
// host only keeps an answer the hub still lists.
func helloAckBootAcks(epoch, hubBoot, ackUp, ackDown uint64, acks ...wire.RelayAck) wire.Frame {
	ack := helloAckFrame(epoch, ackUp, ackDown, watchingAudience(1))
	ack.HubBoot = hubBoot
	ack.RelayAcks = acks
	return ack
}

// TestRelayMemoScopedByHubBoot: a hub restart resets its reqId counter, so the
// same reqId in the new incarnation is a NEW request. It must execute again,
// never inherit the previous hub's tombstone, and the old answer must not
// survive the restart.
func TestRelayMemoScopedByHubBoot(t *testing.T) {
	var calls atomic.Int32
	c := NewClient(Config{
		HostID: "h1", Token: "tok123",
		Local: countingHandler(&calls, func(n int32) string {
			return fmt.Sprintf(`{"call":%d}`, n)
		}),
	})

	first := newSessionHarness(t, c, newTestBridge())
	first.handshake(helloAckBoot(c.epoch, 11, 0, 0))
	first.tr.push(t, requestFrame(1, 0, "1", "GET", "/api/count", ""))
	if got := waitRespond(t, first.tr); !bytes.Contains(got.Body, []byte(`"call":1`)) {
		t.Fatalf("first answer = %d %s", got.RespondStatus(), got.Body)
	}

	// The link dies; a restarted hub comes back with a fresh reqId space.
	first.cancel()
	first.wait(t, 3*time.Second)

	second := newSessionHarness(t, c, newTestBridge())
	second.handshake(helloAckBoot(c.epoch, 12, 0, 0))
	second.tr.push(t, requestFrame(1, 0, "1", "GET", "/api/count", ""))
	got := waitRespond(t, second.tr)
	if !bytes.Contains(got.Body, []byte(`"call":2`)) {
		t.Fatalf("second incarnation answered from the old hub's memo: %s", got.Body)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("executions = %d, want 2 (the new hub's request must run again)", n)
	}
	if left := memoEntriesFor(c.memo, 11); left != 0 {
		t.Fatalf("old hubBoot entries survived the restart: %d", left)
	}
}

// TestRelayAnswerFlushedAfterSessionGap: an answer that finishes with no
// session is resent on the next hello_ack of the same hub incarnation. The hub
// already acked the request and will not replay it, so this resend is the only
// way the browser ever sees the answer.
func TestRelayAnswerFlushedAfterSessionGap(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/slow", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		close(started)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"slow":1}`))
	})
	c := NewClient(Config{HostID: "h1", Token: "tok123", Local: mux})

	first := newSessionHarness(t, c, newTestBridge())
	first.handshake(helloAckBoot(c.epoch, 21, 0, 0))
	first.tr.push(t, requestFrame(1, 0, "1", "GET", "/api/slow", ""))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("relay execution never started")
	}

	// The link drops while the request is still running.
	first.cancel()
	first.wait(t, 3*time.Second)

	// The answer completes with no session at all.
	close(release)
	deadline := time.Now().Add(3 * time.Second)
	for !c.relayOutbox.pending() {
		if time.Now().After(deadline) {
			t.Fatal("the answer never reached the outbox")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Same hub incarnation reconnects: its table still lists the request, so
	// the answer rides the new link from chunk 1.
	second := newSessionHarness(t, c, newTestBridge())
	second.handshake(helloAckBootAcks(c.epoch, 21, 0, 0, wire.RelayAck{ReqID: "1"}))
	got := waitRespond(t, second.tr)
	if got.ReqID != "1" || got.RespondStatus() != 200 || !bytes.Contains(got.Body, []byte(`"slow":1`)) {
		t.Fatalf("resend = %d %s (reqId %q)", got.RespondStatus(), got.Body, got.ReqID)
	}
	// Exactly once: no second copy behind it.
	deadline = time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		f, ok := second.tr.tryFrame(time.Until(deadline))
		if !ok {
			break
		}
		if f.Type == wire.TypeRespond {
			t.Fatalf("the answer was resent twice (reqId %q)", f.ReqID)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("executions = %d, a resend must not re-run the request", n)
	}
}
