package hub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-wire"
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
		Epoch: c.epoch, Audience: audiencePtr(watchingAudience(1)),
		Liveness: &wire.Liveness{Keepalive: 1, Idle: 30}, Limits: limits, Caps: &wire.Caps{Deflate: true},
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
