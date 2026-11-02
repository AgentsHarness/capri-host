package hub

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/AgentsHarness/capri-hub/wire"
)

// The 28MB relay ceiling is the protocol's largest single answer. It must move
// on both transports: QUIC puts it on the request's own stream, WebSocket
// multiplexes the base64 chunks on the control socket. Both reassemble to the
// exact bytes and keep the session alive.

func maxRelayBody(t *testing.T) []byte {
	t.Helper()
	size := wire.DefaultLimits.MaxRelayBody
	const prefix = `{"rows":"`
	const suffix = `"}`
	if size <= len(prefix)+len(suffix) {
		t.Fatalf("relay ceiling %d is too small for the fixture", size)
	}
	body := prefix + strings.Repeat("a", size-len(prefix)-len(suffix)) + suffix
	if len(body) != size {
		t.Fatalf("fixture is %d bytes, want %d", len(body), size)
	}
	return []byte(body)
}

func decodedChunk(t *testing.T, f wire.Frame) []byte {
	t.Helper()
	if f.Encoding != "base64" || f.Chunk == "" {
		t.Fatalf("frame for %s does not use the chunk shape: %+v", f.ReqID, f)
	}
	b, err := base64.StdEncoding.DecodeString(f.Chunk)
	if err != nil {
		t.Fatalf("chunk for %s is not base64: %v", f.ReqID, err)
	}
	return b
}

// TestQUICRelay28MB drives the ceiling through a real QUIC control stream and
// the per-request respond stream.
func TestQUICRelay28MB(t *testing.T) {
	body := maxRelayBody(t)
	hub := newQUICTestHub(t, watchingAudience(1))
	hub.serve()
	c := newQUICClient(t, hub)
	c.local = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _, _ = c.quicSession(ctx, newTestBridge()) }()

	ctrl := hub.control(t)
	if err := hub.writeControl(ctrl, requestFrame(1, 0, "77", "GET", "/api/history", "")); err != nil {
		t.Fatalf("request: %v", err)
	}

	// Chunks are collected by ChunkSeq: a retransmission (the RTO may repeat
	// the oldest unacked chunk, and the hub drops it as a duplicate) must not
	// be mistaken for extra payload.
	chunks := make(map[uint64][]byte)
	var last uint64
	var stream quic.StreamID
	for {
		r := hub.waitRespond(t, 30*time.Second)
		if r.frame.ReqID != "77" {
			continue
		}
		chunks[r.frame.ChunkSeq] = decodedChunk(t, r.frame)
		if !r.frame.More {
			last = r.frame.ChunkSeq
			stream = r.stream
			break
		}
	}
	got := make([]byte, 0, len(body))
	for seq := uint64(1); seq <= last; seq++ {
		c, ok := chunks[seq]
		if !ok {
			t.Fatalf("chunk %d of %d never arrived", seq, last)
		}
		got = append(got, c...)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("relay reassembled %d bytes, want the %d-byte original", len(got), len(body))
	}
	if stream == 0 {
		t.Fatal("the 28MB answer rode the control stream instead of a request stream")
	}
}

// TestWSRelay28MB is the WebSocket variant: no per-request stream, so the
// chunks ride the one socket while the session keeps working.
func TestWSRelay28MB(t *testing.T) {
	body := maxRelayBody(t)
	hub := newWSHub(t, watchingAudience(1))
	c := NewClient(Config{
		URL: hub.url(), HostID: "h1", HostName: "H1", Token: "tok123", DisableQUIC: true,
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(body)
		}),
	})
	runClient(t, c, newTestBridge())
	waitConnected(t, c)

	hub.push(t, requestFrame(1, 0, "78", "GET", "/api/history", ""))
	deadline := time.Now().Add(60 * time.Second)
	for {
		frames := hub.respondsFor("78")
		// The terminal chunk closes the answer; a retransmission can arrive
		// after it, so look for the marker instead of the last frame.
		var last uint64
		for _, f := range frames {
			if !f.More && f.ChunkSeq > last {
				last = f.ChunkSeq
			}
		}
		if last > 0 {
			// Indexed by ChunkSeq: a retransmitted chunk is a duplicate the
			// hub drops, not extra payload.
			chunks := make(map[uint64][]byte, len(frames))
			for _, f := range frames {
				chunks[f.ChunkSeq] = decodedChunk(t, f)
			}
			got := make([]byte, 0, len(body))
			for seq := uint64(1); seq <= last; seq++ {
				c, ok := chunks[seq]
				if !ok {
					t.Fatalf("chunk %d of %d never arrived", seq, last)
				}
				got = append(got, c...)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("relay reassembled %d bytes, want the %d-byte original", len(got), len(body))
			}
			if last < 2 {
				t.Fatalf("28MB answer arrived as %d frame(s); it must be chunked", last)
			}
			if tr := c.State().Transport; tr != TransportWS {
				t.Fatalf("transport = %q, want ws", tr)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d chunk(s) arrived in time", len(frames))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
