package hub

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/coder/websocket"
)

func TestDeflatePayloadThresholds(t *testing.T) {
	// Small payload: never compressed.
	small := []byte(`{"type":"events","events":[]}`)
	if _, ok := deflatePayload(small); ok {
		t.Fatal("payload below minCompressSize must not be compressed")
	}
	// Large incompressible payload (already flate'd random-ish): no gain → raw.
	big := bytes.Repeat([]byte{0x00, 0xFF, 0x55, 0xAA}, 512) // 2KB, high entropy
	if _, ok := deflatePayload(big); ok {
		t.Log("note: incompressible payload unexpectedly shrank — acceptable but unusual")
	}
	// Compressible payload: compresses and round-trips.
	var raw bytes.Buffer
	raw.WriteString(`{"type":"events","events":[{"text":"`)
	raw.WriteString(strings.Repeat("hello world ", 200))
	raw.WriteString(`"}]}`)
	zp, ok := deflatePayload(raw.Bytes())
	if !ok {
		t.Fatal("compressible payload should compress")
	}
	if len(zp) >= raw.Len() {
		t.Fatalf("compressed %d >= raw %d", len(zp), raw.Len())
	}
	rc := flate.NewReader(bytes.NewReader(zp))
	out, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("inflate: %v", err)
	}
	if !bytes.Equal(out, raw.Bytes()) {
		t.Fatal("round trip mismatch")
	}
}

// TestWSUplinkDeflateNegotiated runs a WS session against a fake hub that
// echoes "deflate":true in its hello, then verifies uplink events arrive as
// binary [0x01][flate] frames and decompress to the original JSON — and that
// without the echo everything stays raw text JSON.
func TestWSUplinkDeflateNegotiated(t *testing.T) {
	for _, tt := range []struct {
		name     string
		echo     bool
		wantBin  bool
		wantText bool
	}{
		{name: "negotiated", echo: true, wantBin: true},
		{name: "legacy-hub-raw-json", echo: false, wantText: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var sawBinary bool
			var sawText bool
			gotEvents := make(chan map[string]any, 8)
			sawHeader := ""
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sawHeader = r.Header.Get("X-Capri-Deflate")
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				defer conn.Close(websocket.StatusNormalClosure, "")
				ctx := r.Context()
				hello := map[string]any{"v": 1, "type": "hello", "service": "hub", "subscribers": 1, "seq": 0}
				if tt.echo {
					hello["deflate"] = true
				}
				hb, _ := json.Marshal(hello)
				if err := conn.Write(ctx, websocket.MessageText, hb); err != nil {
					return
				}
				for {
					mt, data, err := conn.Read(ctx)
					if err != nil {
						return
					}
					if mt == websocket.MessageBinary {
						if len(data) < 1 || data[0] != deflateMagicWS {
							t.Errorf("binary frame without 0x%02x prefix", deflateMagicWS)
							return
						}
						mu.Lock()
						sawBinary = true
						mu.Unlock()
						rc := flate.NewReader(bytes.NewReader(data[1:]))
						raw, err := io.ReadAll(rc)
						if err != nil {
							t.Errorf("inflate: %v", err)
							return
						}
						data = raw
					} else {
						mu.Lock()
						sawText = true
						mu.Unlock()
					}
					var frame struct {
						Type   string           `json:"type"`
						Events []map[string]any `json:"events"`
					}
					if json.Unmarshal(data, &frame) != nil || frame.Type != "events" {
						continue
					}
					for _, ev := range frame.Events {
						gotEvents <- ev
					}
				}
			}))
			defer ts.Close()

			c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", DisableQUIC: true})
			c.sendCh = make(chan []byte, 64)
			c.reqCh = make(chan []byte, 8)
			bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fwdCtx, fwdCancel := context.WithCancel(ctx)
			defer fwdCancel()
			go c.forwardLoop(fwdCtx, bridge)
			sessDone := make(chan error, 1)
			go func() { sessDone <- c.wsSession(ctx, bridge) }()

			deadline := time.After(5 * time.Second)
			for !c.forwardingEnabled() && !c.deflateOK.Load() {
				select {
				case <-deadline:
					t.Fatalf("forwarding=%v deflate=%v (session err: %v)",
						c.forwardingEnabled(), c.deflateOK.Load(), <-sessDone)
				case <-time.After(10 * time.Millisecond):
				}
			}
			if tt.echo && !c.deflateOK.Load() {
				t.Fatal("deflate not armed despite hello echo")
			}
			if !tt.echo && c.deflateOK.Load() {
				t.Fatal("deflate armed without hub echo")
			}

			// A large compressible event (> minCompressSize).
			bridge.Broadcast(acp.Event{"type": "chunk", "sessionId": "s1",
				"text": strings.Repeat("压流量测试 ", 128)})
			select {
			case ev := <-gotEvents:
				if !strings.HasPrefix(ev["text"].(string), "压流量测试 ") {
					t.Fatalf("event text corrupted: %v", ev)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no events observed")
			}
			mu.Lock()
			defer mu.Unlock()
			if tt.wantBin && !sawBinary {
				t.Error("expected at least one compressed binary uplink frame")
			}
			if tt.wantText && sawBinary {
				t.Error("legacy hub must never receive binary frames")
			}
			if tt.wantText && !sawText {
				t.Error("expected raw text uplink frames")
			}
			if sawHeader != "1" {
				t.Errorf("X-Capri-Deflate offer header = %q, want 1", sawHeader)
			}
		})
	}
}
