package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/AgentsHarness/capri-hub/wire"
)

// TestWSRecvReassemblesInterleavedUnits: a binary message is one transport
// unit; a complete unit is delivered while another frame's fragments are open,
// and a compressed frame is inflated after reassembly. Text stays a whole JSON
// frame.
func TestWSRecvReassemblesInterleavedUnits(t *testing.T) {
	peerCh := make(chan *websocket.Conn, 1)
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		peerCh <- conn
		<-done
	}))
	defer srv.Close()
	defer close(done)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(wsDownlinkLimit)

	tr := newWSTransport(conn)
	tr.SetMaxFrame(wire.DefaultLimits.MaxFrame)
	peer := <-peerCh

	events := wire.Frame{
		V:    wire.ProtocolVersion,
		Type: wire.TypeEvents,
		Seq:  1,
		Events: []json.RawMessage{
			json.RawMessage(`{"type":"chunk","seq":1,"text":"` + incompressibleString(t, 96<<10) + `"}`),
		},
	}
	payload, err := wire.Encode(events)
	if err != nil {
		t.Fatal(err)
	}
	zp, ok := deflatePayload(payload)
	if !ok {
		t.Fatal("fixture did not compress")
	}
	units, err := wire.SplitFrame(zp, true, 5, wire.UnitSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(units) < 3 {
		t.Fatalf("fixture spans %d units, want at least 3", len(units))
	}

	hb, err := wire.NewHeartbeat(0, wire.HeartbeatStatus{Ready: true})
	if err != nil {
		t.Fatal(err)
	}
	hbPayload, err := wire.Encode(hb)
	if err != nil {
		t.Fatal(err)
	}
	writeUnit := func(unit []byte) {
		if err := peer.Write(ctx, websocket.MessageBinary, unit); err != nil {
			t.Fatalf("peer write: %v", err)
		}
	}
	for i, unit := range units {
		if i == 1 {
			writeUnit(wire.EncodeUnit(wire.Unit{Total: uint32(len(hbPayload)), Data: hbPayload}))
		}
		writeUnit(unit)
	}

	// The complete unit overtakes the rest of the fragmented frame.
	first, err := tr.Recv()
	if err != nil {
		t.Fatalf("Recv (interleaved hb): %v", err)
	}
	if fr, err := wire.Decode(first); err != nil || fr.Type != wire.TypeHeartbeat {
		t.Fatalf("first frame = %s (%v), want the interleaved hb", first, err)
	}

	second, err := tr.Recv()
	if err != nil {
		t.Fatalf("Recv (assembled frame): %v", err)
	}
	fr, err := wire.Decode(second)
	if err != nil {
		t.Fatalf("assembled frame: %v", err)
	}
	if fr.Type != wire.TypeEvents || fr.Seq != 1 || len(fr.Events) != 1 {
		t.Fatalf("assembled frame = %s seq %d with %d events", fr.Type, fr.Seq, len(fr.Events))
	}

	// A text message is one complete JSON frame, cut straight through.
	text, err := wire.Encode(audienceFrame(9, 0, watchingAudience(2, "s1")))
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(ctx, websocket.MessageText, text); err != nil {
		t.Fatalf("peer text write: %v", err)
	}
	third, err := tr.Recv()
	if err != nil {
		t.Fatalf("Recv (text): %v", err)
	}
	if string(third) != string(text) {
		t.Fatalf("text frame = %s, want %s", third, text)
	}

	if d := time.Since(tr.LastActivity()); d > 2*time.Second {
		t.Fatalf("LastActivity is %v stale", d)
	}
}
