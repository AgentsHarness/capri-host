package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/AgentsHarness/capri-wire"
)

// wsHub is a minimal v2 hub over WebSocket: it completes the handshake,
// records what the host sends, auto-acks events, and lets a test push downlink
// frames (audience, request, hb).
type wsHub struct {
	t    *testing.T
	srv  *httptest.Server
	tok  string
	live wire.Liveness

	mu      sync.Mutex
	conns   int
	pairs   int
	helloOf *wire.Hello
	events  []wire.Frame // every events frame the host sent
	hb      []wire.Frame
	respond []wire.Frame
	others  []wire.Frame
	cur     *wsHubConn
	connCh  chan *wsHubConn

	// refuse makes the upgrade answer 401 instead of 101.
	refuse bool
	// audience is what hello_ack (and every audience a test pushes) starts
	// with.
	audience wire.Audience
	// noAutoAck leaves events unacknowledged.
	noAutoAck bool
	// binaryFrames records whether any uplink frame arrived compressed.
	binary int

	ackUp atomic64
}

// atomic64 is a tiny counter so the hub-side ack can be read without mu.
type atomic64 struct {
	mu sync.Mutex
	n  uint64
}

func (a *atomic64) load() uint64 { a.mu.Lock(); defer a.mu.Unlock(); return a.n }
func (a *atomic64) store(n uint64) {
	a.mu.Lock()
	a.n = n
	a.mu.Unlock()
}

func newWSHub(t *testing.T, aud wire.Audience) *wsHub {
	t.Helper()
	h := &wsHub{
		t:        t,
		tok:      "tok123",
		live:     wire.Liveness{Keepalive: 1, Idle: 60},
		audience: aud,
		connCh:   make(chan *wsHubConn, 8),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pair", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Code     string `json:"code"`
			HostID   string `json:"hostId"`
			HostName string `json:"hostName"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		h.mu.Lock()
		h.pairs++
		h.mu.Unlock()
		if body.Code != "ABC123" {
			writeTestJSON(w, 401, map[string]any{"ok": false, "error": "配对码无效或已过期"})
			return
		}
		writeTestJSON(w, 200, map[string]any{"ok": true, "token": h.tok, "hostId": body.HostID})
	})
	mux.HandleFunc("GET /ws/host", h.handle)
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
	return h
}

func (h *wsHub) url() string { return h.srv.URL }

func (h *wsHub) handle(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	refuse := h.refuse
	h.mu.Unlock()
	if refuse || r.Header.Get("Authorization") != "Bearer "+h.tok {
		http.Error(w, "token 无效", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(16 << 20)
	cc := &wsHubConn{conn: conn, h: h}
	h.mu.Lock()
	h.conns++
	h.cur = cc
	h.mu.Unlock()
	h.connCh <- cc

	data, err := cc.read()
	if err != nil {
		return
	}
	fr, err := wire.Decode(data)
	if err != nil || fr.Type != wire.TypeHello {
		h.t.Logf("hub: bad handshake %q: %v", data, err)
		return
	}
	hello, err := fr.Hello()
	if err != nil {
		return
	}
	h.mu.Lock()
	h.helloOf = &hello
	aud := h.audience
	h.mu.Unlock()

	ack := wire.NewHelloAck(wire.HelloAck{
		Epoch:    hello.Epoch,
		AckUp:    h.ackUp.load(),
		AckDown:  0,
		Audience: &aud,
		Liveness: &h.live,
		Limits:   testLimits(),
		Caps:     &wire.Caps{Deflate: true},
	})
	if err := cc.send(ack); err != nil {
		return
	}

	for {
		data, err := cc.read()
		if err != nil {
			return
		}
		f, err := wire.Decode(data)
		if err != nil {
			h.t.Logf("hub: bad frame: %v", err)
			return
		}
		switch f.Type {
		case wire.TypeEvents:
			last := f.Seq + uint64(len(f.Events)) - 1
			h.mu.Lock()
			h.events = append(h.events, f)
			h.mu.Unlock()
			h.mu.Lock()
			noAck := h.noAutoAck
			h.mu.Unlock()
			if noAck {
				continue
			}
			h.ackUp.store(last)
			hb, err := wire.NewHeartbeat(last, wire.HeartbeatStatus{Ready: true})
			if err == nil {
				_ = cc.send(hb)
			}
		case wire.TypeHeartbeat:
			h.mu.Lock()
			h.hb = append(h.hb, f)
			h.mu.Unlock()
			if f.Ack > h.ackUp.load() {
				h.ackUp.store(f.Ack)
			}
		case wire.TypeRespond:
			h.mu.Lock()
			h.respond = append(h.respond, f)
			h.mu.Unlock()
		default:
			h.mu.Lock()
			h.others = append(h.others, f)
			h.mu.Unlock()
		}
	}
}

// conn returns the live host connection, waiting for it.
func (h *wsHub) conn(t *testing.T) *wsHubConn {
	t.Helper()
	h.mu.Lock()
	cc := h.cur
	h.mu.Unlock()
	if cc != nil {
		return cc
	}
	select {
	case cc := <-h.connCh:
		return cc
	case <-time.After(5 * time.Second):
		t.Fatal("host never connected")
	}
	return nil
}

func (h *wsHub) push(t *testing.T, f wire.Frame) {
	t.Helper()
	if err := h.conn(t).send(f); err != nil {
		t.Fatalf("hub write %s: %v", f.Type, err)
	}
}

// waitEvent waits for an events frame whose run includes text.
func (h *wsHub) waitEvent(t *testing.T, text string, timeout time.Duration) wire.Frame {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		var found wire.Frame
		for _, fr := range h.events {
			for _, raw := range fr.Events {
				var ev struct {
					Text string `json:"text"`
				}
				if json.Unmarshal(raw, &ev) == nil && ev.Text == text {
					found = fr
				}
			}
		}
		h.mu.Unlock()
		if found.Seq != 0 {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("event %q never reached the hub", text)
	return wire.Frame{}
}

func (h *wsHub) eventFrames() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.events)
}

func (h *wsHub) eventCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, fr := range h.events {
		n += len(fr.Events)
	}
	return n
}

func (h *wsHub) waitRespond(t *testing.T, reqID string, timeout time.Duration) wire.Frame {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		for _, f := range h.respond {
			if f.ReqID == reqID {
				h.mu.Unlock()
				return f
			}
		}
		h.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no respond for reqId %s", reqID)
	return wire.Frame{}
}

// wsHubConn is one live host connection from the hub's side.
type wsHubConn struct {
	conn *websocket.Conn
	h    *wsHub
	mu   sync.Mutex
}

func (c *wsHubConn) send(f wire.Frame) error {
	b, err := wire.Encode(f)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Write(ctx, websocket.MessageText, b)
}

func (c *wsHubConn) read() ([]byte, error) {
	typ, data, err := c.conn.Read(context.Background())
	if err != nil {
		return nil, err
	}
	if typ == websocket.MessageBinary {
		c.h.mu.Lock()
		c.h.binary++
		c.h.mu.Unlock()
		return inflatePayload(data)
	}
	return data, nil
}

func writeTestJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
