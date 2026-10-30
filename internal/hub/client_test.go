package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"

	"github.com/AgentsHarness/capri-wire"
)

// runClient starts Run against hub and returns a stop func.
func runClient(t *testing.T, c *Client, bridge bridgeSource) (context.CancelFunc, chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx, bridge)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return cancel, done
}

func waitConnected(t *testing.T, c *Client) {
	t.Helper()
	waitConnectedWithin(t, c, 5*time.Second)
}

func waitConnectedWithin(t *testing.T, c *Client, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for !c.connected.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("never connected (last error: %s)", c.State().LastError)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRunForwardsEventsOverWS is the end-to-end happy path: hello, hello_ack,
// events batched into the uplink log, and the hub's ack is what lets the log
// shrink.
func TestRunForwardsEventsOverWS(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1, "s1"))
	bridge := newTestBridge()
	c := NewClient(Config{URL: hub.url(), HostID: "h1", HostName: "H1", Token: "tok123", DisableQUIC: true})
	runClient(t, c, bridge)
	waitConnected(t, c)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "hello-hub"})
	fr := hub.waitEvent(t, "hello-hub", 5*time.Second)
	if fr.Seq != 1 || len(fr.Events) == 0 {
		t.Fatalf("frame = seq %d with %d events", fr.Seq, len(fr.Events))
	}
	var ev struct {
		Seq       uint64 `json:"seq"`
		SessionID string `json:"sessionId"`
		HostID    string `json:"hostId"`
	}
	if err := json.Unmarshal(fr.Events[0], &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 1 || ev.SessionID != "s1" {
		t.Fatalf("event = %+v", ev)
	}
	if ev.HostID != "" {
		t.Fatalf("host tag leaked into the uplink event: %+v", ev)
	}

	// The hub's automatic ack must drain the log.
	deadline := time.Now().Add(3 * time.Second)
	for c.State().OutboxEvents > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("uplink log never drained: %+v", c.State())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRunAudiencePausesAndResumes: watchers == 0 holds the uplink, a later
// snapshot flushes the backlog.
func TestRunAudiencePausesAndResumes(t *testing.T) {
	hub := newWSHub(t, wire.Audience{Gen: 1, Watchers: 0, Sessions: []string{"s1"}})
	bridge := newTestBridge()
	c := NewClient(Config{URL: hub.url(), HostID: "h1", Token: "tok123", DisableQUIC: true})
	runClient(t, c, bridge)
	waitConnected(t, c)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "queued-while-paused"})
	time.Sleep(300 * time.Millisecond)
	if n := hub.eventCount(); n != 0 {
		t.Fatalf("hub received %d events while nobody watched", n)
	}

	hub.push(t, audienceFrame(1, 0, watchingAudience(2, "s1")))
	hub.waitEvent(t, "queued-while-paused", 5*time.Second)
}

// TestRunRelayRoundTrip: a downlink request executes locally and the answer
// rides a respond frame.
func TestRunRelayRoundTrip(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1))
	var hits atomic.Int32
	c := NewClient(Config{
		URL: hub.url(), HostID: "h1", Token: "tok123", DisableQUIC: true,
		Local: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			if got := r.Header.Get("Authorization"); got != "Bearer tok" {
				t.Errorf("relay request lost the access token: %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"path":"` + r.URL.Path + `"}`))
		}),
		AccessToken: "tok",
	})
	runClient(t, c, newTestBridge())
	waitConnected(t, c)

	hub.push(t, requestFrame(1, 0, "41", "POST", "/api/prompt", `{"x":1}`))
	fr := hub.waitRespond(t, "41", 5*time.Second)
	if fr.Seq != 0 {
		t.Errorf("respond seq = %d, want 0 (relay plane)", fr.Seq)
	}
	if fr.RespondStatus() != 200 {
		t.Errorf("respond status = %d", fr.RespondStatus())
	}
	var body struct {
		OK   bool   `json:"ok"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(fr.Body, &body); err != nil {
		t.Fatalf("respond body %s: %v", fr.Body, err)
	}
	if !body.OK || body.Path != "/api/prompt" {
		t.Fatalf("respond body = %+v", body)
	}
	if hits.Load() != 1 {
		t.Fatalf("handler hits = %d", hits.Load())
	}
}

// TestRunCompressesLargeUplinkFrames: past the 256-byte floor the frame rides
// as one raw-deflate binary message.
func TestRunCompressesLargeUplinkFrames(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1, "s1"))
	bridge := newTestBridge()
	c := NewClient(Config{URL: hub.url(), HostID: "h1", Token: "tok123", DisableQUIC: true})
	runClient(t, c, bridge)
	waitConnected(t, c)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": strings.Repeat("compressible ", 200)})
	hub.waitEvent(t, strings.Repeat("compressible ", 200), 5*time.Second)
	hub.mu.Lock()
	binary := hub.binary
	hub.mu.Unlock()
	if binary == 0 {
		t.Fatal("large uplink frame was not compressed")
	}
}

// TestRunResumesAfterHubRestart: a second connection carries the hub's
// hello_ack watermarks and replays what the previous link never acked.
func TestRunResumesAfterHubRestart(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1, "s1"))
	hub.noAutoAck = true
	bridge := newTestBridge()
	c := NewClient(Config{URL: hub.url(), HostID: "h1", Token: "tok123", DisableQUIC: true})
	runClient(t, c, bridge)
	waitConnected(t, c)

	bridge.push(acp.Event{"type": "chunk", "sessionId": "s1", "text": "unacked"})
	hub.waitEvent(t, "unacked", 5*time.Second)

	// Drop the link; the next handshake must replay the unacked event.
	before := hub.eventFrames()
	if err := hub.conn(t).conn.Close(websocketCloseCode(wire.CloseNormal), "restart"); err != nil {
		t.Logf("close: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		conns := hub.conns
		frames := len(hub.events)
		var last wire.Frame
		if frames > 0 {
			last = hub.events[frames-1]
		}
		hub.mu.Unlock()
		if conns > 1 && frames > before && last.Seq == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("host never replayed the unacked event after reconnecting")
}

// TestRunRepairsOnAuthRejection: a rejected credential triggers exactly one
// re-pair, then the session comes up on the new token.
func TestRunRepairsOnAuthRejection(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1))
	hub.refuse = true
	c := NewClient(Config{
		URL: hub.url(), HostID: "h1", PairCode: "ABC123",
		StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true,
	})
	runClient(t, c, newTestBridge())
	deadline := time.Now().Add(10 * time.Second)
	for {
		hub.mu.Lock()
		pairs := hub.pairs
		hub.mu.Unlock()
		if pairs >= 2 {
			break // the first token was refused and the host re-paired
		}
		if time.Now().After(deadline) {
			t.Fatalf("a refused handshake never triggered a re-pair (pairs=%d)", pairs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st := c.State(); st.LastError == "" {
		t.Fatal("a refused handshake left no error behind")
	}
}

// TestRunFallsBackToWS: a UDP path that cannot be established must not cost
// the session; Run dials the WebSocket instead.
func TestRunFallsBackToWS(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1))
	c := NewClient(Config{
		URL: hub.url(), HostID: "h1", Token: "tok123",
		QUICHost: "127.0.0.1:1", // nothing listens on this UDP port
	})
	runClient(t, c, newTestBridge())
	// The QUIC dial has to time out before the fallback starts.
	waitConnectedWithin(t, c, 20*time.Second)
	if tr := c.State().Transport; tr != TransportWS {
		t.Fatalf("transport = %q, want the ws fallback", tr)
	}
}

// TestRunSupersededDoesNotRepair: being replaced by a newer connection keeps
// the credential.
func TestRunSupersededDoesNotRepair(t *testing.T) {
	hub := newWSHub(t, watchingAudience(1))
	path := filepath.Join(t.TempDir(), "hub.json")
	c := NewClient(Config{URL: hub.url(), HostID: "h1", Token: "tok123", PairCode: "ABC123",
		StateFile: path, DisableQUIC: true})
	c.stateMu.Lock()
	c.token = "tok123"
	c.saveStateLocked(stateFile{URL: hub.url(), HostID: "h1", Token: "tok123"})
	c.stateMu.Unlock()
	runClient(t, c, newTestBridge())
	waitConnected(t, c)

	// A superseded close must redial, not re-pair: the token file survives.
	if err := hub.conn(t).conn.Close(websocketCloseCode(wire.CloseSuperseded), "superseded"); err != nil {
		t.Logf("close: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		conns := hub.conns
		hub.mu.Unlock()
		if conns > 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	hub.mu.Lock()
	pairs := hub.pairs
	hub.mu.Unlock()
	if pairs != 0 {
		t.Fatal("superseded redial re-paired")
	}
}
