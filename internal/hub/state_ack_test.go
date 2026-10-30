package hub

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// startStateAckSession runs a real wsSession against a fake hub that replays
// frames, and returns the client plus the channel the fake hub decodes
// state_ack frames into.
func startStateAckSession(t *testing.T, frames []string) (*Client, <-chan map[string]any) {
	t.Helper()
	fh := newFakeHub(t)
	fh.streamFrames = frames
	ts := httptest.NewServer(fh.handler())
	t.Cleanup(ts.Close)

	c := NewClient(Config{URL: ts.URL, HostID: "h1", Token: "tok123", StateFile: filepath.Join(t.TempDir(), "hub.json"), DisableQUIC: true})
	if err := c.ensureToken(context.Background()); err != nil {
		t.Fatalf("ensureToken: %v", err)
	}
	c.sendCh = make(chan []byte, 64)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.wsSession(ctx, bridge) }()
	return c, fh.acks
}

// awaitAck reads state_ack frames until one of kind with gen arrives, failing
// on timeout. Any ack for a different kind is skipped, so the two families
// can be asserted independently.
func awaitAck(t *testing.T, ch <-chan map[string]any, kind string, gen float64) map[string]any {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ack := <-ch:
			if ack["kind"] != kind {
				continue
			}
			if got, _ := ack["gen"].(float64); got != gen {
				t.Fatalf("state_ack kind=%s gen=%v, want gen %v", kind, ack["gen"], gen)
			}
			return ack
		case <-deadline:
			t.Fatalf("no state_ack kind=%s gen=%v within 3s", kind, gen)
		}
	}
}

// A negotiation-era hello that omits `stateAck` no longer exists: confirmed
// state delivery is part of the protocol, so every applied frame is acked.
func TestStateAckSentForAppliedState(t *testing.T) {
	c, acks := startStateAckSession(t, []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"subscribers","count":1,"gen":9}`,
		`{"v":1,"type":"viewing","all":false,"sessions":["s1"],"gen":4}`,
	})

	sub := awaitAck(t, acks, "subscribers", 9)
	if sub["type"] != "state_ack" {
		t.Fatalf("ack frame type = %v, want state_ack", sub["type"])
	}
	if v, _ := sub["v"].(float64); v != 1 {
		t.Fatalf("ack v = %v, want 1", sub["v"])
	}
	awaitAck(t, acks, "viewing", 4)

	if !c.forwardingEnabled() {
		t.Fatal("subscribers:1 should have enabled forwarding")
	}
}

// hello.subsGen is applied and acked on the spot. Without this the hub had to
// retransmit the same subscriber count as a standalone `subscribers` frame a
// second later on every connection; the retransmit remains only as the repair
// path for a lost ack.
func TestStateAckForHelloSubsGen(t *testing.T) {
	c, acks := startStateAckSession(t, []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":1,"subsGen":5}`,
	})
	awaitAck(t, acks, "subscribers", 5)
	if !c.forwardingEnabled() {
		t.Fatal("hello.subsGen count was not applied")
	}
}

// An old hub that stamps nothing sends no gen, so there is nothing to ack —
// and the host must not invent one.
func TestHelloWithoutSubsGenSendsNoAck(t *testing.T) {
	_, acks := startStateAckSession(t, []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":1}`,
	})
	select {
	case ack := <-acks:
		t.Fatalf("unstamped hello was acked: %v", ack)
	case <-time.After(200 * time.Millisecond):
	}
}

// A ping-carried re-assert is acked as well: the hub retransmits until it
// sees an ack, so silence would be read as a lost frame.
func TestStateAckSentForPingCarriedState(t *testing.T) {
	_, acks := startStateAckSession(t, []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":1}`,
		`{"v":1,"type":"subscribers","count":1,"gen":12}`,
	})
	awaitAck(t, acks, "subscribers", 12)
}

// A duplicate of the newest generation is applied once and still acked: a
// lost ack must not turn into a retransmit storm.
func TestStateAckRepeatedGeneration(t *testing.T) {
	c, acks := startStateAckSession(t, []string{
		`{"v":1,"type":"hello","service":"hub","subscribers":0}`,
		`{"v":1,"type":"subscribers","count":1,"gen":7}`,
		`{"v":1,"type":"subscribers","count":1,"gen":7}`,
	})
	awaitAck(t, acks, "subscribers", 7)
	awaitAck(t, acks, "subscribers", 7)
	if !c.forwardingEnabled() {
		t.Fatal("duplicate gen must not have paused forwarding")
	}

	// A stale gen is neither applied nor acked: an ack for it would be
	// answering state the host rejected.
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})
	if c.applySubscribers(0, 6, bridge, true) {
		t.Fatal("stale gen 6 reported as applied")
	}
	select {
	case ack := <-acks:
		t.Fatalf("stale gen was acked: %v", ack)
	case <-time.After(100 * time.Millisecond):
	}
	if !c.forwardingEnabled() {
		t.Fatal("stale gen 6 paused forwarding")
	}
}

// Confirmed state delivery is unconditional: a stamped state frame is always
// acknowledged. An unstamped one carries no generation to confirm.
func TestStateAckGate(t *testing.T) {
	c := NewClient(Config{HostID: "h1"})
	c.sendCh = make(chan []byte, 8)

	c.sendStateAck("subscribers", 1)
	select {
	case payload := <-c.sendCh:
		var f map[string]any
		if json.Unmarshal(payload, &f) != nil || f["type"] != "state_ack" {
			t.Fatalf("client sent %s, want a state_ack", payload)
		}
	default:
		t.Fatal("client sent no state_ack")
	}
	// gen 0 (an unstamped state frame) is never acked.
	c.sendStateAck("subscribers", 0)
	select {
	case payload := <-c.sendCh:
		t.Fatalf("unstamped state was acked: %s", payload)
	default:
	}
}
