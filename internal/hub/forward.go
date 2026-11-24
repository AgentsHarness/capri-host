package hub

import (
	"context"
	"encoding/json"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-hub/wire"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ── event forwarding (bridge → uplink log) ────────────────────────────

// forwardLoop batches bridge events into the uplink log. The log is per
// process, not per session, so a disconnect never loses transcript: the next
// handshake replays from the hub's watermark.
func (c *Client) forwardLoop(ctx context.Context, bridge bridgeSource) {
	ch, unsub := bridge.Subscribe()
	defer unsub()
	batch := make([]acp.Event, 0, 32)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		// No chunk/thought text merge on the hub path: dual-path FE dedupes
		// by (hostId,seq); merging would keep the first seq and concatenate
		// text, so local SSE "a","b","c" (seq 1,2,3) would collide with hub
		// seq1 "abc". Batching (50ms / 32) still packs multiple events into
		// one frame — that is not text merge.
		evs := batch
		batch = make([]acp.Event, 0, 32)
		// Trim BEFORE the log so a later replay sends the same placeholders
		// instead of resurrecting payloads for sessions nobody is watching.
		c.trimUnwatched(evs)
		var overflow error
		for _, ev := range evs {
			raw, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if err := c.out.appendEvent(raw); err != nil {
				// The critical tail no longer fits. Keep logging the rest of
				// the batch — abandoning it would drop a turn terminal that
				// lands behind the overflow point, and the next append would
				// replace its seq with a silent placeholder — then end the
				// session so the hub replays after the reconnect.
				if overflow == nil {
					overflow = err
				}
			}
		}
		if overflow != nil && c.endSession(wire.CloseOverflow, overflow) {
			log.Printf("[hub-client] %v", overflow)
		}
		c.signal()
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			// Clone first: bridge.Broadcast hands the same map to every
			// subscriber (local SSE + hub forwardLoop). Mutating the shared
			// Event (stripFullUpdate) races with handleSSE's json.Marshal
			// and can crash the process with concurrent map write.
			ev = cloneEvent(ev)
			stripFullUpdate(ev)
			// Host registry fields only change on these transitions —
			// mirror them to the hub right away (a heartbeat) instead of
			// waiting for the next keepalive. client_request_resolved is here
			// for the DECREASE: pendingCount only drops when a request is
			// answered or times out.
			switch t, _ := ev["type"].(string); t {
			case "busy", "sessions_changed", "client_request", "client_request_resolved", "error":
				c.notifyStatus()
			}
			batch = append(batch, ev)
			if len(batch) >= 32 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func cloneEvent(ev acp.Event) acp.Event {
	out := make(acp.Event, len(ev))
	for k, v := range ev {
		out[k] = v
	}
	return out
}

func stripFullUpdate(ev acp.Event) {
	delete(ev, "fullUpdate")
}

// ── local HTTP surface ────────────────────────────────────────────────

// listenPort returns the local HTTP port advertised to the hub: Config.Port
// when set, else the port parsed from LocalBase (http://127.0.0.1:8765).
func (c *Client) listenPort() int {
	if c.cfg.Port > 0 && c.cfg.Port <= 65535 {
		return c.cfg.Port
	}
	if c.cfg.LocalBase == "" {
		return 0
	}
	u, err := url.Parse(c.cfg.LocalBase)
	if err != nil {
		return 0
	}
	p := u.Port()
	if p == "" {
		return 0
	}
	n, err := strconv.Atoi(p)
	if err != nil || n <= 0 || n > 65535 {
		return 0
	}
	return n
}

func (c *Client) wsURL() string {
	u := strings.TrimRight(c.cfg.URL, "/")
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	case strings.HasPrefix(u, "wss://") || strings.HasPrefix(u, "ws://"):
		// already ws
	default:
		u = "ws://" + u
	}
	return u + "/ws/host"
}
