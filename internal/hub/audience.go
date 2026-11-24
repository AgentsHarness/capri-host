package hub

import (
	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-hub/wire"
	"github.com/AgentsHarness/capri-hub/wire/events"
	"log"
)

// ── audience / uplink filter ──────────────────────────────────────────

// setAudience installs the hub's absolute viewer state.
func (c *Client) setAudience(a wire.Audience) {
	next := make(map[string]bool, len(a.Sessions))
	for _, sid := range a.Sessions {
		if sid != "" {
			next[sid] = true
		}
	}
	c.audMu.Lock()
	prevN, prevAll, prevW := len(c.audSet), c.aud.All, c.aud.Watchers
	c.aud = a
	c.audKnown = true
	c.audSet = next
	c.audMu.Unlock()
	if prevW != a.Watchers {
		if a.Watchers == 0 {
			log.Printf("[hub-client] 无浏览器订阅，暂停事件上报（继续发送心跳）")
		} else {
			log.Printf("[hub-client] 浏览器在线 (watchers=%d)，开始上报事件", a.Watchers)
		}
	}
	if prevN != len(next) || prevAll != a.All {
		if a.All {
			log.Printf("[hub-client] 订阅页面未声明会话，上行事件不过滤")
		} else {
			log.Printf("[hub-client] 上行只发送 %d 个被查看会话的正文（其余只留序号）", len(next))
		}
	}
}

// forgetAudience drops the viewer state when the link ends: until the next
// hello_ack says otherwise, nobody is watching.
func (c *Client) forgetAudience() {
	c.audMu.Lock()
	c.audKnown = false
	c.audSet = nil
	c.audMu.Unlock()
	c.out.setPaused(true)
}

func (c *Client) watchers() int {
	c.audMu.Lock()
	defer c.audMu.Unlock()
	return c.aud.Watchers
}

// viewingFiltered reports whether events of sessionID must be replaced by a
// placeholder on the wire. Filtering only starts after the hub has sent an
// audience; a host that guessed "nobody is watching" would silently stop the
// transcript.
//
// A session that spawns a subagent stays watched through its children: the
// child transcript feeds the parent page's mini scrollback, and the page
// never declares the child id.
func (c *Client) viewingFiltered(sessionID string) bool {
	c.audMu.Lock()
	defer c.audMu.Unlock()
	if !c.audKnown || c.aud.All {
		return false
	}
	if sessionID == "" || c.audSet[sessionID] {
		return false
	}
	if parent := c.viewChildren[sessionID]; parent != "" && c.audSet[parent] {
		return false
	}
	return true
}

// noteViewingChild records a subagent child session so its stream follows
// its parent's watch state (see viewingFiltered). One level, same rule as the
// hub's guest whitelist; the table is a bounded FIFO because a long-lived host
// spawns without limit. Evicting the oldest relation is safe: the child is
// merely trimmed until the hub re-supplies it.
func (c *Client) noteViewingChild(childSessionID, subagentID, parentSessionID string) {
	if childSessionID == "" || parentSessionID == "" || childSessionID == parentSessionID {
		return
	}
	c.audMu.Lock()
	defer c.audMu.Unlock()
	c.setViewChildLocked(childSessionID, parentSessionID)
	if subagentID != "" {
		c.setViewChildLocked(subagentID, parentSessionID)
	}
}

// setViewChildLocked inserts key without letting viewChildren exceed
// viewingChildCap, evicting oldest-first. Caller holds audMu.
func (c *Client) setViewChildLocked(key, parentSessionID string) {
	if c.viewChildren == nil {
		c.viewChildren = make(map[string]string, 16)
	}
	if _, ok := c.viewChildren[key]; ok {
		c.viewChildren[key] = parentSessionID
		return
	}
	for len(c.viewChildren) >= viewingChildCap {
		old := c.viewChildOrder[0]
		c.viewChildOrder = c.viewChildOrder[1:]
		delete(c.viewChildren, old)
	}
	c.viewChildren[key] = parentSessionID
	c.viewChildOrder = append(c.viewChildOrder, key)
}

// viewingChildCap bounds the parent/child table (FIFO eviction above).
const viewingChildCap = 4096

// trimUnwatched replaces the body of events belonging to sessions no browser
// page displays with a seq-only placeholder, in place, so per-host seq stays
// gapless and the FE's ordered delivery is untouched. The FE rebuilds an
// unwatched session from host history on switch.
//
// Which types carry a conversation body is decided by acp-hub/wire/events
// (trimmable:true); control rails and terminals stay unfiltered so a page
// switching in finds the session consistent.
//
// It runs before the event enters the uplink log, so what a later replay sends
// is the placeholder, not the payload nobody asked for.
func (c *Client) trimUnwatched(evs []acp.Event) {
	for _, ev := range evs {
		t, _ := ev["type"].(string)
		if t == "subagent_spawned" {
			// Register the relation even when the spawn itself is kept: the
			// child's later events must follow the parent's watch state.
			c.noteSpawnEvent(ev)
		}
		if !events.Trimmable(t) {
			continue
		}
		sid := eventSessionID(ev)
		if !c.viewingFiltered(sid) {
			continue
		}
		seq := ev["seq"]
		for k := range ev {
			delete(ev, k)
		}
		ev["type"] = "trimmed"
		ev["sessionId"] = sid
		if seq != nil {
			ev["seq"] = seq
		}
	}
}

// noteSpawnEvent records a subagent_spawned event's parent/child relation
// (see noteViewingChild).
func (c *Client) noteSpawnEvent(ev acp.Event) {
	parent, _ := ev["sessionId"].(string)
	up, _ := ev["update"].(map[string]any)
	if up == nil {
		return
	}
	child, _ := up["child_session_id"].(string)
	subID, _ := up["subagent_id"].(string)
	c.noteViewingChild(child, subID, parent)
}

// eventSessionID reads an event's session id, preferring the top-level
// `sessionId` (the withSid convention) and falling back to the params copy
// older generic carriers use.
func eventSessionID(ev acp.Event) string {
	if sid, _ := ev["sessionId"].(string); sid != "" {
		return sid
	}
	if p, ok := ev["params"].(map[string]any); ok {
		if sid, _ := p["sessionId"].(string); sid != "" {
			return sid
		}
	}
	return ""
}
