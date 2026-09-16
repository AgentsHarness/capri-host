package acp

import "testing"

func TestResponseCompletedPreservesObservedTurnAndTools(t *testing.T) {
	for _, method := range []string{"session/update", "x.ai/session_notification"} {
		t.Run(method, func(t *testing.T) {
			b, _ := metaReadyBridge(t)
			ch, unsub := b.Subscribe()
			defer unsub()
			send := func(update map[string]any) {
				b.onAgentMessage(map[string]any{"method": method, "params": map[string]any{
					"sessionId": "s1", "_meta": map[string]any{"totalTokens": float64(1234)}, "update": update,
				}})
			}
			for i := 0; i < 3; i++ {
				start := map[string]any{"sessionUpdate": "tool_call", "title": "Read", "kind": "read", "status": "pending"}
				send(start)
				id, _ := start["toolCallId"].(string)
				if id == "" {
					t.Fatal("missing synthetic tool ID")
				}
				drainEvents(ch)
				send(map[string]any{"sessionUpdate": "response_completed", "stop_reason": "tool_use"})
				b.mu.Lock()
				s := b.sessions["s1"]
				busy, count, open := s.Busy, s.busyCount, b.turns["s1"].open
				b.mu.Unlock()
				if !busy || count != 0 || !open {
					t.Fatalf("response boundary closed turn: busy=%v count=%d open=%v", busy, count, open)
				}
				usage, rate := false, false
				for _, ev := range drainEvents(ch) {
					switch ev["type"] {
					case "turn_completed", "response_completed", "sessions_changed":
						t.Fatalf("response boundary emitted terminal state: %v", ev)
					case "usage":
						usage = ev["used"] == int64(1234)
					case "gen_rate":
						rate = ev["active"] == false
					}
				}
				if !usage || !rate {
					t.Fatalf("usage=%v generation reset=%v", usage, rate)
				}
				update := map[string]any{"sessionUpdate": "tool_call_update", "status": "completed"}
				send(update)
				if update["toolCallId"] != id {
					t.Fatalf("tool matching lost across response boundary: %v != %s", update, id)
				}
			}
			send(map[string]any{"sessionUpdate": "turn_completed", "stop_reason": "end_turn"})
			b.mu.Lock()
			busy := b.sessions["s1"].Busy
			open := len(b.liveTools["s1"].openCalls)
			b.mu.Unlock()
			if busy || open != 0 {
				t.Fatalf("turn_completed left busy=%v open tools=%d", busy, open)
			}
			if n := countEvents(ch, "turn_completed"); n != 1 {
				t.Fatalf("turn terminal count %d", n)
			}
		})
	}
}

func TestResponseBoundaryInHistoryPreservesToolMatching(t *testing.T) {
	start := map[string]any{"sessionUpdate": "tool_call", "title": "Read", "kind": "read"}
	done := map[string]any{"sessionUpdate": "tool_call_update", "status": "completed"}
	updates := []any{
		sliceEnv(1000, "e1", start),
		sliceEnv(1001, "e2", map[string]any{"sessionUpdate": "response_completed"}),
		sliceEnv(1002, "e3", done),
	}
	normalizeSyntheticToolCallsInSlice(updates)
	if start["toolCallId"] == nil || done["toolCallId"] != start["toolCallId"] {
		t.Fatalf("history tool match lost: start=%v done=%v", start, done)
	}
}

func TestResponseCompletedPreservesSeededToolStarts(t *testing.T) {
	r := &liveToolResolver{
		openCalls:     []openCall{{id: "synth:call:1:0"}},
		pendingStarts: []openCall{{id: "synth:call:1:0", tsMs: 1}},
	}
	r.handleLive("response_completed", nil, 0, "")
	if len(r.openCalls) != 1 || len(r.pendingStarts) != 1 {
		t.Fatal("model response cleared tool matching state")
	}
	r.handleLive("turn_completed", nil, 0, "")
	if len(r.openCalls) != 0 || len(r.pendingStarts) != 0 {
		t.Fatal("turn end retained tool matching state")
	}
}
