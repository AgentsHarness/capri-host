package acp

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// ── session/update full passthrough ─────────────────────────────────

// Any UNMODELED sessionUpdate kind must be forwarded verbatim as a
// session_notification event (the forward-compat carrier — modeled kinds
// now emit only their typed event, see
// TestSessionUpdateKindTypedOnly). Sample the persist-only kinds and a
// couple of hypothetical future kinds.
func TestSessionUpdateFullPassthrough(t *testing.T) {
	kinds := []string{
		"compaction_checkpoint",
		"rewind_marker",
		"unknown",
		"future_kind_alpha",
		"future_kind_beta",
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{"sessionUpdate": kind, "some": "payload"}
			b.handleSessionUpdate(map[string]any{
				"sessionId": "s1",
				"update":    update,
			})
			select {
			case ev := <-ch:
				if ev["type"] != "session_notification" {
					t.Fatalf("event type = %v, want session_notification", ev["type"])
				}
				if ev["method"] != "session/update" {
					t.Errorf("event method = %v, want session/update", ev["method"])
				}
				params, _ := ev["params"].(map[string]any)
				if !reflect.DeepEqual(params, map[string]any{"update": update}) {
					t.Errorf("event params = %v, want the update forwarded verbatim", params)
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("event sessionId = %v, want s1", ev["sessionId"])
				}
				// 只发 generic：不得再有第二个事件。
				select {
				case extra := <-ch:
					t.Fatalf("unexpected extra event: %v", extra)
				case <-time.After(50 * time.Millisecond):
				}
			case <-time.After(time.Second):
				t.Fatal("no session_notification event broadcast")
			}
		})
	}
}

// turn_completed / response_completed 都是回合终态 kind：usage 提取一致
// （_meta.totalTokens + update.usage，handleXaiNotification parity），但只有
// turn_completed 发 typed 事件（FE 回合封口语义, update verbatim）。
// response_completed 实测从不被 agent 发出（updates.jsonl 3383/3383 回合
// 终态均为 turn_completed），FE 也无消费（turnEnd.ts 无 case，events.ts
// 重写回 generic 后 notifApps.ts 显式忽略）——只保留副作用（gen_rate
// active:false、usage 提取）。Modeled → 均无 generic session_notification。
func TestTurnCompletedSessionUpdateTypedAndUsage(t *testing.T) {
	t.Run("turn_completed", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		ch, unsub := b.Subscribe()
		defer unsub()
		usage := map[string]any{"totalTokens": float64(55)}
		b.handleSessionUpdate(map[string]any{
			"sessionId": "s1",
			"_meta":     map[string]any{"totalTokens": float64(1234)},
			"update": map[string]any{
				"sessionUpdate": "turn_completed",
				"usage":         usage,
			},
		})
		var sawTyped, sawUsage bool
		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			select {
			case ev := <-ch:
				switch ev["type"] {
				case "session_notification":
					t.Errorf("unexpected generic session_notification for modeled kind turn_completed: %v", ev)
				case "turn_completed":
					sawTyped = true
					if !reflect.DeepEqual(ev["update"], map[string]any{"sessionUpdate": "turn_completed", "usage": usage}) {
						t.Errorf("typed update = %v, want the original update verbatim", ev["update"])
					}
					if ev["sessionId"] != "s1" {
						t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
					}
				case "usage":
					// 仅 turn 提取的 usage 事件同时带 carrier 级 used 与 usage 对象。
					if used, ok := asInt(ev["used"]); ok && used == 1234 {
						if u, ok := ev["usage"].(map[string]any); ok && reflect.DeepEqual(u, usage) {
							sawUsage = true
						}
					}
				}
			case <-time.After(20 * time.Millisecond):
			}
		}
		if !sawTyped {
			t.Error("no typed turn_completed event")
		}
		if !sawUsage {
			t.Error("no merged usage event (used=1234 + usage object) for turn_completed")
		}
	})
	t.Run("response_completed", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		ch, unsub := b.Subscribe()
		defer unsub()
		usage := map[string]any{"totalTokens": float64(55)}
		b.handleSessionUpdate(map[string]any{
			"sessionId": "s1",
			"_meta":     map[string]any{"totalTokens": float64(1234)},
			"update": map[string]any{
				"sessionUpdate": "response_completed",
				"usage":         usage,
			},
		})
		var sawTyped, sawUsage, sawGenRate bool
		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			select {
			case ev := <-ch:
				switch ev["type"] {
				case "session_notification":
					t.Errorf("unexpected generic session_notification for modeled kind response_completed: %v", ev)
				case "turn_completed", "response_completed":
					sawTyped = true
					t.Errorf("no typed event expected for response_completed, got %s: %v", ev["type"], ev)
				case "gen_rate":
					if active, _ := ev["active"].(bool); !active {
						sawGenRate = true
					}
				case "usage":
					if used, ok := asInt(ev["used"]); ok && used == 1234 {
						if u, ok := ev["usage"].(map[string]any); ok && reflect.DeepEqual(u, usage) {
							sawUsage = true
						}
					}
				}
			case <-time.After(20 * time.Millisecond):
			}
		}
		if sawTyped {
			t.Error("response_completed must not broadcast a typed event")
		}
		if !sawUsage {
			t.Error("no merged usage event (used=1234 + usage object) for response_completed")
		}
		if !sawGenRate {
			t.Error("no gen_rate active:false for response_completed")
		}
	})
}

// ── chunk meta forwarding ───────────────────────────────────────────

// agent_message_chunk must mirror user_message_chunk's meta forwarding:
// hideFromScrollback (chunk meta), displayText / displayAsCron
// (content-block meta) — alongside the existing messageId.
func TestAgentMessageChunkForwardsMeta(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"messageId":     "m-9",
			"_meta":         map[string]any{"hideFromScrollback": true},
			"content": map[string]any{
				"type":  "text",
				"text":  "hello",
				"_meta": map[string]any{"displayText": "Custom label", "displayAsCron": true},
			},
		},
	})
	ev := <-ch
	if ev["type"] != "chunk" || ev["text"] != "hello" {
		t.Fatalf("event = %v, want chunk hello", ev)
	}
	if ev["messageId"] != "m-9" {
		t.Errorf("messageId = %v, want m-9", ev["messageId"])
	}
	if ev["hideFromScrollback"] != true {
		t.Errorf("hideFromScrollback = %v, want true", ev["hideFromScrollback"])
	}
	if ev["displayText"] != "Custom label" || ev["displayAsCron"] != true {
		t.Errorf("content meta = %v, want displayText/displayAsCron", ev)
	}
}

// agent_thought_chunk forwards update._meta verbatim as the event's
// `meta` key.
func TestAgentThoughtChunkForwardsMeta(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	meta := map[string]any{"hideFromScrollback": true, "kind": "internal"}
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update": map[string]any{
			"sessionUpdate": "agent_thought_chunk",
			"_meta":         meta,
			"content":       map[string]any{"type": "text", "text": "thinking…"},
		},
	})
	ev := <-ch
	if ev["type"] != "thought" || ev["text"] != "thinking…" {
		t.Fatalf("event = %v, want thought event", ev)
	}
	if !reflect.DeepEqual(ev["meta"], meta) {
		t.Errorf("meta = %v, want %v", ev["meta"], meta)
	}
}

// ── session/resume ──────────────────────────────────────────────────

// session/resume must mirror LoadSession: same wire params (plus
// additionalDirectories), roster registration, active-session switch,
// ready event with the resume response's models/configOptions, and the
// busy focus-only path that never hits the wire.
func TestResumeSessionWireAndRoster(t *testing.T) {
	b, w := metaReadyBridge(t)
	ch, unsub := b.Subscribe()
	defer unsub()
	ctx := context.Background()

	done := make(chan callResult, 1)
	go func() {
		res, err := b.ResumeSession(ctx, "hist-1", "/ws")
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{
		"sessionId":     "hist-1",
		"models":        map[string]any{"currentModelId": "grok-4"},
		"configOptions": map[string]any{"yoloMode": true},
	})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("ResumeSession: %v", cr.err)
	}

	msg := w.last()
	if msg["method"] != "session/resume" {
		t.Fatalf("wire method = %v, want session/resume", msg["method"])
	}
	wantParams := map[string]any{
		"sessionId":             "hist-1",
		"cwd":                   "/ws",
		"mcpServers":            []any{},
		"additionalDirectories": []any{},
	}
	gotParams, _ := msg["params"].(map[string]any)
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Errorf("params = %v, want %v", gotParams, wantParams)
	}

	// Roster registration + active switch.
	b.mu.Lock()
	act := b.sessions["hist-1"]
	active := b.activeSessionID
	b.mu.Unlock()
	if act == nil {
		t.Fatal("hist-1 not registered in the roster")
	}
	if act.Cwd != "/ws" {
		t.Errorf("session cwd = %q, want /ws", act.Cwd)
	}
	if active != "hist-1" {
		t.Errorf("activeSessionID = %q, want hist-1", active)
	}

	// ready event broadcast with the resume response's models/configOptions.
	select {
	case ev := <-ch:
		if ev["type"] != "ready" || ev["sessionId"] != "hist-1" || ev["cwd"] != "/ws" {
			t.Fatalf("ready event = %v", ev)
		}
		if !reflect.DeepEqual(ev["models"], map[string]any{"currentModelId": "grok-4"}) {
			t.Errorf("ready models = %v", ev["models"])
		}
		if !reflect.DeepEqual(ev["configOptions"], map[string]any{"yoloMode": true}) {
			t.Errorf("ready configOptions = %v", ev["configOptions"])
		}
	case <-time.After(time.Second):
		t.Fatal("no ready event broadcast")
	}
}

// The busy focus-only path re-focuses without calling the agent (same as
// LoadSession): no wire request, ready + busy re-announced, cwd updated.
func TestResumeSessionBusyFocusOnly(t *testing.T) {
	b, w := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].Busy = true
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()

	res, err := b.ResumeSession(context.Background(), "s1", "/new-cwd")
	if err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	if len(w.lines) != 0 {
		t.Errorf("busy focus-only path must not write requests, got %d", len(w.lines))
	}
	if busy, _ := res["busy"].(bool); !busy {
		t.Errorf("result busy = %v, want true", res["busy"])
	}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			if ev["type"] != "ready" && ev["type"] != "busy" {
				t.Fatalf("event = %v, want ready or busy", ev)
			}
		case <-time.After(time.Second):
			t.Fatal("missing ready/busy event")
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sessions["s1"].Cwd != "/new-cwd" {
		t.Errorf("session cwd = %q, want /new-cwd", b.sessions["s1"].Cwd)
	}
	if b.activeSessionID != "s1" {
		t.Errorf("activeSessionID = %q, want s1", b.activeSessionID)
	}
}

// ── session/close ───────────────────────────────────────────────────

// session/close defaults to the active session, removes it from the
// roster, clears activeSessionID, and clears a matching last-session
// pointer (disk file untouched — restoring a closed session would 404).
func TestCloseSessionWireAndRoster(t *testing.T) {
	b, w := metaReadyBridge(t)
	b.mu.Lock()
	b.lastSessionID = "s1"
	b.lastSessionCwd = "/ws"
	b.mu.Unlock()
	ctx := context.Background()

	done := make(chan callResult, 1)
	go func() {
		res, err := b.CloseSession(ctx, "")
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"ok": true})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("CloseSession: %v", cr.err)
	}

	msg := w.last()
	if msg["method"] != "session/close" {
		t.Fatalf("wire method = %v, want session/close", msg["method"])
	}
	gotParams, _ := msg["params"].(map[string]any)
	if !reflect.DeepEqual(gotParams, map[string]any{"sessionId": "s1"}) {
		t.Errorf("params = %v, want {sessionId: s1}", gotParams)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions["s1"]; ok {
		t.Error("s1 still in the roster after close")
	}
	if b.activeSessionID != "" {
		t.Errorf("activeSessionID = %q, want empty", b.activeSessionID)
	}
	if b.lastSessionID != "" || b.lastSessionCwd != "" {
		t.Errorf("last-session pointer = %q/%q, want cleared", b.lastSessionID, b.lastSessionCwd)
	}
}

// Closing an explicit non-active session must not clear the active one.
func TestCloseSessionExplicitID(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	done := make(chan callResult, 1)
	go func() {
		res, err := b.CloseSession(ctx, "other")
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"ok": true})
	if cr := <-done; cr.err != nil {
		t.Fatalf("CloseSession: %v", cr.err)
	}
	msg := w.last()
	gotParams, _ := msg["params"].(map[string]any)
	if gotParams["sessionId"] != "other" {
		t.Errorf("params sessionId = %v, want other", gotParams["sessionId"])
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.activeSessionID != "s1" {
		t.Errorf("activeSessionID = %q, want s1 untouched", b.activeSessionID)
	}
}

// Without any active session, session/close fails fast with a 404.
func TestCloseSessionNoActiveSession(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.ready = true
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()
	_, err := b.CloseSession(context.Background(), "")
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != 404 {
		t.Fatalf("err = %v, want HTTPError 404", err)
	}
}

// ── session/prompt optional fields ──────────────────────────────────

// PromptWithOpts rides the official optional fields on session/prompt:
// messageId and _meta are emitted only when non-empty.
func TestPromptWithOptsWire(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	done := make(chan callResult, 1)
	go func() {
		sr, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, PromptOpts{
			MessageID: "uuid-1",
			Meta:      map[string]any{"yoloMode": true},
		})
		done <- callResult{map[string]any{"stopReason": sr}, err}
	}()
	resolveNext(t, b, w, map[string]any{"stopReason": "end_turn"})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("PromptWithOpts: %v", cr.err)
	}
	if cr.res.(map[string]any)["stopReason"] != "end_turn" {
		t.Errorf("stopReason = %v, want end_turn", cr.res.(map[string]any)["stopReason"])
	}

	msg := w.last()
	if msg["method"] != "session/prompt" {
		t.Fatalf("wire method = %v, want session/prompt", msg["method"])
	}
	params, _ := msg["params"].(map[string]any)
	if params["sessionId"] != "s1" {
		t.Errorf("sessionId = %v, want s1", params["sessionId"])
	}
	if params["messageId"] != "uuid-1" {
		t.Errorf("messageId = %v, want uuid-1", params["messageId"])
	}
	meta, ok := params["_meta"].(map[string]any)
	if !ok || !reflect.DeepEqual(meta, map[string]any{"yoloMode": true}) {
		t.Errorf("_meta = %v, want {yoloMode: true}", params["_meta"])
	}
	if params["prompt"] == nil {
		t.Error("prompt missing from params")
	}
}

// Plain Prompt (thin wrapper) must not emit messageId/_meta — the wire
// stays byte-identical to the pre-opts shape.
func TestPromptOmitsOptsWhenEmpty(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	done := make(chan callResult, 1)
	go func() {
		sr, err := b.Prompt(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}})
		done <- callResult{map[string]any{"stopReason": sr}, err}
	}()
	resolveNext(t, b, w, map[string]any{"stopReason": "end_turn"})
	if cr := <-done; cr.err != nil {
		t.Fatalf("Prompt: %v", cr.err)
	}
	msg := w.last()
	params, _ := msg["params"].(map[string]any)
	if _, ok := params["messageId"]; ok {
		t.Errorf("messageId must be absent: %v", params)
	}
	if _, ok := params["_meta"]; ok {
		t.Errorf("_meta must be absent: %v", params)
	}
	want := map[string]any{
		"sessionId": "s1",
		"prompt":    []any{map[string]any{"type": "text", "text": "hi"}},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}

// The session/prompt response `_meta` must be passed through verbatim:
// returned by PromptWithOpts AND carried on the done event (`meta`). When
// the agent returns no `_meta`, the done event stays byte-identical (no
// `meta` key — absent key ≠ off).
func TestPromptResponseMetaPassthrough(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	done := make(chan callResult, 1)
	go func() {
		sr, meta, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, PromptOpts{})
		done <- callResult{map[string]any{"stopReason": sr, "meta": meta}, err}
	}()
	resolveNext(t, b, w, map[string]any{
		"stopReason": "end_turn",
		"_meta":      map[string]any{"turn_id": "t-1", "cost": float64(42)},
	})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("PromptWithOpts: %v", cr.err)
	}
	meta, ok := cr.res.(map[string]any)["meta"].(map[string]any)
	if !ok || meta["turn_id"] != "t-1" || meta["cost"] != float64(42) {
		t.Errorf("returned meta = %v, want {turn_id:t-1 cost:42}", cr.res.(map[string]any)["meta"])
	}

	// The done event must carry the same meta under `meta`.
	var ev Event
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case ev = <-sub:
			if ev["type"] == "done" {
				break drain
			}
		case <-deadline:
			t.Fatal("no done event broadcast")
		}
	}
	if !reflect.DeepEqual(ev["meta"], map[string]any{"turn_id": "t-1", "cost": float64(42)}) {
		t.Errorf("done event meta = %v, want {turn_id:t-1 cost:42}", ev["meta"])
	}
}

// Without a response `_meta` the done event must not carry a `meta` key.
func TestPromptDoneEventOmitsMetaWhenAbsent(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	var gotMeta map[string]any
	done := make(chan callResult, 1)
	go func() {
		sr, meta, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, PromptOpts{})
		gotMeta = meta
		done <- callResult{map[string]any{"stopReason": sr}, err}
	}()
	resolveNext(t, b, w, map[string]any{"stopReason": "end_turn"})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("PromptWithOpts: %v", cr.err)
	}
	if gotMeta != nil {
		t.Errorf("returned meta = %v, want nil", gotMeta)
	}
	var ev Event
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case ev = <-sub:
			if ev["type"] == "done" {
				break drain
			}
		case <-deadline:
			t.Fatal("no done event broadcast")
		}
	}
	if _, ok := ev["meta"]; ok {
		t.Errorf("done event must not carry meta when the agent returned none: %v", ev)
	}
}

// ── XaiCall ─────────────────────────────────────────────────────────

// XaiCall is the generic "_"-prefixed extension call: an empty
// sessionId/session_id in params is replaced with the active session
// (404 when none is active); keys absent from params stay absent; the
// RAW result envelope is returned untouched.
func TestXaiCall(t *testing.T) {
	ctx := context.Background()

	t.Run("empty sessionId fills active", func(t *testing.T) {
		b, w := readyBridge()
		done := make(chan callResult, 1)
		go func() {
			res, err := b.XaiCall(ctx, "x.ai/foo", map[string]any{"sessionId": ""})
			done <- callResult{res, err}
		}()
		resolveNext(t, b, w, map[string]any{"result": map[string]any{"ok": true}})
		cr := <-done
		if cr.err != nil {
			t.Fatalf("XaiCall: %v", cr.err)
		}
		// RAW result: the ExtMethodResult envelope is not unwrapped.
		if _, ok := cr.res.(map[string]any)["result"].(map[string]any); !ok {
			t.Errorf("result = %v, want raw envelope passthrough", cr.res)
		}
		msg := w.last()
		if msg["method"] != "_x.ai/foo" {
			t.Errorf("wire method = %v, want _x.ai/foo", msg["method"])
		}
		params, _ := msg["params"].(map[string]any)
		if params["sessionId"] != "s1" {
			t.Errorf("params sessionId = %v, want s1", params["sessionId"])
		}
	})

	t.Run("absent sessionId stays absent", func(t *testing.T) {
		b, w := readyBridge()
		done := make(chan callResult, 1)
		go func() {
			res, err := b.XaiCall(ctx, "x.ai/foo", map[string]any{})
			done <- callResult{res, err}
		}()
		resolveNext(t, b, w, map[string]any{"ok": true})
		cr := <-done
		if cr.err != nil {
			t.Fatalf("XaiCall: %v", cr.err)
		}
		params, _ := w.last()["params"].(map[string]any)
		if len(params) != 0 {
			t.Errorf("params = %v, want empty (no sessionId key)", params)
		}
	})

	t.Run("snake_case session_id fills active", func(t *testing.T) {
		b, w := readyBridge()
		done := make(chan callResult, 1)
		go func() {
			res, err := b.XaiCall(ctx, "x.ai/foo", map[string]any{"session_id": ""})
			done <- callResult{res, err}
		}()
		resolveNext(t, b, w, map[string]any{"ok": true})
		cr := <-done
		if cr.err != nil {
			t.Fatalf("XaiCall: %v", cr.err)
		}
		params, _ := w.last()["params"].(map[string]any)
		if params["session_id"] != "s1" {
			t.Errorf("params session_id = %v, want s1", params["session_id"])
		}
	})

	t.Run("no active session 404s", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		b.mu.Lock()
		b.ready = true
		b.stdin = discardWriteCloser{}
		b.mu.Unlock()
		_, err := b.XaiCall(ctx, "x.ai/foo", map[string]any{"sessionId": ""})
		var he *HTTPError
		if !errors.As(err, &he) || he.Code != 404 {
			t.Fatalf("err = %v, want HTTPError 404", err)
		}
	})

	t.Run("bare array result passes through verbatim", func(t *testing.T) {
		// workspace_list_recent's payload is a bare array; it must NOT be
		// coerced to {} (the regression this test guards).
		b, w := readyBridge()
		done := make(chan callResult, 1)
		go func() {
			res, err := b.XaiCall(ctx, "x.ai/foo", map[string]any{})
			done <- callResult{res, err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if msg := w.last(); msg != nil {
				if id := msg["id"]; id != nil {
					if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
						ch.(chan rpcResult) <- rpcResult{raw: []any{map[string]any{"id": "s1"}}}
						break
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		cr := <-done
		if cr.err != nil {
			t.Fatalf("XaiCall: %v", cr.err)
		}
		arr, ok := cr.res.([]any)
		if !ok || len(arr) != 1 {
			t.Fatalf("result = %v, want bare array passthrough", cr.res)
		}
		if m, _ := arr[0].(map[string]any); m["id"] != "s1" {
			t.Errorf("array[0] = %v, want {id:s1}", arr[0])
		}
	})
}

// ── NotificationMeta forwarding ─────────────────────────────────────

// chunk / user_chunk / thought must carry the shell-stamped authoritative
// timestamps (params._meta): turnStartMs (FE "Worked for Xs" anchoring),
// agentTimestampMs (user row ts correction), and the computed elapsedMs
// (agentTimestampMs - streamStartMs, real thought duration). Without them
// the FE measures adopted (queue-drained) turns from the adoption moment.
func TestStreamEventsForwardTurnStartMs(t *testing.T) {
	meta := map[string]any{
		"turnStartMs":      float64(1700000000000),
		"agentTimestampMs": float64(1700000000000 + 30000),
		"streamStartMs":    float64(1700000000000 + 20000),
	}
	cases := []struct {
		name   string
		kind   string
		want   string
		update map[string]any
	}{
		{
			name: "agent_message_chunk",
			kind: "agent_message_chunk",
			want: "chunk",
			update: map[string]any{
				"sessionUpdate": "agent_message_chunk",
				"content":       map[string]any{"type": "text", "text": "hi"},
			},
		},
		{
			name: "user_message_chunk",
			kind: "user_message_chunk",
			want: "user_chunk",
			update: map[string]any{
				"sessionUpdate": "user_message_chunk",
				"content":       map[string]any{"type": "text", "text": "q"},
			},
		},
		{
			name: "agent_thought_chunk",
			kind: "agent_thought_chunk",
			want: "thought",
			update: map[string]any{
				"sessionUpdate": "agent_thought_chunk",
				"content":       map[string]any{"type": "text", "text": "think"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			b.handleSessionUpdate(map[string]any{
				"sessionId": "s1",
				"_meta":     meta,
				"update":    c.update,
			})
			ev := <-ch
			if ev["type"] != c.want {
				t.Fatalf("event type = %v, want %s", ev["type"], c.want)
			}
			if ev["turnStartMs"] != float64(1700000000000) {
				t.Errorf("turnStartMs = %v, want 1700000000000（params._meta 权威盖章）", ev["turnStartMs"])
			}
			if ev["agentTimestampMs"] != float64(1700000030000) {
				t.Errorf("agentTimestampMs = %v, want 1700000030000", ev["agentTimestampMs"])
			}
			// 只有 thought 需要 elapsedMs,但统一计算无害。asInt 侧是 int64。
			if got, _ := ev["elapsedMs"].(int64); got != 10000 {
				t.Errorf("elapsedMs = %v, want 10000（agentTimestampMs - streamStartMs）", ev["elapsedMs"])
			}
		})
	}
}
