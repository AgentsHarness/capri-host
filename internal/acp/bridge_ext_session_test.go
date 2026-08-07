package acp

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Session-scoped x.ai wrappers: wire method always carries the "_" prefix
// (bare names get -32601); REQUIRED sessionIds arrive as "" and XaiCall
// fills the active session ("s1" from readyBridge); OPTIONAL sessionIds
// are omitted entirely. Responses are unwrapped from the ExtMethodResult
// envelope by UnwrapExtResult.
func TestExtSessionMethodsWirePayloads(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			name:   "session/state fills active sessionId (camelCase)",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionState(ctx, "/ws") },
			method: "_x.ai/session/state",
			params: map[string]any{"sessionId": "s1", "cwd": "/ws"},
		},
		{
			name:   "share_session snake_case session_id filled",
			call:   func(b *Bridge) (map[string]any, error) { return b.ShareSession(ctx) },
			method: "_x.ai/share_session",
			params: map[string]any{"session_id": "s1"},
		},
		{
			name:   "session/usage fills active sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionUsage(ctx) },
			method: "_x.ai/session/usage",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "session/search sends query and no sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionSearch(ctx, "mcp", "", 0, 0, nil) },
			method: "_x.ai/session/search",
			params: map[string]any{"query": "mcp"},
		},
		{
			name: "session/search optional fields omitted when zero",
			call: func(b *Bridge) (map[string]any, error) {
				return b.SessionSearch(ctx, "git", "/repo", 20, 5, boolPtr(true))
			},
			method: "_x.ai/session/search",
			params: map[string]any{"query": "git", "cwd": "/repo", "limit": float64(20), "offset": float64(5), "includeContent": true},
		},
		{
			name:   "btw fills sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.Btw(ctx, "what next?") },
			method: "_x.ai/btw",
			params: map[string]any{"sessionId": "s1", "question": "what next?"},
		},
		{
			name:   "prompt_history snake_case session_id",
			call:   func(b *Bridge) (map[string]any, error) { return b.PromptHistory(ctx, "/ws", "s1", "") },
			method: "_x.ai/prompt_history",
			params: map[string]any{"cwd": "/ws", "session_id": "s1"},
		},
		{
			name:   "prompt_history omits optional session ids",
			call:   func(b *Bridge) (map[string]any, error) { return b.PromptHistory(ctx, "/ws", "", "") },
			method: "_x.ai/prompt_history",
			params: map[string]any{"cwd": "/ws"},
		},
		{
			name:   "session/close fills active sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionClose(ctx) },
			method: "_x.ai/session/close",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "session/rehydrate fills sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionRehydrate(ctx, "/src", "/repo", "/wt") },
			method: "_x.ai/session/rehydrate",
			params: map[string]any{"sessionId": "s1", "sourceCwd": "/src", "repoRoot": "/repo", "worktreePath": "/wt"},
		},
		{
			name:   "interject fills sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.Interject(ctx, "hi", "") },
			method: "_x.ai/interject",
			params: map[string]any{"sessionId": "s1", "text": "hi"},
		},
		{
			name:   "session/list omits empty optional fields",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionList(ctx, "", "", "", 0) },
			method: "_x.ai/session/list",
			params: map[string]any{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, w := readyBridge()
			done := make(chan callResult, 1)
			go func() {
				res, err := c.call(b)
				done <- callResult{res, err}
			}()
			// Envelope form: UnwrapExtResult must surface the inner payload.
			resolveNext(t, b, w, map[string]any{"result": map[string]any{"ok": true}})
			cr := <-done
			if cr.err != nil {
				t.Fatalf("call error: %v", cr.err)
			}
			if cr.res["ok"] != true {
				t.Errorf("result = %v, want unwrapped ok:true", cr.res)
			}
			msg := w.last()
			if msg == nil {
				t.Fatal("no request captured")
			}
			if got := msg["method"]; got != c.method {
				t.Errorf("wire method = %v, want %s (bare names get -32601)", got, c.method)
			}
			gotParams, _ := msg["params"].(map[string]any)
			if !reflect.DeepEqual(gotParams, c.params) {
				t.Errorf("params = %v, want %v", gotParams, c.params)
			}
		})
	}
}

// Bare (non-envelope) responses pass through UnwrapExtResult unchanged.
func TestExtSessionMethodBareResult(t *testing.T) {
	b, w := readyBridge()
	done := make(chan callResult, 1)
	go func() {
		res, err := b.SessionUsage(context.Background())
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"usage": map[string]any{"numTurns": 3}})
	cr := <-done
	if cr.err != nil {
		t.Fatal(cr.err)
	}
	if cr.res["usage"] == nil {
		t.Errorf("result = %v, want bare usage passthrough", cr.res)
	}
}

// The x.ai/queue/* methods are fire-and-forget notifications (the agent
// only handles them in its ext_notification path): no JSON-RPC id, and the
// sessionId comes from the active session (the agent routes on it and
// silently drops the command without it).
func TestQueueNotificationWirePayloads(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			name:   "queue/remove",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueRemove(ctx, "q-1") },
			method: "_x.ai/queue/remove",
			params: map[string]any{"id": "q-1", "sessionId": "s1"},
		},
		{
			name:   "queue/reorder",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueReorder(ctx, []string{"q-2", "q-1"}) },
			method: "_x.ai/queue/reorder",
			params: map[string]any{"orderedIds": []any{"q-2", "q-1"}, "sessionId": "s1"},
		},
		{
			name:   "queue/clear",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueClear(ctx) },
			method: "_x.ai/queue/clear",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "queue/edit",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueEdit(ctx, "q-1", "new text") },
			method: "_x.ai/queue/edit",
			params: map[string]any{"id": "q-1", "newText": "new text", "sessionId": "s1"},
		},
		{
			name:   "queue/hold_edit",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueHoldEdit(ctx, "q-1") },
			method: "_x.ai/queue/hold_edit",
			params: map[string]any{"id": "q-1", "sessionId": "s1"},
		},
		{
			name:   "queue/release_edit",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueReleaseEdit(ctx, "q-1") },
			method: "_x.ai/queue/release_edit",
			params: map[string]any{"id": "q-1", "sessionId": "s1"},
		},
		{
			name:   "queue/interject",
			call:   func(b *Bridge) (map[string]any, error) { return b.QueueInterject(ctx, "q-1") },
			method: "_x.ai/queue/interject",
			params: map[string]any{"id": "q-1", "sessionId": "s1"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, w := readyBridge()
			res, err := c.call(b)
			if err != nil {
				t.Fatalf("call error: %v", err)
			}
			if res["ok"] != true {
				t.Errorf("result = %v, want ok:true", res)
			}
			msg := w.last()
			if msg == nil {
				t.Fatal("no message captured")
			}
			if msg["id"] != nil {
				t.Errorf("wire message has id %v, want notification (no id)", msg["id"])
			}
			if got := msg["method"]; got != c.method {
				t.Errorf("wire method = %v, want %s", got, c.method)
			}
			gotParams, _ := msg["params"].(map[string]any)
			if !reflect.DeepEqual(gotParams, c.params) {
				t.Errorf("params = %v, want %v", gotParams, c.params)
			}
		})
	}
}

// Queue methods without an active session fail fast with a 404 HTTPError
// (like toggle_plan_mode) instead of writing a command the agent would
// silently drop.
func TestQueueMethodsNoActiveSession(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.ready = true
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()
	ctx := context.Background()
	for _, call := range []struct {
		name string
		fn   func() error
	}{
		{"queue/remove", func() error { _, err := b.QueueRemove(ctx, "q-1"); return err }},
		{"queue/clear", func() error { _, err := b.QueueClear(ctx); return err }},
		{"queue/interject", func() error { _, err := b.QueueInterject(ctx, "q-1"); return err }},
	} {
		t.Run(call.name, func(t *testing.T) {
			err := call.fn()
			var he *HTTPError
			if !errors.As(err, &he) || he.Code != 404 {
				t.Errorf("err = %v, want HTTPError 404", err)
			}
		})
	}
}

// A REQUIRED sessionId with no active session must 404 fast — XaiCall's
// fill rule, exercised through a session-scoped wrapper.
func TestExtSessionMethodNoActiveSession(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.ready = true
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()
	_, err := b.SessionUsage(context.Background())
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != 404 {
		t.Errorf("err = %v, want HTTPError 404", err)
	}
}

// UnwrapExtResult: ExtMethodResult envelope → inner payload; bare results
// pass through; nil-safe.
func TestUnwrapExtResult(t *testing.T) {
	if got := UnwrapExtResult(map[string]any{"result": map[string]any{"ok": true}, "error": nil}); !reflect.DeepEqual(got, map[string]any{"ok": true}) {
		t.Errorf("envelope = %v, want inner payload", got)
	}
	bare := map[string]any{"success": true}
	if got := UnwrapExtResult(bare); !reflect.DeepEqual(got, bare) {
		t.Errorf("bare = %v, want unchanged", got)
	}
	if got := UnwrapExtResult(nil); got != nil {
		t.Errorf("nil = %v, want nil", got)
	}
	// Non-map payloads (e.g. a JSON scalar result) are left as-is.
	scalar := map[string]any{"result": "plain"}
	if got := UnwrapExtResult(scalar); !reflect.DeepEqual(got, scalar) {
		t.Errorf("scalar envelope = %v, want unchanged", got)
	}
}

func boolPtr(v bool) *bool { return &v }
