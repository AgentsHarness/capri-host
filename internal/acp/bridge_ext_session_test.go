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
			name:   "session/load_history sends beforeId cursor",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionLoadHistory(ctx, "c-42") },
			method: "_x.ai/session/load_history",
			params: map[string]any{"beforeId": "c-42"},
		},
		{
			name:   "session/load_history omits beforeId when empty (first page)",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionLoadHistory(ctx, "") },
			method: "_x.ai/session/load_history",
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
			if cr.res.(map[string]any)["ok"] != true {
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
		res, err := b.SessionLoadHistory(context.Background(), "")
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"usage": map[string]any{"numTurns": 3}})
	cr := <-done
	if cr.err != nil {
		t.Fatal(cr.err)
	}
	if cr.res.(map[string]any)["usage"] == nil {
		t.Errorf("result = %v, want bare usage passthrough", cr.res)
	}
}

// A REQUIRED sessionId with no active session must 404 fast — XaiCall's
// fill rule, exercised directly on the XaiCall surface.
func TestExtMethodNoActiveSession(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.ready = true
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()
	_, err := b.XaiCall(context.Background(), "x.ai/session/usage", map[string]any{"sessionId": ""})
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != 404 {
		t.Errorf("err = %v, want HTTPError 404", err)
	}
}
