package acp

import (
	"context"
	"reflect"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────

func bp(v bool) *bool     { return &v }
func ip(v int) *int       { return &v }
func i64p(v int64) *int64 { return &v }

// extBridgeCall runs a typed extension wrapper against a readyBridge and
// resolves the in-flight RPC with the given raw result; it returns the
// wrapper's (already UnwrapExtResult-ed) result.
func extBridgeCall(t *testing.T, b *Bridge, w *recordingStdin, call func() (map[string]any, error), result map[string]any) map[string]any {
	t.Helper()
	done := make(chan callResult, 1)
	go func() {
		res, err := call()
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, result)
	cr := <-done
	if cr.err != nil {
		t.Fatalf("call error: %v", cr.err)
	}
	return cr.res
}

// wireParams returns the params of the last request the bridge wrote.
func wireParams(t *testing.T, w *recordingStdin) (string, map[string]any) {
	t.Helper()
	msg := w.last()
	if msg == nil {
		t.Fatal("no request captured")
	}
	method, _ := msg["method"].(string)
	params, _ := msg["params"].(map[string]any)
	return method, params
}

// ── UnwrapExtResult ───────────────────────────────────────────────────

func TestUnwrapExtResult(t *testing.T) {
	// Envelope: {"result": <payload>, "error": ...} → the inner payload.
	payload := map[string]any{"branch": "main", "files": []any{"a.go"}}
	got := UnwrapExtResult(map[string]any{"result": payload, "error": nil})
	if !reflect.DeepEqual(got, payload) {
		t.Errorf("envelope unwrap = %v, want %v", got, payload)
	}

	// Non-envelope results pass through unchanged (same map).
	plain := map[string]any{"ok": true}
	if got := UnwrapExtResult(plain); !reflect.DeepEqual(got, plain) {
		t.Errorf("non-envelope = %v, want %v", got, plain)
	}

	// nil-safe.
	if got := UnwrapExtResult(nil); got != nil {
		t.Errorf("nil = %v, want nil", got)
	}

	// Error-only envelope ({"result": null, "error": ...}) has no map
	// payload → returned unchanged so callers can inspect the error.
	errEnv := map[string]any{"result": nil, "error": "boom"}
	if got := UnwrapExtResult(errEnv); !reflect.DeepEqual(got, errEnv) {
		t.Errorf("error-only envelope = %v, want %v", got, errEnv)
	}

	// Envelope whose payload is not a map → unchanged.
	strEnv := map[string]any{"result": "not-a-map"}
	if got := UnwrapExtResult(strEnv); !reflect.DeepEqual(got, strEnv) {
		t.Errorf("non-map payload envelope = %v, want %v", got, strEnv)
	}
}

// ── fs wrappers ───────────────────────────────────────────────────────

func TestExtBridgeFsList(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.FsList(ctx, "/ws", FsListOptions{Depth: ip(2), IncludeHidden: bp(true)})
	}, map[string]any{"result": map[string]any{"entries": []any{}}})
	if res["entries"] == nil {
		t.Errorf("result = %v, want unwrapped payload with entries", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/fs/list" {
		t.Errorf("wire method = %s, want _x.ai/fs/list", method)
	}
	want := map[string]any{"path": "/ws", "depth": float64(2), "includeHidden": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v (camelCase, no sessionId)", params, want)
	}
}

func TestExtBridgeFsReadFile(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.FsReadFile(ctx, "/ws/a.txt", ip(4096), nil, nil, nil, "")
	}, map[string]any{"result": map[string]any{"content": "hi"}})
	if res["content"] != "hi" {
		t.Errorf("result = %v, want unwrapped payload with content", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/fs/read_file" {
		t.Errorf("wire method = %s, want _x.ai/fs/read_file", method)
	}
	want := map[string]any{"path": "/ws/a.txt", "maxBytes": float64(4096)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v (no sessionId)", params, want)
	}
}

// ── git wrappers ──────────────────────────────────────────────────────

func TestExtBridgeGitCommit(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.GitCommit(ctx, "/repo", "feat: x", true, true, false, false)
	}, map[string]any{"result": map[string]any{"commit": "abc"}})
	if res["commit"] != "abc" {
		t.Errorf("result = %v, want unwrapped payload with commit", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/git/commit" {
		t.Errorf("wire method = %s, want _x.ai/git/commit", method)
	}
	want := map[string]any{
		"gitRoot": "/repo",
		"message": "feat: x",
		"amend":   true,
		"signoff": true,
		"push":    false,
		"sync":    false,
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v (message/amend/signoff/push)", params, want)
	}
}

func TestExtBridgeGitCheckoutSessionHead(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	// The agent's request struct requires session_id: the wrapper passes ""
	// and XaiCall fills the active session ("s1").
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.GitCheckoutSessionHead(ctx, "", true)
	}, map[string]any{"gitRoot": "/ws"})
	if res["gitRoot"] != "/ws" {
		t.Errorf("result = %v, want raw (non-envelope) response passed through", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/git/checkout_session_head" {
		t.Errorf("wire method = %s, want _x.ai/git/checkout_session_head", method)
	}
	want := map[string]any{"sessionId": "s1", "stashIfDirty": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v (sessionId filled by XaiCall)", params, want)
	}
}

// ── search wrappers ───────────────────────────────────────────────────

func TestExtBridgeSearchFuzzyChange(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.SearchFuzzyChange(ctx, "s-1", "main.go", true, ip(50))
	}, map[string]any{"result": map[string]any{"searchId": "s-1"}})
	if res["searchId"] != "s-1" {
		t.Errorf("result = %v, want unwrapped payload with searchId", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/search/fuzzy/change" {
		t.Errorf("wire method = %s, want _x.ai/search/fuzzy/change", method)
	}
	want := map[string]any{"searchId": "s-1", "query": "main.go", "dirsOnly": true, "limit": float64(50)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}

// ── terminal wrappers ─────────────────────────────────────────────────

func TestExtBridgeTerminalList(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.TerminalList(ctx)
	}, map[string]any{"result": map[string]any{"terminals": []any{}}})
	if res["terminals"] == nil {
		t.Errorf("result = %v, want unwrapped payload with terminals", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/terminal/list" {
		t.Errorf("wire method = %s, want _x.ai/terminal/list", method)
	}
	if len(params) != 0 {
		t.Errorf("params = %v, want empty (x.ai/terminal/list takes no params)", params)
	}
}

// ── code wrappers ─────────────────────────────────────────────────────

func TestExtBridgeCodeGotoDefinition(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()
	// Code-nav eligibility requires a session id: the wrapper passes ""
	// and XaiCall fills the active session ("s1").
	res := extBridgeCall(t, b, w, func() (map[string]any, error) {
		return b.CodeGotoDefinition(ctx, "/ws", "src/main.go", 10, 5)
	}, map[string]any{"result": map[string]any{"symbol": "main"}})
	if res["symbol"] != "main" {
		t.Errorf("result = %v, want unwrapped payload with symbol", res)
	}
	method, params := wireParams(t, w)
	if method != "_x.ai/code/goto-definition" {
		t.Errorf("wire method = %s, want _x.ai/code/goto-definition", method)
	}
	want := map[string]any{
		"sessionId": "s1",
		"cwd":       "/ws",
		"path":      "src/main.go",
		"row":       float64(10),
		"column":    float64(5),
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v (sessionId filled by XaiCall)", params, want)
	}
}
