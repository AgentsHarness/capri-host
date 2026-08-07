package acp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

// metaReadyBridge is readyBridge with an isolated last-session file (the
// shared readyBridge would persist to the developer's home dir when a
// session becomes active — rememberSessionLocked writes on create/load).
func metaReadyBridge(t *testing.T) (*Bridge, *recordingStdin) {
	t.Helper()
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ready = true
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.activeSessionID = "s1"
	w := &recordingStdin{}
	b.stdin = w
	return b, w
}

// runResolved runs fn (which blocks on a pending RPC) off the test
// goroutine and resolves the request it writes — the ext_methods_test
// pattern (resolveNext calls t.Fatal, so it must stay on the test
// goroutine).
func runResolved(t *testing.T, b *Bridge, w *recordingStdin, result map[string]any, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()
	resolveNext(t, b, w, result)
	if err := <-done; err != nil {
		t.Fatalf("bridge call: %v", err)
	}
}

// lastRequestParams decodes the last JSON-RPC request the bridge wrote.
func lastRequestParams(t *testing.T, w *recordingStdin) (method string, params map[string]any) {
	t.Helper()
	msg := w.last()
	if msg == nil {
		t.Fatal("bridge never wrote a request")
	}
	method, _ = msg["method"].(string)
	params, _ = msg["params"].(map[string]any)
	if params == nil {
		t.Fatalf("request %s has no params object: %v", method, msg)
	}
	return method, params
}

func TestCreateSessionForwardsMeta(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{"sessionId": "s2"}, func() error {
		return b.NewSession(ctx, SessionConfig{
			Cwd: "/ws",
			Meta: map[string]any{
				"yoloMode": true,
				"autoMode": false,
			},
		})
	})

	method, params := lastRequestParams(t, w)
	if method != "session/new" {
		t.Fatalf("method = %q, want session/new", method)
	}
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("params has no _meta: %v", params)
	}
	if !reflect.DeepEqual(meta, map[string]any{"yoloMode": true, "autoMode": false}) {
		t.Errorf("_meta = %v, want yoloMode:true autoMode:false", meta)
	}
}

func TestCreateSessionOmitsMetaWhenAbsent(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{"sessionId": "s2"}, func() error {
		return b.NewSession(ctx, SessionConfig{Cwd: "/ws"})
	})

	_, params := lastRequestParams(t, w)
	if _, ok := params["_meta"]; ok {
		t.Errorf("params must not carry _meta when Meta is absent: %v", params)
	}
	// Request shape stays identical to the pre-meta era: exactly the
	// three original keys.
	want := map[string]any{
		"cwd":                   "/ws",
		"additionalDirectories": []any{},
		"mcpServers":            []any{},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}

func TestLoadSessionForwardsMeta(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{
		"sessionId": "hist-1",
		"modes":     map[string]any{"currentModeId": "auto"},
	}, func() error {
		_, err := b.LoadSession(ctx, "hist-1", "/ws", map[string]any{
			"yoloMode": false,
			"autoMode": true,
		})
		return err
	})

	method, params := lastRequestParams(t, w)
	if method != "session/load" {
		t.Fatalf("method = %q, want session/load", method)
	}
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("params has no _meta: %v", params)
	}
	if !reflect.DeepEqual(meta, map[string]any{"yoloMode": false, "autoMode": true}) {
		t.Errorf("_meta = %v, want yoloMode:false autoMode:true", meta)
	}
}

func TestLoadSessionOmitsMetaWhenAbsent(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{"sessionId": "hist-1"}, func() error {
		_, err := b.LoadSession(ctx, "hist-1", "/ws")
		return err
	})

	_, params := lastRequestParams(t, w)
	if _, ok := params["_meta"]; ok {
		t.Errorf("params must not carry _meta when meta is absent: %v", params)
	}
	want := map[string]any{
		"sessionId":  "hist-1",
		"cwd":        "/ws",
		"mcpServers": []any{},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}

// The busy focus-only path never calls session/load, so meta is ignored
// and no request hits the wire.
func TestLoadSessionBusyFocusOnlyIgnoresMeta(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	b.mu.Lock()
	b.sessions["s1"].Busy = true
	b.mu.Unlock()

	if _, err := b.LoadSession(ctx, "s1", "/ws", map[string]any{"yoloMode": true}); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if n := len(w.lines); n != 0 {
		var msgs []map[string]any
		for _, l := range w.lines {
			var m map[string]any
			_ = json.Unmarshal(l, &m)
			msgs = append(msgs, m)
		}
		t.Errorf("busy focus-only path must not write requests, got %d: %v", n, msgs)
	}
}
