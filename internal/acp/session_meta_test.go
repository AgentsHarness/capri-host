package acp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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

// agent session/load remaps a session's persisted model id to a current
// catalog key (e.g. deepseek-v4-flash → deepseek-v4-flash-go) and answers
// WITHOUT a reasoningEffort — the load must keep the session's last-known
// effort (user's choice) instead of letting the FE fall back to the mapped
// model's default (e.g. low).
func TestLoadSessionPreservesKnownEffortWhenResponseOmitsIt(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "deepseek-v4-flash",
		"reasoningEffort": "max",
		"availableModels": []any{
			map[string]any{
				"modelId": "deepseek-v4-flash-go",
				"name":    "DeepSeek V4 Flash Go",
				"_meta": map[string]any{
					"reasoningEffort": "low",
					"reasoningEfforts": []any{
						map[string]any{"id": "low", "value": "low"},
						map[string]any{"id": "high", "value": "high"},
						map[string]any{"id": "max", "value": "max"},
					},
				},
			},
		},
	}
	b.mu.Unlock()

	// Agent answers with the mapped catalog key and NO top-level effort.
	var resp map[string]any
	runResolved(t, b, w, map[string]any{
		"sessionId": "s1",
		"models": map[string]any{
			"currentModelId": "deepseek-v4-flash-go",
			"availableModels": []any{
				map[string]any{
					"modelId": "deepseek-v4-flash-go",
					"name":    "DeepSeek V4 Flash Go",
					"_meta": map[string]any{
						"reasoningEffort": "low",
						"reasoningEfforts": []any{
							map[string]any{"id": "low", "value": "low"},
							map[string]any{"id": "high", "value": "high"},
							map[string]any{"id": "max", "value": "max"},
						},
					},
				},
			},
		},
	}, func() error {
		var err error
		resp, err = b.LoadSession(ctx, "s1", "/ws")
		return err
	})

	models, _ := resp["models"].(map[string]any)
	if got, _ := models["reasoningEffort"].(string); got != "max" {
		t.Errorf("load response reasoningEffort = %q, want %q (preserved user choice)", got, "max")
	}
	if got, _ := models["currentModelId"].(string); got != "deepseek-v4-flash-go" {
		t.Errorf("load response currentModelId = %q, want deepseek-v4-flash-go", got)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if got, _ := b.sessions["s1"].models.(map[string]any)["reasoningEffort"].(string); got != "max" {
		t.Errorf("roster cache reasoningEffort = %q, want %q", got, "max")
	}
}

// An explicit reasoningEffort in the load response wins over the cached
// value (the agent did a real model/effort switch).
func TestLoadSessionKeepsExplicitResponseEffort(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "deepseek-v4-flash",
		"reasoningEffort": "max",
	}
	b.mu.Unlock()

	var resp map[string]any
	runResolved(t, b, w, map[string]any{
		"sessionId": "s1",
		"models": map[string]any{
			"currentModelId":  "deepseek-v4-pro",
			"reasoningEffort": "high",
		},
	}, func() error {
		var err error
		resp, err = b.LoadSession(ctx, "s1", "/ws")
		return err
	})
	models, _ := resp["models"].(map[string]any)
	if got, _ := models["reasoningEffort"].(string); got != "high" {
		t.Errorf("load response reasoningEffort = %q, want %q (explicit wins)", got, "high")
	}
}

// Same preservation rule on session/resume.
func TestResumeSessionPreservesKnownEffortWhenResponseOmitsIt(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "deepseek-v4-flash",
		"reasoningEffort": "max",
	}
	b.mu.Unlock()

	var resp map[string]any
	runResolved(t, b, w, map[string]any{
		"sessionId": "s1",
		"models": map[string]any{
			"currentModelId": "deepseek-v4-flash-go",
		},
	}, func() error {
		var err error
		resp, err = b.ResumeSession(ctx, "s1", "/ws")
		return err
	})
	models, _ := resp["models"].(map[string]any)
	if got, _ := models["reasoningEffort"].(string); got != "max" {
		t.Errorf("resume response reasoningEffort = %q, want %q (preserved user choice)", got, "max")
	}
}

// ── 响应 `_meta` 透传（session/new | session/load | session/resume）────

// nextEvent drains the subscriber channel until an event of the wanted
// type arrives (broadcasts happen synchronously, but busy/ready may race).
func nextEvent(t *testing.T, sub chan Event, typ string) Event {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-sub:
			if ev["type"] == typ {
				return ev
			}
		case <-deadline:
			t.Fatalf("no %s event broadcast", typ)
		}
	}
}

// The session/new response `_meta` must be stored and passed through: the
// ready event carries `sessionMeta` and Status exposes it too; without a
// response `_meta` neither appears (absent key ≠ off).
func TestCreateSessionResponseMetaPassthrough(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	runResolved(t, b, w, map[string]any{
		"sessionId": "s2",
		"_meta":     map[string]any{"turn_count": float64(3), "kind": "fresh"},
	}, func() error {
		return b.NewSession(ctx, SessionConfig{Cwd: "/ws"})
	})

	ev := nextEvent(t, sub, "ready")
	if !reflect.DeepEqual(ev["sessionMeta"], map[string]any{"turn_count": float64(3), "kind": "fresh"}) {
		t.Errorf("ready sessionMeta = %v, want {turn_count:3 kind:fresh}", ev["sessionMeta"])
	}
	snap := b.Snapshot()
	if !reflect.DeepEqual(snap.SessionMeta, map[string]any{"turn_count": float64(3), "kind": "fresh"}) {
		t.Errorf("Status.SessionMeta = %v, want {turn_count:3 kind:fresh}", snap.SessionMeta)
	}
}

func TestCreateSessionReadyOmitsMetaWhenAbsent(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	runResolved(t, b, w, map[string]any{"sessionId": "s2"}, func() error {
		return b.NewSession(ctx, SessionConfig{Cwd: "/ws"})
	})

	ev := nextEvent(t, sub, "ready")
	if _, ok := ev["sessionMeta"]; ok {
		t.Errorf("ready event must not carry sessionMeta when the agent returned none: %v", ev)
	}
	if snap := b.Snapshot(); snap.SessionMeta != nil {
		t.Errorf("Status.SessionMeta = %v, want nil", snap.SessionMeta)
	}
}

// The session/load response `_meta` is stored and passed through on the
// cold-load ready event.
func TestLoadSessionResponseMetaPassthrough(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	runResolved(t, b, w, map[string]any{
		"sessionId": "hist-1",
		"_meta":     map[string]any{"kind": "restored"},
	}, func() error {
		_, err := b.LoadSession(ctx, "hist-1", "/ws")
		return err
	})

	ev := nextEvent(t, sub, "ready")
	if !reflect.DeepEqual(ev["sessionMeta"], map[string]any{"kind": "restored"}) {
		t.Errorf("ready sessionMeta = %v, want {kind:restored}", ev["sessionMeta"])
	}
	if snap := b.Snapshot(); !reflect.DeepEqual(snap.SessionMeta, map[string]any{"kind": "restored"}) {
		t.Errorf("Status.SessionMeta = %v, want {kind:restored}", snap.SessionMeta)
	}
}

// The busy focus-only path re-announces the session's STORED response meta
// (no agent call happens, so nothing new to fetch).
func TestLoadSessionFocusOnlyCarriesStoredMeta(t *testing.T) {
	b, _ := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	b.mu.Lock()
	b.sessions["s1"].Busy = true
	b.sessions["s1"].sessionMeta = map[string]any{"kind": "restored"}
	b.mu.Unlock()

	if _, err := b.LoadSession(ctx, "s1", "/ws"); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	ev := nextEvent(t, sub, "ready")
	if !reflect.DeepEqual(ev["sessionMeta"], map[string]any{"kind": "restored"}) {
		t.Errorf("ready sessionMeta = %v, want {kind:restored}", ev["sessionMeta"])
	}
}

// The session/resume response `_meta` is stored and passed through on the
// cold-resume ready event.
func TestResumeSessionResponseMetaPassthrough(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	runResolved(t, b, w, map[string]any{
		"sessionId": "paused-1",
		"_meta":     map[string]any{"kind": "resumed"},
	}, func() error {
		_, err := b.ResumeSession(ctx, "paused-1", "/ws")
		return err
	})

	ev := nextEvent(t, sub, "ready")
	if !reflect.DeepEqual(ev["sessionMeta"], map[string]any{"kind": "resumed"}) {
		t.Errorf("ready sessionMeta = %v, want {kind:resumed}", ev["sessionMeta"])
	}
	if snap := b.Snapshot(); !reflect.DeepEqual(snap.SessionMeta, map[string]any{"kind": "resumed"}) {
		t.Errorf("Status.SessionMeta = %v, want {kind:resumed}", snap.SessionMeta)
	}
}

// ── authenticate 响应 `_meta`（AuthMeta）─────────────────────────────

// The authenticate response `_meta` is stored on the bridge and exposed
// via Status.AuthMeta; ready events carry it next to agentInfo.
func TestAuthenticateStoresAuthMeta(t *testing.T) {
	b, w := readyBridge()
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{
		"_meta": map[string]any{"email": "a@b.c", "subscription_tier": "pro"},
	}, func() error {
		return b.authenticate(ctx, map[string]any{
			"authMethods": []any{map[string]any{"id": "cached_token"}},
		})
	})

	want := map[string]any{"email": "a@b.c", "subscription_tier": "pro"}
	if !reflect.DeepEqual(b.authMeta, want) {
		t.Errorf("authMeta = %v, want %v", b.authMeta, want)
	}
	if snap := b.Snapshot(); !reflect.DeepEqual(snap.AuthMeta, want) {
		t.Errorf("Status.AuthMeta = %v, want %v", snap.AuthMeta, want)
	}
}

func TestAuthenticateAuthMetaNilWhenAbsent(t *testing.T) {
	b, w := readyBridge()
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{}, func() error {
		return b.authenticate(ctx, map[string]any{
			"authMethods": []any{map[string]any{"id": "cached_token"}},
		})
	})

	if b.authMeta != nil {
		t.Errorf("authMeta = %v, want nil", b.authMeta)
	}
	if snap := b.Snapshot(); snap.AuthMeta != nil {
		t.Errorf("Status.AuthMeta = %v, want nil", snap.AuthMeta)
	}
}

// The session-ready event carries authMeta (next to agentInfo) when the
// authenticate response had one.
func TestReadyEventCarriesAuthMeta(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	b.mu.Lock()
	b.authMeta = map[string]any{"email": "a@b.c"}
	b.mu.Unlock()

	runResolved(t, b, w, map[string]any{"sessionId": "s2"}, func() error {
		return b.NewSession(ctx, SessionConfig{Cwd: "/ws"})
	})

	ev := nextEvent(t, sub, "ready")
	if !reflect.DeepEqual(ev["authMeta"], map[string]any{"email": "a@b.c"}) {
		t.Errorf("ready authMeta = %v, want {email:a@b.c}", ev["authMeta"])
	}
}

// Without authMeta the ready event must not carry an authMeta key.
func TestReadyEventOmitsAuthMetaWhenAbsent(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	runResolved(t, b, w, map[string]any{"sessionId": "s2"}, func() error {
		return b.NewSession(ctx, SessionConfig{Cwd: "/ws"})
	})

	ev := nextEvent(t, sub, "ready")
	if _, ok := ev["authMeta"]; ok {
		t.Errorf("ready event must not carry authMeta when absent: %v", ev)
	}
}
