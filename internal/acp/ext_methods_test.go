package acp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// recordingStdin captures every JSON-RPC request the bridge writes, so
// tests can assert the exact wire method/params without a real process.
type recordingStdin struct {
	mu    sync.Mutex
	lines [][]byte
}

type callResult struct {
	res map[string]any
	err error
}

func (w *recordingStdin) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lines = append(w.lines, append([]byte{}, p...))
	return len(p), nil
}

func (w *recordingStdin) Close() error { return nil }

func (w *recordingStdin) last() map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.lines) == 0 {
		return nil
	}
	var msg map[string]any
	if json.Unmarshal(w.lines[len(w.lines)-1], &msg) != nil {
		return nil
	}
	return msg
}

// readyBridge returns a booted bridge (ready=true, active session s1) whose
// stdin is a recordingStdin — requests never hit a real process.
func readyBridge() (*Bridge, *recordingStdin) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.ready = true
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.activeSessionID = "s1"
	w := &recordingStdin{}
	b.stdin = w
	b.mu.Unlock()
	return b, w
}

// resolveNext resolves the bridge's in-flight request with a canned result
// by matching the JSON-RPC id the bridge just wrote to the fake stdin.
func resolveNext(t *testing.T, b *Bridge, w *recordingStdin, result map[string]any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msg := w.last(); msg != nil {
			if id := msg["id"]; id != nil {
				if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
					ch.(chan rpcResult) <- rpcResult{result: result}
					return
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("bridge never wrote a request to stdin")
}

// The five new extension methods must hit the wire with the "_" prefix
// (bare method names get -32601) and the expected params; sessionId
// defaults to the active session. Timeouts (60s / 60s / 60s / 60s / 30s)
// are hardcoded in the request calls and not observable here.
func TestExtensionMethodsWirePayloads(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			name:   "session/delete defaults to active",
			call:   func(b *Bridge) (map[string]any, error) { return b.SessionDelete(ctx, "") },
			method: "_x.ai/session/delete",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "compact_conversation with note",
			call:   func(b *Bridge) (map[string]any, error) { return b.CompactConversation(ctx, "", "cleanup") },
			method: "_x.ai/compact_conversation",
			params: map[string]any{"sessionId": "s1", "userContext": "cleanup"},
		},
		{
			name:   "compact_conversation without note",
			call:   func(b *Bridge) (map[string]any, error) { return b.CompactConversation(ctx, "s1", "") },
			method: "_x.ai/compact_conversation",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "rewind/points",
			call:   func(b *Bridge) (map[string]any, error) { return b.RewindPoints(ctx, "") },
			method: "_x.ai/rewind/points",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "rewind/execute",
			call:   func(b *Bridge) (map[string]any, error) { return b.RewindExecute(ctx, "", 3) },
			method: "_x.ai/rewind/execute",
			params: map[string]any{"sessionId": "s1", "targetPromptIndex": float64(3)},
		},
		{
			name:   "scheduler/delete",
			call:   func(b *Bridge) (map[string]any, error) { return b.SchedulerDelete(ctx, "", "t-9") },
			method: "_x.ai/scheduler/delete",
			params: map[string]any{"sessionId": "s1", "taskId": "t-9"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, w := readyBridge()
			// The method call blocks on the pending RPC; resolve it from
			// the test goroutine (vet: no t.Fatal off the test goroutine).
			done := make(chan callResult, 1)
			go func() {
				res, err := c.call(b)
				done <- callResult{res, err}
			}()
			resolveNext(t, b, w, map[string]any{"ok": true})
			cr := <-done
			if cr.err != nil {
				t.Fatalf("call error: %v", cr.err)
			}
			if cr.res["ok"] != true {
				t.Errorf("result = %v, want ok:true", cr.res)
			}
			msg := w.last()
			if msg == nil {
				t.Fatal("no request captured")
			}
			if got := msg["method"]; got != c.method {
				t.Errorf("wire method = %v, want %s (bare names get -32601)", got, c.method)
			}
			gotParams, _ := msg["params"].(map[string]any)
			if len(gotParams) != len(c.params) {
				t.Errorf("params = %v, want %v", gotParams, c.params)
			}
			for k, want := range c.params {
				if gotParams[k] != want {
					t.Errorf("params[%s] = %v, want %v", k, gotParams[k], want)
				}
			}
		})
	}
}

// An explicit sessionId is forwarded as-is instead of the active one.
func TestExtensionMethodExplicitSessionID(t *testing.T) {
	b, w := readyBridge()
	done := make(chan callResult, 1)
	go func() {
		res, err := b.SessionDelete(context.Background(), "other-session")
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"ok": true})
	if cr := <-done; cr.err != nil {
		t.Fatal(cr.err)
	}
	msg := w.last()
	params, _ := msg["params"].(map[string]any)
	if params["sessionId"] != "other-session" {
		t.Errorf("sessionId = %v, want other-session", params["sessionId"])
	}
}

// Without any session, the methods fail fast with a 404 HTTPError (the
// HTTP layer maps it to a 404 response) instead of hanging.
func TestExtensionMethodsNoActiveSession(t *testing.T) {
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
		{"session/delete", func() error { _, err := b.SessionDelete(ctx, ""); return err }},
		{"compact_conversation", func() error { _, err := b.CompactConversation(ctx, "", ""); return err }},
		{"rewind/points", func() error { _, err := b.RewindPoints(ctx, ""); return err }},
		{"rewind/execute", func() error { _, err := b.RewindExecute(ctx, "", 0); return err }},
		{"scheduler/delete", func() error { _, err := b.SchedulerDelete(ctx, "", "t-1"); return err }},
		{"billing", func() error { _, err := b.Billing(ctx, ""); return err }},
		{"memory/flush", func() error { _, err := b.MemoryFlush(ctx, ""); return err }},
		{"memory/rewrite", func() error { _, err := b.MemoryRewrite(ctx, "", "t", "s"); return err }},
		{"toggle_plan_mode", func() error { _, err := b.TogglePlanMode(ctx, ""); return err }},
		{"permissions/reset", func() error { _, err := b.PermissionsReset(ctx, ""); return err }},
		{"mcp/toggle", func() error { _, err := b.MCPToggle(ctx, "fs", true); return err }},
		{"mcp/upsert", func() error { _, err := b.MCPUpsert(ctx, map[string]any{"name": "fs", "command": "npx"}); return err }},
		{"mcp/delete", func() error { _, err := b.MCPDelete(ctx, "fs"); return err }},
		{"mcp/auth_trigger", func() error { _, err := b.MCPAuthTrigger(ctx, "github"); return err }},
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

// The admin extension methods must hit the wire with the "_" prefix and
// the expected params (DeepEqual because mcp/upsert carries flattened
// config keys). sessionId defaults to the active session; the MCP
// mutations use the agent's snake_case contract (session_id/server_name)
// with the active session injected.
func TestAdminExtensionMethodsWirePayloads(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			name:   "billing defaults to active",
			call:   func(b *Bridge) (map[string]any, error) { return b.Billing(ctx, "") },
			method: "_x.ai/billing",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "billing explicit sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.Billing(ctx, "other") },
			method: "_x.ai/billing",
			params: map[string]any{"sessionId": "other"},
		},
		{
			name:   "memory/flush",
			call:   func(b *Bridge) (map[string]any, error) { return b.MemoryFlush(ctx, "") },
			method: "_x.ai/memory/flush",
			params: map[string]any{"session_id": "s1"},
		},
		{
			name:   "memory/rewrite",
			call:   func(b *Bridge) (map[string]any, error) { return b.MemoryRewrite(ctx, "", "new memory", "ctx summary") },
			method: "_x.ai/memory/rewrite",
			params: map[string]any{"sessionId": "s1", "rawText": "new memory", "contextSummary": "ctx summary"},
		},
		{
			name:   "mcp/list injects active sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.MCPList(ctx) },
			method: "_x.ai/mcp/list",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "mcp/toggle",
			call:   func(b *Bridge) (map[string]any, error) { return b.MCPToggle(ctx, "fs", true) },
			method: "_x.ai/mcp/toggle",
			params: map[string]any{"session_id": "s1", "server_name": "fs", "enabled": true},
		},
		{
			name: "mcp/upsert flattens server into agent contract",
			call: func(b *Bridge) (map[string]any, error) {
				return b.MCPUpsert(ctx, map[string]any{"name": "fs", "command": "npx", "args": []any{"-y"}})
			},
			method: "_x.ai/mcp/upsert",
			params: map[string]any{"session_id": "s1", "server_name": "fs", "command": "npx", "args": []any{"-y"}},
		},
		{
			name:   "mcp/delete",
			call:   func(b *Bridge) (map[string]any, error) { return b.MCPDelete(ctx, "fs") },
			method: "_x.ai/mcp/delete",
			params: map[string]any{"session_id": "s1", "server_name": "fs"},
		},
		{
			name:   "mcp/auth_trigger",
			call:   func(b *Bridge) (map[string]any, error) { return b.MCPAuthTrigger(ctx, "github") },
			method: "_x.ai/mcp/auth_trigger",
			params: map[string]any{"session_id": "s1", "server_name": "github"},
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
			resolveNext(t, b, w, map[string]any{"ok": true})
			cr := <-done
			if cr.err != nil {
				t.Fatalf("call error: %v", cr.err)
			}
			if cr.res["ok"] != true {
				t.Errorf("result = %v, want ok:true", cr.res)
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

// toggle_plan_mode and permissions/reset are fire-and-forget notifications
// (the agent handles them only in its ext_notification path — a
// request-style call would get -32601): the bridge writes them WITHOUT a
// JSON-RPC id, returns a bare ok immediately, and never waits for a reply.
func TestNotificationExtensionMethodsWirePayloads(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			name:   "toggle_plan_mode",
			call:   func(b *Bridge) (map[string]any, error) { return b.TogglePlanMode(ctx, "") },
			method: "_x.ai/toggle_plan_mode",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "permissions/reset",
			call:   func(b *Bridge) (map[string]any, error) { return b.PermissionsReset(ctx, "") },
			method: "_x.ai/permissions/reset",
			params: map[string]any{"sessionId": "s1"},
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
			if len(gotParams) != len(c.params) {
				t.Errorf("params = %v, want %v", gotParams, c.params)
			}
			for k, want := range c.params {
				if gotParams[k] != want {
					t.Errorf("params[%s] = %v, want %v", k, gotParams[k], want)
				}
			}
		})
	}
}

// The session-scoped admin methods require an active session; mcp/list is
// the only MCP method that still works without one (it returns the bare
// catalog), while the MCP mutations 404 fast like the rest.
func TestMCPListWithoutSession(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	w := &recordingStdin{}
	b.mu.Lock()
	b.ready = true
	b.stdin = w
	b.mu.Unlock()
	ctx := context.Background()
	done := make(chan callResult, 1)
	go func() {
		res, err := b.MCPList(ctx)
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"ok": true})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("mcp/list without session: %v", cr.err)
	}
	msg := w.last()
	gotParams, _ := msg["params"].(map[string]any)
	if len(gotParams) != 0 {
		t.Errorf("mcp/list params = %v, want empty", gotParams)
	}
	for _, call := range []struct {
		name string
		fn   func() (map[string]any, error)
	}{
		{"mcp/toggle", func() (map[string]any, error) { return b.MCPToggle(ctx, "fs", true) }},
		{"mcp/upsert", func() (map[string]any, error) { return b.MCPUpsert(ctx, map[string]any{"name": "fs", "command": "npx"}) }},
		{"mcp/delete", func() (map[string]any, error) { return b.MCPDelete(ctx, "fs") }},
		{"mcp/auth_trigger", func() (map[string]any, error) { return b.MCPAuthTrigger(ctx, "github") }},
	} {
		t.Run(call.name, func(t *testing.T) {
			_, err := call.fn()
			var he *HTTPError
			if !errors.As(err, &he) || he.Code != 404 {
				t.Errorf("err = %v, want HTTPError 404", err)
			}
		})
	}
}

// ── image content blocks → SSE events ───────────────────────────────

func TestContentImages(t *testing.T) {
	cases := []struct {
		name    string
		content any
		want    int
	}{
		{"plain string", "hello", 0},
		{"text block", map[string]any{"type": "text", "text": "hi"}, 0},
		{"single image block", map[string]any{"type": "image", "data": "aGk="}, 1},
		{"array text+image", []any{
			map[string]any{"type": "text", "text": "hi"},
			map[string]any{"type": "image", "data": "aGk="},
		}, 1},
		{"nested content array", map[string]any{
			"type":    "text",
			"text":    "x",
			"content": []any{map[string]any{"type": "image", "data": "aGk="}},
		}, 1},
		{"multiple images", []any{
			map[string]any{"type": "image", "data": "aGk="},
			map[string]any{"type": "image", "data": "aGk="},
		}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := len(contentImages(c.content)); got != c.want {
				t.Errorf("contentImages = %d, want %d", got, c.want)
			}
		})
	}
}

func TestImageEvent(t *testing.T) {
	// mimeType wins over mime; data passes through untouched.
	ev, ok := imageEvent("s1", map[string]any{
		"type":     "image",
		"data":     "data:image/png;base64,AAA",
		"mimeType": "image/png",
		"mime":     "image/jpeg",
	})
	if !ok || ev["data"] != "data:image/png;base64,AAA" || ev["mimeType"] != "image/png" || ev["sessionId"] != "s1" {
		t.Errorf("ev = %v ok=%v", ev, ok)
	}
	// mime fallback.
	ev, ok = imageEvent("s1", map[string]any{"type": "image", "data": "aGk=", "mime": "image/jpeg"})
	if !ok || ev["mimeType"] != "image/jpeg" {
		t.Errorf("mime fallback: ev = %v ok=%v", ev, ok)
	}
	// Default mimeType.
	ev, ok = imageEvent("s1", map[string]any{"type": "image", "data": "aGk="})
	if !ok || ev["mimeType"] != "image/png" {
		t.Errorf("default mime: ev = %v ok=%v", ev, ok)
	}
	// Non-string data is skipped.
	if _, ok := imageEvent("s1", map[string]any{"type": "image", "data": 123}); ok {
		t.Error("non-string data must be skipped")
	}
	if _, ok := imageEvent("s1", map[string]any{"type": "image"}); ok {
		t.Error("missing data must be skipped")
	}
}

// An agent_message_chunk with text + image blocks yields the usual chunk
// event followed by one typed image event; the text path stays intact.
func TestAgentMessageChunkImageEvents(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content": []any{
				map[string]any{"type": "text", "text": "hello"},
				map[string]any{"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
			},
		},
	})
	ev1 := <-ch
	if ev1["type"] != "chunk" || ev1["text"] != "hello" || ev1["sessionId"] != "s1" {
		t.Fatalf("first event = %v, want chunk hello", ev1)
	}
	ev2 := <-ch
	if ev2["type"] != "image" || ev2["data"] != "aGVsbG8=" || ev2["mimeType"] != "image/png" || ev2["sessionId"] != "s1" {
		t.Fatalf("second event = %v, want image event", ev2)
	}
}

// user_message_chunk carries image blocks the same way.
func TestUserMessageChunkImageEvents(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s2",
		"update": map[string]any{
			"sessionUpdate": "user_message_chunk",
			"content":       map[string]any{"type": "image", "data": "aGk=", "mime": "image/webp"},
		},
	})
	ev := <-ch
	if ev["type"] != "image" || ev["data"] != "aGk=" || ev["mimeType"] != "image/webp" || ev["sessionId"] != "s2" {
		t.Fatalf("event = %v, want image event", ev)
	}
}

// A pure-text chunk must not emit any image event (no behavior change).
func TestTextOnlyChunkNoImageEvents(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       "plain text",
		},
	})
	ev := <-ch
	if ev["type"] != "chunk" || ev["text"] != "plain text" {
		t.Fatalf("event = %v, want chunk plain text", ev)
	}
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra event: %v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

// ── scheduled task notifications ────────────────────────────────────

func TestScheduledTaskCreatedNotification(t *testing.T) {
	cases := []struct {
		name     string
		params   map[string]any
		wantTask map[string]any
	}{
		{
			name: "snake_case in task object",
			params: map[string]any{
				"sessionId": "s1",
				"task": map[string]any{
					"task_id":      "t-1",
					"prompt":       "ping",
					"interval":     "5m",
					"next_fire_at": "2026-08-07T10:00:00Z",
				},
			},
			wantTask: map[string]any{
				"taskId": "t-1", "prompt": "ping", "interval": "5m",
				"nextFireAt": "2026-08-07T10:00:00Z",
			},
		},
		{
			name: "camelCase in task object",
			params: map[string]any{
				"sessionId": "s1",
				"task": map[string]any{
					"taskId":     "t-2",
					"prompt":     "pong",
					"interval":   "1h",
					"nextFireAt": "2026-08-07T11:00:00Z",
				},
			},
			wantTask: map[string]any{
				"taskId": "t-2", "prompt": "pong", "interval": "1h",
				"nextFireAt": "2026-08-07T11:00:00Z",
			},
		},
		{
			name: "taskId at top level",
			params: map[string]any{
				"sessionId": "s1",
				"taskId":    "t-3",
				"prompt":    "top-level prompt",
			},
			wantTask: map[string]any{
				"taskId": "t-3", "prompt": "top-level prompt",
				"interval": nil, "nextFireAt": nil,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			b.handleXaiNotification("x.ai/scheduled_task_created", c.params)
			ev := <-ch
			if ev["type"] != "scheduled_task_created" || ev["sessionId"] != "s1" {
				t.Fatalf("event = %v", ev)
			}
			task, ok := ev["task"].(map[string]any)
			if !ok {
				t.Fatalf("task = %v, want object", ev["task"])
			}
			for k, want := range c.wantTask {
				if task[k] != want {
					t.Errorf("task[%s] = %v, want %v", k, task[k], want)
				}
			}
		})
	}
}

func TestScheduledTaskDeletedNotification(t *testing.T) {
	for _, params := range []map[string]any{
		{"sessionId": "s1", "taskId": "t-9"},
		{"sessionId": "s1", "task_id": "t-9"},
	} {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		ch, unsub := b.Subscribe()
		b.handleXaiNotification("x.ai/scheduled_task_deleted", params)
		ev := <-ch
		unsub()
		if ev["type"] != "scheduled_task_deleted" || ev["taskId"] != "t-9" || ev["sessionId"] != "s1" {
			t.Errorf("event = %v, want scheduled_task_deleted t-9", ev)
		}
	}
}
