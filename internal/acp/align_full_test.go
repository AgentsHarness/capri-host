package acp

import (
	"encoding/json"
	"context"
	"reflect"
	"testing"
)

// align_full_test.go — A/B 对齐：initialize/authenticate meta 种子、
// session/cancel + resume + list 的 `_meta`/可选字段直通、session/updates
// 可选字段、通知字段保全（scheduled_task_* / models/update / chunk
// fullUpdate）。wire 键均与 grok 源码核验（见各测试注释）。

// ── A1: initialize `_meta` / clientCapabilities.meta 种子 ───────────

func TestInitMetaSeedsDefaults(t *testing.T) {
	for _, env := range []string{
		"ACP_INIT_CLIENT_IDENTIFIER", "ACP_INIT_CLIENT_SOURCE",
		"ACP_INIT_SYSTEM_PROMPT_OVERRIDE", "ACP_INIT_RULES",
		"ACP_INIT_MCP_APPS", "ACP_INIT_BUFFERING_SETTINGS", "ACP_INIT_STARTUP_HINTS",
	} {
		t.Setenv(env, "")
	}
	meta := initMetaSeeds()
	// clientType/clientVersion mirror the TUI's PAGER_CLIENT_TYPE /
	// PAGER_CLIENT_VERSION (build-time → host constant).
	want := map[string]any{"clientType": "grok-pager", "clientVersion": Version}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("initMetaSeeds = %v, want %v (env-driven keys must be omitted when absent)", meta, want)
	}
}

func TestInitMetaSeedsEnvDriven(t *testing.T) {
	t.Setenv("ACP_INIT_CLIENT_IDENTIFIER", "my-editor")
	t.Setenv("ACP_INIT_CLIENT_SOURCE", "web")
	t.Setenv("ACP_INIT_SYSTEM_PROMPT_OVERRIDE", "YOU ARE A PIRATE.")
	t.Setenv("ACP_INIT_RULES", "no tests")
	t.Setenv("ACP_INIT_MCP_APPS", "1")
	t.Setenv("ACP_INIT_BUFFERING_SETTINGS", `{"maxItems":5,"maxBytes":1024,"maxDurationMs":20}`)
	t.Setenv("ACP_INIT_STARTUP_HINTS", `{"skipGitStatus":true}`)

	meta := initMetaSeeds()
	want := map[string]any{
		"clientType":           "grok-pager",
		"clientVersion":        Version,
		"clientIdentifier":     "my-editor",
		"clientSource":         "web",
		"systemPromptOverride": "YOU ARE A PIRATE.",
		"rules":                "no tests",
		"mcpApps":              true,
		"bufferingSettings": map[string]any{
			"maxItems": float64(5), "maxBytes": float64(1024), "maxDurationMs": float64(20),
		},
		"startupHints": map[string]any{"skipGitStatus": true},
	}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("initMetaSeeds = %v, want %v", meta, want)
	}
}

func TestInitCapabilitiesMeta(t *testing.T) {
	t.Setenv("ACP_CAP_CODE_NAVIGATION", "")
	t.Setenv("ACP_CAP_FOLDER_TRUST_INTERACTIVE", "")
	t.Setenv("ACP_CAP_FS_NOTIFY", "")
	meta := initCapabilitiesMeta()
	want := map[string]any{
		"x.ai/incrementalBashOutput": true,
		"x.ai/bashOutputNoColor":     true,
		"x.ai/gitHeadChanged":        true,
		"x.ai/hunkTracker":           map[string]any{"mode": "agent_only"},
	}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("initCapabilitiesMeta = %v, want %v (opt-in keys absent by default)", meta, want)
	}

	t.Setenv("ACP_CAP_CODE_NAVIGATION", "true")
	t.Setenv("ACP_CAP_FOLDER_TRUST_INTERACTIVE", "yes")
	t.Setenv("ACP_CAP_FS_NOTIFY", "on")
	meta = initCapabilitiesMeta()
	if v, ok := meta["x.ai/codeNavigation"].(map[string]any); !ok || v["enabled"] != true {
		t.Errorf("x.ai/codeNavigation = %v, want {enabled:true}", meta["x.ai/codeNavigation"])
	}
	if v, ok := meta["x.ai/folderTrust"].(map[string]any); !ok || v["interactive"] != true {
		t.Errorf("x.ai/folderTrust = %v, want {interactive:true}", meta["x.ai/folderTrust"])
	}
	if v, ok := meta["x.ai/fs_notify"].(bool); !ok || !v {
		t.Errorf("x.ai/fs_notify = %v, want true", meta["x.ai/fs_notify"])
	}
}

// ── A3: session/cancel `_meta` 直通 ─────────────────────────────────

// CancelWithMeta forwards cancelTrigger / cancelSubagents /
// rewindIfNoOutput on the session/cancel params `_meta` (grok reads them at
// mvp_agent/acp_agent.rs:2079-2108); bare Cancel keeps the pre-meta wire
// shape (no `_meta` key).
func TestCancelWithMetaWire(t *testing.T) {
	b, w := metaReadyBridge(t)

	b.Cancel("")
	msg := w.last()
	if msg["method"] != "session/cancel" {
		t.Fatalf("method = %v, want session/cancel", msg["method"])
	}
	params, _ := msg["params"].(map[string]any)
	if _, ok := params["_meta"]; ok {
		t.Errorf("bare Cancel must not carry _meta: %v", params)
	}

	b.CancelWithMeta("", map[string]any{
		"cancelTrigger":    "esc",
		"cancelSubagents":  false,
		"rewindIfNoOutput": true,
	})
	msg = w.last()
	params, _ = msg["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/cancel params carry no _meta: %v", params)
	}
	want := map[string]any{"cancelTrigger": "esc", "cancelSubagents": false, "rewindIfNoOutput": true}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("_meta = %v, want %v", meta, want)
	}
	if params["sessionId"] != "s1" {
		t.Errorf("sessionId = %v, want s1", params["sessionId"])
	}
}

// ── A4: session/resume `_meta` 直通 ─────────────────────────────────

func TestResumeSessionForwardsMeta(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	runResolved(t, b, w, map[string]any{"sessionId": "hist-1"}, func() error {
		_, err := b.ResumeSession(ctx, "hist-1", "/ws", map[string]any{"yoloMode": true})
		return err
	})
	method, params := lastRequestParams(t, w)
	if method != "session/resume" {
		t.Fatalf("method = %v, want session/resume", method)
	}
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/resume params carry no _meta: %v", params)
	}
	if !reflect.DeepEqual(meta, map[string]any{"yoloMode": true}) {
		t.Errorf("_meta = %v, want {yoloMode:true}", meta)
	}
	// additionalDirectories stays [] (never extended).
	if _, ok := params["additionalDirectories"].([]any); !ok {
		t.Errorf("additionalDirectories = %v, want []", params["additionalDirectories"])
	}
}

// ── A5: session/list 可选字段直通 ───────────────────────────────────

func TestListSessionsForwardsOpts(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	// 无 opts → wire 保持 {}。
	runResolved(t, b, w, map[string]any{"sessions": []any{}}, func() error {
		_, _, _, err := b.ListSessions(ctx)
		return err
	})
	method, params := lastRequestParams(t, w)
	if method != "session/list" {
		t.Fatalf("method = %v, want session/list", method)
	}
	if !reflect.DeepEqual(params, map[string]any{}) {
		t.Errorf("no-opts params = %v, want {}", params)
	}

	// opts → cwd/cursor/`_meta`（ACP ListSessionsRequest：cwd? cursor?
	// meta→`_meta`，handlers/session.rs:308-310）。
	runResolved(t, b, w, map[string]any{"sessions": []any{}}, func() error {
		_, _, _, err := b.ListSessions(ctx, ListSessionsOpts{
			Cwd:    "/ws",
			Cursor: "abc",
			Meta:   map[string]any{"clientType": "pager"},
		})
		return err
	})
	method, params = lastRequestParams(t, w)
	if method != "session/list" {
		t.Fatalf("method = %v, want session/list", method)
	}
	want := map[string]any{"cwd": "/ws", "cursor": "abc", "_meta": map[string]any{"clientType": "pager"}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("opts params = %v, want %v", params, want)
	}
}

// ── A5b: session/list 响应游标与 `_meta` 透传 ───────────────────────

// The session/list response's pagination cursor (camelCase nextCursor or
// snake_case next_cursor — both accepted) and `_meta` are returned for
// passthrough; "" / nil when absent.
func TestListSessionsReturnsCursorAndMeta(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	// camelCase nextCursor + `_meta`。
	var gotCursor string
	var gotMeta map[string]any
	runResolved(t, b, w, map[string]any{
		"sessions":   []any{},
		"nextCursor": "c2",
		"_meta":      map[string]any{"has_more": true},
	}, func() error {
		var err error
		_, gotCursor, gotMeta, err = b.ListSessions(ctx)
		return err
	})
	if gotCursor != "c2" {
		t.Errorf("nextCursor = %q, want c2", gotCursor)
	}
	if !reflect.DeepEqual(gotMeta, map[string]any{"has_more": true}) {
		t.Errorf("meta = %v, want {has_more:true}", gotMeta)
	}

	// snake_case next_cursor 兼容。
	runResolved(t, b, w, map[string]any{
		"sessions":    []any{},
		"next_cursor": "c3",
	}, func() error {
		var err error
		_, gotCursor, _, err = b.ListSessions(ctx)
		return err
	})
	if gotCursor != "c3" {
		t.Errorf("next_cursor = %q, want c3", gotCursor)
	}

	// 缺省：游标 ""、meta nil。
	runResolved(t, b, w, map[string]any{"sessions": []any{}}, func() error {
		var err error
		_, gotCursor, gotMeta, err = b.ListSessions(ctx)
		return err
	})
	if gotCursor != "" || gotMeta != nil {
		t.Errorf("absent cursor/meta: cursor = %q, meta = %v, want \"\"/nil", gotCursor, gotMeta)
	}
}

// ── x.ai/session/updates 可选字段直通 ───────────────────────────────

func TestSessionUpdatesOptsWire(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	// 无 opts → 原有 wire 形状（sessionId/cwd）。
	runResolved(t, b, w, map[string]any{"updates": []any{}, "totalCount": float64(0), "hasMore": false}, func() error {
		_, err := b.SessionUpdates(ctx, "s1", "/ws")
		return err
	})
	method, params := lastRequestParams(t, w)
	if method != "_x.ai/session/updates" {
		t.Fatalf("method = %v, want _x.ai/session/updates", method)
	}
	if !reflect.DeepEqual(params, map[string]any{"sessionId": "s1", "cwd": "/ws"}) {
		t.Errorf("no-opts params = %v, want {sessionId,cwd}", params)
	}

	// 全字段 → stream/chunkSize/turnIndex 与 offset/limit 并存（camelCase，
	// extensions/session_updates.rs Request）。
	off := int64(-100)
	lim := 5
	cs := 32
	ti := 3
	runResolved(t, b, w, map[string]any{"updates": []any{}, "totalCount": float64(0), "hasMore": false}, func() error {
		_, err := b.SessionUpdates(ctx, "s1", "/ws", SessionUpdatesOpts{
			Offset: &off, Limit: &lim, Stream: true, ChunkSize: &cs, TurnIndex: &ti,
		})
		return err
	})
	method, params = lastRequestParams(t, w)
	want := map[string]any{
		"sessionId": "s1", "cwd": "/ws",
		"offset": float64(-100), "limit": float64(5),
		"stream": true, "chunkSize": float64(32), "turnIndex": float64(3),
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("opts params = %v, want %v", params, want)
	}
}

// ── B: 通知字段保全 ─────────────────────────────────────────────────

// The real agent's x.ai/scheduled_task_created payload is a
// SessionNotification envelope: {sessionId, update:{sessionUpdate:
// "scheduled_task_created", task_id, prompt, human_schedule, next_fire_at},
// _meta:{eventId, agentTimestampMs, x.ai/schedulerGeneration,
// x.ai/schedulerRevision}} (tools/notification_bridge.rs:819). The typed
// event must normalize taskId/prompt/interval/nextFireAt AND carry the full
// raw payloads (rawParams/rawTask/meta) so nothing is dropped.
func TestScheduledTaskCreatedPreservesFullPayload(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()

	params := map[string]any{
		"sessionId": "s1",
		"update": map[string]any{
			"sessionUpdate":  "scheduled_task_created",
			"task_id":        "loop-1",
			"prompt":         "check status",
			"human_schedule": "every hour",
			"next_fire_at":   "2026-08-08T10:00:00Z",
		},
		"_meta": map[string]any{
			"eventId":                  "evt-9",
			"agentTimestampMs":         float64(1750000000000),
			"x.ai/schedulerGeneration": "g-1",
			"x.ai/schedulerRevision":   float64(4),
		},
	}
	b.handleXaiNotification("x.ai/scheduled_task_created", params)
	ev := <-ch

	if ev["type"] != "scheduled_task_created" || ev["sessionId"] != "s1" {
		t.Fatalf("event = %v", ev)
	}
	task, ok := ev["task"].(map[string]any)
	if !ok {
		t.Fatalf("task = %v, want object", ev["task"])
	}
	wantTask := map[string]any{
		"taskId": "loop-1", "prompt": "check status",
		"interval": "every hour", "nextFireAt": "2026-08-08T10:00:00Z",
	}
	for k, want := range wantTask {
		if task[k] != want {
			t.Errorf("task[%s] = %v, want %v", k, task[k], want)
		}
	}
	// rawTask = 完整 update（humanSchedule/status/enabled/… 不丢）。
	rawTask, ok := ev["rawTask"].(map[string]any)
	if !ok || rawTask["task_id"] != "loop-1" || rawTask["human_schedule"] != "every hour" {
		t.Errorf("rawTask = %v, want full update map", ev["rawTask"])
	}
	// rawParams = 完整原始 params（含 _meta）。
	rawParams, ok := ev["rawParams"].(map[string]any)
	if !ok || rawParams["sessionId"] != "s1" {
		t.Errorf("rawParams = %v, want full params", ev["rawParams"])
	}
	// meta = 完整 _meta（eventId / scheduler generation-revision）。
	meta, ok := ev["meta"].(map[string]any)
	if !ok || meta["eventId"] != "evt-9" ||
		meta["x.ai/schedulerGeneration"] != "g-1" || meta["x.ai/schedulerRevision"] != float64(4) {
		t.Errorf("meta = %v, want full _meta", ev["meta"])
	}
}

func TestScheduledTaskDeletedPreservesRawParams(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()

	params := map[string]any{
		"sessionId": "s1",
		"task_id":   "loop-1",
		"_meta":     map[string]any{"eventId": "evt-10", "x.ai/schedulerGeneration": "g-1", "x.ai/schedulerRevision": float64(5)},
	}
	b.handleXaiNotification("x.ai/scheduled_task_deleted", params)
	ev := <-ch
	if ev["type"] != "scheduled_task_deleted" || ev["taskId"] != "loop-1" || ev["sessionId"] != "s1" {
		t.Fatalf("event = %v", ev)
	}
	rawParams, ok := ev["rawParams"].(map[string]any)
	if !ok || rawParams["task_id"] != "loop-1" {
		t.Errorf("rawParams = %v, want full params", ev["rawParams"])
	}
	if meta, ok := ev["meta"].(map[string]any); !ok || meta["eventId"] != "evt-10" {
		t.Errorf("meta = %v, want full _meta", ev["meta"])
	}
	// 无 reason 来源（update/params/task 都没有）→ 缺省 unknown。
	if ev["reason"] != "unknown" {
		t.Errorf("reason = %v, want unknown", ev["reason"])
	}
}

// models/update: 保留合并后的 models_update 广播，并附加原始 params。
func TestModelsUpdateCarriesRaw(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "grok-3",
		"availableModels": []any{map[string]any{"modelId": "grok-3", "name": "Grok 3"}},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()

	incoming := map[string]any{
		"currentModelId": "grok-4",
		// The machine-wide catalog still offers grok-3 → the session's
		// current selection is preserved (TUI update_catalog semantics).
		"availableModels": []any{
			map[string]any{"modelId": "grok-3", "name": "Grok 3"},
			map[string]any{"modelId": "grok-4", "name": "Grok 4"},
		},
	}
	b.handleXaiNotification("x.ai/models/update", incoming)
	ev := <-ch
	if ev["type"] != "models_update" {
		t.Fatalf("event type = %v, want models_update", ev["type"])
	}
	// 合并后的 catalog（保留当前选中模型）。
	merged, _ := ev["params"].(map[string]any)
	if merged["currentModelId"] != "grok-3" {
		t.Errorf("merged currentModelId = %v, want grok-3 preserved", merged["currentModelId"])
	}
	// raw = 原始通知 params（机器级广播不丢）。
	raw, ok := ev["raw"].(map[string]any)
	if !ok || raw["currentModelId"] != "grok-4" {
		t.Errorf("raw = %v, want original params", ev["raw"])
	}
}

// chunk / user_chunk 事件不携带完整原始 update（fullUpdate）：typed 字段即
// wire 契约，两条出口（SSE / hub）本就剥离该键，FE 无消费者。
func TestChunkEventsCarryNoFullUpdate(t *testing.T) {
	for _, tc := range []struct {
		kind string
		want string
	}{
		{"agent_message_chunk", "chunk"},
		{"user_message_chunk", "user_chunk"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{
				"sessionUpdate": tc.kind,
				"messageId":     "m-1",
				"content":       map[string]any{"type": "text", "text": "hello"},
			}
			b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
			ev := <-ch
			if ev["type"] != tc.want {
				t.Fatalf("event type = %v, want %s", ev["type"], tc.want)
			}
			if _, ok := ev["fullUpdate"]; ok {
				t.Errorf("chunk event must not carry fullUpdate: %v", ev["fullUpdate"])
			}
		})
	}
}

// TestSnapshotConcurrentModelMutation is a -race regression test for the
// Snapshot deep-copy fix: serializing a snapshot while the session's
// models map is mutated under the bridge lock must not race.
func TestSnapshotConcurrentModelMutation(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "grok-3",
		"availableModels": []any{map[string]any{"modelId": "grok-3", "name": "Grok 3"}},
	}
	b.sessions["s1"].modes = map[string]any{"currentModeId": "plan"}
	b.sessions["s1"].sessionMeta = map[string]any{"kind": "fresh"}
	b.activeSessionID = "s1"
	b.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Mutation path mirrors readStdout: patchSessionModels /
		// applyModelsCatalog mutate shared maps under b.mu.
		for i := 0; i < 2000; i++ {
			b.mu.Lock()
			if m, ok := b.sessions["s1"].models.(map[string]any); ok {
				m["currentModelId"] = "grok-4"
				if av, ok := m["availableModels"].([]any); ok && len(av) > 0 {
					if mm, ok := av[0].(map[string]any); ok {
						mm["name"] = "Grok 4"
					}
				}
			}
			b.mu.Unlock()
		}
	}()

	for i := 0; i < 2000; i++ {
		snap := b.Snapshot() // serialized later by the caller, lock-free
		if snap.Models != nil {
			_, _ = json.Marshal(snap)
		}
		if snap.Roster != nil {
			_, _ = json.Marshal(snap.Roster)
		}
	}
	<-done
}
