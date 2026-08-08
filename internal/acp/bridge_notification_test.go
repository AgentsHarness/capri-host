package acp

import (
	"reflect"
	"testing"
	"time"
)

// bridge_notification_test.go — 通知 / sessionUpdate 语义化：
//   - Part 1: x.ai/* 通知 typed 化（14 个方法取代 ext_notification 兜底）。
//   - Part 2: sessionUpdate kind 分发（dispatchSessionUpdateKind）在两个
//     载体（官方 session/update 与 x.ai/session_notification）上双发
//     generic session_notification + 增量 typed 事件。

// ── Part 1: x.ai 通知 typed 化 ──────────────────────────────────────

// 14 个 x.ai 通知方法到达 typed SSE 事件（取代 ext_notification 兜底），
// params 原样透传；无第二个事件（typed 事件即唯一事件）。
func TestXaiNotificationTypedEvents(t *testing.T) {
	cases := []struct {
		method string
		want   string
	}{
		{"x.ai/session/updates/chunk", "session_updates_chunk"},
		{"x.ai/queue/changed", "queue_changed"},
		{"x.ai/config_changed", "config_changed"},
		{"x.ai/settings/update", "settings_update"},
		{"x.ai/fs_notify", "fs_notify"},
		{"x.ai/fs/index", "fs_index"},
		{"x.ai/fs/index/delta", "fs_index_delta"},
		{"x.ai/search/fuzzy/status", "search_fuzzy_status"},
		{"x.ai/search/content/status", "search_content_status"},
		{"x.ai/git/worktree/status", "git_worktree_status"},
		{"x.ai/mcp/init_progress", "mcp_init_progress"},
		{"x.ai/terminal/pty/notification", "pty_notification"},
		{"x.ai/session/interjection", "session_interjection"},
		{"x.ai/leader_reconnected", "leader_reconnected"},
	}
	for _, c := range cases {
		t.Run(c.method, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			params := map[string]any{"sessionId": "s1", "payload": "x"}
			b.handleXaiNotification(c.method, params)
			ev := <-ch
			if ev["type"] != c.want {
				t.Fatalf("event type = %v, want %s", ev["type"], c.want)
			}
			if !reflect.DeepEqual(ev["params"], params) {
				t.Errorf("params = %v, want verbatim %v", ev["params"], params)
			}
			if ev["sessionId"] != "s1" {
				t.Errorf("sessionId = %v, want s1", ev["sessionId"])
			}
			// typed 事件取代 ext_notification：不应再有第二个事件。
			select {
			case extra := <-ch:
				t.Fatalf("unexpected extra event: %v", extra)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// 真正未知的 x.ai 方法仍走 ext_notification 兜底（default 分支保留）。
func TestUnknownXaiNotificationStillExtNotification(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	params := map[string]any{"sessionId": "s1"}
	b.handleXaiNotification("x.ai/definitely_not_a_method", params)
	ev := <-ch
	if ev["type"] != "ext_notification" || ev["method"] != "x.ai/definitely_not_a_method" {
		t.Fatalf("event = %v, want ext_notification passthrough", ev)
	}
	if ev["sessionId"] != "s1" {
		t.Errorf("sessionId = %v, want s1", ev["sessionId"])
	}
}

// ── Part 2: sessionUpdate kind 语义化（官方载体） ───────────────────

// 官方 session/update 载体：每个新 typed kind 双发 generic
// session_notification（FE 规范载体，method=session/update，params 原样）
// 与增量 typed 事件 {type: <kind>, update: <原始 update>, sessionId}。
func TestSessionUpdateKindTypedPlusGeneric(t *testing.T) {
	kinds := []string{
		"diff_review", "subagent_spawned", "image_dropped", "retry_state",
		"memory_files", "hook_execution", "workflow_updated",
		"session_summary_generated", "auto_compact_completed", "goal_updated",
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{"sessionUpdate": kind, "some": "payload"}
			b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
			var sawGeneric, sawTyped bool
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && !(sawGeneric && sawTyped) {
				select {
				case ev := <-ch:
					switch ev["type"] {
					case "session_notification":
						sawGeneric = true
						if ev["method"] != "session/update" {
							t.Errorf("generic method = %v, want session/update", ev["method"])
						}
						params, _ := ev["params"].(map[string]any)
						if !reflect.DeepEqual(params, map[string]any{"update": update}) {
							t.Errorf("generic params = %v, want the update forwarded verbatim", params)
						}
					case kind:
						sawTyped = true
						if !reflect.DeepEqual(ev["update"], update) {
							t.Errorf("typed update = %v, want the original update map verbatim", ev["update"])
						}
						if ev["sessionId"] != "s1" {
							t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
						}
					}
				case <-time.After(50 * time.Millisecond):
				}
			}
			if !sawGeneric {
				t.Errorf("no generic session_notification for kind %s", kind)
			}
			if !sawTyped {
				t.Errorf("no typed %s event", kind)
			}
		})
	}
}

// task_backgrounded / task_completed / monitor_event 的 typed 事件形状与
// 现有 x.ai 通道一致（{type, params, sessionId}）—— FE 双通道按 taskId
// 幂等合并。
func TestTaskKindsUseParamsShape(t *testing.T) {
	for _, kind := range []string{"task_backgrounded", "task_completed", "monitor_event"} {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{"sessionUpdate": kind, "task_id": "t-1"}
			params := map[string]any{"sessionId": "s1", "update": update}
			b.handleSessionUpdate(params)
			var sawTyped bool
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && !sawTyped {
				select {
				case ev := <-ch:
					if ev["type"] == kind {
						sawTyped = true
						if !reflect.DeepEqual(ev["params"], params) {
							t.Errorf("typed params = %v, want the carrier params verbatim", ev["params"])
						}
						if ev["sessionId"] != "s1" {
							t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
						}
					}
				case <-time.After(50 * time.Millisecond):
				}
			}
			if !sawTyped {
				t.Fatalf("no typed %s event", kind)
			}
		})
	}
}

// scheduled_task_created 的 KIND（SessionUpdate::ScheduledTaskCreated，
// snake_case 字段）在官方载体上复用 x.ai 通道的归一化 helper：
// task/rawTask/rawParams 保全（meta 无 _meta 时省略）。
func TestScheduledTaskCreatedKindNormalized(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":  "scheduled_task_created",
		"task_id":        "loop-1",
		"prompt":         "check status",
		"human_schedule": "every hour",
		"next_fire_at":   "2026-08-08T10:00:00Z",
	}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var ev Event
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ev == nil {
		select {
		case e := <-ch:
			if e["type"] == "scheduled_task_created" {
				ev = e
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if ev == nil {
		t.Fatal("no typed scheduled_task_created event")
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
	// rawTask = 完整 update（不丢任何原始字段）。
	rawTask, ok := ev["rawTask"].(map[string]any)
	if !ok || rawTask["task_id"] != "loop-1" || rawTask["human_schedule"] != "every hour" {
		t.Errorf("rawTask = %v, want the full update map", ev["rawTask"])
	}
	// rawParams = 完整载体 params。
	rawParams, ok := ev["rawParams"].(map[string]any)
	if !ok || rawParams["sessionId"] != "s1" {
		t.Errorf("rawParams = %v, want the full carrier params", ev["rawParams"])
	}
}

// scheduled_task_deleted 的 KIND 同样归一化（taskId 从 update 内提取）。
func TestScheduledTaskDeletedKindNormalized(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{"sessionUpdate": "scheduled_task_deleted", "task_id": "loop-1"}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var ev Event
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ev == nil {
		select {
		case e := <-ch:
			if e["type"] == "scheduled_task_deleted" {
				ev = e
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if ev == nil {
		t.Fatal("no typed scheduled_task_deleted event")
	}
	if ev["taskId"] != "loop-1" || ev["sessionId"] != "s1" {
		t.Fatalf("event = %v, want scheduled_task_deleted loop-1", ev)
	}
	rawParams, ok := ev["rawParams"].(map[string]any)
	if !ok || rawParams["sessionId"] != "s1" {
		t.Errorf("rawParams = %v, want the full carrier params", ev["rawParams"])
	}
}

// model_changed kind：roster models 缓存更新（currentModelId +
// reasoningEffort）+ models_update / model 事件广播；generic
// session_notification 照发。
func TestModelChangedKindUpdatesRoster(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId": "grok-3",
		"availableModels": []any{
			map[string]any{"modelId": "grok-3", "name": "Grok 3"},
			map[string]any{"modelId": "grok-4", "name": "Grok 4"},
		},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":    "model_changed",
		"model_id":         "grok-4",
		"reasoning_effort": "high",
	}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var sawGeneric, sawModelsUpdate, sawModel bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(sawGeneric && sawModelsUpdate && sawModel) {
		select {
		case ev := <-ch:
			switch ev["type"] {
			case "session_notification":
				sawGeneric = true
			case "models_update":
				sawModelsUpdate = true
				models, _ := ev["params"].(map[string]any)
				if models["currentModelId"] != "grok-4" {
					t.Errorf("models_update currentModelId = %v, want grok-4", models["currentModelId"])
				}
				if models["reasoningEffort"] != "high" {
					t.Errorf("models_update reasoningEffort = %v, want high", models["reasoningEffort"])
				}
			case "model":
				sawModel = true
				if ev["modelId"] != "grok-4" || ev["modelName"] != "Grok 4" || ev["reasoningEffort"] != "high" {
					t.Errorf("model event = %v, want modelId grok-4 / modelName Grok 4 / reasoningEffort high", ev)
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("model sessionId = %v, want s1", ev["sessionId"])
				}
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !sawGeneric || !sawModelsUpdate || !sawModel {
		t.Errorf("sawGeneric=%v sawModelsUpdate=%v sawModel=%v", sawGeneric, sawModelsUpdate, sawModel)
	}
	b.mu.Lock()
	models, _ := b.sessions["s1"].models.(map[string]any)
	b.mu.Unlock()
	if models["currentModelId"] != "grok-4" {
		t.Errorf("roster cache currentModelId = %v, want grok-4", models["currentModelId"])
	}
}

// model_auto_switched kind（previous_model_id/new_model_id/reason）：
// currentModelId 更新；无 reasoningEffort 时保留原 effort（
// patchSessionModels 语义）。model 事件不带 reasoningEffort 键。
func TestModelAutoSwitchedKindUpdatesRoster(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "grok-3",
		"reasoningEffort": "medium",
		"availableModels": []any{
			map[string]any{"modelId": "grok-3", "name": "Grok 3"},
			map[string]any{"modelId": "grok-5", "name": "Grok 5"},
		},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":     "model_auto_switched",
		"previous_model_id": "grok-3",
		"new_model_id":      "grok-5",
		"reason":            "grok-3 不可用",
	}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var sawModelsUpdate, sawModel bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(sawModelsUpdate && sawModel) {
		select {
		case ev := <-ch:
			switch ev["type"] {
			case "models_update":
				sawModelsUpdate = true
				models, _ := ev["params"].(map[string]any)
				if models["currentModelId"] != "grok-5" {
					t.Errorf("models_update currentModelId = %v, want grok-5", models["currentModelId"])
				}
				if models["reasoningEffort"] != "medium" {
					t.Errorf("models_update reasoningEffort = %v, want medium preserved", models["reasoningEffort"])
				}
			case "model":
				sawModel = true
				if ev["modelId"] != "grok-5" || ev["modelName"] != "Grok 5" {
					t.Errorf("model event = %v, want modelId grok-5 / modelName Grok 5", ev)
				}
				if _, ok := ev["reasoningEffort"]; ok {
					t.Errorf("model event must not carry reasoningEffort: %v", ev)
				}
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !sawModelsUpdate || !sawModel {
		t.Errorf("sawModelsUpdate=%v sawModel=%v", sawModelsUpdate, sawModel)
	}
	b.mu.Lock()
	models, _ := b.sessions["s1"].models.(map[string]any)
	b.mu.Unlock()
	if models["currentModelId"] != "grok-5" {
		t.Errorf("roster cache currentModelId = %v, want grok-5", models["currentModelId"])
	}
}

// turn_completed：usage 提取照旧（carrier totalTokens + usage 对象），
// generic session_notification 照发，但绝不新增 standalone typed
// 事件（acp-fe 双路径处理，chat.ts:2768 与 3502）。
func TestTurnCompletedNoTypedEvent(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"_meta":     map[string]any{"totalTokens": float64(1234)},
		"update": map[string]any{
			"sessionUpdate": "turn_completed",
			"prompt_id":     "p-1",
			"stop_reason":   "end_turn",
			"usage":         map[string]any{"totalTokens": float64(55)},
		},
	})
	var sawNotification, sawTurnUsage bool
	var events []Event
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			events = append(events, ev)
			if ev["type"] == "turn_completed" {
				t.Fatalf("unexpected typed turn_completed event: %v", ev)
			}
			switch ev["type"] {
			case "session_notification":
				sawNotification = true
			case "usage":
				// turn 提取的 usage 事件带 usage 对象。
				if _, ok := ev["usage"].(map[string]any); ok {
					sawTurnUsage = true
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawNotification {
		t.Error("no session_notification for turn_completed")
	}
	if !sawTurnUsage {
		t.Error("no usage extraction for turn_completed")
	}
	// 事件集合里不得出现 typed turn_completed（已在循环内断言），且
	// 双 usage（carrier totalTokens + turn 提取）都到达。
	var usageCount int
	for _, ev := range events {
		if ev["type"] == "usage" {
			usageCount++
		}
	}
	if usageCount < 2 {
		t.Errorf("usage events = %d, want carrier-level + turn extraction", usageCount)
	}
}

// ── Part 2: sessionUpdate kind 语义化（x.ai 载体） ──────────────────

// x.ai/session_notification 载体：generic（method 原样、params 含
// `_meta` 保全）+ typed 双发。
func TestXaiSessionNotificationKindDispatch(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":     "subagent_progress",
		"subagent_id":       "sa-1",
		"parent_session_id": "s1",
		"child_session_id":  "s2",
		"duration_ms":       float64(1200),
		"turn_count":        float64(3),
	}
	params := map[string]any{
		"sessionId": "s1",
		"update":    update,
		"_meta":     map[string]any{"eventId": "evt-42"},
	}
	b.handleXaiNotification("x.ai/session_notification", params)
	var sawGeneric, sawTyped bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(sawGeneric && sawTyped) {
		select {
		case ev := <-ch:
			switch ev["type"] {
			case "session_notification":
				sawGeneric = true
				if ev["method"] != "x.ai/session_notification" {
					t.Errorf("generic method = %v, want x.ai/session_notification", ev["method"])
				}
				// `_meta`（eventId 等）保全方式不变。
				p, _ := ev["params"].(map[string]any)
				meta, _ := p["_meta"].(map[string]any)
				if meta["eventId"] != "evt-42" {
					t.Errorf("generic _meta = %v, want eventId preserved", p["_meta"])
				}
			case "subagent_progress":
				sawTyped = true
				if !reflect.DeepEqual(ev["update"], update) {
					t.Errorf("typed update = %v, want the update verbatim", ev["update"])
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
				}
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !sawGeneric || !sawTyped {
		t.Errorf("sawGeneric=%v sawTyped=%v", sawGeneric, sawTyped)
	}
}

// session_info 的 title/updatedAt roster 跟踪在两个载体上都生效：
// 官方载体走官方 kind "session_info_update"，x.ai 载体走其历史 wire
// 形式 "session_info"（dispatch 里两者等价合并；x.ai 通道原先只有
// roster 跟踪，现在与官方载体一致地也发 session_info 事件）。
func TestSessionInfoRosterBothCarriers(t *testing.T) {
	fires := map[string]func(*Bridge, map[string]any){
		"official": func(b *Bridge, params map[string]any) { b.handleSessionUpdate(params) },
		"x.ai":     func(b *Bridge, params map[string]any) { b.handleXaiNotification("x.ai/session_notification", params) },
	}
	kinds := map[string]string{
		"official": "session_info_update",
		"x.ai":     "session_info",
	}
	for name, fire := range fires {
		t.Run(name, func(t *testing.T) {
			b, _ := metaReadyBridge(t)
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{
				"sessionUpdate": kinds[name],
				"title":         "新会话",
				"updatedAt":     "2026-08-08T12:00:00Z",
			}
			fire(b, map[string]any{"sessionId": "s1", "update": update})
			var saw bool
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && !saw {
				select {
				case ev := <-ch:
					if ev["type"] == "session_info" {
						saw = true
						if ev["title"] != "新会话" || ev["updatedAt"] != "2026-08-08T12:00:00Z" {
							t.Errorf("session_info event = %v", ev)
						}
					}
				case <-time.After(50 * time.Millisecond):
				}
			}
			if !saw {
				t.Fatal("no session_info event")
			}
			b.mu.Lock()
			s := b.sessions["s1"]
			b.mu.Unlock()
			if s.Title != "新会话" || s.UpdatedAt != "2026-08-08T12:00:00Z" {
				t.Errorf("roster title/updatedAt = %q/%q, want tracked", s.Title, s.UpdatedAt)
			}
		})
	}
}
