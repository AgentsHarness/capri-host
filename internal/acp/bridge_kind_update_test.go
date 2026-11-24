package acp

import (
	"reflect"
	"testing"
	"time"
)

// ── Part 2: sessionUpdate kind 语义化（官方载体） ───────────────────

// 官方 session/update 载体：每个 modeled kind 单发 typed 事件
// {type: <kind>, update: <原始 update>, sessionId}，无 generic
// session_notification（FE 已适配 typed 消费；generic 仅保留给未建模
// kind 作前向兼容，见 TestUnmodeledKindGenericOnly）。
func TestSessionUpdateKindTypedOnly(t *testing.T) {
	kinds := []string{
		"diff_review", "subagent_spawned", "image_dropped", "retry_state",
		"memory_files", "hook_execution", "workflow_updated",
		"session_summary_generated", "auto_compact_completed", "goal_updated",
		"last_turn_summary",
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{"sessionUpdate": kind, "some": "payload"}
			b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
			ev := <-ch
			if ev["type"] != kind {
				t.Fatalf("event type = %v, want %s", ev["type"], kind)
			}
			if !reflect.DeepEqual(ev["update"], update) {
				t.Errorf("typed update = %v, want the original update map verbatim", ev["update"])
			}
			if ev["sessionId"] != "s1" {
				t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
			}
			if _, ok := ev["meta"]; ok {
				t.Errorf("typed event must not carry meta without params._meta: %v", ev)
			}
			// 单发：无 generic，不得再有第二个事件。
			select {
			case extra := <-ch:
				t.Fatalf("unexpected extra event: %v", extra)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// task_backgrounded / task_completed / monitor_event 的 kind typed 事件
// 形状与其它 kind 统一：{type, update, sessionId}（update = 原始 update
// 对象）；x.ai standalone 通知通道（x.ai/task_backgrounded 等方法）才用
// {type, params, sessionId}（见 TestXaiStandaloneTaskKindsParamsShape）。
func TestTaskKindsUseUpdateShape(t *testing.T) {
	for _, kind := range []string{"task_backgrounded", "task_completed", "monitor_event"} {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{"sessionUpdate": kind, "task_id": "t-1"}
			b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
			ev := <-ch
			if ev["type"] != kind {
				t.Fatalf("event type = %v, want %s", ev["type"], kind)
			}
			if !reflect.DeepEqual(ev["update"], update) {
				t.Errorf("typed update = %v, want the original update verbatim", ev["update"])
			}
			if ev["sessionId"] != "s1" {
				t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
			}
			if _, ok := ev["params"]; ok {
				t.Errorf("typed event must not carry params: %v", ev)
			}
			// 单发：无 generic，不得再有第二个事件。
			select {
			case extra := <-ch:
				t.Fatalf("unexpected extra event: %v", extra)
			case <-time.After(50 * time.Millisecond):
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
	update := map[string]any{"sessionUpdate": "scheduled_task_deleted", "task_id": "loop-1", "reason": "expired"}
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
	// reason 归一化：update.reason 透传到事件（无 reason 时才是 unknown）。
	if ev["reason"] != "expired" {
		t.Errorf("reason = %v, want expired", ev["reason"])
	}
	rawParams, ok := ev["rawParams"].(map[string]any)
	if !ok || rawParams["sessionId"] != "s1" {
		t.Errorf("rawParams = %v, want the full carrier params", ev["rawParams"])
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
