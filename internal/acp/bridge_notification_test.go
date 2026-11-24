package acp

import (
	"reflect"
	"testing"
	"time"
)

// bridge_notification_test.go — 通知 / sessionUpdate 语义化：
//   - Part 1: x.ai/* 通知 typed 化（14 个方法取代 ext_notification 兜底）。
//   - Part 2: sessionUpdate kind 分发（dispatchSessionUpdateKind）在两个
//     载体（官方 session/update 与 x.ai/session_notification）上单发
//     typed 事件；generic session_notification 仅保留给未建模 kind
//     （前向兼容载体）。

// ── Part 1: x.ai 通知 typed 化 ──────────────────────────────────────

// 16 个 x.ai 通知方法到达 typed SSE 事件（取代 ext_notification 兜底），
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
		{"x.ai/follow_ups", "follow_ups"},
		{"x.ai/leader/version_mismatch", "leader_version_mismatch"},
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

// x.ai/session/setup（session/new 的阶段进度）是唯一一个 sessionId 可能为
// **显式 null** 的轨道：会话 id 铸造前的阶段不该被错标到新建之前那个活动会话
// 上。有 id 时照常盖章。
func TestSessionSetupNotificationSessionTagging(t *testing.T) {
	mk := func() (*Bridge, chan Event, func()) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		b.mu.Lock()
		b.sessions["old"] = &SessionState{SessionID: "old", Cwd: "/ws"}
		b.activeSessionID = "old"
		b.mu.Unlock()
		ch, unsub := b.Subscribe()
		return b, ch, unsub
	}

	t.Run("explicit null stays untagged", func(t *testing.T) {
		b, ch, unsub := mk()
		defer unsub()
		params := map[string]any{"method": "session/new", "phase": "auth", "sessionId": nil}
		b.handleXaiNotification("x.ai/session/setup", params)
		ev := <-ch
		if ev["type"] != "ext_notification" || ev["method"] != "x.ai/session/setup" {
			t.Fatalf("event = %v, want the generic passthrough", ev)
		}
		if _, ok := ev["sessionId"]; ok {
			t.Errorf("sessionId = %v, want absent (must not inherit the previous active session)", ev["sessionId"])
		}
		if !reflect.DeepEqual(ev["params"], params) {
			t.Errorf("params = %v, want verbatim %v", ev["params"], params)
		}
	})

	t.Run("minted id is tagged", func(t *testing.T) {
		b, ch, unsub := mk()
		defer unsub()
		b.handleXaiNotification("x.ai/session/setup", map[string]any{
			"method": "session/new", "phase": "finalize_response", "sessionId": "new-1",
		})
		ev := <-ch
		if ev["sessionId"] != "new-1" {
			t.Errorf("sessionId = %v, want new-1", ev["sessionId"])
		}
	})

	t.Run("absent id does not fall back either", func(t *testing.T) {
		b, ch, unsub := mk()
		defer unsub()
		b.handleXaiNotification("x.ai/session/setup", map[string]any{"method": "session/new", "phase": "auth"})
		ev := <-ch
		if _, ok := ev["sessionId"]; ok {
			t.Errorf("sessionId = %v, want absent", ev["sessionId"])
		}
	})
}

// ── Part 2: sessionUpdate kind 语义化（x.ai 载体） ──────────────────

// x.ai/session_notification 载体：modeled kind 只发 typed（无 generic）；
// params._meta 非空时保全进 typed 事件的 meta 字段。
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
	var sawTyped bool
	var events []Event
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			events = append(events, ev)
			switch ev["type"] {
			case "session_notification":
				t.Errorf("unexpected generic session_notification for modeled kind: %v", ev)
			case "subagent_progress":
				sawTyped = true
				if !reflect.DeepEqual(ev["update"], update) {
					t.Errorf("typed update = %v, want the update verbatim", ev["update"])
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
				}
				// `_meta`（eventId 等）保全进 typed 事件 meta 字段。
				meta, _ := ev["meta"].(map[string]any)
				if meta["eventId"] != "evt-42" {
					t.Errorf("typed meta = %v, want eventId preserved", ev["meta"])
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawTyped {
		t.Errorf("no typed subagent_progress event")
	}
}

// x.ai standalone 通知通道（x.ai/task_backgrounded / x.ai/task_completed /
// x.ai/monitor_event 方法）形状保持 {type, params, sessionId}——与 kind
// typed 事件的 {type, update, sessionId} 不同载体、不同形状（见
// TestTaskKindsUseUpdateShape）。
func TestXaiStandaloneTaskKindsParamsShape(t *testing.T) {
	for _, c := range []struct{ method, want string }{
		{"x.ai/task_backgrounded", "task_backgrounded"},
		{"x.ai/task_completed", "task_completed"},
		{"x.ai/monitor_event", "monitor_event"},
	} {
		t.Run(c.method, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			params := map[string]any{"sessionId": "s1", "task_id": "t-1"}
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
		})
	}
}

// 未建模 kind（compaction_checkpoint / rewind_marker / unknown 及未来
// 任何新 kind）：只发 generic session_notification（前向兼容载体），
// 不新增 typed 事件。
func TestUnmodeledKindGenericOnly(t *testing.T) {
	for _, kind := range []string{
		"compaction_checkpoint", "rewind_marker", "unknown", "future_kind_alpha",
	} {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			update := map[string]any{"sessionUpdate": kind, "some": "payload"}
			b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
			ev := <-ch
			if ev["type"] != "session_notification" {
				t.Fatalf("event type = %v, want session_notification", ev["type"])
			}
			if ev["method"] != "session/update" {
				t.Errorf("generic method = %v, want session/update", ev["method"])
			}
			params, _ := ev["params"].(map[string]any)
			if !reflect.DeepEqual(params, map[string]any{"update": update}) {
				t.Errorf("generic params = %v, want the update forwarded verbatim", params)
			}
			if ev["sessionId"] != "s1" {
				t.Errorf("generic sessionId = %v, want s1", ev["sessionId"])
			}
			// 只发 generic：不得再有第二个事件。
			select {
			case extra := <-ch:
				t.Fatalf("unexpected extra event: %v", extra)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// x.ai 载体未建模 kind：只发 generic（method 原样、params 含 `_meta`
// 保全），无 typed 事件。
func TestXaiUnmodeledKindGenericOnly(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{"sessionUpdate": "future_kind_x", "some": "payload"}
	params := map[string]any{
		"sessionId": "s1",
		"update":    update,
		"_meta":     map[string]any{"eventId": "evt-7"},
	}
	b.handleXaiNotification("x.ai/session_notification", params)
	ev := <-ch
	if ev["type"] != "session_notification" || ev["method"] != "x.ai/session_notification" {
		t.Fatalf("event = %v, want generic x.ai/session_notification", ev)
	}
	if !reflect.DeepEqual(ev["params"], params) {
		t.Errorf("generic params = %v, want full original params (含 _meta)", ev["params"])
	}
	if ev["sessionId"] != "s1" {
		t.Errorf("generic sessionId = %v, want s1", ev["sessionId"])
	}
	// 只发 generic：不得再有第二个事件。
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra event: %v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}
