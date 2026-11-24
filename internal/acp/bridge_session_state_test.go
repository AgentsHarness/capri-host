package acp

import (
	"testing"
	"time"
)

// ── L5: sessionId 显式 id 优先于活动会话回落 ─────────────────────────

// 显式 sessionId（camelCase / snake_case）优先于活动会话回落；官方
// session/update 载体恒带 id、缺失时绝不回落（多客户端下回落会误标）；
// _x.ai 包装形式的 leader envelope 外层 sessionId 保全。
func TestNotificationSessionIdExplicitPreference(t *testing.T) {
	seedActive := func(b *Bridge) {
		b.mu.Lock()
		b.sessions["active-a"] = &SessionState{SessionID: "active-a"}
		b.activeSessionID = "active-a"
		b.mu.Unlock()
	}

	t.Run("camelCase params sessionId wins over active", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		seedActive(b)
		ch, unsub := b.Subscribe()
		defer unsub()
		// 活动会话是 active-a，但通知自带 bg-b → 必须标 bg-b。
		b.handleXaiNotification("x.ai/task_completed",
			map[string]any{"sessionId": "bg-b", "taskId": "t1"})
		ev := <-ch
		if ev["sessionId"] != "bg-b" {
			t.Fatalf("sessionId = %v, want bg-b — 显式 id 必须压过活动会话回落", ev["sessionId"])
		}
	})

	t.Run("snake_case session_id also honored", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		seedActive(b)
		ch, unsub := b.Subscribe()
		defer unsub()
		b.handleXaiNotification("x.ai/task_completed",
			map[string]any{"session_id": "bg-b", "taskId": "t1"})
		ev := <-ch
		if ev["sessionId"] != "bg-b" {
			t.Fatalf("sessionId = %v, want bg-b（session_id 载体）", ev["sessionId"])
		}
	})

	t.Run("official session/update never falls back to active", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		seedActive(b)
		ch, unsub := b.Subscribe()
		defer unsub()
		// 官方载体缺 id（正常 wire 不会发生）：不带活动会话标签（保持空），
		// 绝不误标到宿主的活动会话。
		b.handleSessionUpdate(map[string]any{
			"update": map[string]any{"sessionUpdate": "diff_review", "x": 1},
		})
		ev := <-ch
		if sid, _ := ev["sessionId"].(string); sid != "" {
			t.Fatalf("sessionId = %q, want empty — 官方载体不得回落活动会话", sid)
		}
	})

	t.Run("wrapped _x.ai envelope keeps outer sessionId", func(t *testing.T) {
		b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
		seedActive(b)
		ch, unsub := b.Subscribe()
		defer unsub()
		// leader 包装形式：外层 envelope params 带 sessionId，内层 params
		// 不带 → unwrapExtMethod 必须保全外层显式 id。
		b.onAgentMessage(map[string]any{
			"method": "_x.ai/task_completed",
			"params": map[string]any{
				"method":    "x.ai/task_completed",
				"params":    map[string]any{"taskId": "t1"},
				"sessionId": "bg-b",
			},
		})
		ev := <-ch
		if ev["sessionId"] != "bg-b" {
			t.Fatalf("sessionId = %v, want bg-b — 外层 envelope 的显式 id 必须存活", ev["sessionId"])
		}
		params, _ := ev["params"].(map[string]any)
		if params["sessionId"] != "bg-b" {
			t.Errorf("转发 params sessionId = %v, want bg-b", params["sessionId"])
		}
	})
}

// ── Part 3: 队列快照缓存（/api/queue/status 数据源）──────────────

// x.ai/queue/changed 到达后 QueueStatus 返回该会话的克隆快照；未广播
// 过/未知会话返回 nil；agent 进程重建（resetRoster 清理）后缓存作废。
func TestQueueStatusSnapshotCache(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})

	// 空 sid 与未广播会话 → nil。
	if snap := b.QueueStatus(""); snap != nil {
		t.Fatalf("QueueStatus(\"\") = %v, want nil", snap)
	}
	if snap := b.QueueStatus("s1"); snap != nil {
		t.Fatalf("QueueStatus before any broadcast = %v, want nil", snap)
	}

	params := map[string]any{
		"sessionId":         "s1",
		"entries":           []any{map[string]any{"id": "q1", "text": "hello"}},
		"running_prompt_id": "p1",
	}
	b.handleXaiNotification("x.ai/queue/changed", params)

	snap := b.QueueStatus("s1")
	if snap == nil {
		t.Fatal("QueueStatus after broadcast = nil, want snapshot")
	}
	if snap["sessionId"] != "s1" {
		t.Errorf("snapshot sessionId = %v, want s1", snap["sessionId"])
	}
	// 快照是深拷贝：改原 params 不得污染缓存。
	params["running_prompt_id"] = "mutated"
	if got := b.QueueStatus("s1")["running_prompt_id"]; got != "p1" {
		t.Errorf("cached running_prompt_id = %v, want p1（快照必须是克隆）", got)
	}

	// 其他会话不受影响。
	if snap2 := b.QueueStatus("other"); snap2 != nil {
		t.Errorf("QueueStatus(other) = %v, want nil", snap2)
	}

	// 再次广播 → 覆盖为最新快照。
	b.handleXaiNotification("x.ai/queue/changed", map[string]any{
		"sessionId": "s1", "entries": []any{}, "running_prompt_id": nil,
	})
	if got := b.QueueStatus("s1")["running_prompt_id"]; got != nil {
		t.Errorf("running_prompt_id after empty broadcast = %v, want nil", got)
	}

	// agent 进程重建（同 resetRoster 清空路径）→ 缓存作废。
	b.resetRoster("test")
	if snap3 := b.QueueStatus("s1"); snap3 != nil {
		t.Errorf("QueueStatus after resetRoster = %v, want nil", snap3)
	}
}

// ACP CurrentModeUpdate 是 {currentModeId}，不是 {modeState}。agent 经
// enter_plan_mode / exit_plan_mode 进出 plan 时只发 currentModeId；host
// 必须把它写进 s.modes 再广播，否则 FE composer 一直停在旧模式。
func TestCurrentModeUpdatePatchesCurrentModeId(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].modes = map[string]any{
		"currentModeId": "default",
		"availableModes": []any{
			map[string]any{"id": "default"},
			map[string]any{"id": "plan"},
		},
	}
	b.mu.Unlock()

	ch, unsub := b.Subscribe()
	defer unsub()

	drain := func() map[string]any {
		t.Helper()
		select {
		case ev := <-ch:
			return ev
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for modes_update")
			return nil
		}
	}

	b.handleSessionUpdate(map[string]any{
		kSessionID: "s1",
		kUpdate: map[string]any{
			kSessionUpdate: "current_mode_update",
			kCurrentModeId: "plan",
		},
	})
	ev := drain()
	if ev[kType] != "modes_update" {
		t.Fatalf("type = %v, want modes_update", ev[kType])
	}
	if ev[kSessionID] != "s1" {
		t.Errorf("sessionId = %v, want s1", ev[kSessionID])
	}
	modes, _ := ev["modes"].(map[string]any)
	if modes[kCurrentModeId] != "plan" {
		t.Fatalf("broadcast currentModeId = %v, want plan", modes[kCurrentModeId])
	}
	if modes["availableModes"] == nil {
		t.Error("availableModes dropped; patch must keep the existing catalog")
	}
	b.mu.Lock()
	cached, _ := b.sessions["s1"].modes.(map[string]any)
	got := cached[kCurrentModeId]
	b.mu.Unlock()
	if got != "plan" {
		t.Errorf("session cache currentModeId = %v, want plan", got)
	}

	b.handleSessionUpdate(map[string]any{
		kSessionID: "s1",
		kUpdate: map[string]any{
			kSessionUpdate: "current_mode_update",
			kCurrentModeId: "default",
		},
	})
	ev = drain()
	modes, _ = ev["modes"].(map[string]any)
	if modes[kCurrentModeId] != "default" {
		t.Fatalf("exit broadcast currentModeId = %v, want default", modes[kCurrentModeId])
	}
}

func TestCurrentModeUpdateSnakeCaseAndModeState(t *testing.T) {
	b, _ := metaReadyBridge(t)
	ch, unsub := b.Subscribe()
	defer unsub()

	b.handleSessionUpdate(map[string]any{
		kSessionID: "s1",
		kUpdate: map[string]any{
			kSessionUpdate:    "current_mode_update",
			"current_mode_id": "plan",
		},
	})
	ev := <-ch
	modes, _ := ev["modes"].(map[string]any)
	if modes[kCurrentModeId] != "plan" {
		t.Fatalf("snake_case currentModeId = %v, want plan", modes[kCurrentModeId])
	}

	b.handleSessionUpdate(map[string]any{
		kSessionID: "s1",
		kUpdate: map[string]any{
			kSessionUpdate: "current_mode_update",
			kModeState:     map[string]any{kCurrentModeId: "ask"},
		},
	})
	ev = <-ch
	modes, _ = ev["modes"].(map[string]any)
	if modes[kCurrentModeId] != "ask" {
		t.Fatalf("modeState currentModeId = %v, want ask", modes[kCurrentModeId])
	}
}

func TestAvailableCommandsUpdateForwardsAvailableCommands(t *testing.T) {
	b, _ := metaReadyBridge(t)
	ch, unsub := b.Subscribe()
	defer unsub()

	cmds := []any{map[string]any{"name": "deploy", "description": "deploy"}}
	b.handleSessionUpdate(map[string]any{
		kSessionID: "s1",
		kUpdate: map[string]any{
			kSessionUpdate:     "available_commands_update",
			kAvailableCommands: cmds,
		},
	})
	ev := <-ch
	if ev[kType] != "commands_update" {
		t.Fatalf("type = %v, want commands_update", ev[kType])
	}
	got, _ := ev[kCommands].([]any)
	if len(got) != 1 {
		t.Fatalf("commands = %v, want [{name:deploy}]", ev[kCommands])
	}
	row, _ := got[0].(map[string]any)
	if row["name"] != "deploy" {
		t.Errorf("command name = %v, want deploy", row["name"])
	}
}

func TestSessionInfoForwardsTitleIsManual(t *testing.T) {
	b, _ := metaReadyBridge(t)
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(map[string]any{
		kSessionID: "s1",
		kMeta:      map[string]any{"x.ai/titleIsManual": true},
		kUpdate: map[string]any{
			kSessionUpdate: "session_info_update",
			kTitle:         "手动标题",
		},
	})
	ev := <-ch
	if ev[kType] != "session_info" {
		t.Fatalf("type = %v, want session_info", ev[kType])
	}
	if ev["titleIsManual"] != true {
		t.Fatalf("titleIsManual = %v, want true", ev["titleIsManual"])
	}
	if ev[kTitle] != "手动标题" {
		t.Errorf("title = %v, want 手动标题", ev[kTitle])
	}
}
