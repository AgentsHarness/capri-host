package acp

import (
	"testing"
	"time"
)

// kind modeled → 无 generic session_notification。usage 提取照旧
// （carrier totalTokens + usage 对象）。
func TestTurnCompletedTypedAndUsage(t *testing.T) {
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
	var sawTyped, sawTurnUsage bool
	var events []Event
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			events = append(events, ev)
			switch ev["type"] {
			case "session_notification":
				t.Errorf("unexpected generic session_notification for modeled kind turn_completed: %v", ev)
			case "turn_completed":
				sawTyped = true
				up, _ := ev["update"].(map[string]any)
				if up["stop_reason"] != "end_turn" || up["prompt_id"] != "p-1" {
					t.Errorf("typed update = %v, want stop_reason/prompt_id verbatim", ev["update"])
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
				}
				// params._meta 非空 → 保全进 typed 事件 meta 字段。
				meta, _ := ev["meta"].(map[string]any)
				if meta["totalTokens"] != float64(1234) {
					t.Errorf("typed meta = %v, want params._meta preserved", ev["meta"])
				}
			case "usage":
				// turn 提取的 usage 事件带 carrier 级 used（usage
				// 账本对象不再透传）。
				if used, ok := asInt(ev["used"]); ok && used == 1234 {
					sawTurnUsage = true
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawTyped {
		t.Error("no typed turn_completed event")
	}
	if !sawTurnUsage {
		t.Error("no usage extraction for turn_completed")
	}
	// 仅 turn 提取一条 usage（carrier 级顶部广播对回合终态已跳过，
	// 提取事件只带 `used`，不含 usage 账本对象）。
	var usageCount int
	for _, ev := range events {
		if ev["type"] == "usage" {
			usageCount++
		}
	}
	if usageCount != 1 {
		t.Errorf("usage events = %d, want exactly 1 (turn extraction only)", usageCount)
	}
}

// 流水期 usage 顶部广播按值去重：同一 _meta.totalTokens 重复到达只广播
// 一次，值变化才再发——实测输出段内 token 数恒定，连续 70+ 条同值。
func TestUsageStreamDedupByValue(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	send := func(used float64, kind string) {
		b.handleSessionUpdate(map[string]any{
			"sessionId": "s1",
			"_meta":     map[string]any{"totalTokens": used},
			"update":    map[string]any{"sessionUpdate": kind},
		})
	}
	send(100, "agent_message_chunk") // 首次：发
	send(100, "agent_message_chunk") // 同值：跳过
	send(200, "agent_message_chunk") // 变值：发
	send(200, "agent_thought_chunk") // 同值：跳过
	deadline := time.Now().Add(300 * time.Millisecond)
	var usageEvents []Event
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev["type"] == "usage" {
				usageEvents = append(usageEvents, ev)
			}
		case <-time.After(20 * time.Millisecond):
			goto done
		}
	}
done:
	if len(usageEvents) != 2 {
		t.Fatalf("usage events = %d, want 2 (value changes only)", len(usageEvents))
	}
	if u1, _ := asInt(usageEvents[0]["used"]); u1 != 100 {
		t.Errorf("usage[0].used = %v, want 100", usageEvents[0]["used"])
	}
	if u2, _ := asInt(usageEvents[1]["used"]); u2 != 200 {
		t.Errorf("usage[1].used = %v, want 200", usageEvents[1]["used"])
	}
}

// available_commands_update 同样盖着 _meta.totalTokens，但 agent 从不把它
// 落盘——它的值只能来自「模型响应后重新广播命令表」这一时机。把它写进
// 上下文用量缓存会让 /api/session-info 报出日志里不存在、且可能倒退的
// 数（实测 0500f：缓存 114884 vs 日志 118007）。这里锁住两件事：缓存不动，
// 命令表更新照常广播给客户端。
func TestUsageCacheIgnoresAvailableCommandsUpdate(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{SessionID: "s1"}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()

	// 先给一个合法来源（被落盘的 chunk）建立基线。
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"_meta":     map[string]any{"totalTokens": float64(118007)},
		"update":    map[string]any{"sessionUpdate": "agent_message_chunk"},
	})
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"_meta":     map[string]any{"totalTokens": float64(114884)},
		"update": map[string]any{
			"sessionUpdate":     "available_commands_update",
			"availableCommands": []any{map[string]any{"name": "compact"}},
		},
	})

	var sawCommands bool
	var usageValues []int64
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			switch ev["type"] {
			case "commands_update":
				sawCommands = true
			case "usage":
				if u, ok := asInt(ev["used"]); ok {
					usageValues = append(usageValues, u)
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawCommands {
		t.Error("available_commands_update must still reach clients as commands_update")
	}
	for _, u := range usageValues {
		if u == 114884 {
			t.Errorf("usage event carried ACU's totalTokens (%d); it never lands in updates.jsonl", u)
		}
	}
	if got := b.SessionInfo("s1").ContextUsed; got != 118007 {
		t.Errorf("ContextUsed = %d, want the last persisted count 118007 (ACU must not overwrite)", got)
	}
}
