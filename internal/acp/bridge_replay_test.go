package acp

import (
	"testing"
	"time"
)

// bridge_replay_test.go — session/load 重放事件的拦截（契约 lite-replay [F]
// 的 host 侧）。agent 在重放通知的 params._meta 打 isReplay（两条载体都打），
// host 派生的每个事件盖上 kReplayInternal，Broadcast 在分配全局 seq 之前把
// 整条拦掉：SSE 与 hub 中继都看不到重放，但事件序列里也不会留洞（洞会让 FE
// 的有序投递憋死，见 acp-fe/src/api/liveSequencing.ts）。

// drainEvents 非阻塞取空订阅通道（Broadcast 同步投递，调用返回时事件已在
// 通道里）。
func drainEvents(ch chan Event) []Event {
	var out []Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
			continue
		default:
		}
		return out
	}
}

// replayCases 覆盖所有会被重放的派生事件形态：typed kind、未建模 kind、
// 图片块、以及随重放一起出现的 usage 事件。
var replayCases = []struct {
	name   string
	update map[string]any
}{
	{"tool_call", map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "ls"}},
	{"tool_call_update", map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed"}},
	{"chunk", map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}}},
	{"user_chunk", map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "喂"}}},
	{"thought", map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "想想"}}},
	{"plan", map[string]any{"sessionUpdate": "plan", "entries": []any{map[string]any{"content": "a"}}}},
	{"turn_completed", map[string]any{"sessionUpdate": "turn_completed", "stopReason": "end_turn"}},
	{"task_backgrounded", map[string]any{"sessionUpdate": "task_backgrounded", "task_id": "t-1"}},
	{"session_recap", map[string]any{"sessionUpdate": "session_recap", "summary": "s"}},
	{"image", map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "image", "data": "aGk=", "mimeType": "image/png"}}},
	{"text_and_image", map[string]any{"sessionUpdate": "agent_message_chunk", "content": []any{
		map[string]any{"type": "text", "text": "看图"},
		map[string]any{"type": "image", "data": "aGk=", "mimeType": "image/png"},
	}}},
	{"unmodeled", map[string]any{"sessionUpdate": "future_kind_alpha", "payload": "x"}},
}

func replayParams(update map[string]any, replay bool) map[string]any {
	meta := map[string]any{"agentTimestampMs": float64(1_000), "eventId": "s1-1", "totalTokens": float64(42)}
	if replay {
		meta["isReplay"] = true
	}
	return map[string]any{"sessionId": "s1", "update": update, "_meta": meta}
}

// TestOfficialCarrierSuppressesReplay：官方 session/update 载体的每种 kind，
// 非重放照常广播、重放一条都不上总线。
func TestOfficialCarrierSuppressesReplay(t *testing.T) {
	for _, tc := range replayCases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			b.handleSessionUpdate(replayParams(tc.update, false))
			plain := drainEvents(ch)
			if len(plain) == 0 {
				t.Fatal("非重放路径没有广播任何事件")
			}
			for i, ev := range plain {
				if _, ok := ev[kReplayInternal]; ok {
					t.Errorf("非重放事件 %d 带了内部标记: %v", i, ev)
				}
			}
			b.handleSessionUpdate(replayParams(tc.update, true))
			if gated := drainEvents(ch); len(gated) != 0 {
				t.Fatalf("重放事件上了总线: %v", gated)
			}
		})
	}
}

// TestXaiCarrierSuppressesReplay：_x.ai/session/update 载体重放同样被拦。
// 漏掉这条载体（只处理官方载体）会让同一批历史里一半上一半不上，正好制造
// FE 补不回来的 seq 空洞。
func TestXaiCarrierSuppressesReplay(t *testing.T) {
	for _, tc := range replayCases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			b.handleXaiNotification("x.ai/session/update", replayParams(tc.update, false))
			if plain := drainEvents(ch); len(plain) == 0 {
				t.Fatal("非重放的 x.ai 载体没有广播任何事件")
			}
			b.handleXaiNotification("x.ai/session/update", replayParams(tc.update, true))
			if gated := drainEvents(ch); len(gated) != 0 {
				t.Fatalf("x.ai 载体重放事件上了总线: %v", gated)
			}
		})
	}
}

// TestReplayLeavesNoSeqHole：重放夹在两条 live 事件之间时，订阅者看到的 seq
// 必须连续。全局 seq 由 eventBus.publish 分配，所以拦截点必须在 Broadcast 里、
// publish 之前——晚一帧就会留下永久空洞。
func TestReplayLeavesNoSeqHole(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()

	// seq 一律在**同一条** Bridge 上比对：另开一条只发两条 live 事件的对照，
	// 验的是「publish 分配的 seq 天然连续」这个平凡事实，与重放拦截无关。
	seqOf := func(evs []Event) (uint64, bool) {
		var last uint64
		var seen bool
		for _, ev := range evs {
			if s, ok := ev["seq"].(uint64); ok {
				last = s
				seen = true
			}
		}
		return last, seen
	}

	b.handleSessionUpdate(replayParams(replayCases[0].update, false))
	lastBefore, ok := seqOf(drainEvents(ch))
	if !ok || lastBefore == 0 {
		t.Fatalf("重放前的 live 事件没带上 seq: %v", ok)
	}
	for i := 0; i < 40; i++ {
		b.handleSessionUpdate(replayParams(map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": "replayed"},
		}, true))
	}
	if extra := drainEvents(ch); len(extra) != 0 {
		t.Fatalf("重放期间上了 %d 条事件", len(extra))
	}
	b.handleSessionUpdate(replayParams(replayCases[1].update, false))
	after := drainEvents(ch)
	if len(after) == 0 {
		t.Fatal("重放之后的 live 事件没上总线")
	}
	firstAfter, ok := seqOf(after[:1])
	if !ok {
		t.Fatal("重放后的 live 事件没带上 seq")
	}
	// 重放的 40 条必须在 eventBus.publish 分配 seq **之前**就被丢弃，否则
	// 这里会多出 40 的跳号，FE 的 EventSequencer 会一直等那批空洞。
	if firstAfter != lastBefore+1 {
		t.Fatalf("重放留下了 seq 空洞: %d → %d（跳过 %d 条）", lastBefore, firstAfter, firstAfter-lastBefore-1)
	}

	// 同一条 Bridge 上的灵敏度对照：不被拦的 live 事件确实会消耗 seq。
	// 没有这一段，上面那句「+1」在拦截点被挪到 publish 之后时也可能悄悄成立。
	chunk := func() {
		b.handleSessionUpdate(replayParams(map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": "live"},
		}, false))
	}
	chunk()
	gapBase, ok := seqOf(drainEvents(ch))
	if !ok {
		t.Fatal("对照 live 事件没带上 seq")
	}
	for i := 0; i < 40; i++ {
		chunk()
	}
	drainEvents(ch)
	chunk()
	gapEnd, ok := seqOf(drainEvents(ch))
	if !ok {
		t.Fatal("对照末条 live 事件没带上 seq")
	}
	if gapEnd <= gapBase+1 {
		t.Fatalf("对照组 seq 未推进（%d → %d），连续性断言成了空断言", gapBase, gapEnd)
	}
}

// paramsIsReplay 只认真布尔值：字符串/数字/缺省都不算重放。
func TestParamsIsReplayStrict(t *testing.T) {
	if !paramsIsReplay(map[string]any{"_meta": map[string]any{"isReplay": true}}) {
		t.Error("isReplay=true 应判为重放")
	}
	for _, meta := range []any{
		map[string]any{"isReplay": "true"},
		map[string]any{"isReplay": float64(1)},
		map[string]any{"isReplay": false},
		map[string]any{},
		"not-a-map",
		nil,
	} {
		if paramsIsReplay(map[string]any{"_meta": meta}) {
			t.Errorf("_meta=%v 不该判为重放", meta)
		}
	}
	if paramsIsReplay(map[string]any{}) || paramsIsReplay(nil) {
		t.Error("缺 params._meta 不该判为重放")
	}
}

// 边界事件（session_load_started / finished）由 LoadSession 直接广播，不经
// session/update 派生 ⇒ 永远不被拦（FE 的多 tab 门控依赖它们）。
func TestSessionLoadBoundaryEventsPass(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.Broadcast(Event{kType: "session_load_started", kSessionID: "s1", "cwd": "/ws"})
	b.Broadcast(Event{kType: "session_load_finished", kSessionID: "s1", "cwd": "/ws", "ok": true})
	evs := drainEvents(ch)
	if len(evs) != 2 {
		t.Fatalf("边界事件数 = %d, want 2", len(evs))
	}
	for _, ev := range evs {
		if _, ok := ev[kReplayInternal]; ok {
			t.Errorf("边界事件不该带重放标记: %v", ev)
		}
	}
	// Broadcast 同步投递：通道里不该再有别的事件（512 缓冲不会丢）。
	select {
	case extra := <-ch:
		t.Fatalf("多了事件: %v", extra)
	case <-time.After(20 * time.Millisecond):
	}
}
