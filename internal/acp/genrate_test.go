package acp

import (
	"math"
	"strings"
	"testing"
	"time"
)

// ── 生成输出速率（gen_rate 事件）────────────────────────────────────
// 速率 = 段累计估算 token / 段累计有效生成时长（ASCII ≈ 4 字符/token，
// CJK ≈ 1.5 字符/token），轻 EMA(α=0.2)，回合终态 usage 自校准 + 跳变
// 受限精确冻结。见 genrate.go。
// 测试通过 handleSessionUpdate 喂 chunk，用 _meta.agentTimestampMs 控制
// 时间戳（ts < 0 时不带 _meta → 回退 host 时间）。

// genRateFeed 构造一个 agent_message_chunk 的 handleSessionUpdate 输入。
func genRateFeed(sid, text string, ts int64) map[string]any {
	params := map[string]any{
		"sessionId": sid,
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": text},
		},
	}
	if ts >= 0 {
		params["_meta"] = map[string]any{"agentTimestampMs": float64(ts)}
	}
	return params
}

// genRateFeedUsage 构造一个带真实 usage 的 turn_completed 输入
// （output/reasoning 只写非零值）。
func genRateFeedUsage(sid string, output, reasoning int64) map[string]any {
	u := map[string]any{}
	if output > 0 {
		u["output_tokens"] = float64(output)
	}
	if reasoning > 0 {
		u["reasoning_tokens"] = float64(reasoning)
	}
	return map[string]any{
		"sessionId": sid,
		"update":    map[string]any{"sessionUpdate": "turn_completed", "usage": u},
	}
}

// waitGenRate 扫描订阅通道直到出现 gen_rate 事件（忽略其它事件）。
func waitGenRate(t *testing.T, ch chan Event, timeout time.Duration) Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev["type"] == "gen_rate" {
				return ev
			}
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// collectGenRates 在窗口内收集所有 gen_rate 事件（顺带排空通道）。
func collectGenRates(t *testing.T, ch chan Event, window time.Duration) []Event {
	t.Helper()
	var out []Event
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev["type"] == "gen_rate" {
				out = append(out, ev)
			}
		case <-time.After(2 * time.Millisecond):
		}
	}
	return out
}

// 等间隔 chunk（10 ASCII ≈ 2.5 token / 250ms）→ 段累计吞吐稳定。
// 首包门控 500ms：t=1000/1250 不发布；t=1500 起 7.5 token / 500ms = 15。
func TestGenRateChunkSequenceRates(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 10) // 10 ASCII chars ≈ 2.5 token
	for _, ts := range []int64{1000, 1250, 1500, 1750} {
		b.handleSessionUpdate(genRateFeed("s1", text, ts))
	}
	rates := collectGenRates(t, ch, 150*time.Millisecond)
	// t=1500/1750 发布 → 2 条
	if len(rates) < 2 {
		t.Fatalf("gen_rate events = %d, want ≥2", len(rates))
	}
	for _, ev := range rates {
		rate, _ := ev["rate"].(float64)
		// 累计：7.5/0.5=15 → 10/0.75≈13.3，EMA 后落在 [12, 16]
		if rate < 12 || rate > 16 {
			t.Errorf("rate = %v, want in [12, 16]（段累计吞吐）", rate)
		}
		if ev["active"] != true {
			t.Errorf("active = %v, want true", ev["active"])
		}
		if ev["sessionId"] != "s1" {
			t.Errorf("sessionId = %v, want s1", ev["sessionId"])
		}
	}
	// 首条 = 7.5 token / 500ms = 15 tok/s
	if r0, _ := rates[0]["rate"].(float64); r0 < 14.5 || r0 > 15.5 {
		t.Errorf("first rate = %v, want ≈15（7.5 token / 500ms 累计）", r0)
	}
}

// burst：大 chunk 墙钟未满 500ms 不发布；过 500ms 后一次累计发布。
func TestGenRateBurstMergesIntoOnePublish(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48) // 48 ASCII chars = 12 token
	b.handleSessionUpdate(genRateFeed("s1", text, 1000))
	b.handleSessionUpdate(genRateFeed("s1", text, 1005)) // elapsed 5 < 500
	if early := collectGenRates(t, ch, 50*time.Millisecond); len(early) != 0 {
		t.Fatalf("unexpected gen_rate before gate: %v", early)
	}
	// t=1500：墙钟 500ms，segEffMs=500，累计 36 token → 72 tok/s
	b.handleSessionUpdate(genRateFeed("s1", text, 1500))
	rates := collectGenRates(t, ch, 150*time.Millisecond)
	if len(rates) != 1 {
		t.Fatalf("gen_rate events = %d, want 1（门控后首次累计发布）", len(rates))
	}
	if ev := rates[0]; ev["active"] != true || ev["sessionId"] != "s1" {
		t.Errorf("event = %v, want active:true for s1", ev)
	}
	if r, _ := rates[0]["rate"].(float64); r < 70 || r > 74 {
		t.Errorf("rate = %v, want ≈72（36 token / 500ms 累计）", r)
	}
}

// seal：chunk 后跟 tool_call → active:false 冻结最后累计值；新段首包
// 门控再次生效。
func TestGenRateSealOnToolCall(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48)
	b.handleSessionUpdate(genRateFeed("s1", text, 1000))
	b.handleSessionUpdate(genRateFeed("s1", text, 1600)) // 600ms ≥500 → 发布
	live := waitGenRate(t, ch, time.Second)
	if live == nil {
		t.Fatal("no live gen_rate event after chunk")
	}
	liveRate, _ := live["rate"].(float64)
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update":    map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t-1"},
	})
	sealEv := waitGenRate(t, ch, time.Second)
	if sealEv == nil {
		t.Fatal("no seal gen_rate event after tool_call")
	}
	if sealEv["active"] != false {
		t.Errorf("active = %v, want false", sealEv["active"])
	}
	if sealEv["sessionId"] != "s1" {
		t.Errorf("sessionId = %v, want s1", sealEv["sessionId"])
	}
	if frozen, _ := sealEv["rate"].(float64); frozen != liveRate {
		t.Errorf("seal rate = %v, want frozen %v", frozen, liveRate)
	}
	if extra := collectGenRates(t, ch, 100*time.Millisecond); len(extra) != 0 {
		t.Fatalf("unexpected gen_rate events after seal: %v", extra)
	}
	b.handleSessionUpdate(genRateFeed("s1", strings.Repeat("a", 10), 2000))
	if extra := collectGenRates(t, ch, 100*time.Millisecond); len(extra) != 0 {
		t.Errorf("unexpected gen_rate after seal + first chunk: %v", extra)
	}
}

// 多会话隔离：A/B 各自累计，互不影响。
func TestGenRateSessionIsolation(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	big := strings.Repeat("a", 48) // 12 token
	small := strings.Repeat("a", 10)
	// A：两包间隔 600ms → 24 token / 0.6s = 40 tok/s
	b.handleSessionUpdate(genRateFeed("A", big, 1000))
	b.handleSessionUpdate(genRateFeed("A", big, 1600))
	evA := waitGenRate(t, ch, time.Second)
	if evA == nil || evA["sessionId"] != "A" {
		t.Fatalf("A live event = %v, want sessionId A", evA)
	}
	if rateA, _ := evA["rate"].(float64); rateA < 39 || rateA > 41 {
		t.Errorf("A rate = %v, want ≈40（24 token / 600ms 累计）", rateA)
	}
	// B：间隔 500ms → 5 token / 0.5s = 10
	b.handleSessionUpdate(genRateFeed("B", small, 1000))
	b.handleSessionUpdate(genRateFeed("B", small, 1500))
	evB := waitGenRate(t, ch, time.Second)
	if evB == nil || evB["sessionId"] != "B" {
		t.Fatalf("B event = %v, want sessionId B", evB)
	}
	if rateB, _ := evB["rate"].(float64); rateB < 9.5 || rateB > 10.5 {
		t.Errorf("B rate = %v, want ≈10（5 token / 500ms 累计）", rateB)
	}
	b.handleSessionUpdate(map[string]any{
		"sessionId": "A",
		"update":    map[string]any{"sessionUpdate": "tool_call"},
	})
	sealA := waitGenRate(t, ch, time.Second)
	if sealA == nil || sealA["sessionId"] != "A" || sealA["active"] != false {
		t.Fatalf("A seal = %v, want active:false for A", sealA)
	}
	if rateA, _ := sealA["rate"].(float64); rateA < 39 || rateA > 41 {
		t.Errorf("A seal rate = %v, want ≈40", rateA)
	}
}

// 无 _meta（回退 host 时间）：喂数据后仍有 gen_rate 事件。
func TestGenRateFallsBackToHostTime(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48)
	for i := 0; i < 8; i++ {
		b.handleSessionUpdate(genRateFeed("s1", text, -1))
		time.Sleep(80 * time.Millisecond) // 推过 500ms 门控
	}
	ev := waitGenRate(t, ch, time.Second)
	if ev == nil {
		t.Fatal("no gen_rate event without _meta.agentTimestampMs")
	}
	if ev["active"] != true {
		t.Errorf("active = %v, want true", ev["active"])
	}
}

// user_message_chunk → 静默复位；新段首包门控重新生效。
func TestGenRateResetOnUserMessage(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	big := strings.Repeat("a", 48)
	small := strings.Repeat("a", 10)
	b.handleSessionUpdate(genRateFeed("s1", big, 1000))
	b.handleSessionUpdate(genRateFeed("s1", big, 1600))
	if ev := waitGenRate(t, ch, time.Second); ev == nil || ev["active"] != true {
		t.Fatal("no live gen_rate before reset")
	}
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update": map[string]any{
			"sessionUpdate": "user_message_chunk",
			"content":       map[string]any{"type": "text", "text": "hi"},
		},
	})
	b.handleSessionUpdate(genRateFeed("s1", small, 2000))
	if ev := collectGenRates(t, ch, 100*time.Millisecond); len(ev) != 0 {
		t.Fatalf("unexpected gen_rate after reset + first chunk: %v", ev)
	}
	// 间隔 500ms → 5 token / 0.5s = 10
	b.handleSessionUpdate(genRateFeed("s1", small, 2500))
	ev := waitGenRate(t, ch, time.Second)
	if ev == nil {
		t.Fatal("no gen_rate after reset + second chunk")
	}
	if rate, _ := ev["rate"].(float64); rate < 9.5 || rate > 10.5 {
		t.Errorf("rate = %v, want ≈10", rate)
	}
}

// 节流：同段内 250ms 内至多一条 active:true。
func TestGenRateThrottle(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48)
	// t=1000 门控；t=1500 首次；t=1600/1700 节流；t=1750 距 1500≥250 再发
	for _, ts := range []int64{1000, 1500, 1600, 1700, 1750} {
		b.handleSessionUpdate(genRateFeed("s1", text, ts))
	}
	rates := collectGenRates(t, ch, 150*time.Millisecond)
	if len(rates) != 2 {
		t.Fatalf("gen_rate events = %d, want 2（250ms 节流）", len(rates))
	}
	for _, ev := range rates {
		if ev["active"] != true {
			t.Errorf("event = %v, want active:true", ev)
		}
	}
}

// turn_completed / response_completed 无 usage 时冻结最后发布值。
func TestGenRateSealOnTurnCompletedFreezesLastPublished(t *testing.T) {
	for _, kind := range []string{"turn_completed", "response_completed"} {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			text := strings.Repeat("a", 48) // 12 token
			// t=1000 门控；t=1600：24/0.6=40；t=1900：36/0.9=40
			b.handleSessionUpdate(genRateFeed("s1", text, 1000))
			b.handleSessionUpdate(genRateFeed("s1", text, 1600))
			live := waitGenRate(t, ch, time.Second)
			if live == nil {
				t.Fatal("no first live gen_rate")
			}
			b.handleSessionUpdate(genRateFeed("s1", text, 1900))
			live2 := waitGenRate(t, ch, time.Second)
			if live2 == nil {
				t.Fatal("no second live gen_rate")
			}
			lastPublished, _ := live2["rate"].(float64)
			b.handleSessionUpdate(map[string]any{
				"sessionId": "s1",
				"update":    map[string]any{"sessionUpdate": kind},
			})
			sealEv := waitGenRate(t, ch, time.Second)
			if sealEv == nil {
				t.Fatal("no seal gen_rate after " + kind)
			}
			if sealEv["active"] != false {
				t.Errorf("active = %v, want false", sealEv["active"])
			}
			if frozen, _ := sealEv["rate"].(float64); frozen != lastPublished {
				t.Errorf("seal rate = %v, want last published %v", frozen, lastPublished)
			}
		})
	}
}

// agent-ts 长 gap 不 800ms 封顶：5 token / 1500ms ≈ 3.33。
func TestGenRateAgentTsGapNotIdleCapped(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1, factor: 1}
	text := strings.Repeat("a", 10) // 2.5 token
	if _, ok := s.observe(text, 1000, true); ok {
		t.Fatal("first chunk must be pending (no publish)")
	}
	rate, ok := s.observe(text, 2500, true) // wall 1500ms ≥ 500
	if !ok {
		t.Fatal("gate must open on second chunk")
	}
	if math.Abs(rate-10.0/3.0) > 0.05 {
		t.Errorf("rate = %v, want ≈3.33（5 token / 1.5s 累计；非 800ms 封顶）", rate)
	}
}

// host 回退路径 800ms 空闲封顶：5 token / 0.8s = 6.25。
func TestGenRateFallbackKeepsIdleCap(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1, factor: 1}
	text := strings.Repeat("a", 10) // 2.5 token
	s.observe(text, 1000, false)
	rate, ok := s.observe(text, 2500, false)
	if !ok {
		t.Fatal("gate must open on second chunk")
	}
	if math.Abs(rate-6.25) > 0.05 {
		t.Errorf("rate = %v, want ≈6.25（5 token / 0.8s 累计，host 800ms 封顶）", rate)
	}
}

// 禁止 floor-only 首发；墙钟须 ≥500ms。
func TestGenRateNoFloorOnlyFirstPublish(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1, factor: 1}
	big := strings.Repeat("a", 48) // 12 token
	if _, ok := s.observe(big, 1000, true); ok {
		t.Fatal("large first chunk must not publish (no floor-only)")
	}
	if _, ok := s.observe(big, 1000, true); ok {
		t.Fatal("same-timestamp burst must not publish without effective ms")
	}
	// 120ms 仍不足 500ms 门控
	if _, ok := s.observe(big, 1120, true); ok {
		t.Fatal("must not publish before 500ms wall")
	}
	// 500ms → 48 token / 0.5s = 96
	rate, ok := s.observe(big, 1500, true)
	if !ok {
		t.Fatal("must publish after wall ≥500ms with positive segEffMs")
	}
	// 4 包 × 12 = 48 token / 500ms
	if math.Abs(rate-96) > 1 {
		t.Errorf("rate = %v, want ≈96（48 token / 500ms）", rate)
	}
}

// 自校准 + seal 跳变限制。
func TestGenRateCalibrationScalesRates(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 10) // 2.5 token
	// 4 chunk × 250ms：累计 10 token / 750ms；门控后 t=1500/1750 发布
	for _, ts := range []int64{1000, 1250, 1500, 1750} {
		b.handleSessionUpdate(genRateFeed("s1", text, ts))
	}
	rates := collectGenRates(t, ch, 150*time.Millisecond)
	if len(rates) != 2 {
		t.Fatalf("live gen_rate events = %d, want 2（500ms 门控后两条）", len(rates))
	}
	lastLive, _ := rates[len(rates)-1]["rate"].(float64)
	if lastLive < 12 || lastLive > 16 {
		t.Errorf("last live rate = %v, want roughly mid-teens", lastLive)
	}
	b.handleSessionUpdate(genRateFeedUsage("s1", 25, 5)) // real=30
	sealEv := waitGenRate(t, ch, time.Second)
	if sealEv == nil || sealEv["active"] != false {
		t.Fatalf("seal event = %v, want active:false", sealEv)
	}
	// exact=30/0.75=40；跳变上限 last*1.35
	sealed, _ := sealEv["rate"].(float64)
	maxJump := lastLive * (1 + genRateSealMaxJump)
	if sealed < lastLive-0.01 || sealed > maxJump+0.01 {
		t.Errorf("seal rate = %v, want in [lastLive=%v, last*1.35=%v]", sealed, lastLive, maxJump)
	}
	if sealed > 30 {
		t.Errorf("seal rate = %v, should be jump-limited well below exact 40", sealed)
	}
	// factor = 0.7*1 + 0.3*min(2, 30/10=3) = 1.6
	// 新段须满 500ms：2000 + 2500 → 5 token / 0.5s = 10 × 1.6 = 16
	b.handleSessionUpdate(genRateFeed("s1", text, 2000))
	b.handleSessionUpdate(genRateFeed("s1", text, 2500))
	ev := waitGenRate(t, ch, time.Second)
	if ev == nil {
		t.Fatal("no gen_rate after calibration")
	}
	if r, _ := ev["rate"].(float64); r < 15.5 || r > 16.5 {
		t.Errorf("post-calibration rate = %v, want ≈16（10 × factor 1.6）", r)
	}
}

// 精确冻结跨段累加 + 跳变限制。
func TestGenRateExactFreezeAcrossSegments(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	big := strings.Repeat("a", 48) // 12 token
	b.handleSessionUpdate(genRateFeed("s1", big, 1000))
	b.handleSessionUpdate(genRateFeed("s1", big, 1600)) // 24/0.6=40
	if evs := collectGenRates(t, ch, 150*time.Millisecond); len(evs) != 1 {
		t.Fatalf("segment1 live events = %d, want 1", len(evs))
	}
	b.handleSessionUpdate(map[string]any{
		"sessionId": "s1",
		"update":    map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t-1"},
	})
	if ev := waitGenRate(t, ch, time.Second); ev == nil || ev["active"] != false {
		t.Fatalf("tool_call seal event = %v, want active:false", ev)
	}
	// 段2：2000 门控；2600 发布 24/0.6=40
	b.handleSessionUpdate(genRateFeed("s1", big, 2000))
	b.handleSessionUpdate(genRateFeed("s1", big, 2600))
	liveSeg2 := collectGenRates(t, ch, 150*time.Millisecond)
	if len(liveSeg2) != 1 {
		t.Fatalf("segment2 live events = %d, want 1", len(liveSeg2))
	}
	lastLive, _ := liveSeg2[0]["rate"].(float64)
	// accGenMs = 600+600=1200；usage 60 → exact = 60/1.2 = 50
	b.handleSessionUpdate(genRateFeedUsage("s1", 60, 0))
	sealEv := waitGenRate(t, ch, time.Second)
	if sealEv == nil || sealEv["active"] != false {
		t.Fatalf("turn_completed seal event = %v, want active:false", sealEv)
	}
	sealed, _ := sealEv["rate"].(float64)
	maxAllowed := lastLive * (1 + genRateSealMaxJump)
	if sealed < lastLive-0.01 || sealed > maxAllowed+0.5 {
		t.Errorf("seal rate = %v, want in [last=%v, last*1.35=%v]",
			sealed, lastLive, maxAllowed)
	}
	if sealed+0.01 < lastLive {
		t.Errorf("seal rate = %v < last live %v", sealed, lastLive)
	}
}

// limitSealJump 单元：无 live → exact；有 live → ±35% 钳制。
func TestLimitSealJump(t *testing.T) {
	if got := limitSealJump(0, 40); got != 40 {
		t.Errorf("no-live = %v, want 40", got)
	}
	if got := limitSealJump(10, 40); math.Abs(got-13.5) > 0.01 {
		t.Errorf("up jump = %v, want 13.5（10*1.35）", got)
	}
	if got := limitSealJump(100, 10); math.Abs(got-65) > 0.01 {
		t.Errorf("down jump = %v, want 65（100*0.65）", got)
	}
	if got := limitSealJump(50, 55); math.Abs(got-55) > 0.01 {
		t.Errorf("within band = %v, want 55", got)
	}
}
