package acp

import (
	"math"
	"strings"
	"testing"
	"time"
)

// ── 生成输出速率（gen_rate 事件，字符/秒）────────────────────────────
// 两种口径：
//   - agent 路径（_meta.agentTimestampMs + streamStartMs）：速率 = 流起点
//     之后的全部字符（含首包） / 墙钟，累计自平滑、无 EMA；
//   - 回退路径（无 streamStartMs / host 时间）：首包只打钟，速率 = 首包
//     之后字符 / 累计 gap，轻 EMA(α=0.2)，显示硬顶 2000 c/s。
// 无 token 换算、无回合末冻结——只在输出过程中显示，输出结束广播
// active:false（不带 rate）。见 genrate.go。
// 测试通过 handleSessionUpdate 喂 chunk，用 _meta.agentTimestampMs /
// streamStartMs 控制时间戳（ts < 0 时不带 _meta → 回退 host 时间）。

// genRateFeed 构造一个 agent_message_chunk 的 handleSessionUpdate 输入。
// ss 为 _meta.streamStartMs（<0 时不带 → 回退口径）。
func genRateFeed(sid, text string, ts int64, ss int64) map[string]any {
	params := map[string]any{
		"sessionId": sid,
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": text},
		},
	}
	if ts >= 0 {
		meta := map[string]any{"agentTimestampMs": float64(ts)}
		if ss >= 0 {
			meta["streamStartMs"] = float64(ss)
		}
		params["_meta"] = meta
	}
	return params
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

// 等间隔 chunk（10 字符 / 250ms）→ 段累计吞吐稳定。
// 首包只打钟：t=1000/1250 不发布；t=1500 起 20 字符 / 500ms = 40。
func TestGenRateChunkSequenceRates(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 10) // 10 字符
	for _, ts := range []int64{1000, 1250, 1500, 1750} {
		b.handleSessionUpdate(genRateFeed("s1", text, ts, -1))
	}
	rates := collectGenRates(t, ch, 150*time.Millisecond)
	// t=1500/1750 发布 → 2 条
	if len(rates) < 2 {
		t.Fatalf("gen_rate events = %d, want ≥2", len(rates))
	}
	for _, ev := range rates {
		rate, _ := ev["rate"].(float64)
		// 累计：20/0.5=40 → 30/0.75=40（首包不进分子）
		if rate < 39.5 || rate > 40.5 {
			t.Errorf("rate = %v, want in [39.5, 40.5]（首包之后的累计吞吐）", rate)
		}
		if ev["active"] != true {
			t.Errorf("active = %v, want true", ev["active"])
		}
		if ev["sessionId"] != "s1" {
			t.Errorf("sessionId = %v, want s1", ev["sessionId"])
		}
	}
	// 首条 = 20 字符 / 500ms = 40 c/s
	if r0, _ := rates[0]["rate"].(float64); r0 < 39.5 || r0 > 40.5 {
		t.Errorf("first rate = %v, want ≈40（20 字符 / 500ms，不含首包）", r0)
	}
}

// burst：大 chunk 墙钟未满 500ms 不发布；过 500ms 后一次累计发布。
func TestGenRateBurstMergesIntoOnePublish(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48) // 48 字符
	b.handleSessionUpdate(genRateFeed("s1", text, 1000, -1))
	b.handleSessionUpdate(genRateFeed("s1", text, 1005, -1)) // elapsed 5 < 500
	if early := collectGenRates(t, ch, 50*time.Millisecond); len(early) != 0 {
		t.Fatalf("unexpected gen_rate before gate: %v", early)
	}
	// t=1500：墙钟 500ms，segEffMs=500，首包之后 96 字符 → 192 c/s
	b.handleSessionUpdate(genRateFeed("s1", text, 1500, -1))
	rates := collectGenRates(t, ch, 150*time.Millisecond)
	if len(rates) != 1 {
		t.Fatalf("gen_rate events = %d, want 1（门控后首次累计发布）", len(rates))
	}
	if ev := rates[0]; ev["active"] != true || ev["sessionId"] != "s1" {
		t.Errorf("event = %v, want active:true for s1", ev)
	}
	if r, _ := rates[0]["rate"].(float64); r < 190 || r > 194 {
		t.Errorf("rate = %v, want ≈192（96 字符 / 500ms，不含首包）", r)
	}
}

// seal：chunk 后跟 tool_call → active:false 且不带 rate（清除显示，只
// 在输出过程中显示）；新段首包门控再次生效。
func TestGenRateSealOnToolCall(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48)
	b.handleSessionUpdate(genRateFeed("s1", text, 1000, -1))
	b.handleSessionUpdate(genRateFeed("s1", text, 1600, -1)) // 600ms ≥500 → 发布
	live := waitGenRate(t, ch, time.Second)
	if live == nil {
		t.Fatal("no live gen_rate event after chunk")
	}
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
	// 清除语义：seal 事件不带 rate（前端据此清掉速率显示）。
	if _, has := sealEv["rate"]; has {
		t.Errorf("seal event must carry no rate, got %v", sealEv["rate"])
	}
	if extra := collectGenRates(t, ch, 100*time.Millisecond); len(extra) != 0 {
		t.Fatalf("unexpected gen_rate events after seal: %v", extra)
	}
	b.handleSessionUpdate(genRateFeed("s1", strings.Repeat("a", 10), 2000, -1))
	if extra := collectGenRates(t, ch, 100*time.Millisecond); len(extra) != 0 {
		t.Errorf("unexpected gen_rate after seal + first chunk: %v", extra)
	}
}

// 多会话隔离：A/B 各自累计，互不影响。
func TestGenRateSessionIsolation(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	big := strings.Repeat("a", 48) // 48 字符
	small := strings.Repeat("a", 10)
	// A：两包间隔 600ms → 第二包 48 字符 / 0.6s = 80 c/s（首包不进分子）
	b.handleSessionUpdate(genRateFeed("A", big, 1000, -1))
	b.handleSessionUpdate(genRateFeed("A", big, 1600, -1))
	evA := waitGenRate(t, ch, time.Second)
	if evA == nil || evA["sessionId"] != "A" {
		t.Fatalf("A live event = %v, want sessionId A", evA)
	}
	if rateA, _ := evA["rate"].(float64); rateA < 78 || rateA > 82 {
		t.Errorf("A rate = %v, want ≈80（48 字符 / 600ms，不含首包）", rateA)
	}
	// B：间隔 500ms → 10 字符 / 0.5s = 20
	b.handleSessionUpdate(genRateFeed("B", small, 1000, -1))
	b.handleSessionUpdate(genRateFeed("B", small, 1500, -1))
	evB := waitGenRate(t, ch, time.Second)
	if evB == nil || evB["sessionId"] != "B" {
		t.Fatalf("B event = %v, want sessionId B", evB)
	}
	if rateB, _ := evB["rate"].(float64); rateB < 19 || rateB > 21 {
		t.Errorf("B rate = %v, want ≈20（10 字符 / 500ms，不含首包）", rateB)
	}
	b.handleSessionUpdate(map[string]any{
		"sessionId": "A",
		"update":    map[string]any{"sessionUpdate": "tool_call"},
	})
	sealA := waitGenRate(t, ch, time.Second)
	if sealA == nil || sealA["sessionId"] != "A" || sealA["active"] != false {
		t.Fatalf("A seal = %v, want active:false for A", sealA)
	}
	// 清除语义：seal 事件不带 rate（不冻结终值）。
	if _, has := sealA["rate"]; has {
		t.Errorf("A seal event must carry no rate, got %v", sealA["rate"])
	}
}

// 无 _meta（回退 host 时间）：喂数据后仍有 gen_rate 事件。
func TestGenRateFallsBackToHostTime(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	text := strings.Repeat("a", 48)
	for i := 0; i < 8; i++ {
		b.handleSessionUpdate(genRateFeed("s1", text, -1, -1))
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
	b.handleSessionUpdate(genRateFeed("s1", big, 1000, -1))
	b.handleSessionUpdate(genRateFeed("s1", big, 1600, -1))
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
	b.handleSessionUpdate(genRateFeed("s1", small, 2000, -1))
	if ev := collectGenRates(t, ch, 100*time.Millisecond); len(ev) != 0 {
		t.Fatalf("unexpected gen_rate after reset + first chunk: %v", ev)
	}
	// 间隔 500ms → 第二包 10 字符 / 0.5s = 20
	b.handleSessionUpdate(genRateFeed("s1", small, 2500, -1))
	ev := waitGenRate(t, ch, time.Second)
	if ev == nil {
		t.Fatal("no gen_rate after reset + second chunk")
	}
	if rate, _ := ev["rate"].(float64); rate < 19 || rate > 21 {
		t.Errorf("rate = %v, want ≈20", rate)
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
		b.handleSessionUpdate(genRateFeed("s1", text, ts, -1))
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

// ── agent 路径（_meta.streamStartMs）：速率 = 流起点之后全部字符（含
// 首包）/ 墙钟。真实验证（14 回合会话，102 个流）：Grok 的流是「一整
// 段 thought 一大包 + 2ms 后小尾巴」结构，旧 gap 口径 76% 段无发布、
// 发布值 35–600 乱跳；新口径 102/102 段可算、中位 204 c/s 稳定。

// 真实流形态：thought 759 字符 @7507（ss=5441）→ message 43 字符
// @7509（2ms burst）→ 后续包。首包即发布：759/2.066s ≈ 367 c/s；
// burst 包被节流，累计 802/2.068s ≈ 388（不发布）。
func TestGenRateAgentPathStreamStartCumulative(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	ch, unsub := b.Subscribe()
	defer unsub()
	b.handleSessionUpdate(genRateFeed("s1", strings.Repeat("a", 759), 7507, 5441))
	ev := waitGenRate(t, ch, time.Second)
	if ev == nil {
		t.Fatal("no gen_rate on first chunk (wall=2066ms ≥500)")
	}
	if r, _ := ev["rate"].(float64); r < 360 || r > 375 {
		t.Errorf("first rate = %v, want ≈367（759 字符 / 2066ms，含首包）", r)
	}
	// 2ms burst 尾巴：被 250ms 节流挡住（不发布、不跳动）。
	b.handleSessionUpdate(genRateFeed("s1", strings.Repeat("a", 43), 7509, 5441))
	if ev := collectGenRates(t, ch, 100*time.Millisecond); len(ev) != 0 {
		t.Fatalf("burst chunk must be throttled: %v", ev)
	}
	// 下一包（493ms 后）：累计 812 字符 / 2559ms ≈ 317 c/s。
	b.handleSessionUpdate(genRateFeed("s1", strings.Repeat("a", 10), 8000, 5441))
	ev = waitGenRate(t, ch, time.Second)
	if ev == nil {
		t.Fatal("no gen_rate after third chunk")
	}
	if r, _ := ev["rate"].(float64); r < 310 || r > 325 {
		t.Errorf("cumulative rate = %v, want ≈317（812 字符 / 2559ms）", r)
	}
}

// burst（2ms 间隙）不爆炸：瞬时速率被累计墙钟分母吸收，无近 2000 的
// 封顶值污染显示。
func TestGenRateAgentPathBurstNoExplosion(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	text := strings.Repeat("a", 48)
	rate, ok := s.observe(text, 1000, true, 500) // wall=500 → 48/0.5=96
	if !ok || math.Abs(rate-96) > 2 {
		t.Fatalf("first rate = %v, want ≈96（48 字符 / 500ms，含首包）", rate)
	}
	if _, ok := s.observe(text, 1002, true, 500); ok {
		t.Fatal("2ms burst must be throttled")
	}
	rate, ok = s.observe(text, 1300, true, 500) // wall=800 → 144/0.8=180
	if !ok {
		t.Fatal("no publish on third chunk")
	}
	if math.Abs(rate-180) > 2 {
		t.Errorf("rate = %v, want ≈180（144 字符 / 800ms，无爆炸）", rate)
	}
}

// 单包段也能发布（旧 gap 口径下 102 段中 37 段是单包、整段无显示）：
// 首包字符的生成时长 = 首包时间 − streamStart，分子分母同时含首包。
func TestGenRateAgentPathSingleChunkPublishes(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	rate, ok := s.observe(strings.Repeat("a", 759), 7507, true, 5441)
	if !ok {
		t.Fatal("single chunk with wall≥500ms must publish")
	}
	if math.Abs(rate-759/2.066) > 5 {
		t.Errorf("rate = %v, want ≈367.3（759 字符 / 2066ms）", rate)
	}
}

// 流起点变化（段未封口的新流）→ 重开段，按新流起点重新计时。
func TestGenRateAgentPathStreamChangeResets(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	rate, ok := s.observe(strings.Repeat("a", 10), 5600, true, 5000) // 10/0.6≈16.7
	if !ok || math.Abs(rate-16.7) > 0.5 {
		t.Fatalf("first stream rate = %v, want ≈16.7", rate)
	}
	// 新流 ss=7000（未封口）：本 chunk 是新段首包，旧段作废。
	rate, ok = s.observe(strings.Repeat("a", 20), 7700, true, 7000)
	if !ok {
		t.Fatal("new stream must open a fresh segment")
	}
	if math.Abs(rate-28.6) > 0.5 {
		t.Errorf("new stream rate = %v, want ≈28.6（20 字符 / 700ms，从新流起点计时）", rate)
	}
}

// agent 路径门控：墙钟（now − streamStart）< 500ms 不发布；满 500ms 后
// 首包字符即计入。
func TestGenRateAgentPathGateFromStreamStart(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	if _, ok := s.observe(strings.Repeat("a", 10), 1200, true, 1000); ok {
		t.Fatal("must not publish before 500ms wall (200ms)")
	}
	rate, ok := s.observe(strings.Repeat("a", 10), 1500, true, 1000)
	if !ok {
		t.Fatal("must publish at wall=500ms")
	}
	if math.Abs(rate-20/0.5) > 1 {
		t.Errorf("rate = %v, want ≈40（20 字符 / 500ms，含首包）", rate)
	}
}

// 回退口径段中途出现 agent 时钟 + streamStartMs → 切到 agent 口径。
func TestGenRateSwitchToAgentPathMidSegment(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	s.observe(strings.Repeat("a", 48), 1000, false, 0) // host 时钟回退段首包
	// agent chunk 带 ss：切口径，按 agent 墙钟重新计时（本包即新段首包）。
	rate, ok := s.observe(strings.Repeat("a", 10), 1600, true, 1000)
	if !ok {
		t.Fatal("switch chunk must publish on agent path")
	}
	if math.Abs(rate-10/0.6) > 0.5 {
		t.Errorf("switch rate = %v, want ≈16.7（10 字符 / 600ms）", rate)
	}
	rate, ok = s.observe(strings.Repeat("a", 10), 2000, true, 1000)
	if !ok {
		t.Fatal("no publish after switch")
	}
	if math.Abs(rate-20/1.0) > 0.5 {
		t.Errorf("rate = %v, want ≈20（20 字符 / 1000ms）", rate)
	}
}

// 中途重开段必须清 ema：host 回退段已 blend（ema=96）→ 切 agent →
// 再丢 _meta 回回退段，若 ema 未清会粘上一段的值（96*0.8+20*0.2≈81
// 而不是 20）。
func TestGenRateReseedClearsEma(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	big := strings.Repeat("a", 48)
	s.observe(big, 1000, false, 0)
	rate, ok := s.observe(big, 1500, false, 0) // 48/0.5=96 → ema=96
	if !ok || math.Abs(rate-96) > 2 {
		t.Fatalf("fallback rate = %v, want ≈96", rate)
	}
	rate, ok = s.observe(strings.Repeat("a", 10), 2000, true, 1500)
	if !ok || math.Abs(rate-10/0.5) > 0.5 {
		t.Fatalf("agent switch rate = %v, want ≈20", rate)
	}
	rate, ok = s.observe(strings.Repeat("a", 10), 2500, true, 1500)
	if !ok || math.Abs(rate-20/1.0) > 0.5 {
		t.Fatalf("agent rate = %v, want ≈20", rate)
	}
	if _, ok := s.observe(strings.Repeat("a", 10), 3000, false, 0); ok {
		t.Fatal("fallback first chunk must not publish")
	}
	rate, ok = s.observe(strings.Repeat("a", 10), 3500, false, 0)
	if !ok {
		t.Fatal("no publish on fallback path after reseed")
	}
	if math.Abs(rate-20) > 1 {
		t.Errorf("rate = %v, want ≈20（reseed 必须清 ema，不能粘上一段的 96）", rate)
	}
}

// turn_completed / response_completed → active:false 且不带 rate（清除
// 速率显示；不做回合末冻结）。
func TestGenRateSealOnTurnCompletedClearsRate(t *testing.T) {
	for _, kind := range []string{"turn_completed", "response_completed"} {
		t.Run(kind, func(t *testing.T) {
			b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
			ch, unsub := b.Subscribe()
			defer unsub()
			text := strings.Repeat("a", 48) // 48 字符
			// t=1000 门控；t=1600：48/0.6=80；t=1900：96/0.9≈107
			b.handleSessionUpdate(genRateFeed("s1", text, 1000, -1))
			b.handleSessionUpdate(genRateFeed("s1", text, 1600, -1))
			live := waitGenRate(t, ch, time.Second)
			if live == nil {
				t.Fatal("no first live gen_rate")
			}
			b.handleSessionUpdate(genRateFeed("s1", text, 1900, -1))
			live2 := waitGenRate(t, ch, time.Second)
			if live2 == nil {
				t.Fatal("no second live gen_rate")
			}
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
			// 清除语义：seal 事件不带 rate（前端据此清掉速率显示）。
			if _, has := sealEv["rate"]; has {
				t.Errorf("seal event must carry no rate, got %v", sealEv["rate"])
			}
		})
	}
}

// agent-ts 长 gap 不 800ms 封顶：第二包 10 字符 / 1500ms ≈ 6.67。
func TestGenRateAgentTsGapNotIdleCapped(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	text := strings.Repeat("a", 10) // 10 字符
	if _, ok := s.observe(text, 1000, true, 0); ok {
		t.Fatal("first chunk must be pending (no publish)")
	}
	rate, ok := s.observe(text, 2500, true, 0) // wall 1500ms ≥ 500
	if !ok {
		t.Fatal("gate must open on second chunk")
	}
	if math.Abs(rate-10/1.5) > 0.05 {
		t.Errorf("rate = %v, want ≈6.67（10 字符 / 1.5s，不含首包；非 800ms 封顶）", rate)
	}
}

// host 回退路径 800ms 空闲封顶：10 字符 / 0.8s = 12.5。
func TestGenRateFallbackKeepsIdleCap(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	text := strings.Repeat("a", 10) // 10 字符
	s.observe(text, 1000, false, 0)
	rate, ok := s.observe(text, 2500, false, 0)
	if !ok {
		t.Fatal("gate must open on second chunk")
	}
	if math.Abs(rate-10/0.8) > 0.05 {
		t.Errorf("rate = %v, want ≈12.5（10 字符 / 0.8s，不含首包，host 800ms 封顶）", rate)
	}
}

// 禁止 floor-only 首发；墙钟须 ≥500ms。
func TestGenRateNoFloorOnlyFirstPublish(t *testing.T) {
	s := &genRateSeg{lastTs: -1, lastPublishTs: -1}
	big := strings.Repeat("a", 48) // 48 字符
	if _, ok := s.observe(big, 1000, true, 0); ok {
		t.Fatal("large first chunk must not publish (no floor-only)")
	}
	if _, ok := s.observe(big, 1000, true, 0); ok {
		t.Fatal("same-timestamp burst must not publish without effective ms")
	}
	// 120ms 仍不足 500ms 门控
	if _, ok := s.observe(big, 1120, true, 0); ok {
		t.Fatal("must not publish before 500ms wall")
	}
	// 500ms → 首包之后 144 字符 / 0.5s = 288
	rate, ok := s.observe(big, 1500, true, 0)
	if !ok {
		t.Fatal("must publish after wall ≥500ms with positive segEffMs")
	}
	// 后 3 包 × 48 = 144 字符 / 500ms
	if math.Abs(rate-288) > 3 {
		t.Errorf("rate = %v, want ≈288（144 字符 / 500ms，不含首包）", rate)
	}
}
