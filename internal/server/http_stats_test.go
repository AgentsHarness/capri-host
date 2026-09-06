package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// ── POST /api/session-stats ──────────────────────────────────────────
// 单会话聚合统计：手写真实格式的信封（_meta 毫秒时间戳 + usage
// apiDurationMs），断言状态条的全部派生指标。

// statsSessionLine 构造一行存储信封；meta 可空（老版本无 _meta）。
func statsSessionLine(sid string, env map[string]any, meta map[string]any) string {
	params := map[string]any{"sessionId": sid, "update": env}
	if meta != nil {
		params["_meta"] = meta
	}
	line, _ := json.Marshal(map[string]any{
		"timestamp": 100,
		"method":    "session/update",
		"params":    params,
	})
	return string(line)
}

func TestSessionStatsEndpoint(t *testing.T) {
	home := t.TempDir()
	sid := "s1"

	// 回合 1：用户消息(ts=3000) → LLM 流(streamStart=5000) →
	//   tool list_dir(call@6000) → completed@7000 → LLM 流(streamStart=8000)
	//   → turn_completed usage(input=1000, output=100, api=9000ms)
	// 回合 2：用户消息(ts=20000) → LLM 流(streamStart=22000) →
	//   turn_completed usage(input=2000, output=200, api=18000ms)
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(3000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ok"}},
			map[string]any{"agentTimestampMs": float64(5200), "streamStartMs": float64(5000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "list_dir"},
			map[string]any{"agentTimestampMs": float64(6000), "turnStartMs": float64(3000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed", "content": map[string]any{"type": "text", "text": "[]"}},
			map[string]any{"agentTimestampMs": float64(7000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "done"}},
			map[string]any{"agentTimestampMs": float64(8300), "streamStartMs": float64(8000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 1000, "outputTokens": 100, "totalTokens": 1100,
				"cachedReadTokens": 800, "modelCalls": 2, "apiDurationMs": 9000,
			}}, nil),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "again"}},
			map[string]any{"agentTimestampMs": float64(20000), "promptId": "p2"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ok2"}},
			map[string]any{"agentTimestampMs": float64(22500), "streamStartMs": float64(22000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 2000, "outputTokens": 200, "totalTokens": 2200,
				"cachedReadTokens": 1500, "modelCalls": 3, "apiDurationMs": 18000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))

	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	stats, _ := m["stats"].(map[string]any)
	eq := func(name string, want float64) {
		t.Helper()
		if got, _ := stats[name].(float64); got != want {
			t.Errorf("stats.%s = %v, want %v", name, got, want)
		}
	}
	eq("turns", 2)
	eq("steps", 1)
	// LLM 耗时 = 9000 + 18000。
	eq("llmDurationMs", 27000)
	// 工具耗时 = 7000 − 6000。
	eq("toolDurationMs", 1000)
	// 首 token = 用户发出 → 本回合第一条流的 streamStart，每回合一次：
	// (5000−3000 + 22000−20000) / 2 = 2000。工具后的第二流不计入。
	eq("firstTokenAvgMs", 2000)
	// 带 streamStart 的单 chunk 也能覆盖首包批量生成时间。三条流窗口：
	// (5200−5000)+(8300−8000)+(22500−22000) = 1000ms；
	// 300 / 1000 × 1000 = 300 tok/s。
	if got, _ := stats["tokensPerSec"].(float64); got != 300 {
		t.Errorf("stats.tokensPerSec = %v, want 300", got)
	}
	// 缓存命中 = (800+1500) / (1000+2000) = 0.7667。
	if got, _ := stats["cacheHitRate"].(float64); got < 0.766 || got > 0.767 {
		t.Errorf("stats.cacheHitRate = %v, want ≈0.7667", got)
	}
	eq("inputTokens", 3000)
	eq("outputTokens", 300)
	eq("totalTokens", 3300)
	eq("cachedReadTokens", 2300)
	eq("modelCalls", 5)
}

func TestSessionStatsNoMetaFallback(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	// 老版本数据：无 _meta（agentTimestampMs 缺失）→ 耗时类指标省略，
	// 但 token/轮/步照常统计。
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}}, nil),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "grep"}, nil),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 500, "outputTokens": 50, "totalTokens": 550,
				"cachedReadTokens": 400, "modelCalls": 1, "apiDurationMs": 3000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)

	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	if stats["turns"] != float64(1) || stats["steps"] != float64(1) ||
		stats["llmDurationMs"] != float64(3000) || stats["inputTokens"] != float64(500) {
		t.Fatalf("stats = %v, want turns=1 steps=1 llm=3000 input=500", stats)
	}
	if _, ok := stats["toolDurationMs"]; ok {
		t.Errorf("toolDurationMs = %v, want omitted (no _meta)", stats["toolDurationMs"])
	}
	if _, ok := stats["firstTokenAvgMs"]; ok {
		t.Errorf("firstTokenAvgMs = %v, want omitted (no _meta)", stats["firstTokenAvgMs"])
	}
	// 无 chunk / 无 streamStartMs 时没有可观测的纯生成窗口，指标省略，
	// 不使用 apiDurationMs 作为替代。
	if _, ok := stats["tokensPerSec"]; ok {
		t.Errorf("tokensPerSec = %v, want omitted (no pure generation window)", stats["tokensPerSec"])
	}
}

// 进行中会话：未完成回合的生成窗口不得提前计入分母。
func TestSessionStatsInProgressTurnNotCounted(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	// 回合 1 完成：ss=2000 → chunk@2500、ss=4000 → chunk@4500，
	// turn_completed out=500（窗口 1000ms）。
	// 回合 2 进行中：ss=12000 → chunk@12100、tool_call@13000，
	// 无 turn_completed（窗口 100ms 未提交）。
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ok"}},
			map[string]any{"agentTimestampMs": float64(2500), "streamStartMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "list_dir"},
			map[string]any{"agentTimestampMs": float64(3000), "turnStartMs": float64(1000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed"},
			map[string]any{"agentTimestampMs": float64(3500)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "done"}},
			map[string]any{"agentTimestampMs": float64(4500), "streamStartMs": float64(4000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 1000, "outputTokens": 500, "totalTokens": 1500,
				"modelCalls": 2, "apiDurationMs": 10000,
			}}, nil),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "again"}},
			map[string]any{"agentTimestampMs": float64(10000), "promptId": "p2"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ok2"}},
			map[string]any{"agentTimestampMs": float64(12100), "streamStartMs": float64(12000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t2", "title": "grep"},
			map[string]any{"agentTimestampMs": float64(13000), "turnStartMs": float64(10000)}),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	// 回合 1 有两个可完成的流，窗口 = (2500−2000)+(4500−4000)
	// = 1000ms，outputTokens=500，因此纯生成速度为 500 tok/s。
	if got, _ := stats["tokensPerSec"].(float64); got != 500 {
		t.Errorf("tokensPerSec = %v, want 500（进行中回合不得提前计入分母）", got)
	}
	// 首 token = streamStart − 用户发出：回合 1 为 2000−1000=1000，
	// 回合 2 进行中已见到流 12000−10000=2000 → (1000+2000)/2 = 1500。
	if got, _ := stats["firstTokenAvgMs"].(float64); got != 1500 {
		t.Errorf("firstTokenAvgMs = %v, want 1500（(1000+2000)/2）", got)
	}
}

// 同流晚到 chunk：只把窗口从已提交值延到新末包，不从 streamStart 再加一遍。
func TestSessionStatsLateTailExtendsWindow(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	// ss=2000：chunk@2100 → tool_call 封口（窗口 100）→ 同流晚到 @2400
	// → 窗口延到 400，不是 100+400。out=100 → 100/0.4 = 250 tok/s。
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ok"}},
			map[string]any{"agentTimestampMs": float64(2100), "streamStartMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "list_dir"},
			map[string]any{"agentTimestampMs": float64(3000), "turnStartMs": float64(1000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "tail"}},
			map[string]any{"agentTimestampMs": float64(2400), "streamStartMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 1000, "outputTokens": 100, "totalTokens": 1100,
				"modelCalls": 1, "apiDurationMs": 9000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	// 窗口 = 2400−2000 = 400ms；纯生成吞吐 = 100 / 0.4 = 250 tok/s。
	if got, _ := stats["tokensPerSec"].(float64); got != 250 {
		t.Errorf("tokensPerSec = %v, want 250（晚到尾巴只延窗口，不双计）", got)
	}
}

// 单条缺 streamStartMs 不毒化整段。
func TestSessionStatsStrayNoSsChunkNotPoison(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ok"}},
			map[string]any{"agentTimestampMs": float64(2100), "streamStartMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "more"}},
			map[string]any{"agentTimestampMs": float64(2300)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "list_dir"},
			map[string]any{"agentTimestampMs": float64(2400), "turnStartMs": float64(1000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": nil},
			map[string]any{"agentTimestampMs": float64(2500)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 1000, "outputTokens": 100, "totalTokens": 1100,
				"modelCalls": 1, "apiDurationMs": 9000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	// 该流已有 streamStart，后续缺失 streamStart 的 chunk 仍属于同一流；
	// 窗口 = 2300−2000 = 300ms → 100 / 0.3 ≈ 333.3 tok/s。
	if got, _ := stats["tokensPerSec"].(float64); got < 333 || got > 334 {
		t.Errorf("tokensPerSec = %v, want ≈333.3（单条缺 ss 不得改变已有流口径）", got)
	}
}

// 真流式也用 streamStart → 末包（含 thought 首包生成时间）。
func TestSessionStatsStreamingUsesStreamStartToLast(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	// ss=1000，首包@2000，末包@3000。窗口 = 3000−1000=2000，
	// 不是 3000−2000=1000。out=200 → 100 tok/s。
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(100), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "a"}},
			map[string]any{"agentTimestampMs": float64(2000), "streamStartMs": float64(1000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "b"}},
			map[string]any{"agentTimestampMs": float64(3000), "streamStartMs": float64(1000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 100, "outputTokens": 200, "totalTokens": 300,
				"modelCalls": 1, "apiDurationMs": 5000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	// 带 streamStart 的纯生成窗口 = 3000−1000 = 2000ms，200 / 2 = 100 tok/s。
	if got, _ := stats["tokensPerSec"].(float64); got != 100 {
		t.Errorf("tokensPerSec = %v, want 100（streamStart 覆盖首包批量生成时间）", got)
	}
	// 首 token = streamStart − 用户发出 = 1000 − 100 = 900
	// （不含 thought 包生成 2000−1000）。
	if got, _ := stats["firstTokenAvgMs"].(float64); got != 900 {
		t.Errorf("firstTokenAvgMs = %v, want 900", got)
	}
}

// 首包缺 streamStartMs：等后续同回合 chunk，不用可见字时间冒充流起点。
func TestSessionStatsFirstTokenWaitsForStreamStart(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		// 可见字先到，但没有 streamStart → 还不能记首 token。
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "a"}},
			map[string]any{"agentTimestampMs": float64(2500)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "b"}},
			map[string]any{"agentTimestampMs": float64(2800), "streamStartMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 100, "outputTokens": 50, "totalTokens": 150,
				"modelCalls": 1, "apiDurationMs": 3000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	if got, _ := stats["firstTokenAvgMs"].(float64); got != 1000 {
		t.Errorf("firstTokenAvgMs = %v, want 1000（2000−1000，不是 2500−1000）", got)
	}
	// 先无 ss 的可见字与后到的 streamStart 是同一条流，窗口 = 2800−2000，
	// 不是拆成 0 窗口临时流 + 新流（那会省略吞吐或把分母加两次）。
	if got, _ := stats["tokensPerSec"].(float64); got != 62.5 {
		t.Errorf("tokensPerSec = %v, want 62.5（50 / 0.8s，窗口 2800−2000）", got)
	}
}

// 两条无 ss 可见 chunk 之后才出现 streamStart：仍是一条流，窗口用 ss→末包。
func TestSessionStatsNoSsThenStreamStartOneStream(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "a"}},
			map[string]any{"agentTimestampMs": float64(2500)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "a2"}},
			map[string]any{"agentTimestampMs": float64(2600)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "b"}},
			map[string]any{"agentTimestampMs": float64(2800), "streamStartMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 100, "outputTokens": 50, "totalTokens": 150,
				"modelCalls": 1, "apiDurationMs": 3000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	// 若拆成两条流，分母会变成 (2600−2500)+(2800−2000)=900ms → 55.5。
	if got, _ := stats["tokensPerSec"].(float64); got != 62.5 {
		t.Errorf("tokensPerSec = %v, want 62.5（一条流 2800−2000，不是 100+800）", got)
	}
}

// 老数据全程无 streamStart：≥2 个可见 chunk 回退首包 → 末包。
func TestSessionStatsNoStreamStartTwoChunks(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "a"}},
			map[string]any{"agentTimestampMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "b"}},
			map[string]any{"agentTimestampMs": float64(3000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 100, "outputTokens": 100, "totalTokens": 200,
				"modelCalls": 1, "apiDurationMs": 5000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	if got, _ := stats["tokensPerSec"].(float64); got != 100 {
		t.Errorf("tokensPerSec = %v, want 100（无 ss 两包，窗口 3000−2000）", got)
	}
	if _, ok := stats["firstTokenAvgMs"]; ok {
		t.Errorf("firstTokenAvgMs = %v, want omitted（无 streamStart）", stats["firstTokenAvgMs"])
	}
}

// 无 streamStart 的单包无法得到正窗口，省略吞吐，不回退 apiDuration。
func TestSessionStatsNoStreamStartSingleChunkOmits(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "a"}},
			map[string]any{"agentTimestampMs": float64(2000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 100, "outputTokens": 100, "totalTokens": 200,
				"modelCalls": 1, "apiDurationMs": 5000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	if _, ok := stats["tokensPerSec"]; ok {
		t.Errorf("tokensPerSec = %v, want omitted（无 ss 单包）", stats["tokensPerSec"])
	}
}

// 空 thought 仍用其 streamStart 记首 token，且不占一条 0 窗口把吞吐否决掉。
func TestSessionStatsEmptyThoughtStillCountsFirstToken(t *testing.T) {
	home := t.TempDir()
	sid := "s1"
	lines := []string{
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "hi"}},
			map[string]any{"agentTimestampMs": float64(1000), "promptId": "p1"}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": ""}},
			map[string]any{"agentTimestampMs": float64(1500), "streamStartMs": float64(1200)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "list_dir"},
			map[string]any{"agentTimestampMs": float64(1600), "turnStartMs": float64(1000)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "b"}},
			map[string]any{"agentTimestampMs": float64(2000), "streamStartMs": float64(1800)}),
		statsSessionLine(sid,
			map[string]any{"sessionUpdate": "turn_completed", "usage": map[string]any{
				"inputTokens": 100, "outputTokens": 50, "totalTokens": 150,
				"modelCalls": 2, "apiDurationMs": 3000,
			}}, nil),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"s1"}`)
	m := decodeBody(t, rec)
	stats, _ := m["stats"].(map[string]any)
	if got, _ := stats["firstTokenAvgMs"].(float64); got != 200 {
		t.Errorf("firstTokenAvgMs = %v, want 200（1200−1000，不是 1800−1000）", got)
	}
	// 空 thought 不占窗口；message 窗口 = 2000−1800 = 200ms → 250 tok/s。
	if got, _ := stats["tokensPerSec"].(float64); got != 250 {
		t.Errorf("tokensPerSec = %v, want 250（空 thought 不得留下 0 窗口）", got)
	}
}

func TestSessionStatsValidationAndMissing(t *testing.T) {
	home := t.TempDir()
	s := usageServer(t, home)

	// 缺参数 → 400。
	rec := postJSON(t, s, "/api/session-stats", `{"cwd":"/ws"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing sessionId: status = %d, want 400", rec.Code)
	}
	rec = postJSON(t, s, "/api/session-stats", `{"sessionId":"s1"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing cwd: status = %d, want 400", rec.Code)
	}
	// 会话不存在 → ok:true 全零（前端按无数据显示）。
	rec = postJSON(t, s, "/api/session-stats", `{"cwd":"/ws","sessionId":"nope"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("missing session: status = %d, want 200", rec.Code)
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
}
