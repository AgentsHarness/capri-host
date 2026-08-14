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
	// 首 token 平均 = ((5000−3000) + (8000−7000) + (22000−20000)) / 3 = 1666.67 → 1666。
	eq("firstTokenAvgMs", 1666)
	// 吞吐 = 输出 token / llmDuration × 1000 = 300 / 27000 × 1000。
	if got, _ := stats["tokensPerSec"].(float64); got < 11.1 || got > 11.12 {
		t.Errorf("stats.tokensPerSec = %v, want ≈11.11", got)
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
