package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// ── 单会话聚合统计（composer 状态条数据源）────────────────────────────
//
// POST /api/session-stats {cwd, sessionId} → 扫描该会话的 updates.jsonl
// 聚合出状态条需要的全部指标（Claude Code statusline 同款口径）：
//
//   - turns  回合数：user_message_chunk 事件数（按 agentTimestampMs 去重，
//     agent 可能把一条用户消息拆成多个 chunk）；
//   - steps  步数：tool_call 事件数（工具调用次数）；
//   - llmDurationMs    LLM API 总耗时：Σ usage.apiDurationMs（agent 在
//     turn_completed 的 usage 对象里直接报告的权威值，含首 token 等待 +
//     生成）；
//   - toolDurationMs   工具总耗时：Σ (completed tool_call_update 的
//     _meta.agentTimestampMs − 对应 tool_call 的 agentTimestampMs)。
//     老数据无 _meta（agentTimestampMs 缺失）→ 该工具跳过；全无则省略；
//   - firstTokenAvgMs  首 token 平均延迟：每个 LLM 流（agent_thought_chunk
//     / agent_message_chunk 的 _meta.streamStartMs 变化即新流）的
//     (streamStartMs − 上个工具 completed 时间 | 回合起点 turnStartMs)，
//     取平均。回合起点 = 首个 user_message_chunk 的 agentTimestampMs
//     （_meta.turnStartMs 优先，二者同源）；
//   - tokensPerSec     吞吐：Σ outputTokens / llmDurationMs × 1000
//     （输出生成速率视角——prefill 并行处理速率远高于生成，混合平均
//     会拉高数字，输出口径才是用户感知的生成快慢）。
//   - cacheHitRate     缓存命中率：Σ cachedReadTokens / Σ inputTokens
//     （钳制 [0,1]，无输入为 0）；
//   - inputTokens / outputTokens / totalTokens / cachedReadTokens /
//     modelCalls：Σ usage（与 usage-report 同源同口径，逐事件累加无重复）。

// SessionStats 是一次单会话聚合的结果（字段全 optional 语义：老数据
// 缺 _meta 时 toolDurationMs / firstTokenAvgMs 省略，前端显示 '—'）。
type SessionStats struct {
	Turns            int64   `json:"turns"`
	Steps            int64   `json:"steps"`
	LLMDurationMs    int64   `json:"llmDurationMs"`
	ToolDurationMs   int64   `json:"toolDurationMs,omitempty"`
	FirstTokenAvgMs  int64   `json:"firstTokenAvgMs,omitempty"`
	TokensPerSec     float64 `json:"tokensPerSec,omitempty"`
	CacheHitRate     float64 `json:"cacheHitRate"`
	InputTokens      int64   `json:"inputTokens"`
	OutputTokens     int64   `json:"outputTokens"`
	TotalTokens      int64   `json:"totalTokens"`
	CachedReadTokens int64   `json:"cachedReadTokens"`
	ModelCalls       int64   `json:"modelCalls"`
}

// stats 是扫描过程中的累计器。
type sessionStatsAccumulator struct {
	stats SessionStats

	// 回合起点（epoch ms）：首个 user_message_chunk 的 agentTimestampMs。
	turnStartMs int64
	// 已计数的用户 chunk 时间戳（去重）。
	lastUserTsMs int64
	// tool_call 开始时间（epoch ms），按 toolCallId。
	toolStarts map[string]int64
	// 最近一个 completed 工具结果的时间（epoch ms），用于推算后续流的
	// 首 token 延迟（agent 在工具结果后立即发起下一次 LLM 调用）。
	lastToolCompletedMs int64
	// 最近一个 LLM 流的起点（去重新流）。
	lastStreamStartMs int64
	// 首 token 延迟累计（ms）与流数。
	firstTokenSumMs int64
	firstTokenCount int64
}

// SessionStats 聚合指定会话的统计。cwd/sessionId 必填；文件不存在返回
// 零值统计（不报错，前端按无数据显示）。
func (b *Bridge) SessionStats(ctx context.Context, cwd, sessionID string) (*SessionStats, error) {
	p := sessionUpdatesFile(b.grokHome(), cwd, sessionID)
	if p == "" {
		return nil, fmt.Errorf("无法解析会话目录 (cwd=%q)", cwd)
	}
	if _, err := os.Stat(p); err != nil {
		return &SessionStats{}, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	acc := &sessionStatsAccumulator{toolStarts: make(map[string]int64)}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		acc.line(sc.Bytes())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	acc.finish()
	return &acc.stats, nil
}

// statsTags 预过滤：只对可能贡献统计的事件行做 JSON 解析。
var statsTags = [][]byte{
	[]byte(`"sessionUpdate":"user_message_chunk"`),
	[]byte(`"sessionUpdate":"tool_call"`),
	[]byte(`"sessionUpdate":"tool_call_update"`),
	[]byte(`"sessionUpdate":"agent_thought_chunk"`),
	[]byte(`"sessionUpdate":"agent_message_chunk"`),
	[]byte(`"sessionUpdate":"turn_completed"`),
	[]byte(`"sessionUpdate":"response_completed"`),
}

// line 处理一行存储信封。
func (a *sessionStatsAccumulator) line(l []byte) {
	hit := false
	for _, tag := range statsTags {
		if bytes.Contains(l, tag) {
			hit = true
			break
		}
	}
	if !hit {
		return
	}
	var env struct {
		Params struct {
			Update map[string]json.RawMessage `json:"update"`
			Meta   map[string]json.RawMessage `json:"_meta"`
		} `json:"params"`
	}
	if json.Unmarshal(l, &env) != nil {
		return
	}
	upd := env.Params.Update
	kind := jsonStr(upd["sessionUpdate"])
	meta := env.Params.Meta

	// _meta 是 agent 写盘的毫秒时间戳（agentTimestampMs / turnStartMs /
	// streamStartMs），老版本可能缺失。
	var agentTsMs, turnStartMs, streamStartMs int64
	if meta != nil {
		agentTsMs = statsInt64(meta["agentTimestampMs"])
		turnStartMs = statsInt64(meta["turnStartMs"])
		streamStartMs = statsInt64(meta["streamStartMs"])
	}

	switch kind {
	case "user_message_chunk":
		// 一条用户消息可能拆成多个 chunk：按 agentTimestampMs 去重。
		// 老数据无 _meta（agentTsMs=0）→ 按 chunk 行数计（无去重键）。
		if agentTsMs > 0 {
			if agentTsMs != a.lastUserTsMs {
				a.stats.Turns++
				a.lastUserTsMs = agentTsMs
			}
			// 新回合边界：更新回合起点、清除上一回合的工具基线
			// （上一回合的工具完成时间不能作为本回合流的首 token 基准）。
			if turnStartMs > 0 {
				a.turnStartMs = turnStartMs
			} else {
				a.turnStartMs = agentTsMs
			}
			a.lastToolCompletedMs = 0
		} else {
			a.stats.Turns++
		}
	case "tool_call":
		a.stats.Steps++
		if agentTsMs > 0 {
			if id := jsonStr(upd["toolCallId"]); id != "" {
				a.toolStarts[id] = agentTsMs
			}
		}
	case "tool_call_update":
		if a.lastToolCompletedMs == 0 && agentTsMs > 0 {
			// 工具调用第一个分块：结果流开始，更新"最近活动"基线。
			a.lastToolCompletedMs = agentTsMs
		}
		// completed = 工具结果终态（状态字段在 update 顶层）。
		if jsonStr(upd["status"]) == "completed" {
			if id := jsonStr(upd["toolCallId"]); id != "" {
				if start, ok := a.toolStarts[id]; ok && agentTsMs > start {
					a.stats.ToolDurationMs += agentTsMs - start
				}
			}
			if agentTsMs > 0 {
				a.lastToolCompletedMs = agentTsMs
			}
		}
	case "agent_thought_chunk", "agent_message_chunk":
		// 新 LLM 流：streamStartMs 与上次不同（每个流一个起点）。
		if streamStartMs > 0 && streamStartMs != a.lastStreamStartMs {
			base := a.lastToolCompletedMs
			if base == 0 {
				base = a.turnStartMs
			}
			if base > 0 && streamStartMs >= base {
				a.firstTokenSumMs += streamStartMs - base
				a.firstTokenCount++
			}
			a.lastStreamStartMs = streamStartMs
		}
	case "turn_completed", "response_completed":
		rawUsage, ok := upd["usage"]
		if !ok {
			return
		}
		var usage map[string]any
		if json.Unmarshal(rawUsage, &usage) != nil || len(usage) == 0 {
			return
		}
		a.accumulateUsage(usage)
	}
}

// accumulateUsage 把一次回合终态 usage 累加进统计（顶层字段；与
// usage-report 的 accumulateUsage 同口径，但这里不需要 modelUsage
// 分组，只取会话级合计）。
func (a *sessionStatsAccumulator) accumulateUsage(usage map[string]any) {
	in, out, tot, cr, _, _, mc := usageInts(usage)
	s := &a.stats
	s.InputTokens += in
	s.OutputTokens += out
	s.TotalTokens += tot
	s.CachedReadTokens += cr
	s.ModelCalls += mc
	if v, ok := asInt(usage["apiDurationMs"]); ok && v > 0 {
		s.LLMDurationMs += v
	}
}

// finish 计算派生指标（命中率 / 吞吐 / 首 token 平均）。
func (a *sessionStatsAccumulator) finish() {
	s := &a.stats
	if s.InputTokens > 0 {
		rate := float64(s.CachedReadTokens) / float64(s.InputTokens)
		if rate > 1 {
			rate = 1
		}
		s.CacheHitRate = rate
	}
	// 吞吐口径：输出 token ÷ LLM 总耗时（生成速率视角）。输入侧的
	// prefill 是并行处理、速率比生成高一个数量级，把新输入/缓存读
	// 平均进 LLM 总耗时会把数字系统性拉高（实测：有效处理口径
	// 206 tok/s vs 输出口径 81 tok/s）——而用户感知的"快慢"是输出
	// 生成速率（字往外蹦的速度），所以用输出 token 作分子。
	if s.LLMDurationMs > 0 {
		s.TokensPerSec = float64(s.OutputTokens) / float64(s.LLMDurationMs) * 1000
	}
	if a.firstTokenCount > 0 {
		s.FirstTokenAvgMs = a.firstTokenSumMs / a.firstTokenCount
	}
}

// jsonStr / jsonInt 提取 RawMessage 字段值（缺失/非目标类型 → 零值）。
func statsInt64(raw json.RawMessage) int64 {
	if len(raw) == 0 {
		return 0
	}
	var n int64
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	return 0
}
