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
// 聚合出状态条需要的全部指标：
//
//   - turns  回合数：user_message_chunk 事件数（按 agentTimestampMs 去重，
//     agent 可能把一条用户消息拆成多个 chunk）；
//   - steps  步数：tool_call 事件数（工具调用次数）；
//   - llmDurationMs    LLM API 总耗时：Σ usage.apiDurationMs（含等待 +
//     生成，agent 权威值）；
//   - toolDurationMs   工具总耗时：Σ (completed tool_call_update 的
//     _meta.agentTimestampMs − 对应 tool_call 的 agentTimestampMs)。
//     老数据无 _meta → 该工具跳过；全无则省略；
//   - firstTokenAvgMs  用户发出 → 本回合第一个可见字，每回合只计一次：
//     首个 thought/message chunk 的 agentTimestampMs − 回合起点
//     （user_message 的 agentTimestampMs，_meta.turnStartMs 优先）；
//   - tokensPerSec     Σ outputTokens / 生成窗口 × 1000。每流窗口见
//     streamGenWindow：真流式用「首包 → 末包」，Grok 一次性大包用
//     「streamStart → 末包」。窗口与 usage 按回合配对（pending，见
//     accumulateUsage）——进行中回合不进分母。无生成窗口时回退
//     llmDurationMs；
//   - cacheHitRate     Σ cachedReadTokens / Σ inputTokens（钳制 [0,1]）；
//   - inputTokens / outputTokens / totalTokens / cachedReadTokens /
//     modelCalls：Σ usage（与 usage-report 同源同口径）。

// genWindowMinTailMs：末包 − 首包短于此，视为首包一次性吐出（Grok 常见
// thought 大包 + 2ms 正文尾巴），分母用末包 − streamStart；否则视为真
// 流式，分母用末包 − 首包（不含等到首字的时间）。
const genWindowMinTailMs = 500

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
	// 最近一个 LLM 流的起点（去重新流）。
	lastStreamStartMs int64
	// 本回合是否已记过首 token（每回合只计第一个可见字）。
	turnFirstChunkSeen bool
	// 首 token 延迟累计（ms）与回合数。
	firstTokenSumMs int64
	firstTokenCount int64

	// 生成窗口（ms）：已与 usage 配对提交的分母。
	genDurationMs int64
	// 当前回合已封口、尚未随 usage 提交的窗口。新用户消息若尚未见到
	// usage 则丢弃（打断的回合没有分子可配对）。
	pendingGenMs int64
	// 当前打开的流。
	streamOpen         bool
	streamFirstChunkMs int64
	streamLastChunkMs  int64
	// 本流已计入 pending 的窗口（同流晚到尾巴只补差额，避免双计）。
	streamClosedWin int64
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
		a.closeStream()
		a.pendingGenMs = 0
		// 一条用户消息可能拆成多个 chunk：按 agentTimestampMs 去重。
		// 老数据无 _meta（agentTsMs=0）→ 按 chunk 行数计（无去重键）。
		if agentTsMs > 0 {
			if agentTsMs != a.lastUserTsMs {
				a.stats.Turns++
				a.lastUserTsMs = agentTsMs
				a.turnFirstChunkSeen = false
			}
			if turnStartMs > 0 {
				a.turnStartMs = turnStartMs
			} else {
				a.turnStartMs = agentTsMs
			}
		} else {
			a.stats.Turns++
			a.turnFirstChunkSeen = false
		}
	case "tool_call":
		// 工具执行等待不计入生成窗口。
		a.closeStream()
		a.stats.Steps++
		if agentTsMs > 0 {
			if id := jsonStr(upd["toolCallId"]); id != "" {
				a.toolStarts[id] = agentTsMs
			}
		}
	case "tool_call_update":
		// completed = 工具结果终态（状态字段在 update 顶层）。
		if jsonStr(upd["status"]) == "completed" {
			if id := jsonStr(upd["toolCallId"]); id != "" {
				if start, ok := a.toolStarts[id]; ok && agentTsMs > start {
					a.stats.ToolDurationMs += agentTsMs - start
				}
			}
		}
	case "agent_thought_chunk", "agent_message_chunk":
		// 首 token：本回合第一个可见字相对用户发出的时刻。
		if agentTsMs > 0 && !a.turnFirstChunkSeen && a.turnStartMs > 0 && agentTsMs >= a.turnStartMs {
			a.firstTokenSumMs += agentTsMs - a.turnStartMs
			a.firstTokenCount++
			a.turnFirstChunkSeen = true
		}
		if streamStartMs > 0 {
			if streamStartMs != a.lastStreamStartMs {
				a.closeStream()
				a.streamOpen = true
				a.lastStreamStartMs = streamStartMs
				a.streamFirstChunkMs = 0
				a.streamLastChunkMs = 0
				a.streamClosedWin = 0
			} else if !a.streamOpen {
				// 同流晚到尾巴：重开，保留已记的首包与已提交窗口。
				a.streamOpen = true
			}
			a.noteChunk(agentTsMs)
		} else if a.streamOpen {
			// 缺 streamStartMs 但属于已计时流：吸收进当前窗口，不毒化整段。
			a.noteChunk(agentTsMs)
		}
	case "turn_completed", "response_completed":
		a.closeStream()
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

// noteChunk 更新当前流的首/末包时间。
func (a *sessionStatsAccumulator) noteChunk(agentTsMs int64) {
	if agentTsMs <= 0 {
		return
	}
	if a.streamFirstChunkMs == 0 || agentTsMs < a.streamFirstChunkMs {
		a.streamFirstChunkMs = agentTsMs
	}
	if agentTsMs > a.streamLastChunkMs {
		a.streamLastChunkMs = agentTsMs
	}
}

// accumulateUsage 把一次回合终态 usage 累加进统计，并提交本回合 pending
// 生成窗口——分子（outputTokens）与分母按回合配对。
func (a *sessionStatsAccumulator) accumulateUsage(usage map[string]any) {
	in, out, tot, cr, _, _, mc := usageInts(usage)
	s := &a.stats
	s.InputTokens += in
	s.OutputTokens += out
	s.TotalTokens += tot
	s.CachedReadTokens += cr
	s.ModelCalls += mc
	a.genDurationMs += a.pendingGenMs
	a.pendingGenMs = 0
	if v, ok := asInt(usage["apiDurationMs"]); ok && v > 0 {
		s.LLMDurationMs += v
	}
}

// streamGenWindow 一条 LLM 流的生成窗口（ms）。
// 真流式（末包 − 首包 ≥ genWindowMinTailMs）用首包→末包；否则首包即
// 整段输出，用末包 − streamStart。
func streamGenWindow(first, last, streamStart int64) int64 {
	if last <= 0 {
		return 0
	}
	var tail int64
	if first > 0 && last > first {
		tail = last - first
	}
	if tail >= genWindowMinTailMs {
		return tail
	}
	if streamStart > 0 && last > streamStart {
		return last - streamStart
	}
	return tail
}

// closeStream 封口当前流：窗口差额计入 pending。幂等。
func (a *sessionStatsAccumulator) closeStream() {
	if !a.streamOpen {
		return
	}
	a.streamOpen = false
	win := streamGenWindow(a.streamFirstChunkMs, a.streamLastChunkMs, a.lastStreamStartMs)
	if win > a.streamClosedWin {
		a.pendingGenMs += win - a.streamClosedWin
		a.streamClosedWin = win
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
	if a.genDurationMs > 0 {
		s.TokensPerSec = float64(s.OutputTokens) / float64(a.genDurationMs) * 1000
	} else if s.LLMDurationMs > 0 {
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
