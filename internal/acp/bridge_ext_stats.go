package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// ── 单会话聚合统计（composer 状态条数据源）────────────────────────────
//
// POST /api/session-stats {cwd, sessionId} → 扫描该会话的 updates.jsonl
// 聚合出状态条需要的全部指标：
//
//   - turns  回合数：与历史分页 promptStarts 同一套 UserRunTurnTracker
//     （连续 user run + promptIndex；hostTurn 不计；rewind 死分支截掉）；
//   - steps  步数：tool_call 事件数（工具调用次数）；
//   - llmDurationMs    LLM API 总耗时：Σ usage.apiDurationMs（含等待 +
//     生成，agent 权威值）；
//   - toolDurationMs   工具总耗时：Σ (completed tool_call_update 的
//     _meta.agentTimestampMs − 对应 tool_call 的 agentTimestampMs)。
//     老数据无 _meta → 该工具跳过；全无则省略；
//   - firstTokenAvgMs  用户发出 → 本回合第一条 LLM 流起点，每回合只计一次：
//     本回合第一条流的 streamStartMs − 回合起点（user_message 的
//     agentTimestampMs，_meta.turnStartMs 优先）。流起点可由该流的任意
//     事件提供，包括空串内部 chunk 与 tool_call——真实会话里首条流的
//     起点常落在它们上面，只看「有可见文本的 chunk」会把首 token 算到
//     十几条推理步骤之后。缺 streamStartMs 的回合跳过；
//   - tokensPerSec     纯生成吞吐 = Σ outputTokens / Σ 生成窗口 × 1000。带
//     streamStartMs 的流窗口 = 最后一个生成侧事件 − streamStartMs，覆盖
//     Grok 首包批量生成的时间；缺 streamStartMs 时才回退为首包 → 末包。
//     模型的产物都算生成侧时间点：message/thought chunk（含空串内部
//     事件）与 tool_call（工具名与参数由模型产出）；只有工具的执行与
//     等待（tool_call_update）不进分母。同流先无 ss 后出现 streamStartMs
//     时并入当前流，不拆成两条。整条只有空白占位 chunk（agent 1.0.25 起
//     每个推理步骤先发一个）的流不是生成，不占窗口槽。窗口与 usage 按
//     回合配对：观测不到窗口的回合从分子分母一起跳过，而不是让一个坏
//     回合废掉整个会话。全部回合都观测不到时退回 llmDurationMs 兜底，
//     保证有数据就出数。
//   - cacheHitRate     Σ cachedReadTokens / Σ inputTokens（钳制 [0,1]）；
//   - inputTokens / outputTokens / totalTokens / cachedReadTokens /
//     modelCalls：Σ usage（与 usage-report 同源同口径）。

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

	// 回合起点（epoch ms）：首个 counted user_message_chunk 的 agentTimestampMs。
	turnStartMs int64
	// tool_call 开始时间（epoch ms），按 toolCallId。
	toolStarts map[string]int64
	// 最近一个 LLM 流的起点（去重新流），仅用于首 token 延迟。
	lastStreamStartMs int64
	// 本回合是否已记过首 token（每回合只计第一条流的 streamStart）。
	turnFirstChunkSeen bool
	// 首 token 延迟累计（ms）与回合数。
	firstTokenSumMs int64
	firstTokenCount int64

	// 纯生成窗口（ms）：已与 usage 配对提交的分母。
	genDurationMs int64
	// 参与计算的 outputTokens：只累加真正配到窗口的回合，保证分子
	// 分母取自同一批回合。
	genOutputTokens int64
	// 当前回合每条可计量流的生成窗口。usage 到达时，所有流都有正窗口
	// 才用纯生成窗口计量该回合；否则该回合分子分母一起跳过。
	turnStreamWindows []int64
	currentStream     int

	// 当前流是否出现过真实内容（非空白）。整条流只有空白占位 chunk
	// 时不占窗口槽——agent 1.0.25 起每个推理步骤先发一个空占位。
	streamRealContent bool

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
	turnView, err := sessionLineView(p)
	if err != nil {
		return nil, err
	}
	scanView, err := rewindFilteredFileOrder(p)
	if err != nil {
		return nil, err
	}
	want := survivingFileRanks(scanView)

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	acc := &sessionStatsAccumulator{
		toolStarts:    make(map[string]int64),
		currentStream: -1,
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	rank := 0
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if want[rank] {
			acc.line(line)
		}
		rank++
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	acc.finish()
	acc.stats.Turns = int64(len(turnView.promptStarts))
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
		a.turnStreamWindows = nil
		a.currentStream = -1
		if hostTurnUpdate(upd) {
			return
		}
		// 回合数由 sessionLineView.promptStarts 在扫描结束后覆盖；这里
		// 只维护 firstToken 用的回合起点。
		if agentTsMs > 0 {
			if turnStartMs > 0 {
				a.turnStartMs = turnStartMs
			} else {
				a.turnStartMs = agentTsMs
			}
			a.turnFirstChunkSeen = false
		} else {
			a.turnFirstChunkSeen = false
		}
	case "tool_call":
		// tool_call 是模型生成侧的产物（工具名与参数由模型产出），带本流
		// streamStartMs 时并入该流，其时间点算作生成；只有工具的执行与
		// 等待（tool_call_update）不进窗口。无流起点（老数据）或换了流
		// 时，模型这一步到此为止，封口。
		if streamStartMs > 0 {
			a.noteFirstToken(streamStartMs)
			a.noteStreamStart(streamStartMs)
			a.markStreamReal()
			a.noteChunk(agentTsMs)
		} else {
			a.closeStream()
		}
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
		var content any
		text := ""
		if json.Unmarshal(upd["content"], &content) == nil {
			text = contentText(content)
		}
		// 空串是内部事件（不入时间线）；纯空白是 agent 1.0.25 起的步骤
		// 占位——它算该流的时间点，但整条流只有占位时不构成生成窗口。
		visible := text != ""
		real := strings.TrimSpace(text) != ""
		a.noteFirstToken(streamStartMs)
		if streamStartMs > 0 {
			a.noteStreamStart(streamStartMs)
		} else if visible && !a.streamOpen && agentTsMs > 0 {
			// 无 streamStartMs：先记临时流，≥2 个可见 chunk 才有正窗口。
			a.openStream(0)
		}
		if a.streamOpen {
			if real {
				a.markStreamReal()
			}
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

// markStreamReal 让当前流占一个窗口槽（幂等）。流只要有生成侧时间点就
// 该占槽；真实内容只用于区分「整条都是空白占位」的流。
func (a *sessionStatsAccumulator) markStreamReal() {
	if a.streamRealContent {
		return
	}
	a.streamRealContent = true
	if a.currentStream < 0 {
		a.turnStreamWindows = append(a.turnStreamWindows, 0)
		a.currentStream = len(a.turnStreamWindows) - 1
	}
}

// noteFirstToken 记本回合的首 token 延迟（每回合只记第一条流）。流起点
// 由任意生成侧事件提供——真实会话里首条流的起点常落在空串内部 chunk 或
// tool_call 上，因此不看文本是否可见；缺 streamStartMs 时不记，等后续
// 同回合事件，不拿可见字时间冒充流起点。
func (a *sessionStatsAccumulator) noteFirstToken(streamStartMs int64) {
	if a.turnFirstChunkSeen || a.turnStartMs <= 0 || streamStartMs <= 0 || streamStartMs < a.turnStartMs {
		return
	}
	a.firstTokenSumMs += streamStartMs - a.turnStartMs
	a.firstTokenCount++
	a.turnFirstChunkSeen = true
}

// noteStreamStart 按 streamStartMs 维护「当前流」：同一流继续，换流则
// 开新流；临时无 ss 流等到 streamStart 时并入当前流，不拆成两条。
func (a *sessionStatsAccumulator) noteStreamStart(streamStartMs int64) {
	switch {
	case a.streamOpen && a.lastStreamStartMs == 0:
		a.lastStreamStartMs = streamStartMs
	case streamStartMs != a.lastStreamStartMs:
		a.openStream(streamStartMs)
	case !a.streamOpen:
		// 同流晚到尾巴：重开，保留已记的首包与已提交窗口。
		a.streamOpen = true
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

// accumulateUsage 把一次回合终态 usage 累加进统计。纯生成窗口按回合配对：
// 只有该回合每一条内容流都有正窗口，才把这一回合的 outputTokens 与窗口
// 一起计入；观测不到窗口的回合分子分母一起跳过，不拖垮其余回合。
func (a *sessionStatsAccumulator) accumulateUsage(usage map[string]any) {
	in, out, tot, cr, _, _, mc := usageInts(usage)
	s := &a.stats
	s.InputTokens += in
	s.OutputTokens += out
	s.TotalTokens += tot
	s.CachedReadTokens += cr
	s.ModelCalls += mc
	if out > 0 {
		validWindow := len(a.turnStreamWindows) > 0 && a.pendingGenMs > 0
		for _, win := range a.turnStreamWindows {
			if win <= 0 {
				validWindow = false
				break
			}
		}
		if validWindow {
			a.genDurationMs += a.pendingGenMs
			a.genOutputTokens += out
		}
	}
	a.pendingGenMs = 0
	// usage 是回合终态；下一回合由 user_message_chunk 重新建立流列表。
	a.turnStreamWindows = nil
	a.currentStream = -1
	if v, ok := asInt(usage["apiDurationMs"]); ok && v > 0 {
		s.LLMDurationMs += v
	}
}

// streamGenWindow 计算一条流的纯生成窗口（ms）。带 streamStartMs 时，
// streamStartMs 是该流的生成起点，需覆盖首包批量生成；缺失时回退为
// 首个可见输出 chunk 到最后一个可见输出 chunk。
func streamGenWindow(first, last, streamStart int64) int64 {
	if last <= 0 {
		return 0
	}
	if streamStart > 0 && last > streamStart {
		return last - streamStart
	}
	if first > 0 && last > first {
		return last - first
	}
	return 0
}

// openStream 开始一条新流。streamStart=0 表示临时无 ss 流。窗口槽位由
// markStreamReal 在该流出现真实内容时申请，纯空白占位流不留槽。
func (a *sessionStatsAccumulator) openStream(streamStart int64) {
	a.closeStream()
	a.streamOpen = true
	a.lastStreamStartMs = streamStart
	a.streamFirstChunkMs = 0
	a.streamLastChunkMs = 0
	a.streamClosedWin = 0
	a.streamRealContent = false
	a.currentStream = -1
}

// closeStream 封口当前流：含真实内容的流才占窗口槽并计入 pending。幂等。
func (a *sessionStatsAccumulator) closeStream() {
	if !a.streamOpen {
		return
	}
	a.streamOpen = false
	if !a.streamRealContent {
		// 整条流只有空白占位 chunk（agent 1.0.25 起的步骤占位）：不是生成，
		// 不占窗口槽，其时间也不进入分母。
		return
	}
	win := streamGenWindow(a.streamFirstChunkMs, a.streamLastChunkMs, a.lastStreamStartMs)
	if win > a.streamClosedWin {
		a.pendingGenMs += win - a.streamClosedWin
		a.streamClosedWin = win
	}
	if a.currentStream >= 0 && a.currentStream < len(a.turnStreamWindows) && win > a.turnStreamWindows[a.currentStream] {
		a.turnStreamWindows[a.currentStream] = win
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
		// 分子只取配到窗口的那批回合，与分母同源。
		s.TokensPerSec = float64(a.genOutputTokens) / float64(a.genDurationMs) * 1000
	} else if s.OutputTokens > 0 && s.LLMDurationMs > 0 {
		// 一个回合都观测不到生成窗口（缺 _meta 的老数据 / 单批送达）：
		// 退回 API 总耗时兜底，宁可口径粗也不让状态条空着。
		s.TokensPerSec = float64(s.OutputTokens) / float64(s.LLMDurationMs) * 1000
	}
	if a.firstTokenCount > 0 {
		s.FirstTokenAvgMs = a.firstTokenSumMs / a.firstTokenCount
	}
}

func hostTurnUpdate(upd map[string]json.RawMessage) bool {
	raw, ok := upd["_meta"]
	if !ok || len(raw) == 0 {
		return false
	}
	var meta struct {
		HostTurn bool `json:"hostTurn"`
	}
	if json.Unmarshal(raw, &meta) != nil {
		return false
	}
	return meta.HostTurn
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
