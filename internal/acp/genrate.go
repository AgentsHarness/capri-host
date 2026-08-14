package acp

import (
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

// ── 生成输出速率（字符/秒，段累计吞吐）─────────────────────────────────
// 直接显示字符速度：Live 速率 = 时钟开始之后到达的字符数 / 段累计有效
// 生成时长（c/s）。不做任何 token 换算与校准——字符就是字符，没有估
// 算系数，显示什么就是什么。
//
// 速率只在输出过程中显示：live 事件（active:true）带实时速率；输出
// 结束（tool_call / 回合终态）广播 active:false（不带 rate），前端收到
// 后清除显示。不做回合终态冻结——真实 usage 的生成时长不可观测
// （首包时段缺失），冻结值在数学上系统性高估，不如不冻结。
//
// 首包只用来打钟：它的字符是钟响之前生成的，没有对应时长，计入分子
// 会在「两包一段」（Grok 常见：一整段 thought + 一整段正文）时刚好把
// 速率抬到 2 倍。
// 禁止 floor-only 首发：段墙钟进度 <500ms 或尚无正有效时长时不发布。
//
// "生成段" = thought + assistant 连续流式输出：首个 live chunk 开始；
// user_message_chunk 复位（静默）；tool_call / 回合终态收尾（工具执行
// 等待不计入吞吐），收尾后新 chunk 开新段重新计时。
//
// 时间源：优先 params._meta.agentTimestampMs（agent 生成侧墙钟 ms）。
// agent 时间戳口径下 chunk 间的时间差就是模型真实生成间隔，因此：
//   - 取消 800ms 空闲封顶：gap 是真实生成时间（慢就是慢），封顶会把
//     1.5s 生成 4 字符显示成 5 c/s（实际 ≈2.7），系统性虚高；
//     仅 host 到达时间回退路径保留封顶（防攒包——到达时间无法区分
//     "模型慢"和"网络攒包后连灌"）；
//   - 累计口径下 gap≤0（同时间戳 burst）不归因额外时长；仅在 publish
//     时若累计有效时长仍为 0 则拒绝发布（不靠 16ms 地板虚抬速率）；
//   - burst 合并（≤8ms）保留：同 burst 内字符累加，时长按真实 gap
//     计入（通常近 0，靠后续间隔形成有效分母）。
//
// 节流：每个 session 每 ≥250ms 最多一条 active:true 事件（在 chunk 到达
// 时检查，不另开 ticker）；active:false 不受节流限制。节流时间戳跨段
// 保留（per-session 线上约束）；agent 时钟回拨（now < lastPublishTs）
// 时放行发布，避免回拨导致节流永久卡死。

const (
	genRateBurstGapMs      = 8    // 到达间隔小于此值 → 同一 burst
	genRateMaxIdleGapMs    = 800  // [仅 host 到达时间回退路径] 空闲超过此值不计入生成时间（防攒包）
	genRateMinDtMs         = 16   // 有效时长下限（仅 publish 兜底；gap≤0 不逐包加地板）
	genRateMaxPlausibleCps = 2000 // [仅回退路径] 单包可归因的生成速度上限（字符/s）→ 时长地板
	genRateEmaAlpha        = 0.2  // 显示速率 EMA 系数（累计已稳，轻平滑即可）
	genRateDisplayCapCps   = 2000 // 显示硬顶
	genRateFirstPublishMs  = 500  // 首次发布最短段墙钟（开头样本够长才报）
	genRateThrottleMs      = 250  // 每 session 至多一条 active:true 的间隔
)

// genRateTracker 按 sessionId 分桶的生成速率估算器（每 session 一个
// genRateSeg 状态机）。全部方法带锁；observe 返回应广播 gen_rate 事件
// 的速率与是否广播（live）；输出结束由调用方直接广播 active:false 清
// 除显示（无 seal 状态——收尾只是清段）。
type genRateTracker struct {
	mu    sync.Mutex
	bySid map[string]*genRateSeg
}

func newGenRateTracker() *genRateTracker {
	return &genRateTracker{bySid: make(map[string]*genRateSeg)}
}

// observe 观察新到的流式文本（调用方已保证 text 非空）。now 为时间戳
// （agent 侧或 host 到达），agentClock 标记 now 是否来自
// _meta.agentTimestampMs（决定 800ms 封顶 / 2000 c/s 地板是否启用）。
// 返回应广播的速率与是否通过 250ms 节流；无有效速率 / 被节流时
// ok=false（速率仍已 blend 进 EMA，只是不上线）。
func (t *genRateTracker) observe(sid, text string, now int64, agentClock bool) (rate float64, ok bool) {
	if sid == "" || text == "" {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.bySid[sid]
	if s == nil {
		s = &genRateSeg{lastTs: -1, lastPublishTs: -1}
		t.bySid[sid] = s
	}
	return s.observe(text, now, agentClock)
}

// reset 静默复位 sid 的估算器（user_message_chunk 到达时）：清空段状态
// （新一轮从零开始）。节流时间戳保留——250ms 是 per-session 的线上约
// 束，不因复位重置。
func (t *genRateTracker) reset(sid string) {
	if sid == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.bySid[sid]; s != nil {
		s.clearSegment()
	}
}

// genRateSeg 是单个 session 的生成速率估算器状态。口径：时钟开始之后
// 的累计吞吐（segChars / segEffMs，字符/秒），非单间隔瞬时速率。
type genRateSeg struct {
	// lastTs：上一包的时间戳；-1 = 段尚未开始。
	lastTs int64
	// ema：平滑后的显示速率；nil = 段内尚未 blend 过。复位后清空。
	ema *float64
	// 首次发布门控：段墙钟 < firstPublishMs（500ms）时不发布。
	firstPending bool
	segStartTs   int64
	// segChars：速率分子——时钟开始之后到达的字符数（不含首包）。
	segChars float64
	// segEffMs：本段累计有效生成时长（agent：真实 gap 之和；host 回退：
	// 含 800ms 空闲封顶与 burst 地板）。gap≤0 不归因。
	segEffMs float64
	// 节流（per-session，跨段保留）：上次 active:true 广播的时间戳；
	// -1 = 从未广播过。
	lastPublishTs int64
	// agentClock：本段第一个 chunk 的时间源（agent 侧 true / host 到达
	// false）。后续 chunk 时间源与段不一致时按回退路径计算。
	agentClock bool
}

// clearSegment 清空段状态（复位共用；保留节流时间戳）。
func (s *genRateSeg) clearSegment() {
	s.lastTs = -1
	s.ema = nil
	s.firstPending = false
	s.segStartTs = 0
	s.segChars = 0
	s.segEffMs = 0
}

// observe 处理一批新到的流式文本（每个 chunk 调一次）。返回应广播的
// 速率与是否通过节流。Live 速率 = 时钟开始之后的字符 / 累计有效时长。
func (s *genRateSeg) observe(text string, now int64, agentClock bool) (float64, bool) {
	chars := utf8.RuneCountInString(text)
	if chars <= 0 {
		return 0, false
	}

	// ── 段内首包：只打钟，字符不进速率分子 ────────────────────
	if s.lastTs < 0 {
		s.lastTs = now
		s.segStartTs = now
		s.agentClock = agentClock
		s.firstPending = true
		// 禁止 floor-only 首发：单时间戳无法形成有效时长分母。
		return 0, false
	}
	s.segChars += float64(chars)

	// 时钟源一致性：本 chunk 时间源与段既定源不一致时，gap 是跨时钟差
	// 值（无意义），按回退路径（800ms 封顶）计算。
	effClock := agentClock && agentClock == s.agentClock

	sinceLast := now - s.lastTs
	// 归因本间隔有效时长（同时间戳 / 回拨 → 0；host 回退含空闲封顶）。
	// burst（gap < 8ms）仍按真实 gap 计入——通常很小，不虚抬。
	if sinceLast > 0 {
		s.segEffMs += s.effectiveGapMs(float64(chars), sinceLast, effClock)
	}
	s.lastTs = now

	// ── 首次发布门控：段墙钟 ≥500ms 且累计有效时长 > 0 ────────────
	// 代替「大包立刻用 16ms 地板虚报」；host 无定时器，靠后续 chunk
	// 驱动。门控打开后本 chunk 起可走累计吞吐发布。
	if s.firstPending {
		wall := now - s.segStartTs
		if wall < genRateFirstPublishMs || s.segEffMs <= 0 {
			return 0, false
		}
		s.firstPending = false
	}

	return s.publishCumulative(now)
}

// publishCumulative 用段累计吞吐（轻 EMA、显示钳制）尝试发布。
func (s *genRateSeg) publishCumulative(now int64) (float64, bool) {
	if s.segChars <= 0 || s.segEffMs <= 0 {
		return 0, false
	}
	instant := s.segChars / (s.segEffMs / 1000)
	return s.publish(s.blendGenRate(instant), now)
}

// blendGenRate 用轻 EMA 平滑累计吞吐并钳制到 [0, genRateDisplayCapCps]。
func (s *genRateSeg) blendGenRate(instant float64) float64 {
	clamped := math.Min(math.Max(0, instant), genRateDisplayCapCps)
	if s.ema == nil {
		s.ema = &clamped
		return clamped
	}
	*s.ema = *s.ema*(1-genRateEmaAlpha) + clamped*genRateEmaAlpha
	return *s.ema
}

// publish 应用 250ms 节流并返回是否广播。节流判断用时间戳差值；agent
// 时钟回拨（now < lastPublishTs）时放行发布——否则回拨后差值恒为负、
// 恒 < 250ms，节流会永久卡死直到 agent 时钟追平。
func (s *genRateSeg) publish(rate float64, now int64) (float64, bool) {
	if s.lastPublishTs >= 0 && now >= s.lastPublishTs && now-s.lastPublishTs < genRateThrottleMs {
		// 节流：本帧速率已 blend 进 EMA；不广播、不刷新 lastPublishTs。
		return 0, false
	}
	s.lastPublishTs = now
	return rate, true
}

// effectiveGapMs 把一次间隔归因到有效生成时长（ms），累加进 segEffMs：
//   - agent 时间戳口径：真实 gap（>0）。gap≤0 返回 0——同戳 burst 不靠
//     16ms 地板虚抬累计分母；慢就是慢，不做 800ms 封顶。
//   - host 到达时间回退路径：保留 800ms 空闲封顶（防攒包）与 2000 c/s
//     上限拉伸（单包近零 Δt 时用 chars/MAX 做时长地板）。
func (s *genRateSeg) effectiveGapMs(chars float64, gapMs int64, agentClock bool) float64 {
	if agentClock {
		if gapMs <= 0 {
			return 0
		}
		return float64(gapMs)
	}
	idleCapped := math.Min(math.Max(0, float64(gapMs)), genRateMaxIdleGapMs)
	burstFloorMs := (chars / genRateMaxPlausibleCps) * 1000
	return math.Max(idleCapped, math.Max(burstFloorMs, genRateMinDtMs))
}

// metaAgentTs 取 params._meta.agentTimestampMs（agent 生成侧墙钟 ms，
// float64）作为时间源；返回的 bool 标记是否来自 agent 侧（false = 回退
// host 到达时间——该口径保留 800ms 封顶等防攒包启发式）。
func metaAgentTs(params map[string]any) (int64, bool) {
	if meta, ok := params["_meta"].(map[string]any); ok {
		if ts, ok := asInt(meta["agentTimestampMs"]); ok {
			return ts, true
		}
	}
	return time.Now().UnixMilli(), false
}
