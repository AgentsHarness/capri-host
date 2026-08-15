package acp

import (
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

// ── 生成输出速率（字符/秒，段累计吞吐）─────────────────────────────────
// 直接显示字符速度：Live 速率 = 段内字符数 / 段累计有效生成时长（c/s）。
// 不做任何 token 换算与校准——字符就是字符，没有估算系数，显示什么就
// 是什么。
//
// 速率只在输出过程中显示：live 事件（active:true）带实时速率；输出
// 结束（tool_call / 回合终态）广播 active:false（不带 rate），前端收到
// 后清除显示。不做回合终态冻结——真实 usage 的生成时长不可观测
// （首包时段缺失），冻结值在数学上系统性高估，不如不冻结。
//
// 两种口径（按时间源选择）：
//
//   - agent 时钟 + _meta.streamStartMs（现版本数据都有）：流起点就是
//     段的时间零点，速率 = 流起点之后到达的全部字符（含首包）/
//     (now − streamStartMs)。Grok 的流是「一整段 thought 一大包 + 尾部
//     小 chunk」结构，首包常占 85–99% 字符；旧口径把首包排除在分子外、
//     只用 chunk 间隙做分母，会把「2ms 后跟一个小尾巴包」算成几万
//     c/s（实测 12000–29000），或整段无发布（实测 102 段仅 24 段有值，
//     有值的还在 35–600 之间乱跳）——而含首包/流起点的真值稳定在
//     150–300 c/s。累计吞吐天然自平滑，无需 EMA；仅保留 500ms 首发布
//     门控与 250ms 节流。
//   - 回退路径（无 streamStartMs / host 到达时间）：首包只打钟，速率 =
//     首包之后字符 / 累计 gap。保留 800ms 空闲封顶与 2000 c/s 上限拉伸
//     （防攒包——到达时间无法区分「模型慢」和「网络攒包后连灌」）。
//
// 时间源：优先 params._meta.agentTimestampMs（agent 生成侧墙钟 ms），
// 流起点取 _meta.streamStartMs。agent 时间戳口径下 chunk 间的时间差就
// 是模型真实生成间隔；同时间戳 burst（gap≤0）不归因额外时长，靠
// streamStart 起点形成有效分母。
//
// 段边界："生成段" = 一个 LLM 流（thought + assistant 连续流式输出）：
// 首个 live chunk 开始；user_message_chunk 复位（静默）；tool_call /
// 回合终态收尾（工具执行等待不计入吞吐）；流起点（streamStartMs）变化
// 而段未封口 → 视为新流，重开段。
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
	genRateEmaAlpha        = 0.2  // 显示速率 EMA 系数（仅回退路径；agent 累计口径自平滑不用）
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
// _meta.agentTimestampMs，streamStartMs 是 _meta.streamStartMs（当前流
// 起点；0 = 缺失，走 gap 回退口径）。返回应广播的速率与是否通过
// 250ms 节流；无有效速率 / 被节流时 ok=false（agent 累计口径下速率仍
// 已计入下一帧，无需 blend 回退）。
func (t *genRateTracker) observe(sid, text string, now int64, agentClock bool, streamStartMs int64) (rate float64, ok bool) {
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
	return s.observe(text, now, agentClock, streamStartMs)
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

// genRateSeg 是单个 session 的生成速率估算器状态。口径：
//   - agent 路径（streamStart > 0）：时钟开始（流起点）之后的累计吞吐
//     （segChars / (now − streamStart)，字符/秒），含首包；
//   - 回退路径：首包之后字符 / 累计有效 gap。
type genRateSeg struct {
	// lastTs：上一包的时间戳；-1 = 段尚未开始。
	lastTs int64
	// ema：平滑后的显示速率（仅回退路径）；nil = 段内尚未 blend 过。
	ema *float64
	// 首次发布门控：段墙钟 < firstPublishMs（500ms）时不发布。
	firstPending bool
	segStartTs   int64
	// streamStart：当前流的起点（_meta.streamStartMs）；0 = 无（回退路径）。
	streamStart int64
	// segChars：速率分子——agent 路径 = 流起点之后到达的全部字符（含
	// 首包）；回退路径 = 时钟开始之后的字符（不含首包）。
	segChars float64
	// segEffMs：回退路径累计有效生成时长（agent：真实 gap 之和；host
	// 回退：含 800ms 空闲封顶与 burst 地板）。gap≤0 不归因。
	segEffMs float64
	// 节流（per-session，跨段保留）：上次 active:true 广播的时间戳；
	// -1 = 从未广播过。
	lastPublishTs int64
	// agentClock：本段当前的时间源。chunk 与段不一致时 reseed
	// （跨时钟差值无意义）。
	agentClock bool
}

// clearSegment 清空段状态（复位共用；保留节流时间戳）。
func (s *genRateSeg) clearSegment() {
	s.lastTs = -1
	s.ema = nil
	s.firstPending = false
	s.segStartTs = 0
	s.streamStart = 0
	s.segChars = 0
	s.segEffMs = 0
}

// reseed 以本 chunk 为新段首包重开段（首包 / 时钟源切换 / 新流未封口
// 共用）。与 clearSegment 同等清空段状态（含 ema——否则回退路径的
// blendGenRate 会粘上一段的值），仅保留 lastPublishTs。
func (s *genRateSeg) reseed(now int64, agentClock bool, streamStartMs int64) {
	s.lastTs = now
	s.segStartTs = now
	s.agentClock = agentClock
	s.streamStart = 0
	if agentClock && streamStartMs > 0 {
		s.streamStart = streamStartMs
	}
	s.firstPending = true
	s.segChars = 0
	s.segEffMs = 0
	s.ema = nil
}

// observe 处理一批新到的流式文本（每个 chunk 调一次）。返回应广播的
// 速率与是否通过节流。
func (s *genRateSeg) observe(text string, now int64, agentClock bool, streamStartMs int64) (float64, bool) {
	chars := utf8.RuneCountInString(text)
	if chars <= 0 {
		return 0, false
	}

	// ── 段内首包：打钟并确定本段口径 ────────────────────
	if s.lastTs < 0 {
		s.reseed(now, agentClock, streamStartMs)
		// 回退路径（无流起点）：首包只打钟，字符不进分子（无对应时长）。
		// agent 路径继续走下方统一计算——首包字符的生成时长 = 首包时间
		// − streamStart，分母覆盖，分子必须含首包。
		if s.streamStart == 0 {
			return 0, false
		}
	} else if s.streamStart > 0 && (!agentClock || streamStartMs <= 0 || streamStartMs != s.streamStart) {
		// ── agent 路径上时钟源 / 流起点变化 → 重开段 ────
		s.reseed(now, agentClock, streamStartMs)
		if s.streamStart == 0 {
			return 0, false
		}
	} else if s.streamStart == 0 && agentClock && streamStartMs > 0 {
		// ── 回退路径段中途出现 agent 时钟 + 流起点 → 切 agent 口径 ──
		s.reseed(now, agentClock, streamStartMs)
	}

	if s.streamStart > 0 {
		// ── agent 路径：段累计吞吐 = 流起点之后全部字符 / 墙钟 ──
		s.segChars += float64(chars)
		wall := now - s.streamStart
		if s.firstPending {
			if wall < genRateFirstPublishMs || wall <= 0 {
				return 0, false
			}
			s.firstPending = false
		}
		if wall <= 0 {
			return 0, false
		}
		instant := s.segChars / (float64(wall) / 1000)
		// 累计口径自平滑，不 blend EMA；显示硬顶防单包异常。
		return s.publish(math.Min(instant, genRateDisplayCapCps), now)
	}

	// ── 回退路径（无流起点 / host 时钟）：gap 累计口径 ──
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
		// 节流：本帧速率已 blend 进 EMA（回退路径）；不广播、不刷新
		// lastPublishTs。
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
// float64）作为时间源，同时取 _meta.streamStartMs（当前 LLM 流起点，
// 用于 agent 累计口径的段时间零点）。返回的 bool 标记是否来自 agent
// 侧（false = 回退 host 到达时间——该口径保留 800ms 封顶等防攒包启发
// 式）；streamStartMs 缺失时返回 0（genRateSeg 走 gap 回退口径）。
func metaAgentTs(params map[string]any) (ts int64, streamStartMs int64, agentClock bool) {
	if meta, ok := params["_meta"].(map[string]any); ok {
		if v, ok := asInt(meta["agentTimestampMs"]); ok {
			ss, _ := asInt(meta["streamStartMs"])
			return v, ss, true
		}
	}
	return time.Now().UnixMilli(), 0, false
}
