package acp

import (
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

// ── 生成输出速率（段累计吞吐 + usage 自校准，tok/s）───────────────────
// 宿主不提供逐 chunk 的 token 计数（_meta.totalTokens 流式期间恒定、
// usage_update 流式期间为 0，真实 usage 只在回合终态），所以流式期间按
// 字符估算：ASCII ≈ 4 字符/token、CJK ≈ 1.5 字符/token；回合终态用真实
// usage（output_tokens + reasoning_tokens）做两件事：
//   - 自校准：ratio = 真实 token / 自上次校准点以来估算累计，factor 按
//     EMA(α=0.3, 初始 1.0) 收敛、钳制 [0.5, 2]，后续发布的速率 × factor
//     —— 英文/CJK/代码密度差异自动收敛，不再靠拍脑袋的 4 字符/token；
//   - 精确冻结：回合终态 seal 时 rate = 真实 token / 累计生成时长
//     （与 live 同口径：段/回合累计吞吐），相对 last live 限制单次跳变
//     ≤35%，避免结束瞬间「换一套数」；usage 缺失时回退最后发布值。
//
// Live 速率 = 段累计估算 token / 段累计有效生成时长（不是单间隔瞬时
// 速率）。随 chunk 增加单调收敛，可作有效参考。禁止 floor-only 首发：
// 段墙钟进度 <500ms 或尚无正有效时长时不发布（消灭 16ms 地板虚高、
// 开头过短样本不可参考）。
//
// "生成段" = thought + assistant 连续流式输出：首个 live chunk 开始；
// user_message_chunk 复位（静默，同时清空回合累计 accTokens/accGenMs）；
// tool_call / 回合终态 seal 冻结数字（工具执行等待不计入吞吐），seal 后
// 新 chunk 开新段重新计时。
//
// 时间源：优先 params._meta.agentTimestampMs（agent 生成侧墙钟 ms）。
// agent 时间戳口径下 chunk 间的时间差就是模型真实生成间隔，因此：
//   - 取消 800ms 空闲封顶：gap 是真实生成时间（慢就是慢），封顶会把
//     1.5s 生成 2.5 token 显示成 3.1 tok/s（实际 ≈1.7），系统性虚高；
//     仅 host 到达时间回退路径保留封顶（防攒包——到达时间无法区分
//     "模型慢"和"网络攒包后连灌"）；
//   - 累计口径下 gap≤0（同时间戳 burst）不归因额外时长；仅在 publish
//     时若累计有效时长仍为 0 则拒绝发布（不靠 16ms 地板虚抬速率）；
//   - burst 合并（≤8ms）保留：同 burst 内 token 累加，时长按真实 gap
//     计入（通常近 0，靠后续间隔形成有效分母）。
//
// 节流：每个 session 每 ≥250ms 最多一条 active:true 事件（在 chunk 到达
// 时检查，不另开 ticker）；seal 的 active:false 不受节流限制。节流时间戳
// 跨段保留（per-session 线上约束）；agent 时钟回拨（now < lastPublishTs）
// 时放行发布，避免回拨导致节流永久卡死。

const (
	genRateBurstGapMs      = 8   // 到达间隔小于此值 → 同一 burst
	genRateMaxIdleGapMs    = 800 // [仅 host 到达时间回退路径] 空闲超过此值不计入生成时间（防攒包）
	genRateMinDtMs         = 16  // 有效时长下限（仅 publish 兜底；gap≤0 不逐包加地板）
	genRateMaxPlausibleTps = 400 // [仅回退路径] 单包可归因的生成速度上限（tok/s）→ 时长地板
	genRateEmaAlpha        = 0.2 // 显示速率 EMA 系数（累计已稳，轻平滑即可）
	genRateDisplayCapTps   = 500 // 显示硬顶
	genRateFirstPublishMs  = 500 // 首次发布最短段墙钟（开头样本够长才报）
	genRateThrottleMs      = 250 // 每 session 至多一条 active:true 的间隔
	genRateCalibAlpha      = 0.3 // 校准 factor 的 EMA 系数（越大越跟手）
	genRateCalibMin        = 0.5 // 校准 factor 钳制下界
	genRateCalibMax        = 2.0 // 校准 factor 钳制上界
	genRateSealMaxJump     = 0.35 // seal 精确值相对 last live 的最大相对跳变
)

// genRateTracker 按 sessionId 分桶的生成速率估算器（每 session 一个
// genRateSeg 状态机）。全部方法带锁；observe/seal 返回应广播 gen_rate
// 事件的速率与是否广播。
type genRateTracker struct {
	mu    sync.Mutex
	bySid map[string]*genRateSeg
}

func newGenRateTracker() *genRateTracker {
	return &genRateTracker{bySid: make(map[string]*genRateSeg)}
}

// observe 观察新到的流式文本（调用方已保证 text 非空）。now 为时间戳
// （agent 侧或 host 到达），agentClock 标记 now 是否来自
// _meta.agentTimestampMs（决定 800ms 封顶 / 400 拉伸是否启用）。返回应
// 广播的速率与是否通过 250ms 节流；无有效速率 / 被节流时 ok=false（速
// 率仍已 blend 进 EMA，只是不上线）。
func (t *genRateTracker) observe(sid, text string, now int64, agentClock bool) (rate float64, ok bool) {
	if sid == "" || text == "" {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.bySid[sid]
	if s == nil {
		s = &genRateSeg{lastTs: -1, lastPublishTs: -1, factor: 1}
		t.bySid[sid] = s
	}
	return s.observe(text, now, agentClock)
}

// seal 收口 sid 的当前生成段（tool_call）：冻结最后发布的速率，并把当前
// 段生成时长/估算 token 计入回合累计（供回合终态的精确冻结与校准）。
// 本段从未发布过速率 → ok=false（客户端保留旧值）。seal 不受 250ms 节流
// 限制。
func (t *genRateTracker) seal(sid string) (rate float64, ok bool) {
	if sid == "" {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.bySid[sid]
	if s == nil {
		return 0, false
	}
	return s.seal(-1)
}

// sealWithUsage 收口 sid 的当前生成段（turn_completed / response_completed
// 且携带真实 usage）：realTokens = output_tokens + reasoning_tokens（>0）。
// 与 seal 相同之外，还做两件事：
//   - 自校准：factor = EMA(α, ratio=真实/估算累计)，后续发布速率 × factor；
//   - 精确冻结：rate = 真实 / 累计生成时长（与 live 同口径），相对 last
//     live 限制单次跳变 ≤ genRateSealMaxJump。
//
// usage 缺失时（realTokens <= 0）回退 seal 语义（冻结最后发布的校准值）。
func (t *genRateTracker) sealWithUsage(sid string, realTokens float64) (rate float64, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.bySid[sid]
	if s == nil {
		return 0, false
	}
	if realTokens <= 0 {
		return s.seal(-1)
	}
	return s.seal(realTokens)
}

// reset 静默复位 sid 的估算器（user_message_chunk 到达时）：清空段状态
// 与回合累计（新一轮的校准窗口与精确冻结从零开始）。节流时间戳保留——
// 250ms 是 per-session 的线上约束，不因复位重置；校准 factor 保留——
// tokenizer/内容密度特征是 session 级的，跨回合收敛。
func (t *genRateTracker) reset(sid string) {
	if sid == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.bySid[sid]; s != nil {
		s.clearSegment()
		s.accTokens = 0
		s.accGenMs = 0
	}
}

// genRateSeg 是单个 session 的生成速率估算器状态。口径：段累计吞吐
// （segTokens / segEffMs），非单间隔瞬时速率。
type genRateSeg struct {
	// lastTs：上一包的时间戳；-1 = 段尚未开始。
	lastTs int64
	// ema：平滑后的显示速率；nil = 段内尚未 blend 过。seal/复位后清空。
	ema *float64
	// 首次发布门控：段墙钟 < firstPublishMs（500ms）时不发布。
	firstPending bool
	segStartTs   int64
	segTokens    float64
	// segEffMs：本段累计有效生成时长（agent：真实 gap 之和；host 回退：
	// 含 800ms 空闲封顶与 burst 地板）。gap≤0 不归因。
	segEffMs float64
	// 本段是否已广播过 active:true（seal 只有发布过才发 active:false）。
	published bool
	lastRate  float64
	// 节流（per-session，跨段保留）：上次 active:true 广播的时间戳；
	// -1 = 从未广播过。
	lastPublishTs int64
	// agentClock：本段第一个 chunk 的时间源（agent 侧 true / host 到达
	// false）。后续 chunk 时间源与段不一致时按回退路径计算。
	agentClock bool
	// factor：字符估算 → 真实 token 的校准系数（session 级，跨段/跨回
	// 合保留）：发布速率 = 累计估算吞吐 × factor。初始 1.0，回合终态带
	// usage 时 EMA(α=0.3) 更新，钳制 [0.5, 2]。
	factor float64
	// accTokens / accGenMs：回合级累计——自上次校准点以来估算累计 token
	// 与累计生成时长（跨段累加，工具执行不计入）。回合终态带 usage 时
	// 用于校准与精确冻结；user_message_chunk 复位时清空。
	accTokens float64
	accGenMs  float64
}

// clearSegment 清空段状态（seal / 复位共用；保留节流时间戳与 session 级
// 校准状态 factor）。回合累计（accTokens/accGenMs）由调用方决定是否清
// 空：seal 后保留（回合内跨段累加），reset（user_message_chunk）清空。
func (s *genRateSeg) clearSegment() {
	s.lastTs = -1
	s.ema = nil
	s.firstPending = false
	s.segStartTs = 0
	s.segTokens = 0
	s.segEffMs = 0
	s.published = false
	s.lastRate = 0
}

// observe 处理一批新到的流式字符（每个 chunk 调一次）。返回应广播的
// 速率与是否通过节流。Live 速率 = 段累计 token / 段累计有效时长。
func (s *genRateSeg) observe(text string, now int64, agentClock bool) (float64, bool) {
	chars := utf8.RuneCountInString(text)
	dTok := estimateTokens(chars, countCJKChars(text))
	if dTok <= 0 {
		return 0, false
	}
	// 段累计估算 token：每个 chunk 都计入（校准分母与累计吞吐共用）。
	s.segTokens += dTok

	// ── 段内首包 ──────────────────────────────────────────────────
	if s.lastTs < 0 {
		s.lastTs = now
		s.segStartTs = now
		s.agentClock = agentClock
		s.firstPending = true
		// 禁止 floor-only 首发：单时间戳无法形成有效时长分母。
		return 0, false
	}

	// 时钟源一致性：本 chunk 时间源与段既定源不一致时，gap 是跨时钟差
	// 值（无意义），按回退路径（800ms 封顶）计算。
	effClock := agentClock && agentClock == s.agentClock

	sinceLast := now - s.lastTs
	// 归因本间隔有效时长（同时间戳 / 回拨 → 0；host 回退含空闲封顶）。
	// burst（gap < 8ms）仍按真实 gap 计入——通常很小，不虚抬。
	if sinceLast > 0 {
		s.segEffMs += s.effectiveGapMs(dTok, sinceLast, effClock)
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

// publishCumulative 用段累计吞吐（× factor、轻 EMA、显示钳制）尝试发布。
func (s *genRateSeg) publishCumulative(now int64) (float64, bool) {
	if s.segTokens <= 0 || s.segEffMs <= 0 {
		return 0, false
	}
	instant := s.segTokens / (s.segEffMs / 1000)
	return s.publish(s.blendGenRate(instant), now)
}

// seal 收口当前生成段（realTokens <= 0 表示无真实 usage）。先把当前段的
// 有效生成时长与估算 token 计入回合累计（跨段累加；工具执行等待不计入），
// 再按 usage 决定返回值：
//   - realTokens > 0：自校准 factor，精确冻结 = 真实 / 累计时长（与 live
//     同口径），相对 lastRate 限制跳变 ≤ genRateSealMaxJump；
//   - 无 usage：冻结最后发布的速率（已是校准后值）。
//
// 本段从未发布过速率 → ok=false（客户端保留旧值），段状态照常清空。
func (s *genRateSeg) seal(realTokens float64) (float64, bool) {
	// 回合累计时长优先用段内累计有效时长（与 live 同口径）；若因同戳
	// burst 未归因，回退墙钟 lastTs−segStartTs。
	segMs := s.segEffMs
	if segMs <= 0 && s.segStartTs > 0 && s.lastTs > s.segStartTs {
		segMs = float64(s.lastTs - s.segStartTs)
	}
	if segMs > 0 {
		s.accGenMs += segMs
	}
	s.accTokens += s.segTokens

	var exact float64
	if realTokens > 0 {
		// 自校准：ratio = 真实 / 自上次校准点以来估算累计。
		if s.accTokens > 0 {
			ratio := realTokens / s.accTokens
			s.factor = math.Min(genRateCalibMax, math.Max(genRateCalibMin,
				(1-genRateCalibAlpha)*s.factor+genRateCalibAlpha*ratio))
		}
		// 精确冻结：真实 token / 累计生成时长（与 live 累计吞吐同口径）。
		if s.accGenMs > 0 {
			exact = math.Min(math.Max(0, realTokens/(s.accGenMs/1000)), genRateDisplayCapTps)
		}
		s.accTokens = 0
		s.accGenMs = 0
	}
	published := s.published
	rate := s.lastRate
	if exact > 0 {
		rate = limitSealJump(s.lastRate, exact)
	}
	s.clearSegment()
	if !published {
		return 0, false
	}
	return rate, true
}

// limitSealJump 把 exact 相对 last live 的单次跳变限制在 ±genRateSealMaxJump
// 以内，避免回合结束瞬间从估算跳到完全不同量级。last<=0（无 live）时
// 直接采用 exact。
func limitSealJump(last, exact float64) float64 {
	if last <= 0 {
		return exact
	}
	maxDelta := last * genRateSealMaxJump
	delta := exact - last
	if delta > maxDelta {
		delta = maxDelta
	} else if delta < -maxDelta {
		delta = -maxDelta
	}
	return math.Min(math.Max(0, last+delta), genRateDisplayCapTps)
}

// publish 应用 250ms 节流并记录发布状态（lastRate = 冻结值来源）。节流
// 判断用时间戳差值；agent 时钟回拨（now < lastPublishTs）时放行发布——
// 否则回拨后差值恒为负、恒 < 250ms，节流会永久卡死直到 agent 时钟追平。
func (s *genRateSeg) publish(rate float64, now int64) (float64, bool) {
	if s.lastPublishTs >= 0 && now >= s.lastPublishTs && now-s.lastPublishTs < genRateThrottleMs {
		// 节流：仍把本帧速率记入 lastRate/EMA 已在 blend 完成，便于 seal
		// 冻结最新累计值；但不广播、不刷新 lastPublishTs。
		s.lastRate = rate
		return 0, false
	}
	s.lastPublishTs = now
	s.published = true
	s.lastRate = rate
	return rate, true
}

// estimateTokens 按字符估算 token 数：ASCII ≈ 4 字符/token、CJK ≈ 1.5
// 字符/token；cjk 钳制到 [0, chars]。字符口径用 rune 数而非 UTF-16 code
// unit 数：BMP 区间两者一致，补充字符（emoji/生僻字）按 1 rune 计——
// 相比 FE 原版的 text.length（UTF-16）每个补充字符少计 0.5 字符，即每
// 个 emoji 少估 0.25 token（FE 0.5 → host 0.25；真实 BPE 分词通常 1
// token/emoji，两者都低估，host 偏差更大，但 emoji 占比低时影响可忽略，
// 且 usage 自校准会整体吸收）。校准系数见 blendGenRate 的 factor。
func estimateTokens(chars, cjk int) float64 {
	if chars <= 0 {
		return 0
	}
	c := math.Min(math.Max(0, float64(cjk)), float64(chars))
	return (float64(chars)-c)/4 + c/1.5
}

// countCJKChars 统计 CJK 统一表意文字区间（与 FE charCodeAt 判定一致）：
// 0x4e00–0x9fff、0x3400–0x4dbf、0xf900–0xfaff。
func countCJKChars(text string) int {
	n := 0
	for _, r := range text {
		if (r >= 0x4e00 && r <= 0x9fff) || (r >= 0x3400 && r <= 0x4dbf) || (r >= 0xf900 && r <= 0xfaff) {
			n++
		}
	}
	return n
}

// effectiveGapMs 把一次间隔归因到有效生成时长（ms），累加进 segEffMs：
//   - agent 时间戳口径：真实 gap（>0）。gap≤0 返回 0——同戳 burst 不靠
//     16ms 地板虚抬累计分母；慢就是慢，不做 800ms 封顶。
//   - host 到达时间回退路径：保留 800ms 空闲封顶（防攒包）与 400 上限
//     拉伸（单包近零 Δt 时用 tokens/MAX 做时长地板）。
func (s *genRateSeg) effectiveGapMs(dTok float64, gapMs int64, agentClock bool) float64 {
	if agentClock {
		if gapMs <= 0 {
			return 0
		}
		return float64(gapMs)
	}
	idleCapped := math.Min(math.Max(0, float64(gapMs)), genRateMaxIdleGapMs)
	burstFloorMs := (dTok / genRateMaxPlausibleTps) * 1000
	return math.Max(idleCapped, math.Max(burstFloorMs, genRateMinDtMs))
}

// blendGenRate 用轻 EMA 平滑累计吞吐并钳制到 [0, 500]；先乘校准 factor
// （session 级，把字符估算换算成真实 token 口径）。
func (s *genRateSeg) blendGenRate(instant float64) float64 {
	clamped := math.Min(math.Max(0, instant*s.factor), genRateDisplayCapTps)
	if s.ema == nil {
		s.ema = &clamped
		return clamped
	}
	*s.ema = *s.ema*(1-genRateEmaAlpha) + clamped*genRateEmaAlpha
	return *s.ema
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

// usageRealTokens 取回合终态 update 的 usage 对象中的真实输出 token 数
// （output_tokens + reasoning_tokens，camelCase 兼容）。usage 缺失或全为
// 0/非数字时返回 ok=false（跳过校准与精确冻结）。
func usageRealTokens(update map[string]any) (float64, bool) {
	u, ok := update["usage"].(map[string]any)
	if !ok {
		return 0, false
	}
	var sum float64
	for _, k := range []string{"output_tokens", "outputTokens", "reasoning_tokens", "reasoningTokens"} {
		if v, ok := asInt(u[k]); ok && v > 0 {
			sum += float64(v)
		}
	}
	return sum, sum > 0
}
