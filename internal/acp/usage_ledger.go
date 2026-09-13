package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── 用量台账（跨会话 · 落盘 · 不随 agent 的 30 天会话清理失效）────────────
//
// 为什么需要：grok agent 的 cleanup_stale_sessions 按文件 mtime 删除 30 天前
// 的会话文件（DEFAULT_CLEANUP_TTL_DAYS = 30），而 updates.jsonl 是用量的唯一
// 载体——源文件被删后那部分历史用量无法再计算，「全部」窗口因此实际只覆盖
// 约 30 天（实测本机「全部」6.82G 与「近 30 天」6.79G 只差 0.45%）。台账在
// 文件被删之前把回合用量抄写一份到 ~/.capri-host/usage-ledger.jsonl，此后
// 即使源文件消失，历史用量仍可长期回溯（实测 580 个文件 / 1.28GB 的回合
// 事件，台账约 1MB）。
//
// 口径与直扫路径完全一致（复用同一套解析函数 usageInts / parseStoredUsageEvent）：
// 一条 turn_completed 事件 = 一条台账记录，per-model 明细原样保留，不做任何
// 换算或相邻差分（那是把每轮总量误当累计快照，会把第二轮记成两轮之差）。
//
// 幂等键 = sessionId + prompt_id。prompt_id 是 agent 每轮生成的 UUID（实测
// 全库 2691 个 turn_completed 无重复），同一轮被重复读到只覆盖同一行；rewind
// 截断导致行号前移时幸存轮次仍命中原行，不会双算。缺 prompt_id 的老数据退化
// 为「事件时刻 + 文件内序号」派生键，重扫时序号确定，同样幂等。
//
// 落盘：内存 entries 为权威，整表重写（temp + rename 原子替换，0600）。
// 台账是追加型小文件，全量重写远低于维护增量日志的复杂度代价。

const (
	usageLedgerFileName = "usage-ledger.jsonl"
	usageLedgerVersion  = 1
)

// usageLedgerSyncInterval 是台账后台同步周期。台账只在源文件变更时解析，
// 常态一轮几乎是空转；3 分钟足以让新回合很快进入可查历史。
const usageLedgerSyncInterval = 3 * time.Minute

// usageLedgerEntry 是一条回合用量记录：一个 turn_completed 事件的逐模型明细。
// TS 为事件时刻（unix 秒，信封顶层 timestamp），是范围查询与覆盖区间的唯一
// 依据；Cwd 用于沿用 /api/usage-report 的 cwd 范围过滤。
type usageLedgerEntry struct {
	TS        int64  `json:"ts"`
	SessionID string `json:"sessionId"`
	PromptID  string `json:"promptId,omitempty"`
	Cwd       string `json:"cwd,omitempty"`
	// Top 是该事件顶层 usage 的计数——总计口径与直扫路径一致，用顶层值
	// 而非逐模型之和（modelUsage 不完整时两者不等，顶层才是权威）。
	Top ledgerCounters `json:"top"`
	// Models 是逐模型明细；usage 无 modelUsage 时只有 unknown 一项。
	Models map[string]ledgerCounters `json:"models,omitempty"`
	// ResidualInput/ResidualTotal 承接「顶层与 modelUsage 之和的差额」（老
	// 版本只记了部分模型）：与直扫路径一致，只补进 unknown 行的 token，
	// 不计回合数。
	ResidualInput int64 `json:"residualInput,omitempty"`
	ResidualTotal int64 `json:"residualTotal,omitempty"`

	// ordinal 是文件内用量事件序号，仅用于 prompt_id 缺失时的派生幂等键，
	// 不落盘（重扫同一文件时序号确定，幂等性不依赖持久化）。
	ordinal int
}

// ledgerCounters 是单个模型在一轮内的计数。字段名沿用 agent usage 的
// camelCase，便于与盘上真实数据逐字对照。
type ledgerCounters struct {
	Input       int64 `json:"inputTokens"`
	Output      int64 `json:"outputTokens"`
	Total       int64 `json:"totalTokens"`
	Cached      int64 `json:"cachedReadTokens,omitempty"`
	CacheCreate int64 `json:"cacheCreationTokens,omitempty"`
	Reasoning   int64 `json:"reasoningTokens,omitempty"`
	ModelCalls  int64 `json:"modelCalls,omitempty"`
	// CostTicks 是 agent 自报的本轮成本（1 tick = 1e-10 USD，0 = 未提供）。
	// 保留它是为了同一份台账后续能支撑成本视图，无需重扫。
	CostTicks int64 `json:"costTicks,omitempty"`
	// CostPartial 对应上游的 costIsPartial：CostTicks 只是下界。
	CostPartial bool `json:"costPartial,omitempty"`
}

// ledgerMeta 是台账的游标侧车：记录每个 updates.jsonl 已入账的 mtime/size，
// 让后续同步只解析变动的文件。写盘顺序必须在 entries 之后——meta 落后只会
// 导致重扫（幂等），meta 超前则会造成永久性缺口，故顺序不可颠倒。
type ledgerMeta struct {
	Version int                     `json:"version"`
	Cursors map[string]ledgerCursor `json:"cursors"`
}

// ledgerCursor 是一个 updates.jsonl 的入账水位。
type ledgerCursor struct {
	Mtime int64 `json:"mtime"`
	Size  int64 `json:"size"`
}

// usageLedger 是落盘台账的进程内视图。mu 串行化同步（后台）与查询（HTTP）。
type usageLedger struct {
	mu      sync.Mutex
	path    string
	entries map[string]*usageLedgerEntry
	cursors map[string]ledgerCursor
	loaded  bool
	synced  bool
	dirty   bool
}

// newUsageLedger 构造台账视图（不读盘；首次使用惰性加载）。
func newUsageLedger(path string) *usageLedger {
	return &usageLedger{
		path:    path,
		entries: make(map[string]*usageLedgerEntry),
		cursors: make(map[string]ledgerCursor),
	}
}

func (l *usageLedger) metaPath() string {
	return strings.TrimSuffix(l.path, ".jsonl") + "-meta.json"
}

// load 读入台账与游标。entries 文件缺失时游标一并作废：否则会拿着游标跳过
// 重扫，留下永远补不回来的缺口（两者必须同生共死）。
func (l *usageLedger) load() {
	if l.loaded || l.path == "" {
		return
	}
	l.loaded = true
	haveEntries := false
	if raw, err := os.ReadFile(l.path); err == nil {
		haveEntries = true
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var e usageLedgerEntry
			if json.Unmarshal(line, &e) != nil || e.SessionID == "" {
				continue
			}
			entry := e
			l.entries[ledgerEntryKey(&entry)] = &entry
		}
	}
	if haveEntries {
		if raw, err := os.ReadFile(l.metaPath()); err == nil {
			var m ledgerMeta
			if json.Unmarshal(raw, &m) == nil && m.Version == usageLedgerVersion {
				for k, v := range m.Cursors {
					l.cursors[k] = v
				}
			}
		}
	}
}

// save 整表落盘：先 entries 后 meta（见 ledgerMeta 的顺序约束）。
// 调用方持锁。
func (l *usageLedger) save() error {
	if l.path == "" {
		return errors.New("用量台账路径为空")
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	keys := make([]string, 0, len(l.entries))
	for k := range l.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := l.entries[keys[i]], l.entries[keys[j]]
		if a.TS != b.TS {
			return a.TS < b.TS
		}
		if a.SessionID != b.SessionID {
			return a.SessionID < b.SessionID
		}
		return a.PromptID < b.PromptID
	})
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, k := range keys {
		if err := enc.Encode(l.entries[k]); err != nil {
			return err
		}
	}
	if err := writeFileAtomic(l.path, buf.Bytes(), 0o600); err != nil {
		return err
	}
	meta := ledgerMeta{Version: usageLedgerVersion, Cursors: l.cursors}
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return writeFileAtomic(l.metaPath(), raw, 0o600)
}

// usageEventKey 是用量事件的幂等键，直扫路径与台账路径共用同一套派生规则，
// 因此「盘上已有的回合」与「台账里的回合」能精确对齐（去重合并的依据）。
//
// prompt_id 是 agent 每轮生成的 UUID（实测全库 2691 个 turn_completed 无
// 重复），是首选键；缺失的老数据退化为「事件时刻 + 文件内序号」——序号在
// 重扫同一文件时确定，因此仍然幂等。
func usageEventKey(sessionID, promptID string, ordinal int) string {
	if promptID != "" {
		return sessionID + "\x00" + promptID
	}
	return fmt.Sprintf("%s\x00#%d", sessionID, ordinal)
}

// ledgerEntryKey 是 usageEventKey 在台账记录上的取值。
func ledgerEntryKey(e *usageLedgerEntry) string {
	return usageEventKey(e.SessionID, e.PromptID, e.ordinal)
}

// merge 按幂等键并入一条记录，返回是否产生了变化。
// 调用方持锁。
func (l *usageLedger) merge(e *usageLedgerEntry) bool {
	key := ledgerEntryKey(e)
	if prev, ok := l.entries[key]; ok && sameLedgerEntry(prev, e) {
		return false
	}
	l.entries[key] = e
	l.dirty = true
	return true
}

// sameLedgerEntry 比较两条记录是否等价（避免无变化时反复重写整表）。
func sameLedgerEntry(a, b *usageLedgerEntry) bool {
	if a.TS != b.TS || a.SessionID != b.SessionID || a.PromptID != b.PromptID ||
		a.Cwd != b.Cwd || a.Top != b.Top ||
		a.ResidualInput != b.ResidualInput || a.ResidualTotal != b.ResidualTotal ||
		len(a.Models) != len(b.Models) {
		return false
	}
	for m, ca := range a.Models {
		cb, ok := b.Models[m]
		if !ok || ca != cb {
			return false
		}
	}
	return true
}

// coverage 返回台账实际覆盖的时间区间（最早/最晚回合时刻，unix 秒）。
// 调用方持锁。
func (l *usageLedger) coverage() (from, to int64) {
	first := true
	for _, e := range l.entries {
		if first {
			from, to, first = e.TS, e.TS, false
			continue
		}
		if e.TS < from {
			from = e.TS
		}
		if e.TS > to {
			to = e.TS
		}
	}
	return from, to
}

// aggregate 把窗口 [from, to] 内、且不在 skip 里的记录聚合进 rep。cwd/
// sessionId 沿用直扫路径的范围语义（都为空 = 全部会话；sessionId 给定时
// cwd 不再单独过滤，与 session/usage 的「越具体越优先」一致）。
//
// skip 是盘上直扫已经计入的事件键集合：台账里同一条必须跳过，否则同一次
// 消费会被两个数据源各记一遍。命中的会话 ID 并入 sessions（与直扫路径同一
// 集合，调用方最后统一取基数，跨源重复会话不会算重）。调用方持锁。
func (l *usageLedger) aggregate(rep *UsageReport, cwd, sessionID string, from, to int64, skip, sessions map[string]struct{}) {
	for key, e := range l.entries {
		if e.TS < from || e.TS > to {
			continue
		}
		if _, dup := skip[key]; dup {
			continue
		}
		if sessionID != "" {
			if e.SessionID != sessionID {
				continue
			}
		} else if cwd != "" && e.Cwd != cwd {
			continue
		}
		if sessions != nil {
			sessions[e.SessionID] = struct{}{}
		}
		rep.Total.Turns++
		addCountersToStat(&rep.Total, e.Top)
		if rep.CoverageFrom == 0 || e.TS < rep.CoverageFrom {
			rep.CoverageFrom = e.TS
		}
		if e.TS > rep.CoverageTo {
			rep.CoverageTo = e.TS
		}
		for model, c := range e.Models {
			st := rep.ByModel[model]
			addCountersToStat(&st, c)
			st.Turns++
			rep.ByModel[model] = st
		}
		if e.ResidualInput != 0 || e.ResidualTotal != 0 {
			st := rep.ByModel[unknownModel]
			st.InputTokens += e.ResidualInput
			st.TotalTokens += e.ResidualTotal
			rep.ByModel[unknownModel] = st
		}
	}
}

// addCountersToStat 把一条模型计数累加进统计行（不含 Turns——回合数按事件计，
// 与直扫路径的 Turns++ 语义一致）。
func addCountersToStat(st *TokenUsageStat, c ledgerCounters) {
	st.InputTokens += c.Input
	st.OutputTokens += c.Output
	st.TotalTokens += c.Total
	st.CachedReadTokens += c.Cached
	st.CacheCreationTokens += c.CacheCreate
	st.ReasoningTokens += c.Reasoning
	st.ModelCalls += c.ModelCalls
}

// ── 扫描入账 ───────────────────────────────────────────────────────────

// ledgerAccumulator 逐行解析一个 updates.jsonl，产出台账记录。
type ledgerAccumulator struct {
	cwd       string
	sessionID string
	out       []*usageLedgerEntry
	// ordinal 是文件内用量事件序号，用于 prompt_id 缺失时的派生幂等键。
	ordinal int
}

// line 处理一行：解析出 usage 事件则产出一条记录。
func (a *ledgerAccumulator) line(l []byte) {
	env, usage, ok := parseStoredUsageEvent(l)
	if !ok {
		return
	}
	e := &usageLedgerEntry{
		TS:        env.Timestamp,
		SessionID: a.sessionID,
		PromptID:  updateString(env, "prompt_id"),
		Cwd:       a.cwd,
	}
	e.ordinal = a.ordinal
	a.ordinal++
	ledgerFillEntry(e, usage)
	a.out = append(a.out, e)
}

// ledgerFillEntry 把一次 usage 拆进台账记录：Top 取顶层计数（总计口径），
// Models 逐模型入账，差额归 unknown 行。分组规则与 accumulateUsage 严格
// 一致，保证台账与直扫两条路径对同一事件得出相同结果。
func ledgerFillEntry(e *usageLedgerEntry, usage map[string]any) {
	in, out, tot, cr, cc, rk, mc := usageInts(usage)
	ticks, partial := usageCostTicks(usage)
	e.Top = ledgerCounters{
		Input: in, Output: out, Total: tot, Cached: cr,
		CacheCreate: cc, Reasoning: rk, ModelCalls: mc,
		CostTicks: ticks, CostPartial: partial,
	}

	mu, ok := usage["modelUsage"].(map[string]any)
	if !ok || len(mu) == 0 {
		e.Models = map[string]ledgerCounters{
			unknownModel: {
				Input: in, Output: out, Total: tot, Cached: cr,
				CacheCreate: cc, Reasoning: rk, ModelCalls: mc,
				CostTicks: ticks, CostPartial: partial,
			},
		}
		return
	}
	e.Models = make(map[string]ledgerCounters, len(mu))
	var sumIn, sumTot int64
	for model, raw := range mu {
		mm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		mi, mo, mt, mcr, mcc, mrk, mmc := usageInts(mm)
		mticks, mpartial := usageCostTicks(mm)
		e.Models[model] = ledgerCounters{
			Input: mi, Output: mo, Total: mt, Cached: mcr,
			CacheCreate: mcc, Reasoning: mrk, ModelCalls: mmc,
			CostTicks: mticks, CostPartial: partial || mpartial,
		}
		sumIn += mi
		sumTot += mt
	}
	// 顶层与 modelUsage 的差额（仅输入/总量可归因）补记 unknown。
	if diff := in - sumIn; diff > 0 {
		e.ResidualInput = diff
	}
	if diff := tot - sumTot; diff > 0 {
		e.ResidualTotal = diff
	}
}

// updateString 从已解析的信封里取一个顶层 update 字段的字符串值（惰性：
// 只解这一个 RawMessage，大 content 字段不碰）。
func updateString(env *storedUsageEnvelope, key string) string {
	raw, ok := env.Params.Update[key]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

// scanLedgerFile 解析一个 updates.jsonl 的全部回合用量记录。cwd/sessionID 由
// 会话目录路径反解（<sessions>/<enc-cwd>/<sessionId>/updates.jsonl）。
func scanLedgerFile(path string) ([]*usageLedgerEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sessionDir := filepath.Dir(path)
	sessionID := filepath.Base(sessionDir)
	acc := &ledgerAccumulator{
		sessionID: sessionID,
		cwd:       ledgerSessionCwd(filepath.Dir(sessionDir)),
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	for sc.Scan() {
		acc.line(sc.Bytes())
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		// 超长行降级：与服务端扫描同款，放弃流式结果改全扫。
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil, rerr
		}
		acc.out = nil
		acc.ordinal = 0
		for _, line := range bytes.Split(raw, []byte("\n")) {
			acc.line(line)
		}
	}
	return acc.out, nil
}

// ledgerSessionCwd 反解会话目录对应的 cwd：优先 .cwd 侧车（长路径的 hash
// 目录），否则对目录名做 percent 解码（session_tasks.go 的 EncodeCwdDirname
// 的逆运算）。解不出来时返回空串——记录照常入账，仅 cwd 范围过滤不命中。
func ledgerSessionCwd(cwdDir string) string {
	raw, err := os.ReadFile(filepath.Join(cwdDir, ".cwd"))
	if err == nil {
		if s := strings.TrimSpace(string(raw)); s != "" {
			return s
		}
	}
	dec, err := url.PathUnescape(filepath.Base(cwdDir))
	if err != nil {
		return ""
	}
	return dec
}

// ── Bridge 接入面 ──────────────────────────────────────────────────────

// usageLedgerPath 返回台账文件路径。GrokConfig.UsageLedgerFile 可覆盖。
//
// GrokHome 被显式覆盖时（测试注入临时 home、或用户自定数据目录），台账跟着
// 它走：否则测试会读写真实用户的 ~/.capri-host，既污染本机数据又让断言依赖
// 开发机现状。
func (b *Bridge) usageLedgerPath() string {
	if b.cfg.UsageLedgerFile != "" {
		return b.cfg.UsageLedgerFile
	}
	if b.cfg.GrokHome != "" {
		return filepath.Join(b.cfg.GrokHome, ".capri-host", usageLedgerFileName)
	}
	if b.homeDir == "" {
		return ""
	}
	return filepath.Join(b.homeDir, ".capri-host", usageLedgerFileName)
}

// ledgerOn 报告台账是否启用。
func (b *Bridge) ledgerOn() bool {
	return b.cfg.UsageLedgerOn && b.usageLedgerPath() != ""
}

// syncUsageLedger 把尚未入账的 updates.jsonl 回合用量抄进台账。按 mtime/size
// 跳过已入账的文件，因此常态调用只解析变动的少数文件；首次调用（或台账被
// 删）会全量重建。ctx 取消时提前返回，已入账部分照常落盘。
func (b *Bridge) syncUsageLedger() error {
	if !b.ledgerOn() || b.ledger == nil {
		return nil
	}
	led := b.ledger
	led.mu.Lock()
	led.load()
	led.mu.Unlock()

	paths, err := usageFiles(b.grokHome(), "", "")
	if err != nil {
		return err
	}
	changed := false
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		cur := ledgerCursor{Mtime: st.ModTime().UnixNano(), Size: st.Size()}
		led.mu.Lock()
		prev, seen := led.cursors[p]
		led.mu.Unlock()
		if seen && prev == cur {
			continue
		}
		entries, err := scanLedgerFile(p)
		if err != nil {
			continue
		}
		led.mu.Lock()
		for _, e := range entries {
			if led.merge(e) {
				changed = true
			}
		}
		led.cursors[p] = cur
		led.dirty = true
		led.mu.Unlock()
	}
	led.mu.Lock()
	defer led.mu.Unlock()
	led.synced = true
	if !changed && !led.dirty {
		return nil
	}
	if err := led.save(); err != nil {
		return err
	}
	led.dirty = false
	return nil
}

// ensureUsageLedger 保证台账至少完成过一次同步（查询路径的惰性初始化，
// 让首次 /api/usage-report 就能拿到落盘历史）。
func (b *Bridge) ensureUsageLedger() error {
	if !b.ledgerOn() || b.ledger == nil {
		return nil
	}
	b.ledger.mu.Lock()
	done := b.ledger.synced
	b.ledger.mu.Unlock()
	if done {
		return nil
	}
	return b.syncUsageLedger()
}

// StartUsageLedgerSync 后台周期同步台账，首次立即跑一轮（启动对账），
// ctx 结束即停。台账关闭时是 no-op。
func (b *Bridge) StartUsageLedgerSync(ctx context.Context) {
	if !b.ledgerOn() {
		return
	}
	go func() {
		if err := b.syncUsageLedger(); err != nil {
			log.Printf("[capri-host] 用量台账首次同步失败: %v", err)
		}
		ticker := time.NewTicker(usageLedgerSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := b.syncUsageLedger(); err != nil {
					log.Printf("[capri-host] 用量台账同步失败: %v", err)
				}
			}
		}
	}()
}

// writeFileAtomic 写临时文件后 rename 替换（同目录保证 rename 原子）。
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
