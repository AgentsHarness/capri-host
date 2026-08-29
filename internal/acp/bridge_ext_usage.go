package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// ── Token 用量聚合（跨会话 · 时间窗口 · 按模型分组）───────────────────
//
// 宿主侧统计接口：直接扫描 ~/.grok/sessions 下各 session 的 updates.jsonl
// （与 task 时间线同源文件），聚合 [from, to] 时间窗口内回合终态
// （turn_completed / response_completed）携带的真实 usage 对象。与
// x.ai/session/usage（活动会话的实时账本）互补：本统计可回溯历史、跨
// 会话、跨模型，且直接用 agent 写盘的真实数字（含 modelUsage 分组与
// 缓存命中计数），不需要宿主侧估算。
//
// 口径（对照真实数据核验）：
//   - 载体：update.usage 是回合累计（modelCalls/numTurns 字段为回合内
//     调用计数），逐事件累加即总量，无重复计数；
//   - 字段 camelCase（兼容老版本 snake_case）：inputTokens / outputTokens /
//     totalTokens / cachedReadTokens（缓存命中读）/ cacheCreationTokens
//     （缓存写入）/ reasoningTokens / modelCalls / apiDurationMs；
//   - 按模型：usage.modelUsage = {model: {同名字段…}}，与顶层一致；
//     modelUsage 缺失的事件整体记入 "unknown"，顶层与 modelUsage 之和的
//     差额（老版本可能只记了部分模型）同样归入 "unknown"，保证
//     byModel 各模型之和恒等于 total；
//   - 命中率 = cachedReadTokens / inputTokens（cachedRead ⊆ input，
//     钳制 [0,1]；input 为 0 时为 0）；
//   - 时间：事件信封顶层 timestamp（unix 秒；请求 from/to 兼容毫秒）；
//   - rewind 死分支照常计入：回退丢弃的回合是真实消耗过的 token（与
//     task 时间线的 rewind 死分支过滤语义不同——那是会话视图，这是
//     资源消耗账本），文件全量回合终态事件都统计。
//
// 性能（实测：284 个会话 ~1.1GB updates.jsonl，全量聚合 <500ms）：
//   - 并行：worker 数 = min(NumCPU, 文件数)，每 worker 持本地 UsageReport
//     无锁累计，完成后顺序合并；
//   - 流式读：bufio.Scanner 逐行（64KB 起步、64MB 行上限），不做
//     ReadFile + strings.Split 的双份大内存复制；tag 子串预过滤后仅对
//     候选行 JSON 解析（RawMessage 惰性，content 大字段不解析）；
//   - mtime 预过滤：from > 0 时，最后写入早于窗口起点的文件必然没有
//     窗口内事件，整文件跳过（事件 timestamp 与文件 mtime 同源同精度，
//     写入晚于事件，故安全）；
//   - 超长行降级：单行超过 64MB（实际几乎不可能）才放弃流式结果，
//     改用 ReadFile 全扫路径。

// TokenUsageStat 是单个模型（或总计行）的 token 用量。字段与 agent 的
// usage 对象一一对应；Turns 为统计到的回合终态事件数，CacheHitRate 为
// 派生命中率（cachedRead / input）。
type TokenUsageStat struct {
	InputTokens         int64   `json:"inputTokens"`
	OutputTokens        int64   `json:"outputTokens"`
	TotalTokens         int64   `json:"totalTokens"`
	CachedReadTokens    int64   `json:"cachedReadTokens"`
	CacheCreationTokens int64   `json:"cacheCreationTokens"`
	ReasoningTokens     int64   `json:"reasoningTokens"`
	ModelCalls          int64   `json:"modelCalls"`
	Turns               int64   `json:"turns"`
	CacheHitRate        float64 `json:"cacheHitRate"`
}

// UsageReport 是一次用量聚合的结果。
type UsageReport struct {
	// From/To 归一化后的时间窗口（unix 秒）。
	From     int64                     `json:"from,omitempty"`
	To       int64                     `json:"to,omitempty"`
	Sessions int                       `json:"sessions"`
	Total    TokenUsageStat            `json:"total"`
	ByModel  map[string]TokenUsageStat `json:"byModel,omitempty"`
}

// unknownModel 承接无 modelUsage 事件的键（以及 modelUsage 与顶层的差额）。
const unknownModel = "unknown"

// tag 预过滤（与 session_tasks.go 同款技巧：raw substring 精确匹配）。
// []byte 形式供流式路径直接 bytes.Contains（避免逐行 string 转换）。
var (
	tagTurnCompletedB     = []byte(`"sessionUpdate":"turn_completed"`)
	tagResponseCompletedB = []byte(`"sessionUpdate":"response_completed"`)
	// 单行 JSON 上限；超长行降级到 ReadFile 全扫路径（无行限制）。
	maxUsageLineBytes = 64 << 20
)

// UsageReport 聚合 grok 会话在 [from, to] 时间窗口内的真实 token 用量
// （总/输入/输出/缓存命中/命中率），并按模型分组。cwd/sessionId 限定
// 扫描范围（都为空 = 全部会话；仅 cwd = 该工作区所有会话；sessionId 给
// 定时 cwd 必填）。from/to 为 unix 秒（兼容毫秒，自动识别；to<=0 = 当前
// 时刻，from<=0 = 不设下限）。
func (b *Bridge) UsageReport(ctx context.Context, cwd, sessionID string, from, to int64) (*UsageReport, error) {
	from, to = normalizeUsageWindow(from, to)
	paths, err := usageFiles(b.grokHome(), cwd, sessionID)
	if err != nil {
		return nil, err
	}
	rep := &UsageReport{From: from, To: to, ByModel: make(map[string]TokenUsageStat)}
	scanUsageFiles(ctx, paths, rep, from, to)
	finalizeUsageStats(rep)
	return rep, nil
}

// normalizeUsageWindow 把请求窗口归一化为 unix 秒：毫秒（>1e12）转秒，
// to<=0 取当前时刻，from<0 置 0，from>to 交换（防呆）。
func normalizeUsageWindow(from, to int64) (int64, int64) {
	if from > 1_000_000_000_000 {
		from /= 1000
	}
	if to > 1_000_000_000_000 {
		to /= 1000
	}
	if from < 0 {
		from = 0
	}
	if to <= 0 {
		to = time.Now().Unix()
	}
	if from > to {
		from, to = to, from
	}
	return from, to
}

// usageFiles 枚举待扫描的 updates.jsonl：
//   - sessionID 给定 → 该会话文件（cwd 必填；文件不存在返回空集）；
//   - 仅 cwd → 该工作区（sessions/<encoded-cwd>/）下所有会话；
//   - 都为空 → sessions 根目录下全部会话（含 hash-slug 目录）。
func usageFiles(grokHome, cwd, sessionID string) ([]string, error) {
	if grokHome == "" {
		return nil, fmt.Errorf("无法解析会话目录 (home 为空)")
	}
	switch {
	case sessionID != "":
		p := sessionUpdatesFile(grokHome, cwd, sessionID)
		if p == "" {
			return nil, fmt.Errorf("无法解析会话目录 (cwd=%q)", cwd)
		}
		if _, err := os.Stat(p); err != nil {
			return nil, nil
		}
		return []string{p}, nil
	case cwd != "":
		return updatesUnder(sessionsCwdDir(grokHome, cwd))
	default:
		return updatesUnder(filepath.Join(grokHome, "sessions"))
	}
}

// updatesUnder 递归收集目录下所有 updates.jsonl（目录不存在 → 空集）。
func updatesUnder(dir string) ([]string, error) {
	var paths []string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && d.Name() == "updates.jsonl" {
			paths = append(paths, p)
		}
		return nil
	})
	return paths, nil
}

// scanUsageFiles 并行扫描全部 updates.jsonl 并聚合进 rep。worker 数 =
// min(NumCPU, 文件数)，每 worker 持本地 UsageReport 无锁累计，完成后
// 顺序合并；ctx 取消时提前退出。from>0 时先按 mtime 预过滤：最后写入
// 早于窗口起点的文件整文件跳过（事件 timestamp 与 mtime 同源，mtime ≥
// 任一事件 timestamp，故跳过必然无窗口内事件）。
func scanUsageFiles(ctx context.Context, paths []string, rep *UsageReport, from, to int64) {
	if len(paths) == 0 {
		return
	}
	if from > 0 {
		kept := make([]string, 0, len(paths))
		for _, p := range paths {
			if st, err := os.Stat(p); err == nil && st.ModTime().Unix() < from {
				continue
			}
			kept = append(kept, p)
		}
		paths = kept
		if len(paths) == 0 {
			return
		}
	}

	workers := runtime.NumCPU()
	if workers > len(paths) {
		workers = len(paths)
	}
	ch := make(chan string)
	locals := make([]*UsageReport, workers)
	var wg sync.WaitGroup
	for i := range locals {
		locals[i] = &UsageReport{ByModel: make(map[string]TokenUsageStat)}
		wg.Add(1)
		go func(local *UsageReport) {
			defer wg.Done()
			for p := range ch {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if lr, err := scanUsageFile(p, from, to); err == nil && lr.Total.Turns > 0 {
					mergeUsageReport(local, lr)
					local.Sessions++
				}
			}
		}(locals[i])
	}
	for _, p := range paths {
		select {
		case ch <- p:
		case <-ctx.Done():
			// Client went away mid-scan; stop feeding the workers instead
			// of blocking forever on the unbuffered channel (producer leak).
			close(ch)
			wg.Wait()
			return
		}
	}
	close(ch)
	wg.Wait()

	for _, l := range locals {
		mergeUsageReport(rep, l)
	}
}

// mergeUsageReport 把 src 的累计合并进 dst（worker 汇总 / 并行合并）。
func mergeUsageReport(dst, src *UsageReport) {
	dst.Sessions += src.Sessions
	addStat(&dst.Total, src.Total)
	for model, st := range src.ByModel {
		addStatToModel(dst, model, st)
	}
}

// addStat 把一个统计行累加进 dst。
func addStat(dst *TokenUsageStat, src TokenUsageStat) {
	dst.InputTokens += src.InputTokens
	dst.OutputTokens += src.OutputTokens
	dst.TotalTokens += src.TotalTokens
	dst.CachedReadTokens += src.CachedReadTokens
	dst.CacheCreationTokens += src.CacheCreationTokens
	dst.ReasoningTokens += src.ReasoningTokens
	dst.ModelCalls += src.ModelCalls
	dst.Turns += src.Turns
}

// addStatToModel 累加进 ByModel[model]（map 索引不可取地址，先取后写）。
func addStatToModel(rep *UsageReport, model string, src TokenUsageStat) {
	st := rep.ByModel[model]
	addStat(&st, src)
	rep.ByModel[model] = st
}

// scanUsageFile 扫描单个 updates.jsonl，返回该文件的独立聚合（调用方
// 合并）。流式逐行处理：bufio.Scanner + tag 子串预过滤，仅候选行做
// JSON 解析。rewind_marker 行不匹配 usage tag，流式直接跳过——rewind
// 死分支的回合照常计入（真实消耗过的 token，见文件头口径）。仅超长行
// （>64MB，实际几乎不可能）放弃流式结果，降级到 ReadFile 全扫路径。
// 文件缺失/损坏返回 err（调用方跳过，保持健壮）。
func scanUsageFile(path string, from, to int64) (*UsageReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	local := &UsageReport{ByModel: make(map[string]TokenUsageStat)}
	acc := &usageAccumulator{rep: local, from: from, to: to}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	for sc.Scan() {
		acc.line(sc.Bytes())
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		return scanUsageFileReadAll(path, from, to)
	}
	return local, nil
}

// scanUsageFileReadAll 超长行降级路径：ReadFile 后逐行聚合（无 rewind
// 过滤——全计语义，与流式路径结果一致）。
func scanUsageFileReadAll(path string, from, to int64) (*UsageReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	local := &UsageReport{ByModel: make(map[string]TokenUsageStat)}
	acc := &usageAccumulator{rep: local, from: from, to: to}
	for _, line := range strings.Split(string(raw), "\n") {
		acc.line([]byte(line))
	}
	return local, nil
}

// usageAccumulator 是单个文件的逐行聚合器（worker 本地，无锁）。
type usageAccumulator struct {
	rep  *UsageReport
	from int64
	to   int64
}

// line 处理一行存储信封：tag 预过滤后解析，命中窗口内的回合终态 usage
// 事件则累加进 rep。返回该行是否贡献了一个事件。
func (a *usageAccumulator) line(l []byte) bool {
	if !bytes.Contains(l, tagTurnCompletedB) && !bytes.Contains(l, tagResponseCompletedB) {
		return false
	}
	var env struct {
		Timestamp int64 `json:"timestamp"`
		Params    struct {
			Update map[string]json.RawMessage `json:"update"`
		} `json:"params"`
	}
	if json.Unmarshal(l, &env) != nil {
		return false
	}
	if env.Timestamp < a.from || env.Timestamp > a.to {
		return false
	}
	rawUsage, ok := env.Params.Update["usage"]
	if !ok {
		return false
	}
	var usage map[string]any
	if json.Unmarshal(rawUsage, &usage) != nil || len(usage) == 0 {
		return false
	}
	accumulateUsage(a.rep, usage)
	return true
}

// accumulateUsage 把一次回合终态 usage 累加进报告：顶层字段进 Total，
// modelUsage 逐模型进 ByModel（缺失/空 → 整体记 unknown；顶层与
// modelUsage 之和的差额补记 unknown）。
func accumulateUsage(rep *UsageReport, usage map[string]any) {
	in, out, tot, cr, cc, rk, mc := usageInts(usage)
	t := &rep.Total
	t.InputTokens += in
	t.OutputTokens += out
	t.TotalTokens += tot
	t.CachedReadTokens += cr
	t.CacheCreationTokens += cc
	t.ReasoningTokens += rk
	t.ModelCalls += mc
	t.Turns++

	mu, ok := usage["modelUsage"].(map[string]any)
	if !ok || len(mu) == 0 {
		addModelUsage(rep, unknownModel, in, out, tot, cr, cc, rk, mc, 1)
		return
	}
	var sumIn, sumTot int64
	for model, raw := range mu {
		mm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		mi, mo, mt, mcr, mcc, mrk, mmc := usageInts(mm)
		addModelUsage(rep, model, mi, mo, mt, mcr, mcc, mrk, mmc, 1)
		sumIn += mi
		sumTot += mt
	}
	// 顶层与 modelUsage 的差额（仅输入/总量可归因）补记 unknown。
	if diff := in - sumIn; diff > 0 {
		st := rep.ByModel[unknownModel]
		st.InputTokens += diff
		rep.ByModel[unknownModel] = st
	}
	if diff := tot - sumTot; diff > 0 {
		st := rep.ByModel[unknownModel]
		st.TotalTokens += diff
		rep.ByModel[unknownModel] = st
	}
}

// addModelUsage 把一次 usage 累加进 ByModel[model]（不存在则建条目）。
func addModelUsage(rep *UsageReport, model string, in, out, tot, cr, cc, rk, mc, turns int64) {
	st := rep.ByModel[model]
	st.InputTokens += in
	st.OutputTokens += out
	st.TotalTokens += tot
	st.CachedReadTokens += cr
	st.CacheCreationTokens += cc
	st.ReasoningTokens += rk
	st.ModelCalls += mc
	st.Turns += turns
	rep.ByModel[model] = st
}

// usageInts 提取 usage 对象的七个核心计数（camelCase 优先，兼容
// snake_case 老版本）。
func usageInts(u map[string]any) (in, out, tot, cr, cc, rk, mc int64) {
	in = usageInt(u, "inputTokens", "input_tokens")
	out = usageInt(u, "outputTokens", "output_tokens")
	tot = usageInt(u, "totalTokens", "total_tokens")
	cr = usageInt(u, "cachedReadTokens", "cached_read_tokens")
	cc = usageInt(u, "cacheCreationTokens", "cache_creation_tokens")
	rk = usageInt(u, "reasoningTokens", "reasoning_tokens")
	mc = usageInt(u, "modelCalls", "model_calls")
	return
}

// usageInt 按候选键顺序取第一个非零值。
func usageInt(u map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := asInt(u[k]); ok && v > 0 {
			return v
		}
	}
	return 0
}

// finalizeUsageStats 为总计与每个模型条目计算派生命中率。
func finalizeUsageStats(rep *UsageReport) {
	finalizeUsageStat(&rep.Total)
	for model := range rep.ByModel {
		st := rep.ByModel[model]
		finalizeUsageStat(&st)
		rep.ByModel[model] = st
	}
}

// finalizeUsageStat 计算命中率 = cachedReadTokens / inputTokens（钳制
// [0,1]；无输入时为 0）。
func finalizeUsageStat(s *TokenUsageStat) {
	if s.InputTokens > 0 {
		rate := float64(s.CachedReadTokens) / float64(s.InputTokens)
		if rate > 1 {
			rate = 1
		}
		s.CacheHitRate = rate
	}
}
