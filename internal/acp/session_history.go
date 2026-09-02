package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ── 会话历史归一化（msgSeq，契约 tmp/msgseq/CONTRACT.md）──────────────
//
// agent 按落盘顺序写 updates.jsonl，落盘顺序可能与真实顺序不一致（如被
// 取消回合的用户回声在 turn_completed 之后才落盘）。host 读盘后按自己的
// 规则给出会话内密集序号，不借助 eventId：
//
// 排序键（升序）：
//  1. agentTimestampMs（params._meta，事件真实时刻，epoch ms）；
//  2. 文件行号（同毫秒 tiebreak = 落盘行序）。
//
// msgSeq = 归一化后的名次，0 起、会话内密集。eventId 的 N 会跨进程归零、
// 会话内也会撞车，既不当排序键也不当去重键。
//
// 行解析：JSON 失败的行跳过（agent 正在追加时末行半截是常态）；缺
// agentTimestampMs 的信封同样跳过。跳过之后若没有任何带时间戳的信封 →
// 调用方退回 agent RPC 透传。只要还能给出本地视图，就不换坐标空间。

// errMissingAgentMeta 标记「没有任何带 params._meta.agentTimestampMs 的
// 存储信封」，触发调用方的 agent `_x.ai/session/updates` 透传回退（响应
// 无 msgSeq、promptStarts 原样）。
var errMissingAgentMeta = errors.New("存在缺失 _meta.agentTimestampMs 的存储信封")

// 轮次目录（用户消息目录）预览的截断上限（字节，UTF-8 安全）。FE 展示时
// 自己再按首行 80 字符截断（userMessagePreview），这里是线上体积的上限。
const tocPreviewMaxBytes = 512

// tocPreviewRunMaxBytes 单 chunk 文本 / 单 run 累计文本的本地截断（字节）。
// run 取首行前必须先拼完整（跨 chunk 拆分的 prompt，文本可能分布在多个
// chunk 里），拼的过程要有上限；首行通常远短于此，超长行再被
// tocPreviewMaxBytes 收口。
const tocPreviewRunMaxBytes = 4096

// updateLineMeta 是一行存储信封的行级元数据。缓存只存它，不缓存信封内容。
type updateLineMeta struct {
	line        int   // 信封在文件中的名次（0 起，按非空行计，含被跳过的坏行）
	agentTsMs   int64 // params._meta.agentTimestampMs
	isUserChunk bool  // update.sessionUpdate == "user_message_chunk" 且非 hostTurn
	// preview 是该 chunk 的正文文本（displayText 优先），仅 isUserChunk 时
	// 填充（截断到 tocPreviewRunMaxBytes，不取首行——run 累计后才取首行）。
	// 其余行恒为空串。供 promptPreviews（轮次目录）使用。
	preview string
	// 回退标记（update.sessionUpdate == "rewind_marker"）：agent 侧
	// updates.jsonl 是只追加的，/rewind 只落一条标记，被回退掉的那支
	// 「死分支」仍留在文件里。回放前必须按标记截断，否则 FE 会把用户
	// 已经回退掉的回合（含被取消的回合）重新画出来。
	isRewind        bool // update.sessionUpdate == "rewind_marker"
	rewindTarget    int  // update.target_prompt_index
	hasRewindTarget bool // target_prompt_index 存在且可解析
	// 用户 chunk 的 update._meta.promptIndex（agent 回合编号）。缺省时
	// hasPromptIndex=false，走 UserRunTurnTracker 的无标记规则。
	promptIndex    int
	hasPromptIndex bool
}

// normalizedHistory 是一个会话 updates.jsonl 的归一化视图（行级元数据）。
type normalizedHistory struct {
	// lines 为文件行序的元数据（仅含解析成功且带时间戳的行）；order 为
	// msgSeq → lines 下标（归一化序）。
	lines []updateLineMeta
	order []int
	// promptStarts：每个「新用户 prompt」首条 chunk 的 msgSeq（host 重算，
	// 规则见 promptStartsOf / userRunTurnTracker）。
	promptStarts []int
	// promptPreviews 与 promptStarts 平行：每个存活轮次的首行预览（尽力
	// 而为，多 chunk 累计、displayText 优先，见 turnIndexesOf）。
	promptPreviews []string
}

// parseUpdateLineMeta 解析一行存储信封的元数据；行不是合法 JSON →
// ok=false（跳过该行，不拖垮整会话）。
func parseUpdateLineMeta(line []byte) (updateLineMeta, bool) {
	var env struct {
		Params struct {
			Update struct {
				SessionUpdate     string          `json:"sessionUpdate"`
				TargetPromptIndex *int64          `json:"target_prompt_index"`
				Content           json.RawMessage `json:"content"`
				Meta              *struct {
					PromptIndex *int64 `json:"promptIndex"`
					HostTurn    bool   `json:"hostTurn"`
				} `json:"_meta"`
			} `json:"update"`
			Meta struct {
				AgentTimestampMs int64 `json:"agentTimestampMs"`
			} `json:"_meta"`
		} `json:"params"`
	}
	var m updateLineMeta
	if json.Unmarshal(line, &env) != nil {
		return m, false
	}
	m.agentTsMs = env.Params.Meta.AgentTimestampMs
	hostTurn := env.Params.Update.Meta != nil && env.Params.Update.Meta.HostTurn
	m.isUserChunk = env.Params.Update.SessionUpdate == "user_message_chunk" && !hostTurn
	if m.isUserChunk {
		// 只有用户 chunk 行多解析一次 content（RawMessage 零拷贝引用，解析
		// 成本只发生在用户行）；工具行永远不碰 content。
		m.preview = userChunkText(env.Params.Update.Content)
		if env.Params.Update.Meta != nil && env.Params.Update.Meta.PromptIndex != nil {
			m.promptIndex = int(*env.Params.Update.Meta.PromptIndex)
			m.hasPromptIndex = true
		}
	}
	m.isRewind = env.Params.Update.SessionUpdate == "rewind_marker"
	if m.isRewind && env.Params.Update.TargetPromptIndex != nil {
		m.rewindTarget = int(*env.Params.Update.TargetPromptIndex)
		m.hasRewindTarget = true
	}
	return m, true
}

// scanUpdateLineMeta 逐行扫描 updates.jsonl 提取行级元数据（bufio.Scanner
// 流式，与 parseTaskEvents 同款；单行超过 maxUsageLineBytes 降级 ReadFile
// 全扫）。空行不计入行名次。JSON 解析失败的行跳过（保留行名次，供
// readEnvelopesByRank 对账），末行半截因此不会把整会话打去透传。
func scanUpdateLineMeta(path string) ([]updateLineMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	var meta []updateLineMeta
	rank := 0
	skipped := 0
	lastSkipped := false
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		m, ok := parseUpdateLineMeta(line)
		if !ok {
			skipped++
			lastSkipped = true
			rank++
			continue
		}
		lastSkipped = false
		m.line = rank
		meta = append(meta, m)
		rank++
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		return scanUpdateLineMetaReadAll(path)
	}
	logSkippedJSONL(path, skipped, lastSkipped)
	return meta, nil
}

func logSkippedJSONL(path string, skipped int, lastSkipped bool) {
	if skipped == 0 {
		return
	}
	if lastSkipped && skipped == 1 {
		// 典型：agent 正在追加，末行半截。不打日志。
		return
	}
	log.Printf("[capri-host] session history skipped %d unparseable jsonl line(s) in %s (tailIncomplete=%v)", skipped, path, lastSkipped)
}

// scanUpdateLineMetaReadAll 是 scanUpdateLineMeta 的整文件兜底（仅当单行
// 超过 maxUsageLineBytes 时到达）。
func scanUpdateLineMetaReadAll(path string) ([]updateLineMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var meta []updateLineMeta
	rank := 0
	skipped := 0
	lastSkipped := false
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		m, ok := parseUpdateLineMeta([]byte(line))
		if !ok {
			skipped++
			lastSkipped = true
			rank++
			continue
		}
		lastSkipped = false
		m.line = rank
		meta = append(meta, m)
		rank++
	}
	logSkippedJSONL(path, skipped, lastSkipped)
	return meta, nil
}

// buildNormalizedHistory 归一化一个会话的 updates.jsonl（排序 + 分配密集
// msgSeq + 重算 promptStarts）。
func buildNormalizedHistory(path string) (*normalizedHistory, error) {
	meta, err := scanUpdateLineMeta(path)
	if err != nil {
		return nil, err
	}
	kept := meta[:0]
	for _, m := range meta {
		if m.agentTsMs > 0 {
			kept = append(kept, m)
		}
	}
	if len(kept) == 0 {
		if len(meta) == 0 {
			// 空文件 / 只有半行：给空本地视图，不换空间。
			return &normalizedHistory{}, nil
		}
		return nil, errMissingAgentMeta
	}
	if nskip := len(meta) - len(kept); nskip > 0 {
		log.Printf("[capri-host] session history skipped %d envelope(s) missing agentTimestampMs in %s", nskip, path)
	}
	meta = kept
	view := &normalizedHistory{lines: meta}
	view.order = make([]int, len(meta))
	for i := range view.order {
		view.order[i] = i
	}
	sort.SliceStable(view.order, func(a, b int) bool {
		x, y := &meta[view.order[a]], &meta[view.order[b]]
		if x.agentTsMs != y.agentTsMs {
			return x.agentTsMs < y.agentTsMs
		}
		return x.line < y.line
	})
	// 回退死分支截断（在归一化序上做，msgSeq 因此是「存活序列」的名次）。
	if filtered, hadRewind := filterRewindBranch(view.order, meta); hadRewind {
		view.order = filtered
	}
	view.promptStarts, view.promptPreviews = turnIndexesOf(view)
	return view, nil
}

// userRunTurnTracker 是 agent UserRunTurnTracker 的 Go 等价实现（
// xai-grok-shell session/storage/mod.rs）。连续 user_message_chunk 算同一
// run；非 user 结束 run；promptIndex 变化（含有标记 ↔ 无标记）开新 run；
// 见过第一个 promptIndex 之后，无标记 run 不计回合。hostTurn 注入在解析
// 阶段就不当 isUserChunk，走到 onNonUser。
type userRunTurnTracker struct {
	seenMarker   bool
	inUser       bool
	currentRunPI int
	hasCurrentPI bool
}

// onUserChunk 报告本 chunk 是否打开新 user run，以及该 run 是否计入回合。
// newRun&&counts → 新的计数回合；newRun&&!counts → 见过标记后的幽灵 run
// （不计 promptStarts，FE 也不画用户行）；!newRun → 同一 run 的后续 chunk。
func (t *userRunTurnTracker) onUserChunk(hasPI bool, pi int) (newRun, counts bool) {
	if hasPI {
		t.seenMarker = true
	}
	counts = !t.seenMarker || hasPI
	newRun = !t.inUser
	if t.inUser && (t.seenMarker || hasPI) {
		newRun = hasPI != t.hasCurrentPI || (hasPI && pi != t.currentRunPI)
	}
	if newRun {
		t.hasCurrentPI = hasPI
		t.currentRunPI = pi
	}
	t.inUser = true
	return newRun, counts
}

func (t *userRunTurnTracker) onNonUser() {
	t.inUser = false
	t.hasCurrentPI = false
}

// filterRewindBranch 按 rewind_marker 截断归一化序上的「死分支」，是 agent
// 侧 session::storage::filter_rewind_by 的 Go 等价实现（agent 1.0.13 的
// x.ai/session/updates 服务路径不做这层过滤，host 本地服务必须补上，否则
// 回放会把已回退的回合重新画出来）。
//
// 规则与 agent 一致：顺序扫描存活序列，记录每个「新用户 prompt」在结果里的
// 起点（userRunTurnTracker）；遇到 rewind_marker{target} 就把结果截回到第
// target 个 prompt 的起点（即丢掉 target 及其后的全部内容），prompt 起点表
// 同步截断；标记本身不进结果。target 越界（多分支历史里文件序 prompt 与
// 逻辑 prompt 不再一一对应）时不截断——与 agent 的 fold-to-len(result) 兜底
// 一致，宁多留不误删。
//
// 判定 rewind_marker 只认严格解析出的 sessionUpdate 字段，不靠子串匹配
// （工具输出正文里出现 "rewind_marker" 不得当成标记）。
func filterRewindBranch(order []int, lines []updateLineMeta) ([]int, bool) {
	hasRewind := false
	for i := range lines {
		if lines[i].isRewind {
			hasRewind = true
			break
		}
	}
	if !hasRewind {
		return order, false
	}
	out := make([]int, 0, len(order))
	var promptStarts []int
	var tracker userRunTurnTracker
	for _, idx := range order {
		m := &lines[idx]
		if m.isRewind {
			if m.hasRewindTarget && m.rewindTarget < len(promptStarts) {
				out = out[:promptStarts[m.rewindTarget]]
				promptStarts = promptStarts[:m.rewindTarget]
			}
			tracker.onNonUser()
			continue
		}
		if m.isUserChunk {
			if newRun, counts := tracker.onUserChunk(m.hasPromptIndex, m.promptIndex); newRun && counts {
				promptStarts = append(promptStarts, len(out))
			}
		} else {
			tracker.onNonUser()
		}
		out = append(out, idx)
	}
	return out, true
}

// promptStartsOf 重算「新用户 prompt」起点（契约：host 重算，不再透传
// agent 的文件行号）。规则与 filterRewindBranch / agent UserRunTurnTracker
// 相同：连续 user run + promptIndex 优先；值为 msgSeq。
func promptStartsOf(v *normalizedHistory) []int {
	starts, _ := turnIndexesOf(v)
	return starts
}

// turnIndexesOf 一次遍历归一化序同时产出：
//   starts   — 每个「新用户 prompt」首条 chunk 的 msgSeq（与 promptStartsOf
//              完全同一条 UserRunTurnTracker 规则，保证两者永不脱节）；
//   previews — 与 starts 平行的首行预览（轮次目录）：该 run 全部 user chunk
//              正文拼接（displayText 优先，见 userChunkText）后的第一个非空
//              行，截断到 tocPreviewMaxBytes（UTF-8 安全）。
//
// 预览是尽力而为的展示元数据：多 chunk 拆分取拼接首行；空 run / 图块 run
// 回退空串（FE 按自身隐藏规则过滤，host 保持内容无关）。
func turnIndexesOf(v *normalizedHistory) (starts []int, previews []string) {
	var tracker userRunTurnTracker
	var runText strings.Builder
	inRun := false
	flushRun := func() {
		if !inRun {
			return
		}
		previews = append(previews, firstNonEmptyLine(runText.String()))
		runText.Reset()
		inRun = false
	}
	for seq, idx := range v.order {
		m := &v.lines[idx]
		if !m.isUserChunk {
			tracker.onNonUser()
			flushRun()
			continue
		}
		if newRun, counts := tracker.onUserChunk(m.hasPromptIndex, m.promptIndex); newRun {
			flushRun()
			if counts {
				starts = append(starts, seq)
				inRun = true
			}
		}
		if inRun && runText.Len() < tocPreviewRunMaxBytes {
			runText.WriteString(m.preview)
		}
	}
	flushRun()
	return starts, previews
}

// userChunkText 提取一条 user_message_chunk 的正文文本（与 FE
// contentParts / envelopeContentMeta 对齐）：content 为文本块数组、
// {type:"text", text} / {content:...} 嵌套对象或裸字符串；content 是对象时
// _meta.displayText（cron/shell 的可见文案）优先。结果截断到
// tocPreviewRunMaxBytes（不取首行——跨 chunk 拆分由 turnIndexesOf 累计）。
func userChunkText(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var obj map[string]any
	if json.Unmarshal(raw, &obj) == nil && obj != nil {
		if dt, ok := blockDisplayText(obj); ok {
			return truncateRunes(dt, tocPreviewRunMaxBytes)
		}
		var b strings.Builder
		appendChunkText(&b, obj, tocPreviewRunMaxBytes)
		return truncateRunes(b.String(), tocPreviewRunMaxBytes)
	}
	var arr []any
	if json.Unmarshal(raw, &arr) == nil {
		var b strings.Builder
		appendChunkText(&b, arr, tocPreviewRunMaxBytes)
		return truncateRunes(b.String(), tocPreviewRunMaxBytes)
	}
	return ""
}

// blockDisplayText 读 content 对象上的 displayText（_meta 或 meta）。
func blockDisplayText(obj map[string]any) (string, bool) {
	for _, key := range []string{"_meta", "meta"} {
		if meta, ok := obj[key].(map[string]any); ok {
			if dt, ok := meta["displayText"].(string); ok && dt != "" {
				return dt, true
			}
		}
	}
	return "", false
}

// appendChunkText 把 content 节点里的文本追加进 b，语义与 FE 的
// contentParts 文本提取对齐：字符串、{text}、{content} 嵌套递归；
// image 块（{type:"image", data}）不进文本。
func appendChunkText(b *strings.Builder, v any, budget int) {
	if b.Len() >= budget {
		return
	}
	switch t := v.(type) {
	case string:
		b.WriteString(t)
	case []any:
		for _, item := range t {
			appendChunkText(b, item, budget)
		}
	case map[string]any:
		if typ, _ := t["type"].(string); typ == "image" {
			if _, ok := t["data"].(string); ok {
				return
			}
		}
		if s, ok := t["text"].(string); ok {
			b.WriteString(s)
			return
		}
		if inner, ok := t["content"]; ok {
			appendChunkText(b, inner, budget)
		}
	}
}

// firstNonEmptyLine 取首个非空行（trim 后逐行判断，与 FE userMessagePreview
// 同规则），超长按 tocPreviewMaxBytes 截断（UTF-8 安全）。空文本回退空串。
func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return truncateRunes(line, tocPreviewMaxBytes)
		}
	}
	return ""
}

// truncateRunes 把 s 截到最多 maxBytes 字节，按 rune 边界回退、绝不切出半
// 个 UTF-8 字符（否则 FE 侧 JSON 里出现替换符）。
func truncateRunes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := s[:maxBytes]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}

// sessionLineView 返回 rewind 截断后的行视图。优先走时间戳归一化（与
// msgSeq 同一套）；全部信封缺时间戳时退回文件序，让旧日志的任务时间线
// / 状态条仍能按 tracker 截死分支。
func sessionLineView(path string) (*normalizedHistory, error) {
	view, err := buildNormalizedHistory(path)
	if err == nil {
		return view, nil
	}
	if !errors.Is(err, errMissingAgentMeta) {
		return nil, err
	}
	meta, err := scanUpdateLineMeta(path)
	if err != nil {
		return nil, err
	}
	order := make([]int, len(meta))
	for i := range order {
		order[i] = i
	}
	if filtered, had := filterRewindBranch(order, meta); had {
		order = filtered
	}
	view = &normalizedHistory{lines: meta, order: order}
	view.promptStarts, view.promptPreviews = turnIndexesOf(view)
	return view, nil
}

func survivingFileRanks(view *normalizedHistory) map[int]bool {
	want := make(map[int]bool, len(view.order))
	for _, idx := range view.order {
		want[view.lines[idx].line] = true
	}
	return want
}

// rewindFilteredFileOrder 保留缺时间戳的信封（如无 _meta 的 turn_completed
// usage），按文件序做 rewind 截断。状态条的 token/工具耗时走这一套，避免
// 把「有 ts 的 chunk + 无 ts 的终态」拆丢；回合数仍用 sessionLineView。
func rewindFilteredFileOrder(path string) (*normalizedHistory, error) {
	meta, err := scanUpdateLineMeta(path)
	if err != nil {
		return nil, err
	}
	order := make([]int, len(meta))
	for i := range order {
		order[i] = i
	}
	if filtered, had := filterRewindBranch(order, meta); had {
		order = filtered
	}
	view := &normalizedHistory{lines: meta, order: order}
	view.promptStarts, view.promptPreviews = turnIndexesOf(view)
	return view, nil
}

// ── 归一化视图缓存 ──────────────────────────────────────────────────────

// historyCacheEntry 按 (size, mtime) 失效的缓存条目；只存行级元数据，
// 不缓存信封内容（信封每次按窗口从文件现读）。
type historyCacheEntry struct {
	size    int64
	modTime time.Time
	view    *normalizedHistory
}

// historyCache 缓存会话 updates.jsonl 的归一化视图。Bridge 持有一个
// 实例，缓存锁纪律收敛在本类型内。
type historyCache struct {
	mu      sync.Mutex
	entries map[string]*historyCacheEntry
}

// get 命中返回视图；未命中或 (size, mtime) 已变化返回 false。
func (hc *historyCache) get(path string, size int64, mod time.Time) (*normalizedHistory, bool) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if ent, ok := hc.entries[path]; ok && ent.size == size && ent.modTime.Equal(mod) {
		return ent.view, true
	}
	return nil, false
}

func (hc *historyCache) put(path string, size int64, mod time.Time, view *normalizedHistory) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.entries == nil {
		hc.entries = make(map[string]*historyCacheEntry)
	}
	hc.entries[path] = &historyCacheEntry{size: size, modTime: mod, view: view}
}

// normalizedSessionHistory 返回该会话 updates.jsonl 的归一化视图（带
// (size, mtime) 缓存）。ok=false = 触发回退（文件不可读 / 没有任何带
// agentTimestampMs 的信封），调用方走 agent RPC 透传。
func (b *Bridge) normalizedSessionHistory(path string) (*normalizedHistory, bool) {
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return nil, false
	}
	size, mod := st.Size(), st.ModTime()

	if view, ok := b.hist.get(path, size, mod); ok {
		return view, true
	}

	view, err := buildNormalizedHistory(path)
	if err != nil {
		return nil, false
	}

	b.hist.put(path, size, mod, view)
	return view, true
}

// ── 本地分页服务（msgSeq 空间）─────────────────────────────────────────

// errLocalHistoryUnavailable：本地归一化不可用（无文件 / 无时间戳），
// 调用方走 agent RPC 透传。errLocalHistoryRead：已经在 msgSeq 空间里，
// 窗口读失败——返回错误，不得透传换空间。
var (
	errLocalHistoryUnavailable = errors.New("local session history unavailable")
	errLocalHistoryRead        = errors.New("local session history window read failed")
)

// sessionBtwFile 返回会话的 btw_history.jsonl 路径（/btw 侧问日志，agent
// 的 storage::append_btw 写在 updates.jsonl 同目录）。
func sessionBtwFile(grokHome, cwd, sessionID string) string {
	dir := sessionsCwdDir(grokHome, cwd)
	if dir == "" || sessionID == "" {
		return ""
	}
	return filepath.Join(dir, sessionID, "btw_history.jsonl")
}

// btwHistoryRecord 是 btw_history.jsonl 的一行（BtwEntry，camelCase）。
type btwHistoryRecord struct {
	BtwSessionId string `json:"btwSessionId"`
	Question     string `json:"question"`
	Answer       string `json:"answer"`
	Model        string `json:"model"`
	Success      bool   `json:"success"`
	Error        string `json:"error"`
	AskedAt      string `json:"askedAt"`
}

// btwAnchorSeq 返回 askedMs 之前最近一条归一化信封的 msgSeq（排序键
// agentTimestampMs ≤ askedMs 的最大序号）；早于全部信封 → -1。view.order
// 按 (agentTsMs, 行号) 升序，直接二分。
func btwAnchorSeq(view *normalizedHistory, askedMs int64) int {
	return sort.Search(len(view.order), func(i int) bool {
		return view.lines[view.order[i]].agentTsMs > askedMs
	}) - 1
}

// btwWindowRecords 读出本地 btw 侧问记录并换算锚点，按分页窗口 [start, end)
// 切片（窗口互斥划分，锚点落在哪个窗口就由哪页携带；置顶锚点归最老已
// 加载页）。文件不存在/解析失败 → 无记录（不报错；agent 透传路径本就
// 无该文件）。
func btwWindowRecords(grokHome, cwd, sessionID string, view *normalizedHistory, start, end int) []SessionBtw {
	path := sessionBtwFile(grokHome, cwd, sessionID)
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []SessionBtw
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var rec btwHistoryRecord
		if json.Unmarshal(line, &rec) != nil {
			continue
		}
		if rec.AskedAt == "" {
			continue
		}
		asked, err := time.Parse(time.RFC3339Nano, rec.AskedAt)
		if err != nil {
			continue
		}
		askedMs := asked.UnixMilli()
		anchor := btwAnchorSeq(view, askedMs)
		// 窗口归属：锚点落在 [start, end) 内；早于一切信封的记录只在
		// 最老已加载页（start=0）携带。窗口互斥 → 跨页加载恰出现一次。
		if !(anchor >= start && anchor < end) && !(anchor < 0 && start == 0) {
			continue
		}
		out = append(out, SessionBtw{
			BtwSessionId: rec.BtwSessionId,
			AskedAt:      askedMs,
			Question:     rec.Question,
			Answer:       rec.Answer,
			Err:          rec.Error,
			Success:      rec.Success,
			Model:        rec.Model,
			AfterMsgSeq:  anchor,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].AfterMsgSeq != out[j].AfterMsgSeq {
			return out[i].AfterMsgSeq < out[j].AfterMsgSeq
		}
		return out[i].AskedAt < out[j].AskedAt
	})
	return out
}

// localUpdatesPage 在本地归一化可用时从 msgSeq 空间分页服务。分页语义
// （契约）：turnIndex/offset/limit/promptStarts 一律在 msgSeq 空间解释——
// turnIndex:N = 归一化序列的最后 N 个 prompt 轮；offset/limit 切
// [offset, offset+limit) 窗口，负 offset 按 agent 的 wire 语义解释为尾部
// 起点（-N = 倒数第 N 条起）（turnIndex 与 offset/limit 同给时 turnIndex
// 优先）。msgSeq 空间即「回退死分支被截断后的存活序列」（见
// filterRewindBranch），totalCount / promptStarts 同处该空间。每条 update
// 顶层带 msgSeq；promptStarts 由 host 重算；totalCount = 存活条数。
// opts.Detail == meta 时只算窗口与锚点、不读信封（见 lite.go）。
func (b *Bridge) localUpdatesPage(sessionID, cwd string, opts SessionUpdatesOpts) (UpdatesPage, error) {
	path := sessionUpdatesFile(b.grokHome(), cwd, sessionID)
	if path == "" {
		return UpdatesPage{}, errLocalHistoryUnavailable
	}
	view, ok := b.normalizedSessionHistory(path)
	if !ok {
		return UpdatesPage{}, errLocalHistoryUnavailable
	}
	total := len(view.order)
	start, end := 0, total
	switch {
	case opts.TurnIndex != nil:
		n := *opts.TurnIndex
		ps := view.promptStarts
		// 最后 0 轮 / 无用户回合 → 空页（与「按回合数取尾」的数学一致）。
		if n <= 0 || len(ps) == 0 {
			start, end = total, total
		} else {
			if n > len(ps) {
				n = len(ps)
			}
			start = ps[len(ps)-n]
		}
	case opts.Offset != nil || opts.Limit != nil:
		if opts.Offset != nil {
			// agent 的 wire 语义：负 offset = 尾部窗口起点（offset=-N 表示
			// 「从倒数第 N 条开始」），正 offset = 从头数。FE 的子代理时间线
			// 分页只认这一套（先 -100 取最新一页，再 -(已加载+100) 往前翻）；
			// 把负值当 0 会让每一页都返回同一份最早的内容 → 时间线重复显示。
			if *opts.Offset < 0 {
				start = total + int(*opts.Offset)
				if start < 0 {
					start = 0
				}
			} else {
				start = int(*opts.Offset) // wire offset 是 int64，msgSeq 空间为 int
				if start > total {
					start = total
				}
			}
		}
		end = total
		if opts.Limit != nil {
			end = start
			if *opts.Limit > 0 {
				end = start + *opts.Limit
			}
			if end > total {
				end = total
			}
		}
	}

	// 只从文件现读窗口内的信封行（缓存只存元数据）：msgSeq → 行名次。
	// meta 档（契约 [A]）只要锚点，窗口与计数已在 msgSeq 空间算出，一行
	// 信封都不必读盘。
	var updates []any
	if opts.Detail != DetailMeta {
		want := make(map[int]bool, end-start)
		for seq := start; seq < end; seq++ {
			want[view.lines[view.order[seq]].line] = true
		}
		envs, err := readEnvelopesByRank(path, want)
		if err != nil {
			return UpdatesPage{}, fmt.Errorf("%w: %v", errLocalHistoryRead, err)
		}
		updates = make([]any, 0, end-start)
		for seq := start; seq < end; seq++ {
			env := envs[view.lines[view.order[seq]].line]
			obj, ok := env.(map[string]any)
			if !ok {
				// 窗口内有行读不出来（文件在 stat 与读取之间被截断等）。
				// 已经在 msgSeq 空间：报错，绝不透传换空间。
				return UpdatesPage{}, fmt.Errorf("%w: missing envelope at msgSeq %d", errLocalHistoryRead, seq)
			}
			obj["msgSeq"] = seq
			updates = append(updates, obj)
		}
	}
	return UpdatesPage{
		Updates:        updates,
		TotalCount:     total,
		HasMore:        end < total,
		PromptStarts:   view.promptStarts,
		PromptPreviews: view.promptPreviews,
		Btw:            btwWindowRecords(b.grokHome(), cwd, sessionID, view, start, end),
	}, nil
}

// readEnvelopesByRank 读出文件里行名次命中 want 的存储信封（JSON 对象），
// 以 行名次 → 信封 返回；缺行时调用方据 nil 判断回退。流式扫描 + 超长
// 行整文件兜底，与 scanUpdateLineMeta 同款。
func readEnvelopesByRank(path string, want map[int]bool) (map[int]any, error) {
	out := make(map[int]any, len(want))
	collect := func(line []byte, rank int) {
		if !want[rank] {
			return
		}
		var env any
		if json.Unmarshal(line, &env) == nil {
			out[rank] = env
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rank := 0
	tooLong := false
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		collect(line, rank)
		rank++
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		tooLong = true
	}
	if tooLong {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		out = make(map[int]any, len(want))
		rank = 0
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			collect([]byte(line), rank)
			rank++
		}
	}
	return out, nil
}
