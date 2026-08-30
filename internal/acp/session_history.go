package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── 会话历史归一化（msgSeq，契约 tmp/msgseq/CONTRACT.md）──────────────
//
// agent 按落盘顺序写 updates.jsonl，落盘顺序可能与真实顺序不一致（如被
// 取消回合的用户回声在 turn_completed 之后才落盘）。实测存储信封自带的
// params._meta.agentTimestampMs + eventId 足以在 host 侧重建真实顺序，
// 无需改 agent。
//
// 排序键（升序）：
//  1. agentTimestampMs（params._meta，事件真实时刻，epoch ms）；
//  2. N（eventId 去掉 "{sessionId}-" 前缀后的整数——agent 进程级全局
//     计数，跨会话共享、重启归零，只能做同世代内的顺序细化）；
//  3. 文件行号（稳定 tiebreak）。
//
// msgSeq = 归一化后的名次，0 起、会话内密集；归一化只重排、不增减信封。
// 前置条件：该会话全部信封携带 agentTimestampMs——任一缺失（含行解析
// 失败）→ 调用方整体退回 agent RPC 透传（wire 契约回退路径）。全部携带
// 但任一 N 解析失败/缺失 → 整会话退化为文件行号序（msgSeq = 行号名次），
// 不做混合排序。

// errMissingAgentMeta 标记「有信封缺 params._meta.agentTimestampMs」，
// 触发调用方的 agent `_x.ai/session/updates` 透传回退（响应无 msgSeq、
// promptStarts 原样）。
var errMissingAgentMeta = errors.New("存在缺失 _meta.agentTimestampMs 的存储信封")

// updateLineMeta 是一行存储信封的行级元数据。缓存只存它，不缓存信封内容。
type updateLineMeta struct {
	line        int   // 信封在文件中的名次（0 起，按非空行计）
	agentTsMs   int64 // params._meta.agentTimestampMs；缺失为 0
	eventN      int64 // eventId 去掉 "{sessionId}-" 前缀后的整数
	hasEventN   bool  // eventId 存在且 N 可解析
	isUserChunk bool  // update.sessionUpdate == "user_message_chunk"
}

// normalizedHistory 是一个会话 updates.jsonl 的归一化视图（行级元数据）。
type normalizedHistory struct {
	// lines 为文件行序的元数据；order 为 msgSeq → lines 下标（归一化序）。
	lines []updateLineMeta
	order []int
	// promptStarts：每个「新用户 prompt」首条 chunk 的 msgSeq（host 重算，
	// 规则见 promptStartsOf）。
	promptStarts []int
	// degraded：true = 有 N 缺失/解析失败 → 行号序（msgSeq = 行号名次）。
	degraded bool
}

// parseUpdateLineMeta 解析一行存储信封的元数据；行不是合法 JSON →
// ok=false（按缺失 agentTimestampMs 处理，走透传回退）。
func parseUpdateLineMeta(line []byte) (updateLineMeta, bool) {
	var env struct {
		Params struct {
			SessionID string `json:"sessionId"`
			Update    struct {
				SessionUpdate string `json:"sessionUpdate"`
			} `json:"update"`
			Meta struct {
				AgentTimestampMs int64  `json:"agentTimestampMs"`
				EventID          string `json:"eventId"`
			} `json:"_meta"`
		} `json:"params"`
	}
	var m updateLineMeta
	if json.Unmarshal(line, &env) != nil {
		return m, false
	}
	m.agentTsMs = env.Params.Meta.AgentTimestampMs
	m.isUserChunk = env.Params.Update.SessionUpdate == "user_message_chunk"
	if n, ok := eventIDNumber(env.Params.Meta.EventID, env.Params.SessionID); ok {
		m.eventN, m.hasEventN = n, true
	}
	return m, true
}

// eventIDNumber 从 "{sessionId}-{N}" 提取 N；前缀不匹配时按最后一个 '-'
// 兜底，解析失败 ok=false（契约：按缺失 → 退化行号序）。
func eventIDNumber(eventID, sessionID string) (int64, bool) {
	s := strings.TrimPrefix(eventID, sessionID+"-")
	if s == eventID {
		if i := strings.LastIndex(eventID, "-"); i >= 0 {
			s = eventID[i+1:]
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// scanUpdateLineMeta 逐行扫描 updates.jsonl 提取行级元数据（bufio.Scanner
// 流式，与 parseTaskEvents 同款；单行超过 maxUsageLineBytes 降级 ReadFile
// 全扫）。空行不计入行名次。
func scanUpdateLineMeta(path string) ([]updateLineMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	var meta []updateLineMeta
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		m, ok := parseUpdateLineMeta(line)
		if !ok {
			return nil, errMissingAgentMeta
		}
		m.line = len(meta)
		meta = append(meta, m)
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		return scanUpdateLineMetaReadAll(path)
	}
	return meta, nil
}

// scanUpdateLineMetaReadAll 是 scanUpdateLineMeta 的整文件兜底（仅当单行
// 超过 maxUsageLineBytes 时到达）。
func scanUpdateLineMetaReadAll(path string) ([]updateLineMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var meta []updateLineMeta
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		m, ok := parseUpdateLineMeta([]byte(line))
		if !ok {
			return nil, errMissingAgentMeta
		}
		m.line = len(meta)
		meta = append(meta, m)
	}
	return meta, nil
}

// buildNormalizedHistory 归一化一个会话的 updates.jsonl（排序 + 分配密集
// msgSeq + 重算 promptStarts）。
func buildNormalizedHistory(path string) (*normalizedHistory, error) {
	meta, err := scanUpdateLineMeta(path)
	if err != nil {
		return nil, err
	}
	view := &normalizedHistory{lines: meta}
	view.order = make([]int, len(meta))
	for i := range view.order {
		view.order[i] = i
	}
	for i := range meta {
		if meta[i].agentTsMs <= 0 {
			return nil, errMissingAgentMeta
		}
	}
	allN := true
	for i := range meta {
		if !meta[i].hasEventN {
			allN = false
			break
		}
	}
	if !allN {
		// 退化：行号序（msgSeq = 行号名次），不做混合排序。
		view.degraded = true
	} else {
		// 升序：agentTimestampMs → N → 文件行号（order 初值即行号序，
		// SliceStable 的稳定性即行号 tiebreak，显式比较仅为可读）。
		sort.SliceStable(view.order, func(a, b int) bool {
			x, y := &meta[view.order[a]], &meta[view.order[b]]
			if x.agentTsMs != y.agentTsMs {
				return x.agentTsMs < y.agentTsMs
			}
			if x.eventN != y.eventN {
				return x.eventN < y.eventN
			}
			return x.line < y.line
		})
	}
	view.promptStarts = promptStartsOf(view)
	return view, nil
}

// promptStartsOf 重算「新用户 prompt」起点（契约：host 重算，不再透传
// agent 的文件行号）：归一化序上 user_message_chunk 且其 agentTimestampMs
// ≠ 前一条 user_message_chunk 的 agentTimestampMs（与 bridge_ext_stats.go
// 的回合计数规则一致；多条 chunk 同一瞬间属同一条消息）。值为 msgSeq。
func promptStartsOf(v *normalizedHistory) []int {
	var ps []int
	var lastUserTs int64
	for seq, idx := range v.order {
		m := &v.lines[idx]
		if !m.isUserChunk {
			continue
		}
		if len(ps) == 0 || m.agentTsMs != lastUserTs {
			ps = append(ps, seq)
		}
		lastUserTs = m.agentTsMs
	}
	return ps
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
// (size, mtime) 缓存）。ok=false = 触发回退（文件不可读 / 任一信封缺
// agentTimestampMs），调用方走 agent RPC 透传。
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

// localUpdatesPage 在本地归一化可用时从 msgSeq 空间分页服务，ok=false
// 时调用方退回 agent RPC 透传。分页语义（契约）：turnIndex/offset/limit/
// promptStarts 一律在 msgSeq 空间解释——turnIndex:N = 归一化序列的最后
// N 个 prompt 轮；offset/limit 直接切 [offset, offset+limit) 窗口
// （turnIndex 与 offset/limit 同给时 turnIndex 优先）。每条 update 顶层
// 带 msgSeq；promptStarts 由 host 重算；totalCount = 归一化条数。
func (b *Bridge) localUpdatesPage(sessionID, cwd string, opts SessionUpdatesOpts) (UpdatesPage, bool) {
	path := sessionUpdatesFile(b.grokHome(), cwd, sessionID)
	if path == "" {
		return UpdatesPage{}, false
	}
	view, ok := b.normalizedSessionHistory(path)
	if !ok {
		return UpdatesPage{}, false
	}
	total := len(view.lines)
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
		if opts.Offset != nil && *opts.Offset > 0 {
			start = int(*opts.Offset) // wire offset 是 int64，msgSeq 空间为 int
		}
		if start > total {
			start = total
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
	want := make(map[int]bool, end-start)
	for seq := start; seq < end; seq++ {
		want[view.lines[view.order[seq]].line] = true
	}
	envs, err := readEnvelopesByRank(path, want)
	if err != nil {
		return UpdatesPage{}, false
	}
	updates := make([]any, 0, end-start)
	for seq := start; seq < end; seq++ {
		env := envs[view.lines[view.order[seq]].line]
		obj, ok := env.(map[string]any)
		if !ok {
			// 窗口内有行读不出来（文件在 stat 与读取之间被截断等）→
			// 回退透传，绝不吐半页。
			return UpdatesPage{}, false
		}
		obj["msgSeq"] = seq
		updates = append(updates, obj)
	}
	return UpdatesPage{
		Updates:      updates,
		TotalCount:   total,
		HasMore:      end < total,
		PromptStarts: view.promptStarts,
	}, true
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
