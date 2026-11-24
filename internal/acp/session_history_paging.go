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
	"time"
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

// safeSessionID 判断 sessionId 能否安全参与路径拼接（空、分隔符、
// `..` 一律拒绝，绝不允许穿越到 sessions 目录之外）。
func safeSessionID(sessionID string) bool {
	return sessionID != "" &&
		!strings.ContainsAny(sessionID, `/\`) &&
		!strings.Contains(sessionID, "..")
}

// resolveSessionUpdatesFile 解析会话 updates.jsonl 的实际位置，不依赖
// 调用方给的 cwd 一定属于该会话。先按 cwd 拼直接路径：存在（且不是
// 目录）就原样返回，不做任何扫描——这是绝大多数调用的热路径。子代理
// 会话的 cwd 与父会话不同时，FE 只能传父 cwd，直接路径落空，此时按
// sessionId 在 sessions/ 的一级子目录（cwd 编码目录）里查
// <sessionId>/updates.jsonl；畸形 sessionId 会让 Join 越界，先整体拒绝。
// 命中多份（session id 理论上唯一）取 mtime 最新的一份并记录所用路径。
func resolveSessionUpdatesFile(grokHome, cwd, sessionID string) (string, bool) {
	if grokHome == "" || !safeSessionID(sessionID) {
		return "", false
	}
	if path := sessionUpdatesFile(grokHome, cwd, sessionID); path != "" {
		// 打开一次确认可读：stat 成功但无读权限同样算不可用。
		if f, err := os.Open(path); err == nil {
			st, statErr := f.Stat()
			f.Close()
			if statErr == nil && !st.IsDir() {
				return path, true
			}
		}
	}
	root := filepath.Join(grokHome, "sessions")
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", false
	}
	best, bestMod, matches := "", time.Time{}, 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		candidate := filepath.Join(root, e.Name(), sessionID, "updates.jsonl")
		st, err := os.Stat(candidate)
		if err != nil || st.IsDir() {
			continue
		}
		matches++
		if best == "" || st.ModTime().After(bestMod) {
			best, bestMod = candidate, st.ModTime()
		}
	}
	if best == "" {
		return "", false
	}
	log.Printf("[Capri-host] session updates resolved by session id (matches=%d, cwd=%q): %s", matches, cwd, best)
	return best, true
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
	path, found := resolveSessionUpdatesFile(b.grokHome(), cwd, sessionID)
	if !found {
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
			if synthID, ok := view.synthToolCallIDs[seq]; ok {
				injectSynthToolCallID(obj, synthID)
			}
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
