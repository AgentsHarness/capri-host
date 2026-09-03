package acp

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// ─────────────────────────────────────────────────────────────────────
// lite.go — /api/session-updates 的投影档位（契约 lite-replay [A][B]）。
//
// lite 是首屏时间线：折叠工具卡 + 折叠 thought 头 + 可见的 user/assistant
// 文本。full 由 FE 按窗口再拉，把正文填回原行。
//
// 投影做四件事：
//  1. 工具信封：删正文（content / 正文键 / file_matches），rawInput 只留
//     行头字段，command 留短头；
//  2. thought：正文换成 {type, omitted}，连续 thought 合成一封；
//  3. 同 toolCallId 的 tool_call + update 合成一封（空 id 不合，以免误并）；
//  4. params._meta 只留回放真用的时间戳/token。
//
// 条数因此可以少于 full；msgSeq / promptStarts / totalCount / hasMore
// 语义不变。被合成掉的信封把 msgSeqEnd 打在幸存者 _meta.lite 上，FE 按
// [msgSeq, msgSeqEnd] 回拉 full。
//
// omittedBytes = 被删正文 + 被合成掉的信封骨架（与 writeJSON 同口径）。
// ─────────────────────────────────────────────────────────────────────

const (
	DetailFull = "full"
	DetailLite = "lite"
	DetailMeta = "meta"
)

const (
	// liteRawOutputBudget：投影后单条 rawOutput 的尽力而为上限。
	liteRawOutputBudget = 2048
	// liteLongStringBytes：未知工具形状里多长的字符串算正文。
	liteLongStringBytes = 512
	// liteCapStringBytes / liteCapHeadBytes：行头 command 的封顶。
	// FE 折叠卡只看得到命令开头。
	liteCapStringBytes = 512
	liteCapHeadBytes   = 256
)

// liteBodyKeys：工具结果里的正文键，命中即整棵删除。
var liteBodyKeys = map[string]bool{
	"output":            true,
	"output_delta":      true,
	"output_for_prompt": true,
	"content":           true,
	"content_concise":   true,
	"data":              true,
	"stdout":            true,
	"stderr":            true,
	"new_string":        true,
	"old_string":        true,
	"plan_content":      true,
}

// liteKeepKeys：形状判别 / 行头 / id。命中则不按正文删（command 仍封顶）。
var liteKeepKeys = map[string]bool{
	"type":             true,
	"sessionUpdate":    true,
	"kind":             true,
	"status":           true,
	"title":            true,
	"locations":        true,
	"path":             true,
	"file_path":        true,
	"toolCallId":       true,
	"tool_call_id":     true,
	"sessionId":        true,
	"session_id":       true,
	"child_session_id": true,
	"messageId":        true,
	"command":          true,
	"name":             true,
	"label":            true,
	"mime_type":        true,
	"mimeType":         true,
	// 折叠行行头用到的计数：liteBudget 兜底裁剪也不许动，否则前端摘要会退化。
	"match_count":  true,
	"matchCount":   true,
	"result_count": true,
	"resultCount":  true,
	"status_code":  true,
	"statusCode":   true,
	"total_lines":  true,
	"totalLines":   true,
	"total_pages":  true,
	"totalPages":   true,
	"exit_code":    true,
	"exitCode":     true,
	"entry_count":  true,
	"entryCount":   true,
}

// liteHardKeepKeys：一字不动（command 不在此列，超长截头）。
var liteHardKeepKeys = map[string]bool{
	"type":             true,
	"sessionUpdate":    true,
	"kind":             true,
	"status":           true,
	"title":            true,
	"toolCallId":       true,
	"tool_call_id":     true,
	"sessionId":        true,
	"session_id":       true,
	"child_session_id": true,
	"messageId":        true,
	"name":             true,
	"label":            true,
	"mime_type":        true,
	"mimeType":         true,
}

// liteRawInputKeep：折叠卡行头用到的 rawInput 键，其余整键删除。
var liteRawInputKeep = map[string]bool{
	"path":             true,
	"file_path":        true,
	"filePath":         true,
	"target_file":      true,
	"target_directory": true,
	"command":          true,
	"pattern":          true,
	"glob":             true,
	"query":            true,
	"url":              true,
	"variant":          true,
	"offset":           true,
	"limit":            true,
	"head_limit":       true,
	"is_background":    true,
	"description":      true,
	"timeout":          true,
	"merge":            true,
	"type":             true,
	// use_tool 的折叠行行头是「Server + 动作名」，动作名只来自 tool_name。
	"tool_name": true,
}

// liteParamsMetaKeep：FE 回放真读的 params._meta 键。
var liteParamsMetaKeep = map[string]bool{
	"agentTimestampMs": true,
	"turnStartMs":      true,
	"streamStartMs":    true,
	"totalTokens":      true,
}

// liteUpdateMetaKeep：update._meta 里折叠卡 / 去重 / 补全标记需要的键。
var liteUpdateMetaKeep = map[string]bool{
	"lite":               true,
	"x.ai/tool":          true,
	"bash_mode":          true,
	"is_background":      true,
	"child_session_id":   true,
	"promptIndex":        true,
	"hostTurn":           true,
	"hideFromScrollback": true,
	"modelId":            true,
}

type liteAcc struct {
	omitted int
	// 折叠行行头要显示、但正文一删就算不出来的数字：edit 的加减行数、
	// grep 的命中文件数。投影时折出来打进 _meta.lite，前端在全量补全回来
	// 之前照旧能画出行头后缀；补全后 _meta.lite 被抹掉，改由真实正文计算。
	ins   int
	del   int
	files int
}

func (a *liteAcc) note(_ string, removed int) {
	if removed > 0 {
		a.omitted += removed
	}
}

func applyUpdatesDetail(page *UpdatesPage, o SessionUpdatesOpts) {
	switch o.Detail {
	case DetailLite:
		page.Projected = DetailLite
		page.OmittedBytes = liteProjectPage(&page.Updates)
	case DetailMeta:
		page.Projected = DetailMeta
		page.Updates = nil
	}
}

// liteProjectPage 就地投影并可能缩短切片（合成信封）。调用方给的是本次
// 请求现读的副本，不会污染归一化缓存。
func liteProjectPage(updates *[]any) int64 {
	if updates == nil || *updates == nil {
		return 0
	}
	var total int64
	for _, raw := range *updates {
		total += int64(liteProjectEnvelope(raw))
	}
	return total + liteCoalesce(updates)
}

// liteProjectEnvelope 投影单条信封。工具 / thought 裁正文，所有信封都
// 收 params._meta。已打 lite 标记的视为投影过（幂等）。
func liteProjectEnvelope(raw any) int {
	env, ok := raw.(map[string]any)
	if !ok {
		return 0
	}
	params, ok := env[kParams].(map[string]any)
	if !ok {
		return 0
	}
	var acc, metaAcc liteAcc
	liteStripMap(params[kMeta], liteParamsMetaKeep, "params._meta", &metaAcc)
	upd, ok := params[kUpdate].(map[string]any)
	if !ok {
		return acc.omitted + metaAcc.omitted
	}
	kind, _ := upd[kSessionUpdate].(string)
	if meta, ok := upd[kMeta].(map[string]any); ok {
		if _, done := meta["lite"]; done {
			return metaAcc.omitted
		}
	}
	switch kind {
	case "tool_call", "tool_call_update":
		liteProjectTool(upd, &acc)
	case "agent_thought_chunk":
		liteProjectThought(upd, &acc)
	}
	if acc.omitted > 0 && (kind == "tool_call" || kind == "tool_call_update" || kind == "agent_thought_chunk") {
		liteStamp(upd, acc.omitted, -1)
	}
	if kind == "tool_call" || kind == "tool_call_update" {
		liteStampFold(upd, &acc)
	}
	return acc.omitted + metaAcc.omitted
}

func liteProjectTool(upd map[string]any, acc *liteAcc) {
	liteFoldNumbers(upd, acc)
	if blocks, ok := upd[kContent].([]any); ok && len(blocks) > 0 {
		for _, b := range blocks {
			acc.note("content", liteJSONLen(b))
		}
		delete(upd, kContent)
	}
	if v, ok := upd["rawOutput"]; ok && v != nil {
		if s, isStr := v.(string); isStr {
			acc.note("rawOutput", liteJSONLen(s))
			delete(upd, "rawOutput")
		} else {
			liteTrimValue("rawOutput", "", v, acc)
			if ro := upd["rawOutput"]; ro != nil {
				liteBudget("rawOutput", ro, acc)
				if m, ok := ro.(map[string]any); ok && len(m) == 0 {
					delete(upd, "rawOutput")
				}
			}
		}
	}
	if v, ok := upd["rawInput"]; ok && v != nil {
		upd["rawInput"] = liteFilterRawInput(v, acc)
		if m, ok := upd["rawInput"].(map[string]any); ok && len(m) == 0 {
			delete(upd, "rawInput")
		}
	}
	liteStripMap(upd[kMeta], liteUpdateMetaKeep, "update._meta", acc)
}

func liteProjectThought(upd map[string]any, acc *liteAcc) {
	c := upd[kContent]
	if c == nil {
		return
	}
	size := liteJSONLen(c)
	stub := map[string]any{kType: "text", "omitted": size}
	if size <= liteJSONLen(stub) {
		return
	}
	upd[kContent] = stub
	acc.note("content", size)
}

func liteFilterRawInput(v any, acc *liteAcc) any {
	m, ok := v.(map[string]any)
	if !ok {
		if s, isStr := v.(string); isStr {
			nv, ch := liteCapKeepString("rawInput", s, acc)
			if ch {
				return nv
			}
		}
		return v
	}
	for k, item := range m {
		if !liteRawInputKeep[k] {
			acc.note("rawInput."+k, liteJSONLen(item))
			delete(m, k)
			continue
		}
		if s, isStr := item.(string); isStr && k == "command" {
			if nv, ch := liteCapKeepString("rawInput.command", s, acc); ch {
				m[k] = nv
			}
		}
	}
	return m
}

func liteStripMap(v any, keep map[string]bool, path string, acc *liteAcc) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	for k, item := range m {
		if keep[k] {
			continue
		}
		acc.note(path+"."+k, liteJSONLen(item))
		delete(m, k)
	}
}

func liteTrimValue(path, key string, v any, acc *liteAcc) (any, bool) {
	if key == "file_matches" {
		acc.note(path, liteJSONLen(v))
		return nil, true
	}
	if liteBodyKeys[key] {
		acc.note(path, liteJSONLen(v))
		return nil, true
	}
	if s, isStr := v.(string); isStr {
		if liteHardKeepKeys[key] {
			return v, false
		}
		if liteKeepKeys[key] {
			return liteCapKeepString(path, s, acc)
		}
		if liteJSONLen(s) < liteLongStringBytes {
			return v, false
		}
		acc.note(path, liteJSONLen(s))
		return nil, true
	}
	if liteKeepKeys[key] {
		return v, false
	}
	switch t := v.(type) {
	case map[string]any:
		changed := false
		for k, item := range t {
			nv, ch := liteTrimValue(path+"."+k, k, item, acc)
			if !ch {
				continue
			}
			changed = true
			if nv == nil {
				delete(t, k)
			} else {
				t[k] = nv
			}
		}
		return t, changed
	case []any:
		changed := false
		for i, item := range t {
			nv, ch := liteTrimValue(path+"[]", key, item, acc)
			if ch {
				t[i] = nv
				changed = true
			}
		}
		return t, changed
	}
	return v, false
}

func liteCapKeepString(path, s string, acc *liteAcc) (any, bool) {
	if len(s) <= liteCapStringBytes {
		return s, false
	}
	head := s
	if len(head) > liteCapHeadBytes {
		head = s[:liteCapHeadBytes]
	}
	for len(head) > 0 && !utf8.ValidString(head) {
		head = head[:len(head)-1]
	}
	removed := len(s) - len(head)
	acc.note(path, removed)
	return head + fmt.Sprintf("…[已省略 %d 字节]", removed), true
}

func liteBudget(path string, v any, acc *liteAcc) {
	size := liteJSONLen(v)
	if size <= liteRawOutputBudget {
		return
	}
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	type kv struct {
		k    string
		n    int
		keep bool
	}
	var keys []kv
	for k, item := range m {
		keys = append(keys, kv{k: k, n: liteJSONLen(item), keep: liteKeepKeys[k] || liteHardKeepKeys[k]})
	}
	// 非 keep 键按体积从大到小删，直到进预算。
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j].n > keys[i].n {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, it := range keys {
		if size <= liteRawOutputBudget {
			return
		}
		if it.keep {
			continue
		}
		acc.note(path+"."+it.k, it.n)
		delete(m, it.k)
		size -= it.n
	}
}

func liteStamp(upd map[string]any, omitted, msgSeqEnd int) {
	if omitted <= 0 && msgSeqEnd < 0 {
		return
	}
	stamp := liteStampMap(upd)
	prev := liteAsInt(stamp["omitted"])
	if omitted > 0 {
		stamp["omitted"] = prev + omitted
	} else if _, ok := stamp["omitted"]; !ok {
		stamp["omitted"] = prev
	}
	if msgSeqEnd >= 0 {
		if cur := liteAsInt(stamp["msgSeqEnd"]); msgSeqEnd > cur {
			stamp["msgSeqEnd"] = msgSeqEnd
		}
	}
}

func liteStampMap(upd map[string]any) map[string]any {
	meta, ok := upd[kMeta].(map[string]any)
	if !ok {
		meta = map[string]any{}
		upd[kMeta] = meta
	}
	stamp, _ := meta["lite"].(map[string]any)
	if stamp == nil {
		stamp = map[string]any{}
		meta["lite"] = stamp
	}
	return stamp
}

// ── 折叠行行头的数字折算 ──────────────────────────────────────────────
//
// 前端折叠卡的后缀（edit 的 (+N/−M)、grep 的 (N matches in M files)）原先
// 只能从正文算：lite 删掉 content / old_string / new_string / file_matches
// 之后，行头就空着，要等全量补回来才有。这里在删之前把数字折进
// _meta.lite.edits / _meta.lite.files，补全时标记被抹掉、改回由真实正文算，
// 两条路口径一致。

// liteStampFold 挂折算结果；标记本身的字节不计入 omitted。
func liteStampFold(upd map[string]any, acc *liteAcc) {
	if acc.ins == 0 && acc.del == 0 && acc.files == 0 {
		return
	}
	stamp := liteStampMap(upd)
	if acc.ins != 0 || acc.del != 0 {
		stamp["edits"] = map[string]any{"ins": acc.ins, "del": acc.del}
	}
	if acc.files > 0 {
		stamp["files"] = acc.files
	}
}

func liteFoldNumbers(upd map[string]any, acc *liteAcc) {
	// 失败的编辑前端按 error 渲染、行头不画加减行数，这里就不必折。
	if ins, del, ok := liteEditFold(upd); ok && !liteStatusFailed(upd) {
		acc.ins, acc.del = ins, del
	}
	if n, ok := liteFileFold(upd); ok {
		acc.files = n
	}
}

func liteStatusFailed(upd map[string]any) bool {
	s, _ := upd["status"].(string)
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "failed", "error":
		return true
	}
	return false
}

var (
	liteEditTagRx    = regexp.MustCompile(`(?i)search|replace|edit`)
	liteAppliedTagRx = regexp.MustCompile(`(?i)EditsApplied|applied`)
	liteGrepTagRx    = regexp.MustCompile(`(?i)grep|search`)
)

// liteEditFold 端口前端 extractEditHunks 的取数顺序：先认 rawOutput 里结构化的
// edits.details，认不到再看 content 的 diff 块。两处都没有则 ok=false。
func liteEditFold(upd map[string]any) (int, int, bool) {
	if details, ok := liteEditDetails(upd["rawOutput"]); ok {
		ins, del := 0, 0
		for _, item := range details {
			d, ok := item.(map[string]any)
			if !ok {
				continue
			}
			i, l := liteDiffLineCounts(
				liteFirstStr(d, "old_string", "oldString", "old_text"),
				liteFirstStr(d, "new_string", "newString", "new_text"),
			)
			ins += i
			del += l
		}
		return ins, del, true
	}
	blocks, ok := upd[kContent].([]any)
	if !ok {
		return 0, 0, false
	}
	ins, del, found := 0, 0, false
	for _, item := range blocks {
		b, ok := item.(map[string]any)
		if !ok || b[kType] != "diff" {
			continue
		}
		found = true
		i, l := liteDiffLineCounts(liteStr(b["oldText"]), liteStr(b["newText"]))
		ins += i
		del += l
	}
	return ins, del, found
}

// liteEditDetails 剥 rawOutput 的 Rust enum tag 取 edits.details。剥法与前端
// unwrapTagged 一致：单键 = 外部 tag，带 type / variant 键 = 内部 tag（body
// 仍是整包）。
func liteEditDetails(raw any) ([]any, bool) {
	body := raw
	if tag, inner := liteUnwrapTagged(raw); tag != "" && liteEditTagRx.MatchString(tag) {
		body = inner
	}
	if tag, inner := liteUnwrapTagged(body); tag != "" && liteAppliedTagRx.MatchString(tag) {
		body = inner
	}
	m, ok := body.(map[string]any)
	if !ok {
		return nil, false
	}
	src := m
	if inner, ok := m["edits"].(map[string]any); ok {
		src = inner
	}
	for _, cand := range []map[string]any{src, m} {
		if details, ok := cand["details"].([]any); ok && len(details) > 0 {
			for _, item := range details {
				if _, ok := item.(map[string]any); ok {
					return details, true
				}
			}
		}
	}
	return nil, false
}

// liteFileFold 取 grep 的命中文件数：file_matches 长度，退到 file_paths。
// 两者都被裁掉时不折算（前端也就无从显示文件数，宁缺不编）。
func liteFileFold(upd map[string]any) (int, bool) {
	ro := upd["rawOutput"]
	if tag, inner := liteUnwrapTagged(ro); tag != "" && liteGrepTagRx.MatchString(tag) {
		ro = inner
	}
	m, ok := ro.(map[string]any)
	if !ok {
		return 0, false
	}
	for _, key := range []string{"file_matches", "fileMatches", "file_paths", "filePaths"} {
		if arr, ok := m[key].([]any); ok && len(arr) > 0 {
			return len(arr), true
		}
	}
	return 0, false
}

func liteUnwrapTagged(v any) (string, any) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", nil
	}
	if len(m) == 1 {
		for k, item := range m {
			return k, item
		}
	}
	if s, ok := m["type"].(string); ok {
		return s, m
	}
	if s, ok := m["variant"].(string); ok {
		return s, m
	}
	return "", nil
}

// liteDiffLineCounts 端口 simpleDiffLines 的计数规则：只有一侧有文本时整段
// 计增或删；两侧都有则走同一条 LCS（超过 400 行退回「整段全算」的同一路径）。
func liteDiffLineCounts(oldText, newText string) (int, int) {
	switch {
	case oldText == "" && newText == "":
		return 0, 0
	case oldText == "":
		return len(liteSplitDiffLines(newText)), 0
	case newText == "":
		return 0, len(liteSplitDiffLines(oldText))
	}
	oldLines := liteSplitDiffLines(oldText)
	newLines := liteSplitDiffLines(newText)
	if len(oldLines)+len(newLines) <= 400 {
		return liteLCSCounts(oldLines, newLines)
	}
	return len(newLines), len(oldLines)
}

func liteSplitDiffLines(s string) []string {
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// liteLCSCounts 与前端 diffLines 同一条 LCS（等长时优先删），只数增删行数。
func liteLCSCounts(a, b []string) (int, int) {
	m, n := len(a), len(b)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := m - 1; i >= 0; i-- {
		for j := n - 1; j >= 0; j-- {
			if a[i] == b[j] {
				dp[i][j] = dp[i+1][j+1] + 1
			} else if dp[i+1][j] >= dp[i][j+1] {
				dp[i][j] = dp[i+1][j]
			} else {
				dp[i][j] = dp[i][j+1]
			}
		}
	}
	i, j := 0, 0
	ins, del := 0, 0
	for i < m && j < n {
		switch {
		case a[i] == b[j]:
			i++
			j++
		case dp[i+1][j] >= dp[i][j+1]:
			i++
			del++
		default:
			j++
			ins++
		}
	}
	del += m - i
	ins += n - j
	return ins, del
}

func liteFirstStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := liteStr(m[k]); s != "" {
			return s
		}
	}
	return ""
}

func liteStr(v any) string {
	s, _ := v.(string)
	return s
}

func liteAsInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func liteToolID(upd map[string]any) string {
	if s, ok := upd["toolCallId"].(string); ok && s != "" {
		return s
	}
	if s, ok := upd["tool_call_id"].(string); ok && s != "" {
		return s
	}
	return ""
}

func liteEnvSeq(env map[string]any) int {
	return liteAsInt(env["msgSeq"])
}

func liteUpdOf(raw any) (env, params, upd map[string]any, kind string, ok bool) {
	env, ok = raw.(map[string]any)
	if !ok {
		return nil, nil, nil, "", false
	}
	params, _ = env[kParams].(map[string]any)
	if params == nil {
		return env, nil, nil, "", false
	}
	upd, _ = params[kUpdate].(map[string]any)
	if upd == nil {
		return env, params, nil, "", false
	}
	kind, _ = upd[kSessionUpdate].(string)
	return env, params, upd, kind, true
}

// liteCoalesce 合成同 id 工具信封、连续 thought。返回合成掉信封的骨架
// 字节（已计入各信封自己的正文 omitted 的不再加）。第二个返回值恒 0，
// 骨架计入第一个。
func liteCoalesce(updates *[]any) int64 {
	src := *updates
	firstByID := map[string]int{}
	thoughtRun := -1
	var skeleton int64
	for i, raw := range src {
		env, _, upd, kind, ok := liteUpdOf(raw)
		if !ok {
			thoughtRun = -1
			continue
		}
		switch kind {
		case "tool_call", "tool_call_update":
			thoughtRun = -1
			id := liteToolID(upd)
			if id == "" {
				continue
			}
			prev, seen := firstByID[id]
			if !seen {
				firstByID[id] = i
				continue
			}
			skeleton += int64(liteJSONLen(raw))
			liteMergeTool(src[prev], env)
			src[i] = nil
		case "agent_thought_chunk":
			if thoughtRun < 0 {
				thoughtRun = i
				continue
			}
			skeleton += int64(liteJSONLen(raw))
			liteMergeThought(src[thoughtRun], env)
			src[i] = nil
		default:
			thoughtRun = -1
		}
	}
	out := src[:0]
	for _, raw := range src {
		if raw != nil {
			out = append(out, raw)
		}
	}
	*updates = out
	return skeleton
}

func liteMergeTool(dstRaw any, srcEnv map[string]any) {
	_, _, dstUpd, _, ok := liteUpdOf(dstRaw)
	if !ok || dstUpd == nil {
		return
	}
	_, _, srcUpd, _, ok := liteUpdOf(srcEnv)
	if !ok || srcUpd == nil {
		return
	}
	for k, v := range srcUpd {
		if k == kSessionUpdate {
			continue
		}
		if k == "rawInput" || k == "rawOutput" || k == kMeta {
			dm, dok := dstUpd[k].(map[string]any)
			sm, sok := v.(map[string]any)
			if dok && sok {
				if k == kMeta {
					liteMergeMeta(dm, sm)
				} else {
					for mk, mv := range sm {
						dm[mk] = mv
					}
				}
				continue
			}
		}
		dstUpd[k] = v
	}
	seq := liteEnvSeq(srcEnv)
	if sm, ok := srcUpd[kMeta].(map[string]any); ok {
		if lite, ok := sm["lite"].(map[string]any); ok {
			if end := liteAsInt(lite["msgSeqEnd"]); end > seq {
				seq = end
			}
		}
	}
	// 正文 omitted 已在 liteMergeMeta 里累加；这里只推进 msgSeqEnd。
	liteStamp(dstUpd, 0, seq)
}

func liteMergeMeta(dst, src map[string]any) {
	for k, v := range src {
		if k == "lite" {
			sm, _ := v.(map[string]any)
			dm, _ := dst[k].(map[string]any)
			if dm == nil {
				dm = map[string]any{}
				dst[k] = dm
			}
			dm["omitted"] = liteAsInt(dm["omitted"]) + liteAsInt(sm["omitted"])
			end := liteAsInt(sm["msgSeqEnd"])
			if end > liteAsInt(dm["msgSeqEnd"]) {
				dm["msgSeqEnd"] = end
			}
			// edits / files 是「这一封正文折出来的行头数字」，合成时后到覆盖
			// 先到——与前端整包替换 content / rawOutput 的取数口径一致；
			// 累加会把同一次编辑数两遍。
			for _, num := range []string{"edits", "files"} {
				if v, ok := sm[num]; ok {
					dm[num] = v
				}
			}
			continue
		}
		dst[k] = v
	}
}

func liteMergeThought(dstRaw any, srcEnv map[string]any) {
	_, _, dstUpd, _, ok := liteUpdOf(dstRaw)
	if !ok || dstUpd == nil {
		return
	}
	_, _, srcUpd, _, ok := liteUpdOf(srcEnv)
	if !ok {
		return
	}
	extra := 0
	if sm, ok := srcUpd[kMeta].(map[string]any); ok {
		if lite, ok := sm["lite"].(map[string]any); ok {
			extra = liteAsInt(lite["omitted"])
		}
	}
	if extra == 0 {
		extra = liteJSONLen(srcUpd[kContent])
	}
	if c, ok := dstUpd[kContent].(map[string]any); ok {
		c["omitted"] = liteAsInt(c["omitted"]) + extra
	}
	seq := liteEnvSeq(srcEnv)
	if sm, ok := srcUpd[kMeta].(map[string]any); ok {
		if lite, ok := sm["lite"].(map[string]any); ok {
			if end := liteAsInt(lite["msgSeqEnd"]); end > seq {
				seq = end
			}
		}
	}
	liteStamp(dstUpd, extra, seq)
}

func liteJSONLen(v any) int {
	if v == nil {
		return 0
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(raw)
}
