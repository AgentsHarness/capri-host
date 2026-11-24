package acp

import (
	"fmt"
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
//     行头字段，command 原样（回放行头要完整命令）；
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
	// liteCapStringBytes / liteCapHeadBytes：保留字符串（path / title 等）的封顶。
	// command 不走这里——回放行头要完整命令，见 liteHardKeepKeys。
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

// liteHardKeepKeys：一字不动（含 command——回放行头要完整命令，不截头）。
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
	"command":          true,
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
