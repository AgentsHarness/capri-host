package acp

import (
	"encoding/json"
	"regexp"
	"strings"
)

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
