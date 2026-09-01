package acp

import (
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"
)

// ─────────────────────────────────────────────────────────────────────
// lite.go — /api/session-updates 的投影档位（契约 lite-replay [A][B][C]）。
//
// 历史页里绝大多数字节是工具正文（Bash 输出、Read 文件内容、Edit 双向
// diff、Grep 的 stdout）。lite 只裁这些正文，时间线骨架一个字都不动：信封
// 条数、顺序、顶层 msgSeq、params._meta、promptStarts/totalCount/hasMore
// 与 btw 锚点归属全部原样，FE 拿到的是同一份滚动条，展开某条工具卡时再按
// 需拉全量把字段填回原行。meta 档更进一步：连信封都不回，只回锚点。
//
// 计量口径统一为 encoding/json 的紧凑序列化字节数（与 writeJSON 同口径）：
// lite.omitted / omittedBytes 是「被裁掉内容」的体积，不含替换进来的
// {"omitted": N} 摘要与 lite 标记自身新增的字节。
// ─────────────────────────────────────────────────────────────────────

// detail 档位（[A]）。"full"、缺省与任何未知取值都落回「不投影」，即今天
// 的逐字节原样行为——旧 FE 不发 detail 时响应不得有任何变化。
const (
	DetailFull = "full"
	DetailLite = "lite"
	DetailMeta = "meta"
)

const (
	// liteRawOutputBudget 是 [C]4 的兜底预算：投影后单条 update.rawOutput
	// 的序列化字节数上限。它是尽力而为的天花板，不是硬不变量——两条结构性
	// 例外优先于它：[C]3 的硬保留清单（plan_content 实测单条 19.6KB，是
	// planDoc.ts 的依赖）与不参与裁剪的标量数组（Grep 每个 match 的
	// line_number，20 个 file_matches 就能堆到 9KB）。本机 20 个大会话
	// 34361 条信封实测越界 229 条（0.7%），最大 19.7KB，合计约占 lite 后
	// 字节的 1%；command 这类保留键的截断头不在越界样本里（0/229）。
	liteRawOutputBudget = 2048
	// liteMaxContentBlocks / liteMaxFileMatches 是摘要保留上限（[C]1 /
	// [C]3）：超出部分整项丢弃并计入 omitted。
	liteMaxContentBlocks = 4
	liteMaxFileMatches   = 20
	// liteLongStringBytes：实测正文键之外，未知工具形状里多长的字符串算
	// 正文。短文本（NotFound/error 之类）原样保留（[C]3）。
	liteLongStringBytes = 512
	// liteCapStringBytes 是对**保留清单里的键**也生效的兜底上限：直连 bash
	// 的 heredoc 会把整个文件塞进 command（实测单页 rawOutput.command 100KB
	// + rawInput.command 62KB），一律留头部 + 省略标记。FE 的行头只看得到
	// 命令开头，全文在补全时回来；lite 行的去重键已改用
	// toolCallId+status+kind+title，截断不会打碎 live↔快照去重。
	liteCapStringBytes = 8192
	// liteCapHeadBytes：截断后保留的头部长度。
	liteCapHeadBytes = 2048
)

// liteBodyKeys：实测工具结果里的「正文」键，即 [C]2 列举路径的末级键名
// （Bash 的 output/output_for_prompt/output_delta、FileContent.content
// (_concise)、ImageContent.data、Grep 的 stdout/stderr、EditsApplied 的
// old/new_string、Content.content、MultiResult.results[].output、
// Result.output、rawInput 的 content/new_string/old_string）。
//
// 命中即**整棵换形状**，不看值的类型：Bash/Grep 的 output 与 stdout 实测是
// **行数组**（单页里一项就 1.0MB / 555KB），只裁字符串会整类漏掉；逐元素裁
// 则留下上千个 {"omitted":N} 壳。容器键（edits / results / details /
// file_matches …）不在表内，交给递归下钻。
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
}

// liteKeepKeys：[C]3 保留清单里的字符串型键（数字/布尔标量本就不在裁剪
// 范围内，无需登记）。命中即整棵子树不碰：title/kind/status/toolCallId/
// locations 是 FE 画工具卡与算去重键的依赖，plan_content 是 planDoc.ts 的
// 依赖，path/command 是「展开前也要能认出是哪个文件/哪条命令」的依赖，
// type/sessionUpdate 是形状判别键。
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
	"plan_content":     true,
	"command":          true,
	"name":             true,
	"label":            true,
	"mime_type":        true,
	"mimeType":         true,
}

// liteHardKeepKeys 是保留清单里**一字都不能动**的那部分：形状判别键、
// FE 的行头标题与 id、以及 planDoc.ts 依赖的 plan 正文。其余保留键
// （command / current_dir / path 之类）超过 liteCapStringBytes 仍会被截成
// 头部 + 省略标记——它们是给人看的文本，不是键。
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
	"plan_content":     true,
	"name":             true,
	"label":            true,
	"mime_type":        true,
	"mimeType":         true,
}

// liteAcc 累计单条信封的裁剪结果。
type liteAcc struct {
	omitted int
	fields  []string
}

// note 记下一处裁剪：removed 是被裁掉内容的序列化字节数。
func (a *liteAcc) note(path string, removed int) {
	if removed <= 0 {
		return
	}
	a.omitted += removed
	a.fields = append(a.fields, path)
}

// applyUpdatesDetail 在 SessionUpdates 的两条出口（本地分页 /
// _x.ai/session/updates 透传）上统一施加投影——回退路径不得漏裁。
// stream=true 不参与：信封以 session_updates_chunk 通知推流，响应里没有
// 可投影的页，此时不回显 projected（FE 按「host 不支持」处理）。
func applyUpdatesDetail(page *UpdatesPage, o SessionUpdatesOpts) {
	switch o.Detail {
	case DetailLite:
		page.Projected = DetailLite
		page.OmittedBytes = liteProjectPage(page.Updates)
	case DetailMeta:
		page.Projected = DetailMeta
		// [B]：不回 updates 键（handler 据此省略），而不是回空数组伪装成
		// 「无历史」。锚点字段仍在 page 上，语义不变。
		page.Updates = nil
	}
}

// liteProjectPage 对一页存储信封就地施加 lite 投影（[C]），返回整页被裁掉
// 的总字节数。信封一律原地改写：调用方给的是本次请求从 updates.jsonl（或
// agent 响应）现读的副本，投影不会污染归一化缓存或下一次服务。
func liteProjectPage(updates []any) int64 {
	var total int64
	for _, raw := range updates {
		total += int64(liteProjectEnvelope(raw))
	}
	return total
}

// liteProjectEnvelope 投影单条存储信封，返回该信封被裁掉的字节数
// （0 = 一字未动）。[C]6 的禁区在这里是结构性的：只有 params.update 的
// content / rawOutput / rawInput 三个键会被写，信封其余部分（timestamp、
// method、params.sessionId、params._meta、顶层 msgSeq）一概不读不写。
func liteProjectEnvelope(raw any) int {
	env, ok := raw.(map[string]any)
	if !ok {
		return 0
	}
	params, ok := env[kParams].(map[string]any)
	if !ok {
		return 0
	}
	upd, ok := params[kUpdate].(map[string]any)
	if !ok {
		return 0
	}
	// 只允许触碰工具信封（[C]）；agent_message_chunk / thought /
	// user_message_chunk / plan / task_* / turn_completed / recap /
	// compaction_* / _x.ai/session/update 载体全部原样穿过去。
	switch kind, _ := upd[kSessionUpdate].(string); kind {
	case "tool_call", "tool_call_update":
	default:
		return 0
	}
	// 幂等：一页最多投影一次，重复调用不得把摘要再摘要一遍。
	if meta, ok := upd[kMeta].(map[string]any); ok {
		if _, done := meta["lite"]; done {
			return 0
		}
	}

	var acc liteAcc
	if blocks, ok := upd[kContent].([]any); ok && len(blocks) > 0 {
		upd[kContent] = liteContentSummary(blocks, &acc)
	}
	for _, key := range []string{"rawOutput", "rawInput"} {
		v, present := upd[key]
		if !present || v == nil {
			continue
		}
		// 根节点自身是字符串（个别工具把 rawOutput 发成正文）时得单独换，
		// 递归只能改容器内部的元素。
		if s, isStr := v.(string); isStr {
			if stub, removed, ok := liteStubString(s); ok {
				upd[key] = stub
				acc.note(key, removed)
			}
			continue
		}
		if nv, changed := liteTrimValue(key, "", v, &acc); changed {
			upd[key] = nv
		}
	}
	// [C]4 的兜底预算只管 rawOutput；rawInput 已由正文键 + 长度阈值裁完。
	if ro := upd["rawOutput"]; ro != nil {
		liteBudget("rawOutput", ro, &acc)
	}
	if len(acc.fields) == 0 {
		return 0
	}
	liteStamp(upd, acc)
	return acc.omitted
}

// liteContentSummary 把 update.content 换成摘要块（[C]1）：每块只留
// {"type": 原块的 type, "omitted": 原块序列化字节数}，最多
// liteMaxContentBlocks 块，其余整块丢弃并计入 omitted。
func liteContentSummary(blocks []any, acc *liteAcc) []any {
	if len(blocks) > liteMaxContentBlocks {
		for _, dropped := range blocks[liteMaxContentBlocks:] {
			acc.note("content", liteJSONLen(dropped))
		}
		blocks = blocks[:liteMaxContentBlocks]
	}
	out := make([]any, 0, len(blocks))
	for _, b := range blocks {
		size := liteJSONLen(b)
		stub := map[string]any{"omitted": size}
		if block, ok := b.(map[string]any); ok {
			if typ, hasType := block[kType]; hasType {
				stub[kType] = typ
			}
		}
		out = append(out, stub)
		acc.note("content", size)
	}
	return out
}

// liteTrimValue 递归处理 rawOutput / rawInput 里的每个值，返回替换后的值与
// 是否改动。规则与键名无关地通用（[C]2「不能只 hard-code」），优先级：
//  1. 正文键 → 值不论形状（字符串、Bash/Grep 的行数组、结果对象）整块换成
//     统一摘要 {"omitted": N}；
//  2. 硬保留键（形状判别键、id、title、plan_content）→ 一字不动；
//  3. 其余保留键的字符串超 liteCapStringBytes → 截成头部 + 省略标记
//     （heredoc 直连 bash 的 command 实测单页 100KB+）；
//  4. 未知键：字符串按 liteLongStringBytes 阈值当正文裁，容器下钻；
//  5. 数字/布尔/null 永远原样。
//
// path 用 agent 落盘的 serde 记法（数组元素写作 `[]`），与 [C]2 列举的路径
// 同形，FE 直接当展示用的字段名。
func liteTrimValue(path, key string, v any, acc *liteAcc) (any, bool) {
	if liteBodyKeys[key] {
		if stub, removed, ok := liteStubValue(v); ok {
			acc.note(path, removed)
			return stub, true
		}
		return v, false
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
		stub, removed, ok := liteStubString(s)
		if !ok {
			return v, false
		}
		acc.note(path, removed)
		return stub, true
	}
	if liteKeepKeys[key] {
		// locations / x.ai/tool 这类结构：FE 直接依赖，整棵不钻。
		return v, false
	}
	switch t := v.(type) {
	case map[string]any:
		changed := false
		for k, item := range t {
			nv, ch := liteTrimValue(path+"."+k, k, item, acc)
			if ch {
				t[k] = nv
				changed = true
			}
		}
		return t, changed
	case []any:
		out := t
		changed := false
		// [C]3：file_matches 超 20 项截到 20 项，追加 {"omittedCount": M}
		// ——FE 靠它写「还有 N 个文件」，保留项里的 path 由 liteKeepKeys 保住。
		if key == "file_matches" && len(out) > liteMaxFileMatches {
			dropped := out[liteMaxFileMatches:]
			kept := make([]any, 0, liteMaxFileMatches+1)
			kept = append(kept, out[:liteMaxFileMatches]...)
			kept = append(kept, map[string]any{"omittedCount": len(dropped)})
			for _, d := range dropped {
				acc.note(path, liteJSONLen(d))
			}
			out = kept
			changed = true
		}
		for i, item := range out {
			nv, ch := liteTrimValue(path+"[]", key, item, acc)
			if ch {
				out[i] = nv
				changed = true
			}
		}
		return out, changed
	}
	return v, false
}

// liteCapKeepString 给保留键的巨型字符串封顶：命令类键不是正文，但 heredoc
// 直连 bash 会把整个文件内容写进 command，留头部足够画行头，全文在补全时回来。
// 按 rune 边界回退，绝不切出半个 UTF-8 字符（否则 FE 侧 JSON 里出现替换符）。
func liteCapKeepString(path, s string, acc *liteAcc) (any, bool) {
	if len(s) <= liteCapStringBytes {
		return s, false
	}
	head := s[:liteCapHeadBytes]
	for len(head) > 0 && !utf8.ValidString(head) {
		head = head[:len(head)-1]
	}
	removed := len(s) - len(head)
	acc.note(path, removed)
	return head + fmt.Sprintf("…[已省略 %d 字节]", removed), true
}

// liteStubString 把正文字符串换成统一形状 {"omitted": 原序列化字节数}；
// 换完更大就不换（短正文换摘要纯属浪费，也让 omitted 恒为正）。
func liteStubString(s string) (map[string]any, int, bool) {
	return liteStubValue(s)
}

// liteStubValue 是 liteStubString 的通用版：正文键的值不论形状（字符串、
// 行数组、结果对象）都按同一口径换形状。
func liteStubValue(v any) (map[string]any, int, bool) {
	size := liteJSONLen(v)
	stub := map[string]any{"omitted": size}
	if size <= liteJSONLen(stub) {
		return nil, 0, false
	}
	return stub, size, true
}

// liteBudget 是 [C]4 的兜底：投影后 rawOutput 仍超预算时，按「最长字符串
// 优先」继续裁。候选集合不含 keep 键下的子树（[C]3 是硬约束）与已是摘要
// 的值；裁无可裁仍超限就到此为止，宁可超限也不违反保留清单（超限的实际
// 来源见 liteRawOutputBudget 注释）。
func liteBudget(path string, v any, acc *liteAcc) {
	size := liteJSONLen(v)
	if size <= liteRawOutputBudget {
		return
	}
	var refs []liteStrRef
	liteCollectStrings(path, "", v, &refs)
	sort.SliceStable(refs, func(i, j int) bool { return refs[i].size > refs[j].size })
	for i := range refs {
		if size <= liteRawOutputBudget {
			return
		}
		r := &refs[i]
		stub, removed, ok := liteStubString(r.s)
		if !ok {
			continue
		}
		r.set(stub)
		acc.note(r.path, removed)
		size -= r.size - liteJSONLen(stub)
	}
}

// liteStrRef 是一个仍可裁的字符串叶子及其写回位置。
type liteStrRef struct {
	m    map[string]any // 与 arr 二选一
	key  string
	arr  []any
	idx  int
	s    string
	size int
	path string
}

func (r *liteStrRef) set(v any) {
	if r.m != nil {
		r.m[r.key] = v
		return
	}
	r.arr[r.idx] = v
}

func liteCollectStrings(path, key string, v any, out *[]liteStrRef) {
	if liteKeepKeys[key] {
		return
	}
	switch t := v.(type) {
	case map[string]any:
		for k, item := range t {
			if liteKeepKeys[k] {
				continue
			}
			if s, ok := item.(string); ok {
				*out = append(*out, liteStrRef{m: t, key: k, s: s, size: liteJSONLen(s), path: path + "." + k})
				continue
			}
			liteCollectStrings(path+"."+k, k, item, out)
		}
	case []any:
		for i, item := range t {
			if s, ok := item.(string); ok {
				*out = append(*out, liteStrRef{arr: t, idx: i, s: s, size: liteJSONLen(s), path: path + "[]"})
				continue
			}
			liteCollectStrings(path+"[]", key, item, out)
		}
	}
}

// liteStamp 在 params.update._meta 打投影标记（[C]5）。放 _meta 是有意的：
// FE 的去重键函数 toolReplayPayload 会 delete _meta，标记因此不参与
// live↔历史合并。update 原本没有 _meta 时新建一个——只有真被裁过的信封
// 才会多出这个键。fields 排序去重，保证同一次请求的多次投影结果可比。
func liteStamp(upd map[string]any, acc liteAcc) {
	fields := acc.fields
	sort.Strings(fields)
	deduped := make([]string, 0, len(fields))
	for _, f := range fields {
		if len(deduped) == 0 || deduped[len(deduped)-1] != f {
			deduped = append(deduped, f)
		}
	}
	meta, ok := upd[kMeta].(map[string]any)
	if !ok {
		meta = map[string]any{}
		upd[kMeta] = meta
	}
	meta["lite"] = map[string]any{"omitted": acc.omitted, "fields": deduped}
}

// liteJSONLen 按 encoding/json 的紧凑序列化口径量字节数；量不出来的值记
// 0（等价于「没有可裁的东西」，宁可不裁也不改形状）。
func liteJSONLen(v any) int {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(raw)
}
