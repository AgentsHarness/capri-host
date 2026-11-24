package acp

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// lite_test.go — lite/meta 投影（契约 lite-replay [A][B][C]）验收：真实
// 信封形状下条数 / msgSeq / promptStarts / totalCount 与 full 完全一致；
// params._meta 与 [C]3 保留清单一个字未动；lite.omitted 与实际裁掉的字节吻
// 合；rawOutput 兜底预算生效；detail 缺省 / full / 未知值与今天逐字节相等；
// meta 档不回 updates 键。本地分页与 _x.ai/session/updates 透传两条出口都
// 要裁（回退路径不得漏裁）。

// mustJSON 序列化夹具/结果，失败即 fatal（字面量不可能失败）。
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// liteFixture 是一页真实形状的存储信封 + 投影测试要对着看的原值。被裁的
// 正文一律纯 ASCII（无 JSON 转义）⇒ 序列化长度 = 字节数 + 2，可手工核对。
type liteFixture struct {
	lines   []string
	body    map[string]string
	blocks  []any // Bash 信封的原始 content 块（6 块全部被摘要/丢弃）
	matches []any // Grep 的 25 条 file_matches（后 5 条被截掉）
}

func liteFixtureOf(t *testing.T) *liteFixture {
	t.Helper()
	const sid = "sess-1"
	f := &liteFixture{body: map[string]string{
		"bashOutput":  strings.Repeat("o", 9000),
		"bashPrompt":  strings.Repeat("p", 1200),
		"fileContent": strings.Repeat("f", 40000),
		"fileConcise": strings.Repeat("c", 8000),
		"grepStdout":  strings.Repeat("s", 30000),
		"oldString":   strings.Repeat("a", 6000),
		"newString":   strings.Repeat("b", 7000),
		"imageData":   strings.Repeat("i", 500000),
		"mcpText":     strings.Repeat("m", 4000),
		"planContent": strings.Repeat("#", 4096),
		"agentText":   strings.Repeat("z", 1<<20),
	}}
	for i := 0; i < 6; i++ {
		f.blocks = append(f.blocks, map[string]any{
			"type": "content",
			"content": map[string]any{
				"type": "text",
				"text": strings.Repeat(string(rune('A'+i)), 400),
			},
		})
	}
	for i := 0; i < 25; i++ {
		f.matches = append(f.matches, map[string]any{
			"line_numbers": []any{float64(i), float64(i + 1)},
			"path":         "/ws/src/file" + string(rune('a'+i%26)) + ".go",
		})
	}
	f.lines = []string{
		// 0：用户 prompt（promptStarts 锚点，一个字都不许动）。
		histEnvelope(sid, 0, 1000, msgUserChunkMeta("跑测试", map[string]any{"promptIndex": float64(0)})),
		// 1：1MB 的 agent_message_chunk —— [C]6 明文禁止裁。
		histEnvelope(sid, 1, 1100, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": f.body["agentText"]},
		}),
		// 2：Bash tool_call —— content 6 块（超 4，测丢弃）+ 两个正文键 +
		// 数字标量 + 保留清单。
		histEnvelope(sid, 2, 1200, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "tool-bash",
			"status":        "in_progress",
			"kind":          "execute",
			"title":         "npm test",
			"locations":     []any{map[string]any{"path": "/ws/pkg/main.go", "line": float64(12)}},
			"content":       f.blocks,
			"rawOutput": map[string]any{
				"exit_code":         float64(0),
				"output":            f.body["bashOutput"],
				"output_for_prompt": f.body["bashPrompt"],
				"signal":            nil,
				"truncated":         false,
			},
			"rawInput": map[string]any{"command": "npm test", "timeout": float64(300)},
			"_meta":    liteToolMeta(),
		}),
		// 3：Read 终态 —— FileContent 嵌在 rawOutput.file 里（深层递归）。
		histEnvelope(sid, 3, 1300, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-read",
			"status":        "completed",
			"kind":          "read",
			"title":         "Read main.go",
			"rawOutput": map[string]any{
				"file": map[string]any{
					"content":         f.body["fileContent"],
					"content_concise": f.body["fileConcise"],
					"number_lines":    float64(1200),
					"path":            "/ws/pkg/main.go",
					"total_lines":     float64(1200),
					"truncated":       true,
				},
				"type": "file",
			},
			"rawInput": map[string]any{"limit": float64(2000), "offset": float64(0), "path": "/ws/pkg/main.go"},
			"_meta":    liteToolMeta(),
		}),
		// 4：Grep —— stdout 正文 + 25 项 file_matches（截到 20 + omittedCount）。
		histEnvelope(sid, 4, 1400, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-grep",
			"status":        "completed",
			"kind":          "search",
			"title":         "grep foo",
			"rawOutput": map[string]any{
				"file_matches": f.matches,
				"match_count":  float64(25),
				"mode":         "content",
				"stderr":       "",
				"stdout":       f.body["grepStdout"],
				"total_lines":  float64(25),
			},
			"rawInput": map[string]any{"pattern": "foo", "path": "/ws"},
			"_meta":    liteToolMeta(),
		}),
		// 5：Edit —— 顶层与 edits[].details[] 的 old/new_string；
		// insertions/deletions 等数字标量必须原样。
		histEnvelope(sid, 5, 1500, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-edit",
			"status":        "completed",
			"kind":          "edit",
			"title":         "Edit main.go",
			"rawOutput": map[string]any{
				"deletions":  float64(3),
				"edit_count": float64(1),
				"edits": []any{map[string]any{
					"details": map[string]any{
						"new_string": f.body["newString"],
						"old_string": f.body["oldString"],
					},
					"new_string": f.body["newString"],
					"old_string": f.body["oldString"],
				}},
				"file_path":  "/ws/pkg/main.go",
				"insertions": float64(12),
				"new_string": f.body["newString"],
				"not_found":  "NotFound: 没有第二处",
				"old_string": f.body["oldString"],
				"type":       "EditsApplied",
			},
			"rawInput": map[string]any{
				"new_string": f.body["newString"],
				"old_string": f.body["oldString"],
				"path":       "/ws/pkg/main.go",
			},
			"_meta": liteToolMeta(),
		}),
		// 6：带 ImageContent.data 的图。
		histEnvelope(sid, 6, 1600, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-img",
			"status":        "completed",
			"kind":          "read",
			"title":         "Read shot.png",
			"rawOutput": map[string]any{
				"data":     f.body["imageData"],
				"mimeType": "image/png",
				"type":     "image",
			},
			"_meta": liteToolMeta(),
		}),
		// 7：未知工具形状（MCP）—— 没有实测键名，只能靠通用递归兜底。
		histEnvelope(sid, 7, 1700, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-mcp",
			"status":        "completed",
			"kind":          "other",
			"title":         "mcp__fs__fetch",
			"rawOutput": map[string]any{
				"content": []any{map[string]any{"text": f.body["mcpText"], "type": "text"}},
				"isError": false,
				"meta":    map[string]any{"count": float64(7), "note": "短说明"},
			},
			"_meta": liteToolMeta(),
		}),
		// 8：PlanReady —— plan_content 是 FE planDoc.ts 的依赖，必须整段留下
		// （代价：这条的 rawOutput 超 2048 兜底预算，见 lite.go 的取舍）。
		histEnvelope(sid, 8, 1800, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-plan",
			"status":        "completed",
			"kind":          "think",
			"title":         "Plan",
			"rawOutput": map[string]any{
				"ok":           true,
				"plan_content": f.body["planContent"],
				"type":         "PlanReady",
			},
			"_meta": liteToolMeta(),
		}),
		// 9：第二个 prompt 起点。
		histEnvelope(sid, 9, 1900, msgUserChunkMeta("再来", map[string]any{"promptIndex": float64(1)})),
		// 10-14：非工具载体，全部要求逐字节原样。
		histEnvelope(sid, 10, 2000, map[string]any{
			"sessionUpdate": "plan",
			"entries":       []any{map[string]any{"content": strings.Repeat("P", 3000), "priority": "medium", "status": "pending"}},
		}),
		histEnvelope(sid, 11, 2100, map[string]any{
			"sessionUpdate": "task_backgrounded",
			"task_id":       "t-1",
			"log":           strings.Repeat("L", 5000),
		}),
		histEnvelope(sid, 12, 2200, map[string]any{
			"sessionUpdate": "session_recap",
			"summary":       strings.Repeat("R", 5000),
		}),
		histEnvelope(sid, 13, 2300, map[string]any{
			"sessionUpdate": "compaction_checkpoint",
			"payload":       strings.Repeat("K", 5000),
		}),
		histEnvelope(sid, 14, 2400, map[string]any{
			"sessionUpdate": "turn_completed",
			"stopReason":    "end_turn",
		}),
	}
	return f
}

// liteToolMeta 是工具信封的 update._meta：[C]3 点名的归属键都在这里，投影
// 只能往里加 lite 一个键。
func liteToolMeta() map[string]any {
	return map[string]any{
		"bash_mode":        "prompt",
		"child_session_id": "child-1",
		"hostTurn":         false,
		"is_background":    false,
		"promptIndex":      float64(0),
		"x.ai/tool":        map[string]any{"kind": "bash", "label": "shell", "name": "Bash"},
	}
}

// litePageOf 把信封行解析成服务页的 updates 切片（与 readEnvelopesByRank
// 同形状：map[string]any + float64 数字 + 顶层 msgSeq）。
func litePageOf(t *testing.T, lines []string) []any {
	t.Helper()
	out := make([]any, 0, len(lines))
	for i, l := range lines {
		var env any
		if err := json.Unmarshal([]byte(l), &env); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		env.(map[string]any)["msgSeq"] = i
		out = append(out, env)
	}
	return out
}

// liteUpdate 取出页内第 i 条信封的 params.update。
func liteUpdate(t *testing.T, updates []any, i int) map[string]any {
	t.Helper()
	env, ok := updates[i].(map[string]any)
	if !ok {
		t.Fatalf("update %d 不是信封对象: %T", i, updates[i])
	}
	params, ok := env[kParams].(map[string]any)
	if !ok {
		t.Fatalf("update %d 缺 params: %v", i, env)
	}
	upd, ok := params[kUpdate].(map[string]any)
	if !ok {
		t.Fatalf("update %d 缺 params.update: %v", i, params)
	}
	return upd
}

// liteStampOf 取某条信封的 update._meta.lite 标记（不存在返回 ok=false）。
func liteStampOf(t *testing.T, updates []any, i int) (map[string]any, bool) {
	upd := liteUpdate(t, updates, i)
	meta, ok := upd[kMeta].(map[string]any)
	if !ok {
		return nil, false
	}
	stamp, ok := meta["lite"].(map[string]any)
	return stamp, ok
}

// liteOmittedOf 取某条信封的 lite.omitted（没有标记即 fatal）。
func liteOmittedOf(t *testing.T, updates []any, i int) int {
	t.Helper()
	stamp, ok := liteStampOf(t, updates, i)
	if !ok {
		t.Fatalf("信封 %d 没有 lite 标记", i)
	}
	n := liteAsInt(stamp["omitted"])
	if n <= 0 {
		t.Fatalf("信封 %d 的 lite.omitted = %v", i, stamp["omitted"])
	}
	return n
}

// liteSkeleton 抽出时间线位置（msgSeq / method / kind）。params._meta 会被
// 收到回放真用的键，不进骨架。
func liteSkeleton(t *testing.T, updates []any) []string {
	t.Helper()
	out := make([]string, 0, len(updates))
	for _, raw := range updates {
		env := raw.(map[string]any)
		params := env[kParams].(map[string]any)
		upd := params[kUpdate].(map[string]any)
		kind, _ := upd[kSessionUpdate].(string)
		out = append(out, strings.Join([]string{
			string(mustJSON(t, env["msgSeq"])),
			env[kMethod].(string),
			kind,
		}, "|"))
	}
	return out
}

// 夹具里工具 id 各不相同、无连续 thought：条数 / 顺序 / msgSeq / kind 与
// full 相同；二次投影是空操作。
func TestLiteProjectionKeepsTimelineSkeleton(t *testing.T) {
	f := liteFixtureOf(t)
	full := litePageOf(t, f.lines)
	lite := litePageOf(t, f.lines)

	omitted := liteProjectPage(&lite)
	if omitted <= 0 {
		t.Fatalf("omittedBytes = %d, want > 0", omitted)
	}
	if len(lite) != len(full) {
		t.Fatalf("条数 = %d, want %d（夹具无合成对象）", len(lite), len(full))
	}
	if got, want := liteSkeleton(t, lite), liteSkeleton(t, full); !reflect.DeepEqual(got, want) {
		t.Errorf("骨架被改了:\ngot  %q\nwant %q", got, want)
	}
	for i := range full {
		fullEnv := full[i].(map[string]any)
		liteEnv := lite[i].(map[string]any)
		if !reflect.DeepEqual(fullEnv["msgSeq"], liteEnv["msgSeq"]) {
			t.Errorf("信封 %d msgSeq = %v, want %v", i, liteEnv["msgSeq"], fullEnv["msgSeq"])
		}
		liteParams := liteEnv[kParams].(map[string]any)
		meta, _ := liteParams[kMeta].(map[string]any)
		for k := range meta {
			if !liteParamsMetaKeep[k] {
				t.Errorf("信封 %d params._meta 多了 %s", i, k)
			}
		}
		if _, ok := meta["agentTimestampMs"]; !ok {
			t.Errorf("信封 %d 丢了 agentTimestampMs", i)
		}
	}
	before := string(mustJSON(t, lite))
	if again := liteProjectPage(&lite); again != 0 {
		t.Errorf("同一页二次投影又裁掉 %d 字节，want 0", again)
	}
	if after := string(mustJSON(t, lite)); after != before {
		t.Error("同一页二次投影改了信封内容")
	}
}

// omitted 与实际裁掉的字节吻合：按夹具手工算出每条被裁正文的序列化长度，
// 与 lite.omitted 逐项对账（纯 ASCII ⇒ 序列化长度 = 字节数 + 2）。
func TestLiteOmittedMatchesCutBytes(t *testing.T) {
	f := liteFixtureOf(t)
	lite := litePageOf(t, f.lines)
	total := liteProjectPage(&lite)

	strLen := func(s string) int { return len(s) + 2 }
	// 信封 2：Bash —— 6 个 content 块（4 块摘要 + 2 块丢弃，全计入）+
	// output + output_for_prompt；rawInput.command 与数字标量不在清单里。
	blockBytes := 0
	for _, b := range f.blocks {
		blockBytes += len(mustJSON(t, b))
	}
	if got, want := liteOmittedOf(t, lite, 2), blockBytes+strLen(f.body["bashOutput"])+strLen(f.body["bashPrompt"]); got != want {
		t.Errorf("Bash lite.omitted = %d, want %d", got, want)
	}
	// 信封 3：Read —— FileContent.content + content_concise。
	if got, want := liteOmittedOf(t, lite, 3), strLen(f.body["fileContent"])+strLen(f.body["fileConcise"]); got != want {
		t.Errorf("Read lite.omitted = %d, want %d", got, want)
	}
	// 信封 4：Grep —— stdout + 整棵 file_matches + 空 stderr（正文键一律删）。
	wantGrep := strLen(f.body["grepStdout"]) + len(mustJSON(t, f.matches)) + strLen("")
	if got := liteOmittedOf(t, lite, 4); got != wantGrep {
		t.Errorf("Grep lite.omitted = %d, want %d", got, wantGrep)
	}
	// 信封 5：Edit —— 顶层、edits[]、edits[].details[]、rawInput 各一份
	// old_string/new_string。
	wantEdit := 4*strLen(f.body["oldString"]) + 4*strLen(f.body["newString"])
	if got := liteOmittedOf(t, lite, 5); got != wantEdit {
		t.Errorf("Edit lite.omitted = %d, want %d", got, wantEdit)
	}
	// 信封 6：ImageContent.data（mimeType 保留）。
	if got, want := liteOmittedOf(t, lite, 6), strLen(f.body["imageData"]); got != want {
		t.Errorf("image lite.omitted = %d, want %d", got, want)
	}
	// 信封 7：未知 MCP 形状 —— 通用递归按长度阈值兜底。`content` 本身是
	// 正文键，数组整棵换形状（裁掉的是整个数组的序列化字节，含数组框架）。
	if got, want := liteOmittedOf(t, lite, 7), len(mustJSON(t, []any{map[string]any{"text": f.body["mcpText"], "type": "text"}})); got != want {
		t.Errorf("unknown MCP lite.omitted = %d, want %d", got, want)
	}
	// 信封 8：plan_content 按正文删，打 lite 标记。
	if got, want := liteOmittedOf(t, lite, 8), strLen(f.body["planContent"]); got != want {
		t.Errorf("PlanReady lite.omitted = %d, want %d", got, want)
	}
	// 非工具信封不打标记。
	for _, i := range []int{0, 1, 9, 10, 11, 12, 13, 14} {
		if _, ok := liteStampOf(t, lite, i); ok {
			t.Errorf("信封 %d 不是工具信封，不得打 lite 标记", i)
		}
	}
	// 页总量对账。
	var sum int
	for i := range lite {
		if _, ok := liteStampOf(t, lite, i); !ok {
			continue
		}
		sum += liteOmittedOf(t, lite, i)
	}
	if int64(sum) > total {
		t.Errorf("omittedBytes = %d < Σ lite.omitted = %d", total, sum)
	}
}

// [C]3 保留清单：数字/布尔标量、file_matches[].path、plan_content、
// _meta 归属键、toolCallId/status/kind/title/locations 一字未动。
func TestLiteKeepsPreserveList(t *testing.T) {
	f := liteFixtureOf(t)
	full := litePageOf(t, f.lines)
	lite := litePageOf(t, f.lines)
	liteProjectPage(&lite)

	fullUpd := liteUpdate(t, full, 2)
	liteUpd := liteUpdate(t, lite, 2)
	for _, key := range []string{"toolCallId", "status", "kind", "title", "locations"} {
		if !reflect.DeepEqual(fullUpd[key], liteUpd[key]) {
			t.Errorf("Bash %v 被改: %v, want %v", key, liteUpd[key], fullUpd[key])
		}
	}
	// update._meta 只能多出 lite 一个键。
	fullMeta := fullUpd[kMeta].(map[string]any)
	liteMeta := liteUpd[kMeta].(map[string]any)
	for k, v := range fullMeta {
		if !reflect.DeepEqual(liteMeta[k], v) {
			t.Errorf("update._meta.%v 被改: %v", k, liteMeta[k])
		}
	}
	if _, ok := liteMeta["lite"]; !ok {
		t.Error("update._meta 没打 lite 标记")
	}
	if _, ok := liteMeta["x.ai/tool"]; !ok {
		t.Error("update._meta 丢了 x.ai/tool")
	}
	// rawOutput 里的数字/布尔/null 标量与 rawInput 全貌。
	ro := liteUpd["rawOutput"].(map[string]any)
	if ro["exit_code"] != float64(0) || ro["truncated"] != false || ro["signal"] != nil {
		t.Errorf("Bash rawOutput 标量被改: %v", ro)
	}
	if want := map[string]any{"command": "npm test", "timeout": float64(300)}; !reflect.DeepEqual(liteUpd["rawInput"], want) {
		t.Errorf("Bash rawInput 被改: %v, want %v", liteUpd["rawInput"], want)
	}
	// Read：路径与行数字段保留，正文换成摘要形状。
	file := liteUpdate(t, lite, 3)["rawOutput"].(map[string]any)["file"].(map[string]any)
	if file["path"] != "/ws/pkg/main.go" || file["number_lines"] != float64(1200) ||
		file["total_lines"] != float64(1200) || file["truncated"] != true {
		t.Errorf("Read 保留清单被改: %v", file)
	}
	if _, ok := file["content"]; ok {
		t.Errorf("Read content 还在: %v", file["content"])
	}
	if _, ok := file["content_concise"]; ok {
		t.Errorf("Read content_concise 还在: %v", file["content_concise"])
	}
	// Grep：计数标量不动，file_matches / stdout / stderr 整键删除。
	grepRO := liteUpdate(t, lite, 4)["rawOutput"].(map[string]any)
	if grepRO["match_count"] != float64(25) || grepRO["total_lines"] != float64(25) || grepRO["mode"] != "content" {
		t.Errorf("Grep 计数被改: %v", grepRO)
	}
	if _, ok := grepRO["file_matches"]; ok {
		t.Errorf("file_matches 还在: %v", grepRO["file_matches"])
	}
	if _, ok := grepRO["stdout"]; ok {
		t.Errorf("stdout 还在: %v", grepRO["stdout"])
	}
	// Edit：insertions/deletions 与 not_found 短文本原样，正文换摘要。
	editRO := liteUpdate(t, lite, 5)["rawOutput"].(map[string]any)
	if editRO["insertions"] != float64(12) || editRO["deletions"] != float64(3) || editRO["edit_count"] != float64(1) {
		t.Errorf("Edit 数字标量被改: %v", editRO)
	}
	if editRO["not_found"] != "NotFound: 没有第二处" || editRO["file_path"] != "/ws/pkg/main.go" || editRO["type"] != "EditsApplied" {
		t.Errorf("Edit 短文本/路径被改: %v", editRO)
	}
	planRO := liteUpdate(t, lite, 8)["rawOutput"].(map[string]any)
	if planRO["ok"] != true {
		t.Errorf("PlanReady.ok 被改: %v", planRO["ok"])
	}
	if _, ok := planRO["plan_content"]; ok {
		t.Errorf("plan_content 还在: %v", planRO["plan_content"])
	}
	// 未知形状的短文本与布尔不动。
	mcpRO := liteUpdate(t, lite, 7)["rawOutput"].(map[string]any)
	if mcpRO["isError"] != false {
		t.Errorf("未知形状 isError 被改: %v", mcpRO["isError"])
	}
	if meta, ok := mcpRO["meta"].(map[string]any); !ok || meta["note"] != "短说明" || meta["count"] != float64(7) {
		t.Errorf("未知形状短文本/数字被改: %v", mcpRO["meta"])
	}
	// 图片的 mimeType 保留、data 换摘要。
	imgRO := liteUpdate(t, lite, 6)["rawOutput"].(map[string]any)
	if imgRO["mimeType"] != "image/png" || imgRO["type"] != "image" {
		t.Errorf("image 元数据被改: %v", imgRO)
	}
	if _, ok := imgRO["data"]; ok {
		t.Errorf("image data 还在: %v", imgRO["data"])
	}
}

func strOf(t *testing.T, s string) int {
	t.Helper()
	return len(s) + 2
}

// 回放行头的命令必须完整：rawInput.command 不封顶、不拼「…[已省略 N 字节]」，
// 也不计入 lite.omitted（回归：曾按 512/256 截头，620 字节的命令会显示成
// 前 256 字节 +「…[已省略 364 字节]」）。
func TestLiteKeepsLongCommandWhole(t *testing.T) {
	const sid = "sess-cmd"
	cmd := "ha" + strings.Repeat("x", 618) // 620 字节 > 512 封顶阈值
	body := strings.Repeat("o", 9000)
	lines := []string{
		histEnvelope(sid, 0, 1000, msgUserChunkMeta("跑", map[string]any{"promptIndex": float64(0)})),
		histEnvelope(sid, 1, 1100, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "call-cmd",
			"status":        "completed",
			"kind":          "execute",
			"title":         "Run",
			"rawInput":      map[string]any{"command": cmd, "timeout": float64(300)},
			"rawOutput":     map[string]any{"exit_code": float64(0), "output": body},
		}),
	}
	lite := litePageOf(t, lines)
	liteProjectPage(&lite)

	ri, ok := liteUpdate(t, lite, 1)["rawInput"].(map[string]any)
	if !ok {
		t.Fatalf("rawInput 没了: %v", liteUpdate(t, lite, 1))
	}
	if ri["command"] != cmd {
		t.Errorf("rawInput.command 被裁: len=%v, want %d", len(ri["command"].(string)), len(cmd))
	}
	if got, want := liteOmittedOf(t, lite, 1), strOf(t, body); got != want {
		t.Errorf("lite.omitted = %d, want %d（只算被删的 output，不含 command）", got, want)
	}
}

// isOmittedStub 判断 v 是否是 {"omitted": want} 统一形状。
func isOmittedStub(v any, want int) bool {
	stub, ok := v.(map[string]any)
	if !ok || len(stub) != 1 {
		return false
	}
	n, ok := stub["omitted"].(int)
	return ok && n == want
}

func TestLiteDropsContentKey(t *testing.T) {
	f := liteFixtureOf(t)
	lite := litePageOf(t, f.lines)
	liteProjectPage(&lite)

	upd := liteUpdate(t, lite, 2)
	if _, ok := upd[kContent]; ok {
		t.Fatalf("content 还在: %v", upd[kContent])
	}
	if _, ok := upd["locations"].([]any); !ok {
		t.Errorf("locations 被改: %v", upd["locations"])
	}
	if _, ok := liteStampOf(t, lite, 2); !ok {
		t.Fatal("Bash 信封没打 lite 标记")
	}
}

// [C]4：未知形状里的长字符串数组，按「最长字符串优先」继续裁到 2048 以内；
// 数组条数与数字标量不许动。
func TestLiteRawOutputBudgetKeepsLongestFirst(t *testing.T) {
	lengths := []int{400, 390, 380, 370, 360, 350, 340, 330}
	var items []any
	for _, n := range lengths {
		items = append(items, strings.Repeat("y", n))
	}
	env := map[string]any{
		"timestamp": 1, "method": "session/update",
		"params": map[string]any{
			"sessionId": "s1",
			"update": map[string]any{
				"sessionUpdate": "tool_call_update",
				"toolCallId":    "tool-x",
				"status":        "completed",
				"rawOutput":     map[string]any{"count": float64(len(items)), "lines": items},
			},
		},
	}
	rawOf := func() map[string]any {
		return env["params"].(map[string]any)["update"].(map[string]any)["rawOutput"].(map[string]any)
	}
	if n := len(mustJSON(t, rawOf())); n <= liteRawOutputBudget {
		t.Fatalf("夹具本身只有 %d 字节，测不到兜底预算", n)
	}
	if got := liteProjectEnvelope(env); got == 0 {
		t.Fatal("兜底预算没裁任何东西")
	}
	ro := rawOf()
	if n := len(mustJSON(t, ro)); n > liteRawOutputBudget {
		t.Errorf("投影后 rawOutput = %d 字节, want ≤ %d", n, liteRawOutputBudget)
	}
	if ro["count"] != float64(8) {
		t.Errorf("数字标量被改: %v", ro["count"])
	}
	if _, ok := liteStampOf(t, []any{env}, 0); !ok {
		t.Fatal("没打 lite 标记")
	}
}

// 真实工具形状投影后都得落进 2048 兜底预算（plan_content 那条例外：保留
// 清单优先，见 TestLiteKeepsPreserveList）。
func TestLiteRawOutputBudgetOnRealShapes(t *testing.T) {
	f := liteFixtureOf(t)
	lite := litePageOf(t, f.lines)
	liteProjectPage(&lite)
	for _, i := range []int{2, 3, 4, 5, 6, 7} {
		upd := liteUpdate(t, lite, i)
		ro, ok := upd["rawOutput"]
		if !ok {
			continue
		}
		if n := len(mustJSON(t, ro)); n > liteRawOutputBudget {
			t.Errorf("信封 %d 投影后 rawOutput = %d 字节, want ≤ %d", i, n, liteRawOutputBudget)
		}
	}
}

// 可见文本载体（user / agent_message）的 update 正文不动；params._meta
// 收到保留清单。
func TestLiteNeverTouchesVisibleText(t *testing.T) {
	f := liteFixtureOf(t)
	full := litePageOf(t, f.lines)
	lite := litePageOf(t, f.lines)
	liteProjectPage(&lite)
	for _, i := range []int{0, 1, 9} {
		fu := liteUpdate(t, full, i)
		lu := liteUpdate(t, lite, i)
		if !reflect.DeepEqual(fu[kContent], lu[kContent]) {
			t.Errorf("信封 %d 的可见正文被改", i)
		}
		if fu[kSessionUpdate] != lu[kSessionUpdate] {
			t.Errorf("信封 %d kind 被改", i)
		}
	}
}
