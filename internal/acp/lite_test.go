package acp

import (
	"bytes"
	"context"
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
	n, ok := stamp["omitted"].(int)
	if !ok {
		t.Fatalf("信封 %d 的 lite.omitted 类型 = %T", i, stamp["omitted"])
	}
	fields, ok := stamp["fields"].([]string)
	if !ok || len(fields) == 0 {
		t.Fatalf("信封 %d 的 lite.fields = %v", i, stamp["fields"])
	}
	return n
}

// liteSkeleton 抽出与正文无关的时间线骨架（位置、msgSeq、信封 method、
// kind、params._meta），用于「lite 只裁正文」的对比断言。
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
			string(mustJSON(t, env["timestamp"])),
			env[kMethod].(string),
			kind,
			string(mustJSON(t, params[kMeta])),
		}, "|"))
	}
	return out
}

// 骨架一致：条数 / 顺序 / msgSeq / kind / params._meta 与 full 完全相同，
// 整页裁掉的字节 = 各信封 lite.omitted 之和，且二次投影是空操作。
func TestLiteProjectionKeepsTimelineSkeleton(t *testing.T) {
	f := liteFixtureOf(t)
	full := litePageOf(t, f.lines)
	lite := litePageOf(t, f.lines)

	omitted := liteProjectPage(lite)
	if omitted <= 0 {
		t.Fatalf("omittedBytes = %d, want > 0", omitted)
	}
	if len(lite) != len(full) {
		t.Fatalf("条数 = %d, want %d（绝不允许删信封行）", len(lite), len(full))
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
		fullParams := fullEnv[kParams].(map[string]any)
		liteParams := liteEnv[kParams].(map[string]any)
		if !reflect.DeepEqual(fullParams[kMeta], liteParams[kMeta]) {
			t.Errorf("信封 %d params._meta 被改: %v, want %v", i, liteParams[kMeta], fullParams[kMeta])
		}
	}
	var sum int
	for i := range lite {
		if _, ok := liteStampOf(t, lite, i); !ok {
			continue
		}
		sum += liteOmittedOf(t, lite, i)
	}
	if int64(sum) != omitted {
		t.Errorf("omittedBytes = %d, want Σ lite.omitted = %d", omitted, sum)
	}
	before := string(mustJSON(t, lite))
	if again := liteProjectPage(lite); again != 0 {
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
	total := liteProjectPage(lite)

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
	// 信封 4：Grep —— stdout + 被截掉的 5 条 file_matches（"" 的 stderr 不换形状）。
	wantGrep := strLen(f.body["grepStdout"])
	for _, m := range f.matches[20:] {
		wantGrep += len(mustJSON(t, m))
	}
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
	// 信封 8：plan_content 是唯一字段，整条不该被动 → 无标记。
	if _, ok := liteStampOf(t, lite, 8); ok {
		t.Error("PlanReady 信封被打上了 lite 标记")
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
	if int64(sum) != total {
		t.Errorf("omittedBytes = %d, want %d", total, sum)
	}
}

// [C]3 保留清单：数字/布尔标量、file_matches[].path、plan_content、
// _meta 归属键、toolCallId/status/kind/title/locations 一字未动。
func TestLiteKeepsPreserveList(t *testing.T) {
	f := liteFixtureOf(t)
	full := litePageOf(t, f.lines)
	lite := litePageOf(t, f.lines)
	liteProjectPage(lite)

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
	if len(liteMeta) != len(fullMeta)+1 {
		t.Errorf("update._meta 键数 = %d, want %d（只多 lite）", len(liteMeta), len(fullMeta)+1)
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
	for _, tc := range []struct{ field, bodyKey string }{
		{"content", "fileContent"},
		{"content_concise", "fileConcise"},
	} {
		want := strOf(t, f.body[tc.bodyKey])
		if file[tc.field] == nil || !isOmittedStub(file[tc.field], want) {
			t.Errorf("Read rawOutput.file.%v 没换成 {omitted:%d}：%v", tc.field, want, file[tc.field])
		}
	}
	// Grep：前 20 条 path 原样，尾部 omittedCount，计数标量不动。
	grepRO := liteUpdate(t, lite, 4)["rawOutput"].(map[string]any)
	if grepRO["match_count"] != float64(25) || grepRO["total_lines"] != float64(25) || grepRO["mode"] != "content" || grepRO["stderr"] != "" {
		t.Errorf("Grep 计数/短文本被改: %v", grepRO)
	}
	kept, _ := json.Marshal(grepRO["file_matches"])
	var items []map[string]any
	if err := json.Unmarshal(kept, &items); err != nil {
		t.Fatalf("file_matches 解码: %v", err)
	}
	if len(items) != liteMaxFileMatches+1 {
		t.Fatalf("file_matches 长度 = %d, want %d（20 条 + omittedCount）", len(items), liteMaxFileMatches+1)
	}
	for i := 0; i < liteMaxFileMatches; i++ {
		want := f.matches[i].(map[string]any)
		if items[i]["path"] != want["path"] {
			t.Errorf("file_matches[%d].path 被改: %v, want %v", i, items[i]["path"], want["path"])
		}
		if !reflect.DeepEqual(items[i]["line_numbers"], want["line_numbers"]) {
			t.Errorf("file_matches[%d].line_numbers 被改: %v", i, items[i]["line_numbers"])
		}
	}
	if items[liteMaxFileMatches]["omittedCount"] != float64(5) {
		t.Errorf("file_matches 尾部 = %v, want {omittedCount:5}", items[liteMaxFileMatches])
	}
	// Edit：insertions/deletions 与 not_found 短文本原样，正文换摘要。
	editRO := liteUpdate(t, lite, 5)["rawOutput"].(map[string]any)
	if editRO["insertions"] != float64(12) || editRO["deletions"] != float64(3) || editRO["edit_count"] != float64(1) {
		t.Errorf("Edit 数字标量被改: %v", editRO)
	}
	if editRO["not_found"] != "NotFound: 没有第二处" || editRO["file_path"] != "/ws/pkg/main.go" || editRO["type"] != "EditsApplied" {
		t.Errorf("Edit 短文本/路径被改: %v", editRO)
	}
	// plan_content 整段留下（保留清单优先于 2048 兜底预算）。
	planRO := liteUpdate(t, lite, 8)["rawOutput"].(map[string]any)
	if planRO["plan_content"] != f.body["planContent"] || planRO["ok"] != true {
		t.Errorf("PlanReady.plan_content 被动过: %v", planRO["plan_content"])
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
	if want := strOf(t, f.body["imageData"]); !isOmittedStub(imgRO["data"], want) {
		t.Errorf("image data 没换成 {omitted:%d}：%v", want, imgRO["data"])
	}
}

func strOf(t *testing.T, s string) int {
	t.Helper()
	return len(s) + 2
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

// [C]1：content 数组整体换成 {type, omitted} 摘要，最多 4 块。
func TestLiteContentSummaryShape(t *testing.T) {
	f := liteFixtureOf(t)
	lite := litePageOf(t, f.lines)
	liteProjectPage(lite)

	upd := liteUpdate(t, lite, 2)
	blocks, ok := upd[kContent].([]any)
	if !ok {
		t.Fatalf("content 不是数组: %T", upd[kContent])
	}
	if len(blocks) != liteMaxContentBlocks {
		t.Fatalf("content 块数 = %d, want %d", len(blocks), liteMaxContentBlocks)
	}
	for i, b := range blocks {
		block, ok := b.(map[string]any)
		if !ok || len(block) != 2 {
			t.Fatalf("content[%d] = %v, want 只有 type+omitted 两个键", i, b)
		}
		if block["type"] != "content" {
			t.Errorf("content[%d].type = %v, want 原块的 type", i, block["type"])
		}
		if want := len(mustJSON(t, f.blocks[i])); block["omitted"] != want {
			t.Errorf("content[%d].omitted = %v, want 原块序列化字节 %d", i, block["omitted"], want)
		}
	}
	if _, ok := upd["locations"].([]any); !ok {
		t.Errorf("locations 被改: %v", upd["locations"])
	}
	stamp, ok := liteStampOf(t, lite, 2)
	if !ok {
		t.Fatal("Bash 信封没打 lite 标记")
	}
	fields, _ := stamp["fields"].([]string)
	if !reflect.DeepEqual(fields, []string{"content", "rawOutput.output", "rawOutput.output_for_prompt"}) {
		t.Errorf("fields = %v, want 排序去重后的三个路径", fields)
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
	lines, ok := ro["lines"].([]any)
	if !ok || len(lines) != len(lengths) {
		t.Fatalf("数组长度被改（条数不许动）: %v", ro["lines"])
	}
	// 最长优先 ⇒ 被换掉的是前 k 项，剩下的仍全是字符串。
	k := 0
	for k < len(lines) {
		if _, isStub := lines[k].(map[string]any); !isStub {
			break
		}
		k++
	}
	if k == 0 || k == len(lines) {
		t.Fatalf("摘要前缀长度 = %d, want 0 < k < %d", k, len(lines))
	}
	for i := k; i < len(lines); i++ {
		if _, isStr := lines[i].(string); !isStr {
			t.Errorf("lines[%d] 不是被保留的字符串而是 %T（顺序被打乱？）", i, lines[i])
		}
	}
	stamp, ok := liteStampOf(t, []any{env}, 0)
	if !ok {
		t.Fatal("没打 lite 标记")
	}
	if fields, _ := stamp["fields"].([]string); !reflect.DeepEqual(fields, []string{"rawOutput.lines[]"}) {
		t.Errorf("fields = %v, want [rawOutput.lines[]]", fields)
	}
}

// 真实工具形状投影后都得落进 2048 兜底预算（plan_content 那条例外：保留
// 清单优先，见 TestLiteKeepsPreserveList）。
func TestLiteRawOutputBudgetOnRealShapes(t *testing.T) {
	f := liteFixtureOf(t)
	lite := litePageOf(t, f.lines)
	liteProjectPage(lite)
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

// [C]6：非工具载体逐字节原样（1MB 的 agent_message_chunk、plan、task_*、
// session_recap、compaction_*、turn_completed）。
func TestLiteNeverTouchesNonToolCarriers(t *testing.T) {
	f := liteFixtureOf(t)
	full := litePageOf(t, f.lines)
	lite := litePageOf(t, f.lines)
	liteProjectPage(lite)
	for _, i := range []int{0, 1, 9, 10, 11, 12, 13, 14} {
		if a, b := mustJSON(t, full[i]), mustJSON(t, lite[i]); !reflect.DeepEqual(a, b) {
			t.Errorf("信封 %d（%v）不是逐字节原样：full %d 字节 / lite %d 字节",
				i, liteUpdate(t, full, i)[kSessionUpdate], len(a), len(b))
		}
	}
}

// 桥级回归：detail 缺省 / full / 未知值 → 与今天的响应逐字节相等，且响应
// 里不存在 lite 痕迹、不回显 projected。
func TestSessionUpdatesDetailFullIsByteIdentical(t *testing.T) {
	f := liteFixtureOf(t)
	home := t.TempDir()
	sid := "sess-1"
	writeSessionFile(t, home, "/ws", sid, f.lines)
	ctx := context.Background()

	b, w := historyBridge(t, home)
	base, err := b.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatalf("基线: %v", err)
	}
	if len(w.lines) != 0 {
		t.Fatal("基线应走本地分页（夹具带 _meta）")
	}
	baseRaw := mustJSON(t, base)
	for _, detail := range []string{"", DetailFull, "LITE", "none", "meta "} {
		b2, _ := historyBridge(t, home)
		got, err := b2.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: detail})
		if err != nil {
			t.Fatalf("detail=%q: %v", detail, err)
		}
		if got.Projected != "" || got.OmittedBytes != 0 {
			t.Errorf("detail=%q 回显了投影: %q/%d", detail, got.Projected, got.OmittedBytes)
		}
		raw := mustJSON(t, got)
		if !reflect.DeepEqual(raw, baseRaw) {
			t.Errorf("detail=%q 与今天的响应不逐字节相等（%d vs %d 字节）", detail, len(raw), len(baseRaw))
		}
		if strings.Contains(string(raw), `"lite"`) {
			t.Errorf("detail=%q 响应里出现了 lite 标记", detail)
		}
	}
}

// 本地分页 + lite：骨架字段与 full 完全一致，工具正文被裁。
func TestSessionUpdatesLiteLocalPageMatchesFull(t *testing.T) {
	f := liteFixtureOf(t)
	home := t.TempDir()
	sid := "sess-1"
	writeSessionFile(t, home, "/ws", sid, f.lines)
	ctx := context.Background()

	b, _ := historyBridge(t, home)
	full, err := b.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	b2, w2 := historyBridge(t, home)
	lite, err := b2.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: DetailLite, TurnIndex: intPtr(2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(w2.lines) != 0 {
		t.Error("lite 本地命中时不该问 agent")
	}
	if lite.Projected != DetailLite || lite.OmittedBytes <= 0 {
		t.Errorf("回显 = %q/%d, want lite/正数", lite.Projected, lite.OmittedBytes)
	}
	if len(lite.Updates) != len(full.Updates) {
		t.Fatalf("条数 = %d, want %d", len(lite.Updates), len(full.Updates))
	}
	if got, want := pageSeqs(t, lite), pageSeqs(t, full); !reflect.DeepEqual(got, want) {
		t.Errorf("msgSeq = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(lite.PromptStarts, full.PromptStarts) || lite.TotalCount != full.TotalCount || lite.HasMore != full.HasMore {
		t.Errorf("锚点被改: promptStarts %v vs %v, total %d vs %d, hasMore %v vs %v",
			lite.PromptStarts, full.PromptStarts, lite.TotalCount, full.TotalCount, lite.HasMore, full.HasMore)
	}
	if got, want := liteSkeleton(t, lite.Updates), liteSkeleton(t, full.Updates); !reflect.DeepEqual(got, want) {
		t.Error("lite 页的骨架与 full 页不同")
	}
	if n, want := len(mustJSON(t, lite.Updates)), len(mustJSON(t, full.Updates)); n >= want {
		t.Errorf("lite 页 %d 字节没有比 full 页 %d 小", n, want)
	}
	// 同一个 bridge 上再来一次全量：投影不影响后续请求（信封每次现读，
	// 归一化缓存里只有行级元数据）。
	again, err := b2.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if a, bRaw := mustJSON(t, full), mustJSON(t, again); !reflect.DeepEqual(a, bRaw) {
		t.Error("投影污染了后续的全量请求（信封不是现读的）")
	}
}

// 回退路径（_x.ai/session/updates 透传）同样投影，且 detail 不下发 agent。
func TestSessionUpdatesLitePassthroughAlsoProjected(t *testing.T) {
	home := t.TempDir()
	sid := "sess-1"
	// 唯一一行信封缺 params._meta.agentTimestampMs → 本地不可用 → 透传。
	writeSessionFile(t, home, "/ws", sid, []string{
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
	})
	lines := liteFixtureOf(t).lines
	page := map[string]any{
		"updates":      litePageOf(t, lines),
		"totalCount":   float64(len(lines)),
		"hasMore":      true,
		"promptStarts": []any{float64(0), float64(9)},
	}
	b, w := historyBridge(t, home)
	ctx := context.Background()
	var got UpdatesPage
	var err error
	off := int64(0)
	runResolved(t, b, w, page, func() error {
		got, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: DetailLite, Offset: &off})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	method, params := lastRequestParams(t, w)
	if method != "_x.ai/session/updates" {
		t.Fatalf("method = %v, want 透传", method)
	}
	if _, ok := params["detail"]; ok {
		t.Errorf("detail 不该下发 agent: %v", params)
	}
	if got.Projected != DetailLite || got.OmittedBytes <= 0 {
		t.Errorf("透传路径回显 = %q/%d, want lite/正数", got.Projected, got.OmittedBytes)
	}
	if _, ok := liteStampOf(t, got.Updates, 2); !ok {
		t.Error("透传路径的工具体没被投影")
	}
	if want := liteSkeleton(t, litePageOf(t, lines)); !reflect.DeepEqual(liteSkeleton(t, got.Updates), want) {
		t.Error("透传路径的 lite 页骨架与 full 不同")
	}
	if !reflect.DeepEqual(got.PromptStarts, []int{0, 9}) || got.TotalCount != len(lines) || !got.HasMore {
		t.Errorf("锚点被改: %v %d %v", got.PromptStarts, got.TotalCount, got.HasMore)
	}
}

// meta 档：不回 updates（本地与透传两条出口都不回），锚点语义不变。
func TestSessionUpdatesMetaModeDropsUpdates(t *testing.T) {
	f := liteFixtureOf(t)
	home := t.TempDir()
	sid := "sess-1"
	writeSessionFile(t, home, "/ws", sid, f.lines)
	ctx := context.Background()

	b, w := historyBridge(t, home)
	meta, err := b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: DetailMeta})
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Error("meta 档本地可用时不该问 agent（信封一行都不必读）")
	}
	if meta.Updates != nil {
		t.Errorf("meta 档仍回了 %d 条信封", len(meta.Updates))
	}
	if meta.Projected != DetailMeta {
		t.Errorf("projected = %q, want meta", meta.Projected)
	}
	b2, _ := historyBridge(t, home)
	full, err := b2.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TotalCount != full.TotalCount || meta.HasMore != full.HasMore || !reflect.DeepEqual(meta.PromptStarts, full.PromptStarts) {
		t.Errorf("锚点不等: total %d vs %d, hasMore %v vs %v, promptStarts %v vs %v",
			meta.TotalCount, full.TotalCount, meta.HasMore, full.HasMore, meta.PromptStarts, full.PromptStarts)
	}
	if !reflect.DeepEqual(meta.Btw, full.Btw) {
		t.Errorf("btw 语义变了: %v vs %v", meta.Btw, full.Btw)
	}

	// 透传出口的 meta：agent 照旧回信封，host 丢弃。
	lines := liteFixtureOf(t).lines
	page := map[string]any{
		"updates":      litePageOf(t, lines),
		"totalCount":   float64(len(lines)),
		"hasMore":      false,
		"promptStarts": []any{float64(0)},
	}
	writeSessionFile(t, home, "/other", sid, []string{
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
	})
	b3, w3 := historyBridge(t, home)
	var got UpdatesPage
	runResolved(t, b3, w3, page, func() error {
		var err error
		got, err = b3.SessionUpdates(ctx, sid, "/other", SessionUpdatesOpts{Detail: DetailMeta})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Updates != nil || got.Projected != DetailMeta {
		t.Errorf("透传 meta: updates=%v projected=%q", got.Updates, got.Projected)
	}
	if got.TotalCount != len(lines) || !reflect.DeepEqual(got.PromptStarts, []int{0}) {
		t.Errorf("透传 meta 锚点被改: %d %v", got.TotalCount, got.PromptStarts)
	}
	if method, _ := lastRequestParams(t, w3); method != "_x.ai/session/updates" {
		t.Errorf("meta 档仍该走同一条透传，method = %v", method)
	}
}

// stream=true 不参与投影：响应里没有可裁的页，也不回显 projected。
func TestSessionUpdatesStreamIgnoresDetail(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	var got UpdatesPage
	var err error
	runResolved(t, b, w, map[string]any{"totalCount": float64(3), "chunkCount": float64(1)}, func() error {
		got, err = b.SessionUpdates(ctx, "s1", "/ws", SessionUpdatesOpts{Stream: true, Detail: DetailLite})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Projected != "" || got.OmittedBytes != 0 {
		t.Errorf("stream 页回显了投影: %q/%d", got.Projected, got.OmittedBytes)
	}
	method, params := lastRequestParams(t, w)
	if method != "_x.ai/session/updates" || params["stream"] != true {
		t.Errorf("stream 请求被改: %v %v", method, params)
	}
	if _, ok := params["detail"]; ok {
		t.Errorf("detail 不该下发 agent: %v", params)
	}
}

// TestLiteCollapsesArrayBodies：真实 Bash/Grep 落盘的 output 与 stdout 是
// **行数组**（不是字符串）——实测某会话单页里一页 6.3MB 有 5.2MB 堆在一个
// Bash 数组上，只裁字符串会整类漏掉。数组/对象形状的正文键必须整棵换成一个
// 摘要，而不是逐元素留下上千个 {"omitted":N} 壳。
func TestLiteCollapsesArrayBodies(t *testing.T) {
	sid := "sess-array"
	single := histEnvelope(sid, 0, 1700, map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    "call-array",
		"status":        "completed",
		"kind":          "execute",
		"title":         "make",
		"rawOutput": map[string]any{
			"type":      "Bash",
			"output":    repeatLiteStrs("line-", 1200),
			"stdout":    repeatLiteStrs("out-", 900),
			"exit_code": float64(0),
		},
		"_meta": liteToolMeta(),
	})
	lite := litePageOf(t, []string{single})
	if liteProjectPage(lite) == 0 {
		t.Fatal("数组正文一个字都没裁")
	}
	ro := liteUpdate(t, lite, 0)["rawOutput"].(map[string]any)
	if ro["exit_code"] != float64(0) {
		t.Errorf("标量被改: %v", ro)
	}
	for _, key := range []string{"output", "stdout"} {
		stub, ok := ro[key].(map[string]any)
		if !ok {
			t.Fatalf("rawOutput.%v 没换成摘要对象（数组形状漏裁）: %T", key, ro[key])
		}
		n, isNum := stub["omitted"].(int)
		if !isNum || n <= 0 || len(stub) != 1 {
			t.Fatalf("rawOutput.%v 摘要形状不对: %v", key, stub)
		}
	}
	if _, ok := liteUpdate(t, lite, 0)[kMeta].(map[string]any)["lite"]; !ok {
		t.Error("数组正文被裁却没打 lite 标记")
	}
	// 幂等：再投影一次不得把摘要再摘要一遍。
	after, _ := json.Marshal(lite)
	if again := liteProjectPage(lite); again != 0 {
		t.Errorf("重复投影 omitted = %d, want 0", again)
	}
	if now, _ := json.Marshal(lite); !bytes.Equal(now, after) {
		t.Error("重复投影改动了信封")
	}
}

// repeatLiteStrs 造一个行数组（Bash output 的真实形状）。
func repeatLiteStrs(prefix string, n int) []any {
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, prefix+strings.Repeat("x", 40))
	}
	return out
}
