package acp

import (
	"strings"
	"testing"
)

// lite_fold_test.go — 折叠行行头数字（契约 lite-replay [C]7）：lite 删正文前
// 把 edit 的加减行数、grep 的命中文件数折进 _meta.lite，前端在全量补回来
// 之前照样能画 (+N/−M) / (N matches in M files)。计数口径必须与前端
// extractEditHunks + simpleDiffLines 一致，否则补全回来后数字会跳。

func foldEnv(sid string, seq int64, upd map[string]any) string {
	return histEnvelope(sid, seq, 1000+seq, upd)
}

func diffBlock(oldText, newText string) map[string]any {
	return map[string]any{
		"type":    "diff",
		"path":    "/ws/main.go",
		"oldText": oldText,
		"newText": newText,
	}
}

// liteFoldStatsOf 投影一页并回看指定信封的 _meta.lite。
func liteFoldStatsOf(t *testing.T, lines []string) map[string]any {
	t.Helper()
	page := litePageOf(t, lines)
	liteProjectPage(&page)
	stamp, ok := liteStampOf(t, page, len(lines)-1)
	if !ok {
		t.Fatalf("信封没有 lite 标记: %v", liteUpdate(t, page, len(lines)-1))
	}
	return stamp
}

func editsOf(t *testing.T, stamp map[string]any) (int, int) {
	t.Helper()
	m, ok := stamp["edits"].(map[string]any)
	if !ok {
		t.Fatalf("标记里没有 edits: %v", stamp)
	}
	return liteAsInt(m["ins"]), liteAsInt(m["del"])
}

func editEnvelope(upd map[string]any) map[string]any {
	base := map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    "tool-edit",
		"status":        "completed",
		"kind":          "edit",
		"title":         "Edit main.go",
		"_meta":         liteToolMeta(),
	}
	for k, v := range upd {
		base[k] = v
	}
	return base
}

func TestLiteEditFoldCountsFollowFrontendDiffSemantics(t *testing.T) {
	cases := []struct {
		name             string
		oldText, newText string
		ins, del         int
	}{
		{"纯插入", "", "x\ny", 2, 0},
		{"纯删除", "x\ny", "", 0, 2},
		{"公共行只数真正变化的", "a\nb\nc", "a\nx\nc", 1, 1},
		{"整段重写", "a\nb", "c\nd\ne", 3, 2},
		{"行尾换行不算多一行", "x\ny\n", "x\nz\n", 1, 1},
		{"超 400 行退回整段口径", strings.Repeat("u\n", 300), strings.Repeat("v\n", 200), 200, 300},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stamp := liteFoldStatsOf(t, []string{foldEnv("s", 0, editEnvelope(map[string]any{
				"content": []any{diffBlock(tc.oldText, tc.newText)},
			}))})
			ins, del := editsOf(t, stamp)
			if ins != tc.ins || del != tc.del {
				t.Errorf("edits = {%d,%d}, want {%d,%d}", ins, del, tc.ins, tc.del)
			}
		})
	}
}

func TestLiteEditFoldCountsStructuredDetails(t *testing.T) {
	stamp := liteFoldStatsOf(t, []string{foldEnv("s", 0, editEnvelope(map[string]any{
		"rawOutput": map[string]any{
			"EditsApplied": map[string]any{
				"absolute_path": "/ws/main.go",
				"edits": map[string]any{
					"details": []any{
						map[string]any{"old_string": "a\nb\nc", "new_string": "a\nx\nc", "old_line": 1.0},
						map[string]any{"old_string": "q", "new_string": "q\nr", "old_line": 9.0},
					},
				},
			},
		},
	}))})
	ins, del := editsOf(t, stamp)
	// 两处 hunk 累加：第一处 +1/−1，第二处 +1/−0。
	if ins != 2 || del != 1 {
		t.Errorf("details 口径 edits = {%d,%d}, want {2,1}", ins, del)
	}
}

// 内部 tag 形状（{"type":"SearchReplace","EditsApplied":…}）前端走不到结构化
// 分支、只认 content 的 diff 块。lite 必须同样算不出数就不标——否则行头会在
// 全量档里根本看不到的位置上凭空多数字。
func TestLiteEditFoldSkipsInternallyTaggedDetailsWithoutDiff(t *testing.T) {
	page := litePageOf(t, []string{foldEnv("s", 0, editEnvelope(map[string]any{
		"rawOutput": map[string]any{
			"type": "SearchReplace",
			"EditsApplied": map[string]any{
				"edits": map[string]any{
					"details": []any{map[string]any{"old_string": "a", "new_string": "b"}},
				},
			},
		},
	}))})
	liteProjectPage(&page)
	stamp, _ := liteStampOf(t, page, 0)
	if _, ok := stamp["edits"]; ok {
		t.Errorf("内部 tag + 无 diff 块不该折 edits: %v", stamp)
	}
}

func TestLiteEditFoldSkippedForFailedAndPlainTools(t *testing.T) {
	failed := liteFoldStatsOf(t, []string{foldEnv("s", 0, editEnvelope(map[string]any{
		"status":  "failed",
		"content": []any{diffBlock("a", "b\nb")},
	}))})
	if _, ok := failed["edits"]; ok {
		t.Errorf("失败编辑不该折 edits（前端按 error 渲染行头）: %v", failed["edits"])
	}

	bash := litePageOf(t, []string{foldEnv("s", 0, map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    "tool-bash",
		"status":        "completed",
		"kind":          "execute",
		"title":         "npm test",
		"content":       []any{map[string]any{"type": "content", "content": map[string]any{"type": "text", "text": "ok"}}},
		"rawOutput":     map[string]any{"exit_code": 0.0, "output": strings.Repeat("o", 900)},
		"_meta":         liteToolMeta(),
	})})
	liteProjectPage(&bash)
	stamp, _ := liteStampOf(t, bash, 0)
	if _, ok := stamp["edits"]; ok {
		t.Errorf("非编辑工具不该折 edits: %v", stamp)
	}
}

func TestLiteGrepFoldFileCount(t *testing.T) {
	page := litePageOf(t, []string{foldEnv("s", 0, map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    "tool-grep",
		"status":        "completed",
		"kind":          "search",
		"title":         "grep foo",
		"rawOutput": map[string]any{
			"type": "GrepSearch",
			"file_matches": []any{
				map[string]any{"path": "/ws/a.go", "matches": []any{map[string]any{"line_number": 1.0, "content": "foo"}}},
				map[string]any{"path": "/ws/b.go", "matches": []any{map[string]any{"line_number": 2.0, "content": "foo"}}},
				map[string]any{"path": "/ws/c.go", "matches": []any{map[string]any{"line_number": 3.0, "content": "foo"}}},
			},
			"match_count": 25.0,
			"stdout":      strings.Repeat("s", 3000),
		},
		"rawInput": map[string]any{"pattern": "foo", "path": "/ws"},
		"_meta":    liteToolMeta(),
	})})
	liteProjectPage(&page)
	stamp, ok := liteStampOf(t, page, 0)
	if !ok {
		t.Fatalf("grep 信封没有 lite 标记")
	}
	if got := liteAsInt(stamp["files"]); got != 3 {
		t.Errorf("files = %d, want 3", got)
	}
	// match_count 是行头摘要的分子，必须原样留下。
	ro := liteUpdate(t, page, 0)["rawOutput"].(map[string]any)
	if got := liteAsInt(ro["match_count"]); got != 25 {
		t.Errorf("match_count = %v, want 25 原样", ro["match_count"])
	}
	if _, still := ro["file_matches"]; still {
		t.Errorf("file_matches 该被裁掉，只剩计数")
	}
}

func TestLiteFoldStatsLastEnvelopeWinsOnCoalesce(t *testing.T) {
	first := foldEnv("s", 0, editEnvelope(map[string]any{
		"sessionUpdate": "tool_call",
		"status":        nil,
		"content":       []any{diffBlock("a\nb", "a\nx")},
	}))
	second := foldEnv("s", 1, editEnvelope(map[string]any{
		"content": []any{diffBlock("1\n2\n3", "1\n9\n8\n7")},
	}))
	page := litePageOf(t, []string{first, second})
	liteProjectPage(&page)
	if len(page) != 1 {
		t.Fatalf("同 id 信封该合成一封，剩 %d", len(page))
	}
	stamp, _ := liteStampOf(t, page, 0)
	ins, del := editsOf(t, stamp)
	// 前端整包替换 content：后一封才是行头该显示的数字（1 行公共 + 3 增 2 删），
	// 累加会把同一次编辑数两遍。
	if ins != 3 || del != 2 {
		t.Errorf("edits = {%d,%d}, want {3,2}（后到覆盖先到）", ins, del)
	}
}

func TestLiteFoldMarkerBytesNotCountedInOmitted(t *testing.T) {
	block := diffBlock("a\nb\nc", "a\nx\nc")
	lines := []string{foldEnv("s", 0, editEnvelope(map[string]any{
		"content": []any{block},
	}))}
	page := litePageOf(t, lines)
	liteProjectPage(&page)
	stamp, _ := liteStampOf(t, page, 0)
	if got, want := liteAsInt(stamp["omitted"]), liteJSONLen(block); got != want {
		t.Errorf("omitted = %d, want %d（标记自身的字节不该算进被裁正文）", got, want)
	}
}

func TestLiteKeepsMCPToolName(t *testing.T) {
	page := litePageOf(t, []string{foldEnv("s", 0, map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "tool-mcp",
		"kind":          "use_tool",
		"title":         "use_tool",
		"rawInput": map[string]any{
			"tool_name":  "tasks__list",
			"tool_input": map[string]any{"limit": 50, "blob": strings.Repeat("x", 5000)},
		},
		"_meta": liteToolMeta(),
	})})
	liteProjectPage(&page)
	ri := liteUpdate(t, page, 0)["rawInput"].(map[string]any)
	if got, _ := ri["tool_name"].(string); got != "tasks__list" {
		t.Errorf("tool_name = %v，行头动作名要靠它", ri["tool_name"])
	}
	if _, ok := ri["tool_input"]; ok {
		t.Errorf("tool_input 属正文，该继续裁掉")
	}
}
