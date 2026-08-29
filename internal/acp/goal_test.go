package acp

import (
	"testing"
)

func TestAnalyzeGoalText(t *testing.T) {
	cases := []struct {
		name                   string
		text                   string
		claim, block, evidence bool
	}{
		{
			name:     "explicit completion with evidence",
			text:     "目标已完成。修改了 src/api/client.ts 和 src/store/chat.ts，运行 npm run build 通过，测试全部 passed。",
			claim:    true,
			evidence: true,
		},
		{
			name:     "explicit completion without evidence",
			text:     "好的，目标已完成。",
			claim:    true,
			evidence: false,
		},
		{
			name:     "not complete stays open",
			text:     "目前尚未完成，还在处理编译错误，继续推进。",
			claim:    false,
			evidence: false,
		},
		{
			name:     "blocked",
			text:     "我卡住了：第三方库 API 不兼容，无法继续，需要确认方案。",
			claim:    false,
			block:    true,
			evidence: false,
		},
		{
			name:     "english done with command",
			text:     "Done. Ran go test ./... and cargo build, all passed. Files changed: internal/acp/goal.go",
			claim:    true,
			evidence: true,
		},
		{
			name:     "work in progress",
			text:     "正在重构模块，还剩两个文件没改，继续处理。",
			claim:    false,
			evidence: false,
		},
		{
			name:     "empty",
			text:     "",
			claim:    false,
			evidence: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim, block, evidence := analyzeGoalText(tc.text)
			if claim != tc.claim || block != tc.block || evidence != tc.evidence {
				t.Fatalf("analyzeGoalText(%q) = (%v,%v,%v), want (%v,%v,%v)",
					tc.text, claim, block, evidence, tc.claim, tc.block, tc.evidence)
			}
		})
	}
}

func TestIsUpdateGoalToolCall(t *testing.T) {
	if !isUpdateGoalToolCall(map[string]any{"title": "update_goal"}) {
		t.Fatal("title update_goal should match")
	}
	if !isUpdateGoalToolCall(map[string]any{"kind": "UpdateGoal"}) {
		t.Fatal("kind UpdateGoal should match")
	}
	if !isUpdateGoalToolCall(map[string]any{"title": "Goal: 迁移认证模块"}) {
		t.Fatal("Goal: title should match")
	}
	if isUpdateGoalToolCall(map[string]any{"title": "read_file"}) {
		t.Fatal("read_file should not match")
	}
}

func TestToolRawInput(t *testing.T) {
	// Parsed object passthrough.
	tc := map[string]any{"rawInput": map[string]any{"completed": true}}
	if m, ok := toolRawInput(tc).(map[string]any); !ok || m["completed"] != true {
		t.Fatalf("object rawInput not passed through: %v", toolRawInput(tc))
	}
	// JSON string parsed.
	tc2 := map[string]any{"rawInput": `{"blocked_reason":"stuck"}`}
	if m, ok := toolRawInput(tc2).(map[string]any); !ok || m["blocked_reason"] != "stuck" {
		t.Fatalf("string rawInput not parsed: %v", toolRawInput(tc2))
	}
}

func TestGoalInstructionCarriesObjective(t *testing.T) {
	ins := goalInstruction("迁移认证模块")
	if !contains(ins, "迁移认证模块") {
		t.Fatal("instruction must carry the objective verbatim")
	}
	if !contains(ins, "VERIFY AS YOU GO") {
		t.Fatal("instruction must keep the verify-as-you-go rule")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
