package acp

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

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
	if liteProjectPage(&lite) == 0 {
		t.Fatal("数组正文一个字都没裁")
	}
	ro := liteUpdate(t, lite, 0)["rawOutput"].(map[string]any)
	if ro["exit_code"] != float64(0) {
		t.Errorf("标量被改: %v", ro)
	}
	for _, key := range []string{"output", "stdout"} {
		if _, ok := ro[key]; ok {
			t.Fatalf("rawOutput.%v 还在（数组正文应整键删除）: %v", key, ro[key])
		}
	}
	if _, ok := liteUpdate(t, lite, 0)[kMeta].(map[string]any)["lite"]; !ok {
		t.Error("数组正文被裁却没打 lite 标记")
	}
	// 幂等：再投影一次不得把摘要再摘要一遍。
	after, _ := json.Marshal(lite)
	if again := liteProjectPage(&lite); again != 0 {
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

func TestLiteCoalescesSameToolCall(t *testing.T) {
	sid := "sess-coalesce"
	lines := []string{
		histEnvelope(sid, 0, 1000, msgUserChunkMeta("跑", map[string]any{"promptIndex": float64(0)})),
		histEnvelope(sid, 1, 1100, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "call-1",
			"status":        "in_progress",
			"kind":          "execute",
			"title":         "ls",
			"rawInput":      map[string]any{"command": "ls"},
		}),
		histEnvelope(sid, 2, 1200, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "call-1",
			"status":        "completed",
			"rawOutput":     map[string]any{"exit_code": float64(0), "output": strings.Repeat("o", 9000)},
		}),
		histEnvelope(sid, 3, 1300, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": "好了"},
		}),
	}
	lite := litePageOf(t, lines)
	if liteProjectPage(&lite) == 0 {
		t.Fatal("没裁")
	}
	if len(lite) != 3 {
		t.Fatalf("条数 = %d, want 3（user + 合成工具 + agent）", len(lite))
	}
	upd := liteUpdate(t, lite, 1)
	if upd["sessionUpdate"] != "tool_call" {
		t.Errorf("幸存者 kind = %v, want 首封 tool_call", upd["sessionUpdate"])
	}
	if upd["status"] != "completed" {
		t.Errorf("没合并终态 status: %v", upd["status"])
	}
	if upd["toolCallId"] != "call-1" {
		t.Errorf("toolCallId = %v", upd["toolCallId"])
	}
	ro, _ := upd["rawOutput"].(map[string]any)
	if ro["exit_code"] != float64(0) {
		t.Errorf("exit_code 丢了: %v", ro)
	}
	if _, ok := ro["output"]; ok {
		t.Errorf("output 还在: %v", ro["output"])
	}
	stamp, ok := liteStampOf(t, lite, 1)
	if !ok {
		t.Fatal("没打 lite")
	}
	if liteAsInt(stamp["msgSeqEnd"]) != 2 {
		t.Errorf("msgSeqEnd = %v, want 2", stamp["msgSeqEnd"])
	}
	if liteEnvSeq(lite[1].(map[string]any)) != 1 {
		t.Errorf("幸存者 msgSeq = %v, want 1", lite[1].(map[string]any)["msgSeq"])
	}
}

func TestLiteCoalescesThoughtRun(t *testing.T) {
	sid := "sess-th"
	lines := []string{
		histEnvelope(sid, 0, 1000, msgUserChunk("q")),
		histEnvelope(sid, 1, 1100, map[string]any{
			"sessionUpdate": "agent_thought_chunk",
			"content":       map[string]any{"type": "text", "text": strings.Repeat("a", 800)},
		}),
		histEnvelope(sid, 2, 1200, map[string]any{
			"sessionUpdate": "agent_thought_chunk",
			"content":       map[string]any{"type": "text", "text": strings.Repeat("b", 900)},
		}),
		histEnvelope(sid, 3, 1300, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": "答"},
		}),
	}
	lite := litePageOf(t, lines)
	liteProjectPage(&lite)
	if len(lite) != 3 {
		t.Fatalf("条数 = %d, want 3（user + 合成 thought + agent）", len(lite))
	}
	upd := liteUpdate(t, lite, 1)
	if upd["sessionUpdate"] != "agent_thought_chunk" {
		t.Fatalf("kind = %v", upd["sessionUpdate"])
	}
	c, _ := upd[kContent].(map[string]any)
	if c[kType] != "text" {
		t.Errorf("thought stub type = %v", c[kType])
	}
	if liteAsInt(c["omitted"]) <= 0 {
		t.Errorf("thought stub omitted = %v", c["omitted"])
	}
	stamp, ok := liteStampOf(t, lite, 1)
	if !ok {
		t.Fatal("thought 没打 lite")
	}
	if liteAsInt(stamp["msgSeqEnd"]) != 2 {
		t.Errorf("thought msgSeqEnd = %v, want 2", stamp["msgSeqEnd"])
	}
	agent := liteUpdate(t, lite, 2)
	if agent[kContent].(map[string]any)["text"] != "答" {
		t.Errorf("agent 正文被改: %v", agent[kContent])
	}
}

func TestLiteEmptyToolCallIdNotCoalesced(t *testing.T) {
	sid := "sess-empty"
	lines := []string{
		histEnvelope(sid, 0, 1000, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "a",
			"rawOutput":     map[string]any{"output": strings.Repeat("x", 900)},
		}),
		histEnvelope(sid, 1, 1100, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "b",
			"rawOutput":     map[string]any{"output": strings.Repeat("y", 900)},
		}),
	}
	lite := litePageOf(t, lines)
	liteProjectPage(&lite)
	if len(lite) != 2 {
		t.Fatalf("空 id 被合成了：条数 = %d, want 2", len(lite))
	}
	if liteUpdate(t, lite, 0)["title"] != "a" || liteUpdate(t, lite, 1)["title"] != "b" {
		t.Errorf("空 id 工具被并到一起: %v / %v", liteUpdate(t, lite, 0)["title"], liteUpdate(t, lite, 1)["title"])
	}
}

// thoughtChunkEnv 造一条连续 agent_thought_chunk 信封（正文足够大才会被裁成
// {type, omitted} 占位）。msgSeq 由 litePageOf 按下标盖。
func thoughtChunkEnv(sid string, text string) string {
	return histEnvelope(sid, 0, 1000, map[string]any{
		"sessionUpdate": "agent_thought_chunk",
		"content":       map[string]any{"type": "text", "text": text},
	})
}

// 连续多条 thought chunk 合成一封后，幸存信封必须带 msgSeqEnd = 末条 msgSeq，
// 否则 FE 只能按 [msgSeq, msgSeq] 回拉第一条 chunk，展开后思考缺尾巴。
func TestLiteThoughtCoalesceStampsMsgSeqEnd(t *testing.T) {
	const sid = "sess-th"
	big := func(r rune) string { return strings.Repeat(string(r), 300) }
	f := &liteFixture{
		lines: []string{
			histEnvelope(sid, 0, 1000, msgUserChunkMeta("问", map[string]any{"promptIndex": float64(0)})),
			thoughtChunkEnv(sid, big('甲')), // msgSeq 1
			thoughtChunkEnv(sid, big('乙')), // msgSeq 2
			thoughtChunkEnv(sid, big('丙')), // msgSeq 3
			thoughtChunkEnv(sid, big('丁')), // msgSeq 4
			histEnvelope(sid, 0, 1000, map[string]any{
				"sessionUpdate": "agent_message_chunk",
				"content":       map[string]any{"type": "text", "text": "答"},
			}), // msgSeq 5
		},
		body: map[string]string{},
	}
	page := litePageOf(t, f.lines)
	liteProjectPage(&page)

	// 4 条连续 thought 合成成 1 条 → 总条数应从 6 降到 3。
	if len(page) != 3 {
		t.Fatalf("合成后条数 = %d, want 3（user + 1 thought + agent）", len(page))
	}
	stamp, ok := liteStampOf(t, page, 1)
	if !ok {
		t.Fatalf("thought 幸存信封没有 lite 标记: %v", liteUpdate(t, page, 1))
	}
	if got := liteAsInt(stamp["omitted"]); got <= 0 {
		t.Errorf("thought lite.omitted = %d, want > 0", got)
	}
	end, hasEnd := stamp["msgSeqEnd"]
	if !hasEnd {
		t.Fatalf("thought 幸存信封缺 msgSeqEnd（FE 会把窗口塌成第 1 条 chunk）: stamp=%v", stamp)
	}
	if got := liteAsInt(end); got != 4 {
		t.Errorf("thought msgSeqEnd = %d, want 4（末条 chunk 的 msgSeq）", got)
	}
}
