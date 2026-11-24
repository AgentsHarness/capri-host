package acp

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// 验收：/rewind 之后的「死分支」不得再服务出去。agent 1.0.13 的
// x.ai/session/updates 不做这层过滤（updates.jsonl 只追加，rewind_marker
// 之后的回放原样吐回被回退的回合），FE 直接把整轮已回退/已取消的对话重新
// 画出来——host 本地服务必须按标记截断（agent filter_rewind_by 等价）。
func TestSessionUpdatesRewindBranchTruncation(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),   // msgSeq 0（prompt 0 起点）
		histEnvelope(sid, 1, 20, msgAgentChunk("a1")), // msgSeq 1
		histEnvelope(sid, 2, 30, msgUserChunk("B")),   // 死分支：prompt 1
		histEnvelope(sid, 3, 40, msgAgentChunk("b1")), // 死分支
		histEnvelope(sid, 4, 50, msgRewindMarker(1)),  // 回退到 prompt 1 → 截掉 1 及以后
		histEnvelope(sid, 5, 60, msgAgentChunk("c1")), // 回退后新分支的内容
	})
	b, w := historyBridge(t, home)

	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Errorf("本地命中时不得请求 agent RPC（写了 %d 条）", len(w.lines))
	}
	// 存活序列 = A / a1 / c1，msgSeq 密集重排，标记本身不进结果。
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Errorf("截断后 msgSeq = %v, want [0 1 2]", got)
	}
	if got := pageKinds(t, page); !reflect.DeepEqual(got,
		[]string{"user_message_chunk", "agent_message_chunk", "agent_message_chunk"}) {
		t.Errorf("截断后 sessionUpdate = %v, want user/agent/agent（无 rewind_marker）", got)
	}
	if page.TotalCount != 3 {
		t.Errorf("totalCount = %d, want 3（死分支与标记不计入）", page.TotalCount)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("promptStarts = %v, want [0]", page.PromptStarts)
	}

	// turnIndex:1 取最后一轮：存活序列里只有 A 那一轮 → 全量。
	page, err = b.SessionUpdates(context.Background(), sid, "/ws", SessionUpdatesOpts{TurnIndex: intPtr(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Errorf("turnIndex:1 msgSeq = %v, want [0 1 2]", got)
	}
}

// 验收：target 越界（多分支历史里文件序 prompt 与逻辑 prompt 对不上）时
// 宁多留不误删——与 agent 的 fold-to-len(result) 兜底一致，但标记本身仍被丢弃。
func TestSessionUpdatesRewindCutsAtPromptIndexBoundary(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunkMeta("A", map[string]any{"promptIndex": 0})),
		histEnvelope(sid, 1, 20, msgUserChunkMeta("B", map[string]any{"promptIndex": 1})),
		histEnvelope(sid, 2, 25, msgAgentChunk("b1")),
		histEnvelope(sid, 3, 30, msgRewindMarker(1)),
		histEnvelope(sid, 4, 40, msgUserChunkMeta("C", map[string]any{"promptIndex": 1})),
		histEnvelope(sid, 5, 50, msgAgentChunk("c1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if got := pageKinds(t, page); !reflect.DeepEqual(got,
		[]string{"user_message_chunk", "user_message_chunk", "agent_message_chunk"}) {
		t.Errorf("截断后 kinds = %v, want A / C / c1（B 支被剪掉）", got)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 1}) {
		t.Errorf("promptStarts = %v, want [0 1]", page.PromptStarts)
	}
	if page.TotalCount != 3 {
		t.Errorf("totalCount = %d, want 3", page.TotalCount)
	}
}

func TestSessionUpdatesRewindOutOfRangeKeepsBranch(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
		histEnvelope(sid, 1, 20, msgUserChunk("B")),
		histEnvelope(sid, 2, 30, msgRewindMarker(7)),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1}) {
		t.Errorf("越界 target msgSeq = %v, want [0 1]（保留两支，仅丢标记）", got)
	}
	// A、B 连续无 promptIndex → 同一 run，只有一个起点。
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("promptStarts = %v, want [0]", page.PromptStarts)
	}
}

// 验收：promptPreviews 与 promptStarts 平行且对齐（轮次目录）——
// 多行取首行、图块 run 取后续文本 chunk、displayText 优先、见过
// promptIndex 标记后的无标记 run（幽灵 run）不计入也不出预览。
func TestSessionUpdatesPromptPreviews(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		// run A：多行文本 → 首行。
		histEnvelope(sid, 0, 100, msgUserChunk("A 第一行\n第二行")),
		histEnvelope(sid, 1, 200, msgAgentChunk("a1")),
		// run B：先图块后文本（连续 user chunk = 同一 run）→ 文本来自第二个 chunk。
		histEnvelope(sid, 2, 300, map[string]any{
			"sessionUpdate": "user_message_chunk",
			"content":       []any{map[string]any{"type": "image", "data": "data:image/png;base64,AAA"}},
		}),
		histEnvelope(sid, 3, 400, map[string]any{
			"sessionUpdate": "user_message_chunk",
			"content":       []any{map[string]any{"type": "text", "text": "B 文本"}},
		}),
		histEnvelope(sid, 4, 500, msgAgentChunk("b1")),
		// run C：content 对象带 _meta.displayText → 显示文案优先于正文。
		histEnvelope(sid, 5, 600, map[string]any{
			"sessionUpdate": "user_message_chunk",
			"content": map[string]any{
				"type":  "text",
				"text":  "C 原文（不进预览）",
				"_meta": map[string]any{"displayText": "C 显示文案"},
			},
		}),
		histEnvelope(sid, 6, 700, msgAgentChunk("c1")),
		// run D：带 promptIndex 的 run（见过标记后的计数 run）。
		histEnvelope(sid, 7, 800, msgUserChunkMeta("D 有标记", map[string]any{"promptIndex": 3})),
		// run E：见过 promptIndex 标记后的无标记 run → 幽灵 run，不计回合。
		histEnvelope(sid, 8, 900, msgUserChunk("E 幽灵")),
		histEnvelope(sid, 9, 1000, msgAgentChunk("e1")),
	})
	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("本地命中时不得请求 agent RPC（写了 %d 条）", len(w.lines))
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 2, 5, 7}) {
		t.Fatalf("promptStarts = %v, want [0 2 5 7]（E 幽灵不计入）", page.PromptStarts)
	}
	want := []string{"A 第一行", "B 文本", "C 显示文案", "D 有标记"}
	if !reflect.DeepEqual(page.PromptPreviews, want) {
		t.Errorf("promptPreviews = %v, want %v", page.PromptPreviews, want)
	}
	if len(page.PromptPreviews) != len(page.PromptStarts) {
		t.Errorf("promptPreviews 必须与 promptStarts 平行：%d vs %d", len(page.PromptPreviews), len(page.PromptStarts))
	}
}

// 验收：预览截断——单行超长按 tocPreviewMaxBytes 收口，且绝不切出半个
// UTF-8 字符；多 chunk 拆分（文本跨 chunk 分布）时取拼接后的首行。
func TestSessionUpdatesPromptPreviewsTruncated(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	longLine := strings.Repeat("长", 600) // 600 rune × 3B = 1800B > 512
	// 跨 chunk 拆分：第一个 chunk 在行中间截断（无换行），第二个 chunk 补
	// 上后半段——拼接后首行 = 完整长行。
	half := len(longLine) / 2
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk(longLine[:half])),
		histEnvelope(sid, 1, 110, msgUserChunk(longLine[half:])),
		histEnvelope(sid, 2, 200, msgAgentChunk("a1")),
		histEnvelope(sid, 3, 300, msgUserChunk("多行\n第二行")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 3}) {
		t.Fatalf("promptStarts = %v, want [0 3]", page.PromptStarts)
	}
	if len(page.PromptPreviews) != 2 {
		t.Fatalf("promptPreviews = %d 条, want 2", len(page.PromptPreviews))
	}
	p0 := page.PromptPreviews[0]
	if !utf8.ValidString(p0) {
		t.Errorf("预览切出了半个 UTF-8 字符: %q", p0)
	}
	if len(p0) > tocPreviewMaxBytes {
		t.Errorf("预览 %d 字节 > %d", len(p0), tocPreviewMaxBytes)
	}
	if !strings.HasPrefix(p0, "长长长") {
		t.Errorf("截断预览应保留行首: %q", p0)
	}
	if page.PromptPreviews[1] != "多行" {
		t.Errorf("多行预览 = %q, want 多行", page.PromptPreviews[1])
	}
}

// 验收：rewind 截断后 promptPreviews 只覆盖存活轮次（与 promptStarts 同步
// 对齐，死分支预览不得出现）。
func TestSessionUpdatesPromptPreviewsAfterRewind(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
		histEnvelope(sid, 1, 20, msgAgentChunk("a1")),
		histEnvelope(sid, 2, 30, msgUserChunk("B 死分支")),
		histEnvelope(sid, 3, 40, msgAgentChunk("b1")),
		histEnvelope(sid, 4, 50, msgRewindMarker(1)),
		histEnvelope(sid, 5, 60, msgAgentChunk("c1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Fatalf("promptStarts = %v, want [0]", page.PromptStarts)
	}
	if !reflect.DeepEqual(page.PromptPreviews, []string{"A"}) {
		t.Errorf("rewind 后 promptPreviews = %v, want [A]（死分支预览不得出现）", page.PromptPreviews)
	}
}

func TestSessionUpdatesRewindMarkerInToolOutputIgnored(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
		histEnvelope(sid, 1, 20, msgAgentChunk(`see rewind_marker in docs`)),
		histEnvelope(sid, 2, 30, msgUserChunk("B")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalCount != 3 {
		t.Errorf("正文里的 rewind_marker 不得当标记截断，totalCount = %d, want 3", page.TotalCount)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 2}) {
		t.Errorf("promptStarts = %v, want [0 2]", page.PromptStarts)
	}
}
