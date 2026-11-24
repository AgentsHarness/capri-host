package acp

import (
	"context"
	"os"
	"reflect"
	"testing"
)

// 验收：负 offset = agent 的尾部窗口语义（offset=-N 从倒数第 N 条起，limit
// 在该起点上开窗）。FE 的子代理时间线只会用这一套：先 offset=-100 取最新
// 一页，再 offset=-(已加载+100) 往前翻——把负值当 0 会让每页都返回最早的
// 同一份内容，时间线于是重复显示。
func TestSessionUpdatesNegativeOffsetPaging(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
		histEnvelope(sid, 1, 20, msgAgentChunk("a1")),
		histEnvelope(sid, 2, 30, msgAgentChunk("a2")),
		histEnvelope(sid, 3, 40, msgUserChunk("B")),
		histEnvelope(sid, 4, 50, msgAgentChunk("b1")),
		histEnvelope(sid, 5, 60, msgAgentChunk("b2")),
	})
	b, _ := historyBridge(t, home)
	ctx := context.Background()

	cases := []struct {
		name        string
		offset      int64
		limit       int
		want        []int
		wantHasMore bool
	}{
		// hasMore 沿用 agent 的语义：end < total（窗口之后还有更新的），
		// 最新一页因此是 false。
		{"最新一页", -2, 2, []int{4, 5}, false},
		{"往前一页", -4, 2, []int{2, 3}, true},
		{"再往前", -6, 2, []int{0, 1}, true},
		{"窗口大于总量", -100, 100, []int{0, 1, 2, 3, 4, 5}, false},
		{"负 offset 无 limit", -3, 0, []int{3, 4, 5}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := SessionUpdatesOpts{Offset: int64Ptr(tc.offset)}
			if tc.limit > 0 {
				opts.Limit = intPtr(tc.limit)
			}
			page, err := b.SessionUpdates(ctx, sid, "/ws", opts)
			if err != nil {
				t.Fatal(err)
			}
			if got := pageSeqs(t, page); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("offset:%d limit:%d msgSeq = %v, want %v",
					tc.offset, tc.limit, got, tc.want)
			}
			if page.HasMore != tc.wantHasMore {
				t.Errorf("offset:%d limit:%d hasMore = %v, want %v",
					tc.offset, tc.limit, page.HasMore, tc.wantHasMore)
			}
		})
	}
}

// 同毫秒 tiebreak 走文件行号，不读 eventId N（N 更小的后写行不得排到前面）。
func TestSessionUpdatesSameMsOrderByFileLine(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 9, 100, msgAgentChunk("a")),
		histEnvelope(sid, 1, 100, msgUserChunk("u")),
	})
	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("本地命中时不得请求 agent（写了 %d 条）", len(w.lines))
	}
	if got := pageKinds(t, page); !reflect.DeepEqual(got, []string{"agent_message_chunk", "user_message_chunk"}) {
		t.Errorf("同毫秒 kinds = %v, want 文件行序 agent/user（不是 N 升序）", got)
	}
}

func TestSessionUpdatesConsecutiveUsersOneRunWithoutPromptIndex(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk("A")),
		histEnvelope(sid, 1, 200, msgUserChunk("B")),
		histEnvelope(sid, 2, 300, msgAgentChunk("a1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("无 promptIndex 的连续 user 应合并为 1 个 run，got %v", page.PromptStarts)
	}
}

func TestSessionUpdatesPromptIndexOpensNewRun(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunkMeta("A", map[string]any{"promptIndex": 0})),
		histEnvelope(sid, 1, 200, msgUserChunkMeta("B", map[string]any{"promptIndex": 1})),
		histEnvelope(sid, 2, 300, msgAgentChunk("a1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 1}) {
		t.Errorf("promptIndex 变化应开新 run，got %v", page.PromptStarts)
	}
}

func TestSessionUpdatesHostTurnNotAPromptStart(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk("A")),
		histEnvelope(sid, 1, 150, msgUserChunkMeta("injected", map[string]any{"hostTurn": true})),
		histEnvelope(sid, 2, 200, msgAgentChunk("a1")),
		histEnvelope(sid, 3, 300, msgUserChunk("B")),
		histEnvelope(sid, 4, 400, msgAgentChunk("b1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 3}) {
		t.Errorf("hostTurn 不得记为 prompt 起点，got %v", page.PromptStarts)
	}
	if page.TotalCount != 5 {
		t.Errorf("hostTurn 行仍在存活序列里，totalCount = %d, want 5", page.TotalCount)
	}
}

func TestSessionUpdatesUnmarkedPhantomAfterPromptIndex(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunkMeta("A", map[string]any{"promptIndex": 0})),
		histEnvelope(sid, 1, 200, msgAgentChunk("a1")),
		histEnvelope(sid, 2, 300, msgUserChunk("phantom")),
		histEnvelope(sid, 3, 400, msgAgentChunk("p1")),
		histEnvelope(sid, 4, 500, msgUserChunkMeta("B", map[string]any{"promptIndex": 1})),
		histEnvelope(sid, 5, 600, msgAgentChunk("b1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 4}) {
		t.Errorf("见过 promptIndex 后的无标记 run 不计回合，got %v", page.PromptStarts)
	}
}

func TestSessionUpdatesIncompleteLastLineStillLocal(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	path := writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk("A")),
		histEnvelope(sid, 1, 200, msgAgentChunk("a1")),
	})
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"timestamp":300,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk"`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("末行半截不得换空间透传（写了 %d 条请求）", len(w.lines))
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1}) {
		t.Errorf("半截末行跳过后 msgSeq = %v, want [0 1]", got)
	}
	if page.TotalCount != 2 {
		t.Errorf("totalCount = %d, want 2", page.TotalCount)
	}
}
