package acp

import (
	"context"
	"testing"
)

func TestStreamGenWindow(t *testing.T) {
	// 单包：末包 − streamStart。
	if g := streamGenWindow(2100, 2100, 2000); g != 100 {
		t.Errorf("single chunk = %d, want 100", g)
	}
	// Grok 2ms 尾巴：末包 − streamStart，避免分母只剩 2ms。
	if g := streamGenWindow(7507, 7509, 5441); g != 7509-5441 {
		t.Errorf("tiny tail = %d, want %d", g, 7509-5441)
	}
	// 真流式也用末包 − streamStart（含首包生成，不再切 500ms）。
	if g := streamGenWindow(1500, 2500, 1000); g != 1500 {
		t.Errorf("streaming = %d, want 1500", g)
	}
	// 缺首包：仍是末包 − streamStart。
	if g := streamGenWindow(0, 2500, 1000); g != 1500 {
		t.Errorf("missing first = %d, want 1500", g)
	}
	// 缺 streamStart：回退首包 → 末包。
	if g := streamGenWindow(1500, 2500, 0); g != 1000 {
		t.Errorf("missing streamStart = %d, want 1000", g)
	}
	// 全缺：0。
	if g := streamGenWindow(0, 0, 1000); g != 0 {
		t.Errorf("empty = %d, want 0", g)
	}
}

func TestSessionStatsTurnsFollowTracker(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
		histEnvelope(sid, 1, 20, msgUserChunk("B")), // 连续无 PI → 同一 run
		histEnvelope(sid, 2, 30, msgAgentChunk("a1")),
		histEnvelope(sid, 3, 40, msgUserChunkMeta("inj", map[string]any{"hostTurn": true})),
		histEnvelope(sid, 4, 50, msgUserChunk("C")),
		histEnvelope(sid, 5, 60, msgAgentChunk("c1")),
	})
	b, _ := historyBridge(t, home)
	st, err := b.SessionStats(context.Background(), "/ws", sid)
	if err != nil {
		t.Fatal(err)
	}
	if st.Turns != 2 {
		t.Errorf("turns = %d, want 2（连续 user 合并、hostTurn 不计）", st.Turns)
	}
}

func TestSessionStatsTurnsDropRewindBranch(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunkMeta("A", map[string]any{"promptIndex": 0})),
		histEnvelope(sid, 1, 20, msgAgentChunk("a1")),
		histEnvelope(sid, 2, 30, msgUserChunkMeta("B", map[string]any{"promptIndex": 1})),
		histEnvelope(sid, 3, 40, map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t-dead"}),
		histEnvelope(sid, 4, 50, msgRewindMarker(1)),
		histEnvelope(sid, 5, 60, msgUserChunkMeta("C", map[string]any{"promptIndex": 1})),
		histEnvelope(sid, 6, 70, map[string]any{"sessionUpdate": "tool_call", "toolCallId": "t-live"}),
	})
	b, _ := historyBridge(t, home)
	st, err := b.SessionStats(context.Background(), "/ws", sid)
	if err != nil {
		t.Fatal(err)
	}
	if st.Turns != 2 {
		t.Errorf("turns = %d, want 2（A + C，B 支截掉）", st.Turns)
	}
	if st.Steps != 1 {
		t.Errorf("steps = %d, want 1（死分支 tool_call 不计）", st.Steps)
	}
}
