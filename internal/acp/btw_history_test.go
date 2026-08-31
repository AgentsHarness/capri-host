package acp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// btw_fixture 写一个会话的 btw_history.jsonl（BtwEntry camelCase 一行一问）。
func writeBtwFile(t *testing.T, grokHome, cwd, sessionID string, lines []string) {
	t.Helper()
	dir := filepath.Join(grokHome, "sessions", EncodeCwdDirname(cwd), sessionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var sb []byte
	for _, l := range lines {
		sb = append(sb, []byte(l)...)
		sb = append(sb, '\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "btw_history.jsonl"), sb, 0o600); err != nil {
		t.Fatal(err)
	}
}

// btwLine 生成一条落盘记录（字段名与 agent 的 BtwEntry serde camelCase 对齐）。
func btwLine(btwId string, askedAtMs int64, q, a string) string {
	raw, _ := json.Marshal(map[string]any{
		"btwSessionId":    btwId,
		"parentSessionId": "sess-1",
		"askedAt":         time.UnixMilli(askedAtMs).UTC().Format("2006-01-02T15:04:05.999999Z07:00"),
		"question":        q,
		"answer":          a,
		"model":           "grok-4.6",
		"success":         a != "",
		"error":           nil,
		"attempts":        1,
	})
	return string(raw)
}

func btwAnchors(page UpdatesPage) []int {
	out := make([]int, 0, len(page.Btw))
	for _, r := range page.Btw {
		out = append(out, r.AfterMsgSeq)
	}
	return out
}

func TestBtwReplayJoinedToLocalPage(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 1, 1000, msgUserChunk("u1")),
		histEnvelope(sid, 2, 2000, msgAgentChunk("a1")),
		histEnvelope(sid, 3, 3000, msgAgentChunk("a2")),
	})
	writeBtwFile(t, home, "/ws", sid, []string{
		btwLine("btw-a", 1500, "问题A", "答案A"),  // 锚点 seq0
		btwLine("btw-b", 2500, "问题B", ""), // 锚点 seq1
		btwLine("btw-c", 3500, "问题C", "答案C"),  // 锚点 seq2
		btwLine("btw-top", 500, "问顶", ""), // 早于一切信封 → -1
		"{broken json",
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	// 全量页 [0,3)：四条解析成功的记录全带，按锚点升序；坏行跳过。
	if got := btwAnchors(page); !reflect.DeepEqual(got, []int{-1, 0, 1, 2}) {
		t.Fatalf("anchors = %v, want [-1 0 1 2]（坏行已跳过）", got)
	}
	if page.Btw[0].BtwSessionId != "btw-top" || page.Btw[0].AfterMsgSeq != -1 {
		t.Errorf("置顶记录 = %+v", page.Btw[0])
	}
	if page.Btw[1].Question != "问题A" || page.Btw[1].Answer != "答案A" || page.Btw[1].AskedAt != 1500 {
		t.Errorf("A 记录字段错位: %+v", page.Btw[1])
	}
	if page.Btw[2].Question != "问题B" || page.Btw[2].Answer != "" || page.Btw[2].Success {
		t.Errorf("失败记录应有 error/success=false: %+v", page.Btw[2])
	}
	if page.Btw[2].AfterMsgSeq != 1 {
		t.Errorf("B 锚点 = %d, want 1", page.Btw[2].AfterMsgSeq)
	}
	if page.Btw[3].AfterMsgSeq != 2 {
		t.Errorf("C 锚点 = %d, want 2", page.Btw[3].AfterMsgSeq)
	}
	// 分页窗口互斥：[1,3) 只带锚点 1 和 2 的记录。
	off, lim := int64(1), 2
	p2, err := b.SessionUpdates(context.Background(), sid, "/ws", SessionUpdatesOpts{Offset: &off, Limit: &lim})
	if err != nil {
		t.Fatal(err)
	}
	if got := btwAnchors(p2); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("窗口 [1,3) anchors = %v, want [1 2]", got)
	}
	// 最老页 [0,1)：锚点 0 + 置顶。
	o0, l1 := int64(0), 1
	p3, err := b.SessionUpdates(context.Background(), sid, "/ws", SessionUpdatesOpts{Offset: &o0, Limit: &l1})
	if err != nil {
		t.Fatal(err)
	}
	if got := btwAnchors(p3); len(got) != 2 || got[0]+got[1] != -1 {
		t.Fatalf("窗口 [0,1) anchors = %v, want [-1 0]", got)
	}
}

func TestBtwReplayAbsentFileOmitsField(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 1, 1000, msgUserChunk("u1")),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if page.Btw != nil {
		t.Fatalf("无 btw 文件不得带记录: %v", page.Btw)
	}
	bj, _ := json.Marshal(page)
	if strings.Contains(string(bj), "btw") {
		t.Errorf("无记录时响应不得出现 btw 键: %s", bj)
	}
}