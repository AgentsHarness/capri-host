package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// session_history_test.go — msgSeq 契约（tmp/msgseq/CONTRACT.md）验收用例：
// 真实错序形态归一化（不读 eventId N）、promptStarts 按 UserRunTurnTracker
// 重算、turnIndex 与 offset/limit 分页、缺 _meta 回退透传、末行半截仍走
// 本地、eventId 透传、归一化缓存 (size, mtime) 失效。

// histEnvelope 生成一条带 _meta（eventId + agentTimestampMs）的存储信封
// （agent 落盘形态：{timestamp, method, params:{sessionId, update, _meta}}）。
func histEnvelope(sessionID string, n int64, tsMs int64, update any) string {
	raw, _ := json.Marshal(map[string]any{
		"timestamp": tsMs / 1000,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": sessionID,
			"update":    update,
			"_meta": map[string]any{
				"eventId":          fmt.Sprintf("%s-%d", sessionID, n),
				"agentTimestampMs": tsMs,
			},
		},
	})
	return string(raw)
}

func msgUserChunk(text string) map[string]any {
	return map[string]any{
		"sessionUpdate": "user_message_chunk",
		"content":       map[string]any{"type": "text", "text": text},
	}
}

func msgUserChunkMeta(text string, chunkMeta map[string]any) map[string]any {
	m := msgUserChunk(text)
	m["_meta"] = chunkMeta
	return m
}

func msgAgentChunk(text string) map[string]any {
	return map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": text},
	}
}

func msgCancelledTurn() map[string]any {
	return map[string]any{"sessionUpdate": "turn_completed", "stopReason": "cancelled"}
}

// historyBridge 与 metaReadyBridge 同款（不落真实 home），外加 GrokHome
// 指向临时目录。
func historyBridge(t *testing.T, grokHome string) (*Bridge, *recordingStdin) {
	t.Helper()
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        grokHome,
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ready = true
	b.sessions["sess-1"] = &SessionState{SessionID: "sess-1", Cwd: "/ws"}
	b.activeSessionID = "sess-1"
	w := &recordingStdin{}
	b.stdin = w
	return b, w
}

// pageSeqs 按服务序取出每条信封顶层的 msgSeq。
func pageSeqs(t *testing.T, page UpdatesPage) []int {
	t.Helper()
	seqs := make([]int, 0, len(page.Updates))
	for _, u := range page.Updates {
		env, ok := u.(map[string]any)
		if !ok {
			t.Fatalf("update 不是信封对象: %T", u)
		}
		ms, ok := env["msgSeq"].(int)
		if !ok {
			t.Fatalf("信封缺顶层 msgSeq: %v", env)
		}
		seqs = append(seqs, ms)
	}
	return seqs
}

// pageKinds 按服务序取出每条信封的 update.sessionUpdate。
func pageKinds(t *testing.T, page UpdatesPage) []string {
	t.Helper()
	kinds := make([]string, 0, len(page.Updates))
	for _, u := range page.Updates {
		env := u.(map[string]any)
		params := env["params"].(map[string]any)
		upd := params["update"].(map[string]any)
		kinds = append(kinds, upd["sessionUpdate"].(string))
	}
	return kinds
}

// 验收 1：契约真实错序形态——落盘序 [retry(N=9,ts=T+9), terminal
// cancelled(N=13,ts=T+18), echo(N=8,ts=T)] → 服务序 echo(0)/retry(1)/
// terminal(2)；本地命中时不得请求 agent。
func TestSessionUpdatesNormalizedOrderContractFixture(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	const T = int64(1_000_000)
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 9, T+9, msgAgentChunk("retry")),
		histEnvelope(sid, 13, T+18, msgCancelledTurn()),
		histEnvelope(sid, 8, T, msgUserChunk("echo")),
	})

	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("本地归一化命中时不得请求 agent RPC（写了 %d 条请求）", len(w.lines))
	}
	if page.TotalCount != 3 {
		t.Errorf("totalCount = %d, want 3（归一化条数）", page.TotalCount)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Errorf("服务序 msgSeq = %v, want [0 1 2]", got)
	}
	wantKinds := []string{"user_message_chunk", "agent_message_chunk", "turn_completed"}
	if got := pageKinds(t, page); !reflect.DeepEqual(got, wantKinds) {
		t.Errorf("服务序 kinds = %v, want %v", got, wantKinds)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("promptStarts = %v, want [0]（echo 是唯一用户 prompt）", page.PromptStarts)
	}
	// 信封原样透传（timestamp/method/params 保留），msgSeq 只是顶层新增。
	env := page.Updates[0].(map[string]any)
	if env["method"] != "session/update" {
		t.Errorf("信封 method = %v, want session/update（信封内容不被改写）", env["method"])
	}
}

// 验收 2：promptStarts 重算——连续 user run 合并为一个 prompt 起点
// （UserRunTurnTracker：非 user 事件才结束 run）。
func TestSessionUpdatesPromptStartsRecomputed(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	// 落盘序故意打乱；归一化序（ts, 行号）：A1(100)=0, A2(100)=1,
	// c1(105)=2, C1(150)=3, c2(160)=4, B1(200)=5, c3(210)=6。
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 4, 200, msgUserChunk("B1")),
		histEnvelope(sid, 1, 100, msgUserChunk("A1")),
		histEnvelope(sid, 7, 160, msgAgentChunk("c2")),
		histEnvelope(sid, 2, 100, msgUserChunk("A2")),
		histEnvelope(sid, 6, 150, msgUserChunk("C1")),
		histEnvelope(sid, 3, 105, msgAgentChunk("c1")),
		histEnvelope(sid, 8, 210, msgAgentChunk("c3")),
	})

	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("本地归一化命中时不得请求 agent RPC（写了 %d 条请求）", len(w.lines))
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1, 2, 3, 4, 5, 6}) {
		t.Errorf("归一化 msgSeq = %v, want [0 1 2 3 4 5 6]", got)
	}
	// A1+A2 连续 user → 同一 run；c1 结束 run；C1 新 run；c2 结束；B1 新 run。
	if !reflect.DeepEqual(page.PromptStarts, []int{0, 3, 5}) {
		t.Errorf("promptStarts = %v, want [0 3 5]", page.PromptStarts)
	}
	if page.TotalCount != 7 {
		t.Errorf("totalCount = %d, want 7", page.TotalCount)
	}
}

// 验收 3：turnIndex 与 offset/limit 分页一律在 msgSeq 空间解释。
// 序：preamble(0) A(1,user) a1(2) a2(3) B(4,user) b1(5) →
// promptStarts=[1,4]，total=6。
func TestSessionUpdatesTurnIndexAndOffsetPaging(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 3, 105, msgAgentChunk("a1")), // 行2
		histEnvelope(sid, 5, 205, msgAgentChunk("b1")), // 行5
		histEnvelope(sid, 1, 100, msgUserChunk("A")),   // 行1
		histEnvelope(sid, 0, 10, msgAgentChunk("pre")), // 行0（首条 user 之前的 preamble）
		histEnvelope(sid, 4, 200, msgUserChunk("B")),   // 行4
		histEnvelope(sid, 2, 103, msgAgentChunk("a2")), // 行3
	})
	b, w := historyBridge(t, home)
	ctx := context.Background()

	// turnIndex:1 = 最后 1 个 prompt 轮 → msgSeq [4, 5]，不分页截尾。
	page, err := b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{TurnIndex: intPtr(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{4, 5}) {
		t.Errorf("turnIndex:1 msgSeq = %v, want [4 5]", got)
	}
	if page.TotalCount != 6 {
		t.Errorf("turnIndex:1 totalCount = %d, want 6（归一化总条数）", page.TotalCount)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{1, 4}) {
		t.Errorf("turnIndex:1 promptStarts = %v, want [1 4]", page.PromptStarts)
	}

	// turnIndex 超过回合数 → 全部回合（msgSeq [ps[0], total)）。
	page, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{TurnIndex: intPtr(9)})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{1, 2, 3, 4, 5}) {
		t.Errorf("turnIndex:9 msgSeq = %v, want [1 2 3 4 5]", got)
	}

	// turnIndex:0 = 最后 0 轮 → 空页。
	page, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{TurnIndex: intPtr(0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Updates) != 0 || page.HasMore {
		t.Errorf("turnIndex:0 应为空页，got %d 条 hasMore=%v", len(page.Updates), page.HasMore)
	}

	// offset/limit 直接切 msgSeq 空间窗口。
	page, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Offset: int64Ptr(2), Limit: intPtr(2)})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{2, 3}) {
		t.Errorf("offset:2 limit:2 msgSeq = %v, want [2 3]", got)
	}
	if !page.HasMore {
		t.Error("offset:2 limit:2 hasMore = false, want true（末尾还有 [4,5)）")
	}

	// 不带 limit → 到结尾；hasMore=false。
	page, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Offset: int64Ptr(4)})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{4, 5}) || page.HasMore {
		t.Errorf("offset:4 = %v hasMore=%v, want [4 5] false", got, page.HasMore)
	}

	// FE previousTurnWindow 数学（见 integrationNotes）：promptStarts=[1,4]、
	// k=1、loadedStart=4 → {offset: ps[0]=1, limit: min(ps[1],4)-1=3}，
	// 服务序必须恰为第 0 轮 msgSeq [1,2,3]，与已加载区 [4,6) 严格相接。
	page, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Offset: int64Ptr(1), Limit: intPtr(3)})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Errorf("win-path offset:1 limit:3 msgSeq = %v, want [1 2 3]", got)
	}

	// offset 越界 → 空页不报错。
	page, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Offset: int64Ptr(10), Limit: intPtr(2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Updates) != 0 || page.HasMore || page.TotalCount != 6 {
		t.Errorf("offset 越界应为空页：updates=%d hasMore=%v total=%d",
			len(page.Updates), page.HasMore, page.TotalCount)
	}
	if len(w.lines) != 0 {
		t.Errorf("本地归一化命中时不得请求 agent RPC（写了 %d 条请求）", len(w.lines))
	}
}

// 验收 4：全部信封缺 _meta（或文件不存在）→ 走既有 agent RPC 透传：
// 响应无 msgSeq、promptStarts 原样透传（行号空间）、分页参数照旧下发。
// 夹杂缺时间戳的行则跳过该行、其余仍走本地（不换空间）。
func TestSessionUpdatesMissingMetaFallsBackToRPC(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws", "sess-1", []string{
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"a"}}}}`,
		`{"timestamp":2,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
	})
	b, w := historyBridge(t, home)
	ctx := context.Background()

	off := int64(0)
	lim := 10
	var page UpdatesPage
	runResolved(t, b, w, map[string]any{
		"updates":      []any{map[string]any{"passthrough": true}},
		"totalCount":   float64(2),
		"hasMore":      true,
		"promptStarts": []any{float64(0)},
	}, func() error {
		var err error
		page, err = b.SessionUpdates(ctx, "sess-1", "/ws", SessionUpdatesOpts{Offset: &off, Limit: &lim})
		return err
	})
	method, params := lastRequestParams(t, w)
	if method != "_x.ai/session/updates" {
		t.Fatalf("全部缺 _meta 应回退 agent RPC，method = %q", method)
	}
	if params["offset"] != float64(0) || params["limit"] != float64(10) {
		t.Errorf("回退路径分页参数须照旧透传：offset=%v limit=%v", params["offset"], params["limit"])
	}
	if len(page.Updates) != 1 {
		t.Fatalf("透传 updates = %d 条, want 1", len(page.Updates))
	}
	if env, ok := page.Updates[0].(map[string]any); ok {
		if _, bad := env["msgSeq"]; bad {
			t.Errorf("回退路径响应不得带 msgSeq: %v", env)
		}
		if env["passthrough"] != true {
			t.Errorf("透传信封内容被改写: %v", env)
		}
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("promptStarts 应原样透传 agent 的行号, got %v", page.PromptStarts)
	}
	if len(page.PromptPreviews) != 0 {
		t.Errorf("透传路径不得带 promptPreviews（宿主无从计算）, got %v", page.PromptPreviews)
	}
	if page.TotalCount != 2 || !page.HasMore {
		t.Errorf("透传 totalCount/hasMore: %d %v", page.TotalCount, page.HasMore)
	}

	// 文件不存在（他机会话 / 未落盘）→ 同样走透传。
	b2, w2 := historyBridge(t, home)
	runResolved(t, b2, w2, map[string]any{"updates": []any{}, "totalCount": float64(0), "hasMore": false}, func() error {
		_, err := b2.SessionUpdates(ctx, "no-such", "/ws")
		return err
	})
	if method, _ := lastRequestParams(t, w2); method != "_x.ai/session/updates" {
		t.Fatalf("文件不存在应回退 agent RPC，method = %q", method)
	}
}

func TestSessionUpdatesSkipsEnvelopeMissingTimestamp(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 1, 100, msgUserChunk("a")),
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
		histEnvelope(sid, 3, 200, msgAgentChunk("c")),
	})
	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("夹杂缺时间戳不得换空间透传（写了 %d 条请求）", len(w.lines))
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1}) {
		t.Errorf("跳过无 ts 行后 msgSeq = %v, want [0 1]", got)
	}
	if got := pageKinds(t, page); !reflect.DeepEqual(got, []string{"user_message_chunk", "agent_message_chunk"}) {
		t.Errorf("跳过无 ts 行后 kinds = %v", got)
	}
	if page.TotalCount != 2 {
		t.Errorf("totalCount = %d, want 2", page.TotalCount)
	}
}

// 验收 5：attachStreamMeta 把 params._meta.eventId 提升为事件顶层；
// params._meta 无该键时不带。经 dispatchSessionUpdateKind 的真实出口验证。
func TestAttachStreamMetaEventId(t *testing.T) {
	// 直连：eventId 与其余 meta 字段并列提升。
	ev := attachStreamMeta(Event{"type": "chunk"}, map[string]any{
		"_meta": map[string]any{"eventId": "s1-42", "agentTimestampMs": float64(1234)},
	})
	if ev["eventId"] != "s1-42" {
		t.Errorf("eventId 未提升到事件顶层: %v", ev)
	}
	// 无该键时不带（absent key ≠ off）。
	ev = attachStreamMeta(Event{"type": "chunk"}, map[string]any{
		"_meta": map[string]any{"agentTimestampMs": float64(1234)},
	})
	if _, ok := ev["eventId"]; ok {
		t.Errorf("_meta 无 eventId 时事件不得带 eventId: %v", ev)
	}

	// 经 user_message_chunk 分发出口（FE /api/events 实际收到的事件）。
	b, _ := historyBridge(t, t.TempDir())
	sub, unsub := b.Subscribe()
	defer unsub()
	params := map[string]any{
		"sessionId": "sess-1",
		"update":    msgUserChunk("hi"),
		"_meta":     map[string]any{"eventId": "sess-1-7", "agentTimestampMs": float64(5678)},
	}
	if !b.dispatchSessionUpdateKind("sess-1", params, func(ev Event) Event { return ev }) {
		t.Fatal("user_message_chunk 未被建模分发")
	}
	got := nextEvent(t, sub, "user_chunk")
	if got["eventId"] != "sess-1-7" {
		t.Errorf("user_chunk eventId = %v, want sess-1-7", got["eventId"])
	}
}

// 验收 6：归一化缓存按 (size, mtime) 失效——同 size 不同 mtime、同 mtime
// 不同 size 都必须重建；未变更文件命中同一视图。
func TestNormalizedHistoryCacheInvalidation(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	// 两行仅 ts 数字不同（等长），支撑「同 size 不同 mtime」用例。
	lineA := histEnvelope(sid, 1, 1000, msgUserChunk("a"))
	lineB := histEnvelope(sid, 2, 9000, msgUserChunk("b"))
	path := writeSessionFile(t, home, "/ws", sid, []string{lineA, lineB})

	b, _ := historyBridge(t, home)
	v1, ok := b.normalizedSessionHistory(path)
	if !ok {
		t.Fatal("归一化失败")
	}
	if !reflect.DeepEqual(v1.order, []int{0, 1}) {
		t.Fatalf("v1 order = %v, want [0 1]（ts 升序）", v1.order)
	}
	v2, ok := b.normalizedSessionHistory(path)
	if !ok {
		t.Fatal("归一化失败")
	}
	if v1 != v2 {
		t.Error("文件未变更应命中缓存（同一视图指针）")
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 同 size、不同 mtime：交换两行 ts（字节数不变），拨 mtime → 必须失效。
	swapped := []string{
		histEnvelope(sid, 1, 9000, msgUserChunk("a")),
		histEnvelope(sid, 2, 1000, msgUserChunk("b")),
	}
	if err := os.WriteFile(path, []byte(strings.Join(swapped, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	future := st.ModTime().Add(time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	v3, ok := b.normalizedSessionHistory(path)
	if !ok {
		t.Fatal("归一化失败")
	}
	if !reflect.DeepEqual(v3.order, []int{1, 0}) {
		t.Fatalf("同 size 不同 mtime 未失效缓存：order = %v, want [1 0]", v3.order)
	}

	// size 变化（追加一行）、mtime 拨回与 v3 相同 → 仍须按 size 失效。
	with3 := append(swapped, histEnvelope(sid, 3, 5000, msgAgentChunk("c")))
	if err := os.WriteFile(path, []byte(strings.Join(with3, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	v4, ok := b.normalizedSessionHistory(path)
	if !ok {
		t.Fatal("归一化失败")
	}
	if len(v4.lines) != 3 {
		t.Fatalf("size 变化未失效缓存：lines = %d, want 3", len(v4.lines))
	}
	if v4 == v3 {
		t.Error("size 变化必须重建视图")
	}
}

// eventId N 解析失败不得影响排序：仍按 (ts, 行号)，不退回行号序。
func TestSessionUpdatesIgnoresBrokenEventId(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	broken := `{"timestamp":300,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"x"}},"_meta":{"agentTimestampMs":300,"eventId":"sess-1-x"}}}`
	writeSessionFile(t, home, "/ws", sid, []string{
		broken, // 行0：ts=300，eventId 的 N 解析失败
		histEnvelope(sid, 8, 100, msgUserChunk("echo")), // 行1：ts=100
		histEnvelope(sid, 13, 200, msgCancelledTurn()),  // 行2：ts=200
	})

	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("N 解析失败仍是本地服务，不得请求 agent（写了 %d 条）", len(w.lines))
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Errorf("msgSeq = %v, want [0 1 2]", got)
	}
	if got := pageKinds(t, page); !reflect.DeepEqual(got, []string{"user_message_chunk", "turn_completed", "agent_message_chunk"}) {
		t.Errorf("kinds = %v, want ts 升序（echo/terminal/broken）", got)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("promptStarts = %v, want [0]", page.PromptStarts)
	}
}

func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

// msgRewindMarker 生成一条 /rewind 落盘的标记信封（死分支截断锚点）。
func msgRewindMarker(target int) map[string]any {
	return map[string]any{
		"sessionUpdate":       "rewind_marker",
		"target_prompt_index": target,
	}
}

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

func TestSessionUpdatesWindowReadFailureDoesNotPassthrough(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	path := writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
	})
	b, w := historyBridge(t, home)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 缓存一份指向不存在行名次的视图：读窗口必然缺信封。
	b.hist.put(path, st.Size(), st.ModTime(), &normalizedHistory{
		lines:        []updateLineMeta{{line: 99, agentTsMs: 10, isUserChunk: true}},
		order:        []int{0},
		promptStarts: []int{0},
	})
	_, err = b.SessionUpdates(context.Background(), sid, "/ws")
	if err == nil {
		t.Fatal("窗口缺行应返回错误，不得静默成功")
	}
	if !errors.Is(err, errLocalHistoryRead) {
		t.Fatalf("err = %v, want errLocalHistoryRead", err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("已在 msgSeq 空间时不得透传 agent RPC（写了 %d 条）", len(w.lines))
	}
}
