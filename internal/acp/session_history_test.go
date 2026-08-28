package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// session_history_test.go — msgSeq 契约（tmp/msgseq/CONTRACT.md）验收用例：
// 真实错序形态归一化、promptStarts 重算、turnIndex 与 offset/limit 分页、
// 缺 _meta 回退透传、eventId 透传、归一化缓存 (size, mtime) 失效。

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

// 验收 2：promptStarts 重算——同瞬间多 chunk 合并为一个 prompt 起点，
// 且只与前一条 user_message_chunk 的时间戳比较（回合计数规则）。
func TestSessionUpdatesPromptStartsRecomputed(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	// 落盘序故意打乱；归一化序（ts,N）：A1(100,1)=0, A2(100,2)=1,
	// c1(105,3)=2, C1(150,6)=3, c2(160,7)=4, B1(200,4)=5, c3(210,8)=6。
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
	// A2 与 A1 同瞬间 → 并入；C1(150)≠前一条 user A1(100) → 新起点；
	// B1(200)≠前一条 user C1(150) → 新起点（比的是前一条 user chunk，
	// 不是前一条任意事件）。
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

// 验收 4：任一信封缺 _meta（含文件不存在）→ 走既有 agent RPC 透传：
// 响应无 msgSeq、promptStarts 原样透传（行号空间）、分页参数照旧下发。
func TestSessionUpdatesMissingMetaFallsBackToRPC(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws", "sess-1", []string{
		histEnvelope("sess-1", 1, 100, msgUserChunk("a")),
		// 第二行无 _meta → 整会话触发回退。
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
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
		t.Fatalf("缺 _meta 应回退 agent RPC，method = %q", method)
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

// 契约退化路径：全部信封带 agentTimestampMs 但任一 N 解析失败 → 整会话
// 退化为文件行号序（msgSeq = 行号名次），仍走本地（带 msgSeq）。
func TestSessionUpdatesDegradedToLineOrderOnBrokenEventId(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	broken := `{"timestamp":300,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"x"}},"_meta":{"agentTimestampMs":300,"eventId":"sess-1-x"}}}`
	writeSessionFile(t, home, "/ws", sid, []string{
		broken,                              // 行0：ts=300，eventId 的 N 解析失败
		histEnvelope(sid, 8, 100, msgUserChunk("echo")), // 行1：ts=100
		histEnvelope(sid, 13, 200, msgCancelledTurn()),  // 行2：ts=200
	})

	view, ok := (&Bridge{cfg: GrokConfig{GrokHome: home}}).normalizedSessionHistory(
		sessionUpdatesFile(home, "/ws", sid))
	if !ok {
		t.Fatal("归一化不应回退（agentTimestampMs 全在）")
	}
	if !view.degraded {
		t.Error("N 解析失败应标记退化行号序")
	}

	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("退化行号序仍是本地服务，不得请求 agent（写了 %d 条）", len(w.lines))
	}
	// 行号序：不按 ts 排，msgSeq = 行名次 0,1,2。
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Errorf("退化序 msgSeq = %v, want [0 1 2]", got)
	}
	if got := pageKinds(t, page); !reflect.DeepEqual(got, []string{"agent_message_chunk", "user_message_chunk", "turn_completed"}) {
		t.Errorf("退化序 kinds = %v, want 文件行序", got)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{1}) {
		t.Errorf("promptStarts = %v, want [1]（user chunk 在 msgSeq 1）", page.PromptStarts)
	}
}

func intPtr(v int) *int            { return &v }
func int64Ptr(v int64) *int64      { return &v }
