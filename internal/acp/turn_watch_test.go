package acp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// turn_watch_test.go — Busy 的观察腿（agent 事件流派生的回合态）。
//
// 回归的原始事故：一个回合跑过 promptTimeout（30 分钟）后，host 的
// session/prompt RPC 超时返回，releaseBusy 把 busyCount 清零、会话翻成
// idle，而 agent 还在同一个回合里干活——session 列表于是永远显示空闲。
// 修法是把"有没有回合在飞"改为两条腿的投影：host 自己发出的 prompt 计数
// （busyCount）+ 从 session/update 流观察到的回合（Bridge.turns）。

// feedUpdate drives one session/update envelope through the real stdin
// parsing path (the same entry the agent reader goroutine uses).
func feedUpdate(t *testing.T, b *Bridge, sid, kind string) {
	t.Helper()
	msg := map[string]any{
		kJSONRPC: "2.0",
		kMethod:  "session/update",
		kParams: map[string]any{
			kSessionID: sid,
			kUpdate: map[string]any{
				kSessionUpdate: kind,
				kContent:       map[string]any{kType: "text", "text": "x"},
			},
		},
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	b.handleStdoutLine(raw)
}

// statusOf pulls one session row out of a ListSessions result.
func statusOf(t *testing.T, sessions []any, sid string) map[string]any {
	t.Helper()
	for _, it := range sessions {
		m, _ := it.(map[string]any)
		if m[kSessionID] == sid {
			st, _ := m["status"].(map[string]any)
			if st == nil {
				t.Fatalf("session %s carries no status: %v", sid, m)
			}
			return st
		}
	}
	t.Fatalf("session %s missing from list: %v", sid, sessions)
	return nil
}

// runListSessions resolves the session/list the bridge writes with the
// given agent-side rows.
func runListSessions(t *testing.T, b *Bridge, w *recordingStdin, rows ...map[string]any) []any {
	t.Helper()
	sessions := make([]any, 0, len(rows))
	for _, r := range rows {
		sessions = append(sessions, r)
	}
	var got []any
	runResolved(t, b, w, map[string]any{"sessions": sessions}, func() error {
		var err error
		got, _, _, err = b.ListSessions(context.Background())
		return err
	})
	return got
}

// 观察腿的正题：host 的 prompt 计数因传输失败（超时）释放后，仍在流式输出
// 的会话必须保持 busy；回合真正结束（turn_completed）才翻回 idle。
func TestObservedTurnSurvivesPromptTimeout(t *testing.T) {
	b, _ := metaReadyBridge(t)
	sub, unsub := b.Subscribe()
	defer unsub()

	feedUpdate(t, b, "s1", "agent_message_chunk")
	b.mu.Lock()
	s := b.sessions["s1"]
	if !s.Busy || s.State() != "active" {
		b.mu.Unlock()
		t.Fatalf("after chunk: busy=%v state=%s, want busy/active", s.Busy, s.State())
	}
	if s.LastActiveAt == 0 {
		b.mu.Unlock()
		t.Fatal("chunk did not refresh LastActiveAt")
	}
	// 模拟 promptTimeout 到期的那条腿：busyCount 归零，回合本身没结束。
	s.busyCount = 1
	b.mu.Unlock()

	b.releaseBusy(s, "s1", false) // transport failure: the turn is not over
	b.mu.Lock()
	if !s.Busy || s.State() != "active" {
		b.mu.Unlock()
		t.Fatalf("after timeout release: busy=%v state=%s, want still active", s.Busy, s.State())
	}
	b.mu.Unlock()

	// 事件纪律：idle→busy 翻转只广播一次，之后的 release/update 不得再刷。
	if n := countEvents(sub, "sessions_changed"); n != 1 {
		t.Errorf("open flip → %d sessions_changed, want 1", n)
	}

	feedUpdate(t, b, "s1", "turn_completed")
	b.mu.Lock()
	if s.Busy || s.State() != "idle" {
		b.mu.Unlock()
		t.Fatalf("after turn_completed: busy=%v state=%s, want idle", s.Busy, s.State())
	}
	b.mu.Unlock()

	if n := countEvents(sub, "sessions_changed"); n != 1 {
		t.Errorf("busy→idle flip → %d sessions_changed, want 1", n)
	}
}

// 反题：agent 明确答复了 session/prompt（回合真的结束）时，releaseBusy 必须
// 连观察腿一并收口，会话回到 idle——不得留下永久 running。
func TestReleaseWithTurnEndedClearsObserved(t *testing.T) {
	b, _ := metaReadyBridge(t)
	feedUpdate(t, b, "s1", "agent_message_chunk")

	b.mu.Lock()
	s := b.sessions["s1"]
	s.busyCount = 1
	b.mu.Unlock()

	b.releaseBusy(s, "s1", true)

	b.mu.Lock()
	defer b.mu.Unlock()
	if s.Busy {
		t.Error("session still busy after an answered prompt — observed turn leaked")
	}
	if w := b.turns["s1"]; w != nil && w.open {
		t.Error("observed turn still open after an answered prompt")
	}
}

// 只有执行类 kind 算回合证据。会话元数据（改模型、标题、可用命令）与
// usage_update（也在回合末尾出现）都不得把空闲会话点成 running——假 busy
// 需要终态事件或过期才能消，代价比假 idle 高。
func TestObservedTurnKindGating(t *testing.T) {
	b, _ := metaReadyBridge(t)

	for _, kind := range []string{
		"config_option_update", "current_mode_update", "available_commands_update",
		"session_info_update", "usage_update", "task_completed", "model_changed",
		"some_future_unmodelled_kind",
	} {
		feedUpdate(t, b, "s1", kind)
		b.mu.Lock()
		busy := b.sessions["s1"].Busy
		b.mu.Unlock()
		if busy {
			t.Errorf("kind %q armed the observed turn — want ignored", kind)
		}
	}

	for _, kind := range []string{"tool_call", "agent_thought_chunk", "plan"} {
		b.mu.Lock()
		delete(b.turns, "s1")
		b.sessions["s1"].Busy = false
		b.mu.Unlock()

		feedUpdate(t, b, "s1", kind)
		b.mu.Lock()
		busy := b.sessions["s1"].Busy
		b.mu.Unlock()
		if !busy {
			t.Errorf("kind %q did not arm the observed turn — want active", kind)
		}
	}
}

// 等待用户输入本身就是"回合在飞"的证据：State() 只在 busy 时才报 awaiting，
// 超时释放后到达的权限请求此前会被直接显示成 idle。
func TestAwaitingInputArmsObservedTurn(t *testing.T) {
	b, _ := metaReadyBridge(t)

	b.setSessionAwaiting("s1", true)

	b.mu.Lock()
	s := b.sessions["s1"]
	state := s.State()
	last := s.LastActiveAt
	b.mu.Unlock()
	if state != "awaiting" {
		t.Fatalf("state = %s, want awaiting (busy leg was empty before the prompt)", state)
	}
	if last == 0 {
		t.Error("awaiting did not refresh LastActiveAt")
	}

	// 用户答完不等于回合结束——agent 还要继续跑，所以观察腿保持张开，
	// 直到 turn_completed 才收口。
	b.setSessionAwaiting("s1", false)
	b.mu.Lock()
	if !s.Busy || s.State() != "active" {
		b.mu.Unlock()
		t.Fatalf("after the interaction resolved: busy=%v state=%s, want still active", s.Busy, s.State())
	}
	b.mu.Unlock()

	feedUpdate(t, b, "s1", "turn_completed")
	b.mu.Lock()
	if s.Busy || s.State() != "idle" {
		b.mu.Unlock()
		t.Fatalf("after turn_completed: busy=%v state=%s, want idle", s.Busy, s.State())
	}
	b.mu.Unlock()
}

// roster 外的会话（agent 列表里有、本进程从未 create/load）此前被硬编码成
// idle；现在按观察到的回合上报。
func TestListSessionsObservedForUnrosteredSession(t *testing.T) {
	b, w := metaReadyBridge(t)

	feedUpdate(t, b, "s2", "agent_message_chunk")

	rows := []map[string]any{
		{kSessionID: "s1", "cwd": "/ws"},
		{kSessionID: "s2", "cwd": "/ws"},
	}
	sessions := runListSessions(t, b, w, rows...)

	live := statusOf(t, sessions, "s1")
	if live["state"] != "idle" || live["busy"] != false {
		t.Errorf("rostered idle session = %v, want state=idle busy=false", live)
	}
	foreign := statusOf(t, sessions, "s2")
	if foreign["state"] != "active" || foreign["busy"] != true {
		t.Errorf("unrostered running session = %v, want state=active busy=true", foreign)
	}
	if seen, _ := foreign["lastActiveAt"].(int64); seen == 0 {
		t.Errorf("unrostered running session carries no lastActiveAt: %v", foreign)
	}
}

// 观察腿的兜底：只开了口、再没有任何 update 也永远等不到终态事件的回合，
// 超过 turnStaleAfter 后按 idle 上报（等待用户输入的除外），并且 host 自己
// 还在等的 prompt 计数不受影响。
func TestStaleObservedTurnExpires(t *testing.T) {
	b, _ := metaReadyBridge(t)
	stale := time.Now().Add(-turnStaleAfter - time.Minute).UnixMilli()

	b.mu.Lock()
	b.turns["s1"] = &observedTurn{open: true, seenAt: stale}
	b.sessions["s1"].Busy = true
	// 等用户输入的会话：agent 沉默是合法的，不得按过期处理。
	b.turns["s2"] = &observedTurn{open: true, seenAt: stale}
	b.sessions["s2"] = &SessionState{SessionID: "s2", Cwd: "/ws", Busy: true, AwaitingInput: true}
	// host 自己还在等 prompt 应答：观察腿过期也不能让 busyCount 那条腿失效。
	b.turns["s3"] = &observedTurn{open: true, seenAt: stale}
	b.sessions["s3"] = &SessionState{SessionID: "s3", Cwd: "/ws", Busy: true, busyCount: 1}
	// 已关闭但过期的条目应当被回收，map 不会无界增长。
	b.turns["s4"] = &observedTurn{open: false, seenAt: stale}
	b.mu.Unlock()

	b.mu.Lock()
	b.settleTurnsLocked(time.Now().UnixMilli())
	_, keepsTurn := b.turns["s4"]
	st1 := b.sessions["s1"]
	st2 := b.sessions["s2"]
	st3 := b.sessions["s3"]
	b.mu.Unlock()

	if keepsTurn {
		t.Error("stale closed entry not pruned from the observed-turn map")
	}
	if st1.Busy {
		t.Error("stale observed turn still busy — want expired to idle")
	}
	if !st2.Busy {
		t.Error("awaiting-input session expired — a turn waiting on the user is not silent")
	}
	if !st3.Busy {
		t.Error("host-driven prompt in flight was cleared by the observed-leg expiry")
	}
}

// busy→idle / idle→busy 之外的 update 不得刷屏：一条 sessions_changed 对应
// 一次真实翻转。
func TestObservedTurnBroadcastsOnlyOnFlip(t *testing.T) {
	b, _ := metaReadyBridge(t)
	sub, unsub := b.Subscribe()
	defer unsub()

	for i := 0; i < 5; i++ {
		feedUpdate(t, b, "s1", "agent_message_chunk")
	}
	if n := countEvents(sub, "sessions_changed"); n != 1 {
		t.Errorf("5 chunks → %d sessions_changed, want 1 (open only)", n)
	}
	feedUpdate(t, b, "s1", "turn_completed")
	if n := countEvents(sub, "sessions_changed"); n != 1 {
		t.Errorf("chunk→turn_completed → %d sessions_changed, want 1 (close only)", n)
	}
	feedUpdate(t, b, "s1", "plan")
	feedUpdate(t, b, "s1", "tool_call")
	if n := countEvents(sub, "sessions_changed"); n != 1 {
		t.Errorf("mid-turn updates → %d sessions_changed, want none beyond the two flips", n)
	}
}

// countEvents drains the subscription and counts events of a type.
func countEvents(sub chan Event, typ string) int {
	n := 0
	for {
		select {
		case ev := <-sub:
			if ev[kType] == typ {
				n++
			}
		default:
			return n
		}
	}
}

// firstEvent drains the subscription and returns the first event of a type.
func firstEvent(sub chan Event, typ string) (Event, bool) {
	for {
		select {
		case ev := <-sub:
			if ev[kType] == typ {
				return ev, true
			}
		default:
			return nil, false
		}
	}
}

// resolveLineWithError answers the n-th request the bridge wrote with a
// JSON-RPC error — resolveLine's error twin.
func resolveLineWithError(t *testing.T, b *Bridge, w *recordingStdin, n int, agentErr error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		have := len(w.lines) > n
		var msg map[string]any
		if have {
			_ = json.Unmarshal(w.lines[n], &msg)
		}
		w.mu.Unlock()
		if have {
			id, _ := msg["id"].(float64)
			if id == 0 {
				t.Fatalf("line %d carries no JSON-RPC id: %v", n, msg)
			}
			if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
				ch.(chan rpcResult) <- rpcResult{err: agentErr}
				return
			}
			t.Fatalf("line %d (id %v) is no longer pending", n, id)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("bridge never wrote line", n)
}

// promptTimeout 撞上一个还在正常输出的回合时，错误不得上事件流：前端收到
// 带 sessionId 的 error 会收口当前流、往 scrollback 塞一条带「重启 agent」
// 按钮的错误行——而重启会真的杀掉那个健康在跑的回合。
func TestPromptBudgetTimeoutOnLiveTurnIsNotSurfaced(t *testing.T) {
	b, _ := metaReadyBridge(t)
	sub, unsub := b.Subscribe()
	defer unsub()

	feedUpdate(t, b, "s1", "agent_message_chunk")
	countEvents(sub, "sessions_changed") // 张开回合的那一次翻转，不计入本测试

	if b.reportPromptFailure("s1", errors.New("session/prompt 超时")) {
		t.Error("prompt-budget timeout on a streaming turn was surfaced as a turn error")
	}
	if ev, ok := firstEvent(sub, kError); ok {
		t.Errorf("live channel carried an unexpected error event: %v", ev)
	}
	b.mu.Lock()
	busy := b.sessions["s1"].Busy
	b.mu.Unlock()
	if !busy {
		t.Error("suppressed failure must not change the session state — want still active")
	}
}

// 反例：agent 静默（观察腿过期或根本没有）时的传输失败必须照报，否则真卡死
// 的回合就没人告知了。
func TestSilentTransportFailureStillSurfaced(t *testing.T) {
	b, _ := metaReadyBridge(t)
	sub, unsub := b.Subscribe()
	defer unsub()

	if !b.reportPromptFailure("s1", errors.New("session/prompt 超时")) {
		t.Error("timeout with no agent activity at all was suppressed — want reported")
	}
	ev, ok := firstEvent(sub, kError)
	if !ok {
		t.Fatal("no error event on the live channel")
	}
	if ev["source"] != "transport" {
		t.Errorf("error source = %v, want transport", ev["source"])
	}
	if ev[kSessionID] != "s1" {
		t.Errorf("error sessionId = %v, want s1", ev[kSessionID])
	}

	// 观察腿张着但已静默超过 turnLivenessWindow：等同卡死，照报。
	b.mu.Lock()
	b.turns["s2"] = &observedTurn{open: true, seenAt: time.Now().Add(-turnLivenessWindow - time.Minute).UnixMilli()}
	b.mu.Unlock()
	if !b.reportPromptFailure("s2", errors.New("session/prompt 超时")) {
		t.Error("timeout after a long silent turn was suppressed — want reported")
	}
}

// agent 自己回了错误（进程活着、拒绝了回合）时，即便回合正在输出也必须上
// 事件流——那是真失败。走完整 PromptWithOpts 路径验证接线。
func TestAgentRejectedPromptAlwaysSurfaced(t *testing.T) {
	b, w := metaReadyBridge(t)
	sub, unsub := b.Subscribe()
	defer unsub()
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		_, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{kType: "text", "text": "hi"}}, PromptOpts{})
		done <- err
	}()
	waitLineCount(t, w, 1)
	// 回合已经在流式输出（prompt 送出时观察腿就已张开）。
	feedUpdate(t, b, "s1", "agent_message_chunk")
	resolveLineWithError(t, b, w, 0, &RPCError{Code: -32603, Msg: "Internal Error from the model API"})

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("prompt returned no error although the agent rejected the turn")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prompt never resolved")
	}

	ev, ok := firstEvent(sub, kError)
	if !ok {
		t.Fatal("an agent-rejected turn produced no error event")
	}
	if ev["source"] != "agent" {
		t.Errorf("error source = %v, want agent", ev["source"])
	}
	b.mu.Lock()
	busy := b.sessions["s1"].Busy
	b.mu.Unlock()
	if busy {
		t.Error("session still busy after the agent answered with an error")
	}
}
