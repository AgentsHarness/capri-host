package acp

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// prompt_busy_test.go — busy 会话的并发 prompt 语义（FE 队列对齐
// TUI server-authoritative 架构的 host 侧改动）：busy 不再 409，第二个
// session/prompt 照常转发（agent 自己排队）；Busy 状态机保持"任一在飞
// 即 busy"——先 resolve 的回合不得把会话清成 idle，只有最后一个
// resolve 才清。

// waitLineCount waits until the bridge has written at least n request
// lines to the fake stdin.
func waitLineCount(t *testing.T, w *recordingStdin, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		c := len(w.lines)
		w.mu.Unlock()
		if c >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	w.mu.Lock()
	c := len(w.lines)
	w.mu.Unlock()
	t.Fatalf("bridge wrote %d requests, want %d", c, n)
}

// resolveLine resolves the request the bridge wrote as the n-th line
// (0-based) with the given canned result — resolveNext's indexed cousin,
// needed when several requests are in flight concurrently (resolveNext
// only ever resolves the last-written one).
func resolveLine(t *testing.T, b *Bridge, w *recordingStdin, n int, result map[string]any) {
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
		if !have {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		id, ok := msg["id"].(float64)
		if !ok || id == 0 {
			t.Fatalf("line %d carries no JSON-RPC id: %v", n, msg)
		}
		if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
			ch.(chan rpcResult) <- rpcResult{result: result}
			return
		}
		t.Fatalf("line %d (id %v) is no longer pending — double resolve?", n, id)
	}
	t.Fatal("bridge never wrote line", n)
}

// 两个并发 prompt（同一会话）：A 在飞时 B 照常转发（不 409）；A 先
// resolve 后会话必须仍 busy（B 还在跑）；只有 B（最后一个）resolve 才
// 清成 idle。事件纪律：busy 只在 0→1 报一次；sessions_changed 在
// busy 翻转的两端各报一次（prompt 开始 active 行、回合收尾 idle 行）。
func TestPromptConcurrentBusyRefcount(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	type turnRes struct {
		sr  string
		err error
	}
	doneA := make(chan turnRes, 1)
	go func() {
		sr, _, err := b.PromptWithOpts(ctx, "s1",
			[]ContentBlock{{"type": "text", "text": "a"}}, PromptOpts{})
		doneA <- turnRes{sr, err}
	}()
	waitLineCount(t, w, 1) // A 已上 wire（未被 409）

	doneB := make(chan turnRes, 1)
	go func() {
		sr, _, err := b.PromptWithOpts(ctx, "s1",
			[]ContentBlock{{"type": "text", "text": "b"}}, PromptOpts{})
		doneB <- turnRes{sr, err}
	}()
	waitLineCount(t, w, 2) // B 已上 wire（busy 会话照常转发）

	// 两个回合在飞：Busy=true、busyCount=2。
	b.mu.Lock()
	st := b.sessions["s1"]
	if !st.Busy || st.busyCount != 2 {
		b.mu.Unlock()
		t.Fatalf("mid-flight state = busy:%v count:%d, want busy:true count:2",
			st.Busy, st.busyCount)
	}
	b.mu.Unlock()

	// A 先 resolve：A 完成，但 B 仍在飞 → 会话必须保持 busy。
	resolveLine(t, b, w, 0, map[string]any{"stopReason": "end_turn"})
	select {
	case r := <-doneA:
		if r.err != nil || r.sr != "end_turn" {
			t.Fatalf("prompt A = %q, %v — want end_turn", r.sr, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prompt A never resolved")
	}
	b.mu.Lock()
	st = b.sessions["s1"]
	if !st.Busy || st.busyCount != 1 {
		b.mu.Unlock()
		t.Fatalf("after A resolved = busy:%v count:%d, want busy:true count:1",
			st.Busy, st.busyCount)
	}
	b.mu.Unlock()

	// B 后 resolve：最后一个 → 清成 idle。
	resolveLine(t, b, w, 1, map[string]any{"stopReason": "end_turn"})
	select {
	case r := <-doneB:
		if r.err != nil || r.sr != "end_turn" {
			t.Fatalf("prompt B = %q, %v — want end_turn", r.sr, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prompt B never resolved")
	}
	b.mu.Lock()
	st = b.sessions["s1"]
	if st.Busy || st.busyCount != 0 {
		b.mu.Unlock()
		t.Fatalf("after B resolved = busy:%v count:%d, want busy:false count:0",
			st.Busy, st.busyCount)
	}
	b.mu.Unlock()

	// 事件纪律：busy 只在 A 的 0→1 报一次；sessions_changed 有两条——
	// prompt 开始（Busy 翻 true，行里 busy=true/state=active）和最后一个
	// 回合收尾（busy→idle）。A 的 resolve 不得报行。
	type rosterSnap struct {
		busy  any
		state any
	}
	var busyEvents int
	var rosters []rosterSnap
	deadline := time.After(2 * time.Second)
	for busyEvents < 1 || len(rosters) < 2 {
		select {
		case ev := <-sub:
			switch ev["type"] {
			case "busy":
				busyEvents++
			case "sessions_changed":
				params, _ := ev["params"].(map[string]any)
				upserted, _ := params["upserted"].([]any)
				if len(upserted) != 1 {
					t.Fatalf("sessions_changed params = %+v, want one upserted row", params)
				}
				row, _ := upserted[0].(map[string]any)
				status, _ := row["status"].(map[string]any)
				rosters = append(rosters, rosterSnap{busy: status["busy"], state: status["state"]})
			}
		case <-deadline:
			t.Fatalf("events exhausted: busy=%d sessions_changed=%d, want 1/2",
				busyEvents, len(rosters))
		}
	}
	if busyEvents != 1 {
		t.Errorf("busy events = %d, want 1 (only the 0→1, B must not re-announce)", busyEvents)
	}
	// 第一条是 prompt 0→1 的 active 行，第二条是最后一个回合的 idle 行；
	// A 先 resolve 时 B 仍在飞，不应多出第三条。
	if rosters[0].busy != true || rosters[0].state != "active" {
		t.Errorf("start row status = busy:%v state:%v, want busy:true state:active",
			rosters[0].busy, rosters[0].state)
	}
	if rosters[1].busy != false || rosters[1].state != "idle" {
		t.Errorf("end row status = busy:%v state:%v, want busy:false state:idle",
			rosters[1].busy, rosters[1].state)
	}
	// 事件已全部在订阅队列里：静默窗口内再出现 busy/sessions_changed 即误报。
	quiet := time.After(100 * time.Millisecond)
	for {
		select {
		case ev := <-sub:
			if typ := ev["type"]; typ == "busy" || typ == "sessions_changed" {
				t.Errorf("unexpected extra %v event after the last turn resolved", typ)
			}
		case <-quiet:
			return
		}
	}
}

// TestPromptStartBroadcastsBusyRow 盯住 prompt 路径自己的 0→1 广播：回合
// 还在等应答时，客户端就必须收到带 active 行的 sessions_changed（不等回合
// 结束、不等下一次列表拉取）。
func TestPromptStartBroadcastsBusyRow(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	sub, unsub := b.Subscribe()
	defer unsub()

	done := make(chan error, 1)
	go func() {
		_, _, err := b.PromptWithOpts(ctx, "s1",
			[]ContentBlock{{"type": "text", "text": "hi"}}, PromptOpts{})
		done <- err
	}()
	waitLineCount(t, w, 1) // 回合已上 wire，但还没有应答

	var gotBusy bool
	var startRow map[string]any
	deadline := time.After(5 * time.Second)
	for !gotBusy || startRow == nil {
		select {
		case ev := <-sub:
			switch ev["type"] {
			case "busy":
				gotBusy = true
			case "sessions_changed":
				if startRow != nil {
					continue
				}
				params, _ := ev["params"].(map[string]any)
				upserted, _ := params["upserted"].([]any)
				if len(upserted) != 1 {
					t.Fatalf("sessions_changed params = %+v, want one upserted row", params)
				}
				startRow, _ = upserted[0].(map[string]any)
			}
		case <-deadline:
			t.Fatalf("prompt 0→1 while still in flight: busy=%v row=%v",
				gotBusy, startRow != nil)
		}
	}
	if startRow["sessionId"] != "s1" {
		t.Errorf("upserted row sessionId = %v, want s1", startRow["sessionId"])
	}
	status, _ := startRow["status"].(map[string]any)
	if status["busy"] != true || status["state"] != "active" {
		t.Fatalf("start row status = %+v, want busy:true state:active", status)
	}

	resolveLine(t, b, w, 0, map[string]any{"stopReason": "end_turn"})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("prompt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prompt never resolved")
	}
}

// 传输失败（roster 清理）路径在并发 prompt 下不 panic、不误清：
// agent 进程死亡触发 resetRoster（roster 清空 + 全部 pending 失败），
// A、B 都随之失败；restoreLastSession 重建的会话对象与 A/B 启动时的
// 对象不是同一个，releaseBusy 必须跳过（不得把新会话的 busy 清掉）。
func TestPromptConcurrentTransportFailureNoPanic(t *testing.T) {
	b, w := metaReadyBridge(t)
	b.mu.Lock()
	// 让 restoreLastSession 无会话可恢复：lastSessionID 留空。
	b.lastSessionID = ""
	b.mu.Unlock()
	ctx := context.Background()

	doneA := make(chan error, 1)
	go func() {
		_, _, err := b.PromptWithOpts(ctx, "s1",
			[]ContentBlock{{"type": "text", "text": "a"}}, PromptOpts{})
		doneA <- err
	}()
	waitLineCount(t, w, 1)

	doneB := make(chan error, 1)
	go func() {
		_, _, err := b.PromptWithOpts(ctx, "s1",
			[]ContentBlock{{"type": "text", "text": "b"}}, PromptOpts{})
		doneB <- err
	}()
	waitLineCount(t, w, 2)

	// 模拟 agent 进程死亡：resetRoster（生产路径中由 waitProcess 在
	// 进程退出时触发），两个 pending RPC 都随之失败返回。
	b.resetRoster("test")
	for i, done := range []chan error{doneA, doneB} {
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("prompt %c must fail after resetRoster", 'a'+i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("prompt %c never returned after resetRoster", 'a'+i)
		}
	}

	// roster 已重建（resetRoster 清空 + restore 失败时为空）：无 panic，
	// 无残留 busy 引用。
	b.mu.Lock()
	st := b.sessions["s1"]
	if st != nil && (st.Busy || st.busyCount != 0) {
		b.mu.Unlock()
		t.Fatalf("session state after resetRoster = busy:%v count:%d, want idle",
			st.Busy, st.busyCount)
	}
	b.mu.Unlock()
}
