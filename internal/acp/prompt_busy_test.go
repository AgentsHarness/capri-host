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
// 清成 idle。事件纪律：busy 只在 0→1 报一次，sessions_changed 只在
// busy→idle 报一次。
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

	// 事件纪律：busy 只报一次（A 的 0→1），sessions_changed 只报一次
	// （B 的 busy→idle）——A 的 resolve 不得误报 idle。
	var busyEvents, rosterEvents int
	deadline := time.After(2 * time.Second)
	for busyEvents+rosterEvents < 2 {
		select {
		case ev := <-sub:
			switch ev["type"] {
			case "busy":
				busyEvents++
			case "sessions_changed":
				rosterEvents++
			}
		case <-deadline:
			t.Fatalf("events exhausted: busy=%d sessions_changed=%d",
				busyEvents, rosterEvents)
		}
	}
	if busyEvents != 1 || rosterEvents != 1 {
		t.Errorf("event counts = busy:%d sessions_changed:%d, want 1/1",
			busyEvents, rosterEvents)
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
