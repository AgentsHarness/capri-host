package acp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func idleUnloadBridge(t *testing.T, capN int) (*Bridge, *recordingStdin) {
	t.Helper()
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        t.TempDir(),
		ResidentCap:     capN,
	})
	w := &recordingStdin{}
	b.mu.Lock()
	b.ready = true
	b.stdin = w
	b.mu.Unlock()
	return b, w
}

func addIdle(b *Bridge, id string, last, created int64) {
	b.mu.Lock()
	b.sessions[id] = &SessionState{
		SessionID:    id,
		Cwd:          "/ws",
		LastActiveAt: last,
		CreatedAt:    created,
	}
	b.mu.Unlock()
}

func TestSweepIdleUnloadClosesOldestKeepsListed(t *testing.T) {
	b, w := idleUnloadBridge(t, 2)
	addIdle(b, "keep-new", 900, 100)
	addIdle(b, "drop-old", 100, 50)
	addIdle(b, "drop-mid", 200, 60)
	b.mu.Lock()
	b.activeSessionID = "keep-new"
	b.lastSessionID = "keep-new"
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan int, 1)
	go func() { done <- b.SweepIdleUnload(ctx) }()
	waitLineCount(t, w, 1)
	resolveNext(t, b, w, map[string]any{"ok": true})
	n := <-done
	if n != 1 {
		t.Fatalf("unloaded %d, want 1 (3 resident, cap 2, 1 pinned)", n)
	}

	if !b.HasSession("drop-old") {
		t.Fatal("unloaded sessions must stay listable (HasSession)")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sessions["drop-old"].unloaded {
		t.Fatal("oldest idle session should be unloaded")
	}
	if b.sessions["drop-mid"].unloaded {
		t.Fatal("newer idle session fills the remaining cap slot")
	}
	if b.sessions["keep-new"].unloaded {
		t.Fatal("focused session must stay resident")
	}
	if b.activeSessionID != "keep-new" || b.lastSessionID != "keep-new" {
		t.Fatalf("focus pointers cleared: active=%q last=%q", b.activeSessionID, b.lastSessionID)
	}
}

func TestSweepIdleUnloadPinsBusyAndAwaiting(t *testing.T) {
	b, w := idleUnloadBridge(t, 1)
	addIdle(b, "idle", 10, 1)
	b.mu.Lock()
	b.sessions["busy"] = &SessionState{SessionID: "busy", Cwd: "/ws", Busy: true, LastActiveAt: 1, CreatedAt: 1}
	b.sessions["await"] = &SessionState{SessionID: "await", Cwd: "/ws", Busy: true, AwaitingInput: true, LastActiveAt: 2, CreatedAt: 1}
	b.activeSessionID = "busy"
	b.lastSessionID = "busy"
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan int, 1)
	go func() { done <- b.SweepIdleUnload(ctx) }()
	waitLineCount(t, w, 1)
	resolveNext(t, b, w, map[string]any{"ok": true})
	if n := <-done; n != 1 {
		t.Fatalf("unloaded %d, want 1 (only the idle one)", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sessions["idle"].unloaded {
		t.Fatal("idle session should be unloaded")
	}
	if b.sessions["busy"].unloaded || b.sessions["await"].unloaded {
		t.Fatal("busy/awaiting sessions must stay resident")
	}
}

func TestDefaultResidentCapUnloadsDownToFour(t *testing.T) {
	b, w := idleUnloadBridge(t, 0) // 0 → DefaultResidentCap (4)
	for i, id := range []string{"a", "b", "c", "d", "e"} {
		addIdle(b, id, int64(i+1), 1)
	}
	ctx := context.Background()
	done := make(chan int, 1)
	go func() { done <- b.SweepIdleUnload(ctx) }()
	waitLineCount(t, w, 1)
	resolveNext(t, b, w, map[string]any{"ok": true})
	if n := <-done; n != 1 {
		t.Fatalf("unloaded %d, want 1 (5 resident, cap 4)", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sessions["a"].unloaded {
		t.Fatal("oldest (a) should be unloaded")
	}
	for _, id := range []string{"b", "c", "d", "e"} {
		if b.sessions[id].unloaded {
			t.Fatalf("%s should stay resident", id)
		}
	}
}

func TestSweepIdleUnloadDisabledWhenCapNegative(t *testing.T) {
	b, _ := idleUnloadBridge(t, -1)
	addIdle(b, "a", 1, 1)
	addIdle(b, "b", 2, 1)
	if n := b.SweepIdleUnload(context.Background()); n != 0 {
		t.Fatalf("disabled cap unloaded %d", n)
	}
}

func TestSweepIdleUnloadNoopAtOrUnderCap(t *testing.T) {
	b, w := idleUnloadBridge(t, 4)
	addIdle(b, "a", 1, 1)
	addIdle(b, "b", 2, 1)
	b.mu.Lock()
	b.activeSessionID = "a"
	b.lastSessionID = "a"
	b.mu.Unlock()
	if n := b.SweepIdleUnload(context.Background()); n != 0 {
		t.Fatalf("under cap unloaded %d", n)
	}
	if len(w.lines) != 0 {
		t.Fatalf("wrote %d RPCs, want 0", len(w.lines))
	}
}

func TestLoadSessionClearsUnloaded(t *testing.T) {
	b, w := idleUnloadBridge(t, 4)
	addIdle(b, "s1", 1, 1)
	b.mu.Lock()
	b.sessions["s1"].unloaded = true
	b.activeSessionID = "other"
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		_, err := b.LoadSession(ctx, "s1", "/ws")
		done <- err
	}()
	resolveNext(t, b, w, map[string]any{"sessionId": "s1"})
	if err := <-done; err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	method, params := lastRequestParams(t, w)
	if method != "session/load" {
		t.Fatalf("method = %s, want session/load", method)
	}
	if params["sessionId"] != "s1" {
		t.Fatalf("sessionId = %v", params["sessionId"])
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sessions["s1"].unloaded {
		t.Fatal("load must mark the session resident again")
	}
}

func TestQueueHoldsWork(t *testing.T) {
	cases := []struct {
		name string
		q    map[string]any
		want bool
	}{
		{"nil", nil, false},
		{"empty map", map[string]any{}, false},
		{"post-turn empty entries", map[string]any{"sessionId": "s", "entries": []any{}}, false},
		{"nil running id", map[string]any{"entries": []any{}, "running_prompt_id": nil}, false},
		{"empty runningPromptId", map[string]any{"entries": []any{}, "runningPromptId": ""}, false},
		{"pending entry", map[string]any{"entries": []any{map[string]any{"id": "q1"}}}, true},
		{"runningPromptId", map[string]any{"entries": []any{}, "runningPromptId": "p1"}, true},
		{"running_prompt_id", map[string]any{"entries": []any{}, "running_prompt_id": "p1"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := queueHoldsWork(tc.q); got != tc.want {
				t.Fatalf("queueHoldsWork(%v) = %v, want %v", tc.q, got, tc.want)
			}
		})
	}
}

// 空 {entries:[], sessionId} 是回合结束后的常态广播，不得把会话钉在
// grok 里（现网曾因此 25h 只卸掉 2 个）。有 pending entries 或
// runningPromptId 的才 pin。
func TestSweepIdleUnloadEmptyQueueSnapshotDoesNotPin(t *testing.T) {
	b, w := idleUnloadBridge(t, 1)
	addIdle(b, "empty-q", 10, 1)
	addIdle(b, "keep", 20, 1)
	b.mu.Lock()
	b.activeSessionID = "keep"
	b.lastSessionID = "keep"
	b.queueSnapshots = map[string]map[string]any{
		"empty-q": {"sessionId": "empty-q", "entries": []any{}},
		"keep":    {"sessionId": "keep", "entries": []any{}},
	}
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan int, 1)
	go func() { done <- b.SweepIdleUnload(ctx) }()
	waitLineCount(t, w, 1)
	resolveNext(t, b, w, map[string]any{"ok": true})
	if n := <-done; n != 1 {
		t.Fatalf("unloaded %d, want 1 (empty queue snapshot must not pin)", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sessions["empty-q"].unloaded {
		t.Fatal("empty-q should be unloaded")
	}
	if b.sessions["keep"].unloaded {
		t.Fatal("focused keep must stay resident")
	}
}

func TestSweepIdleUnloadPendingQueuePins(t *testing.T) {
	b, w := idleUnloadBridge(t, 1)
	addIdle(b, "queued", 10, 1)
	addIdle(b, "idle", 5, 1)
	b.mu.Lock()
	b.queueSnapshots = map[string]map[string]any{
		"queued": {"sessionId": "queued", "entries": []any{map[string]any{"id": "q1", "text": "hi"}}},
	}
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan int, 1)
	go func() { done <- b.SweepIdleUnload(ctx) }()
	waitLineCount(t, w, 1)
	resolveNext(t, b, w, map[string]any{"ok": true})
	if n := <-done; n != 1 {
		t.Fatalf("unloaded %d, want 1 (idle goes, queued stays)", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sessions["idle"].unloaded {
		t.Fatal("idle session should be unloaded")
	}
	if b.sessions["queued"].unloaded {
		t.Fatal("session with pending queue entries must stay resident")
	}
}

func TestSweepIdleUnloadRunningPromptQueuePins(t *testing.T) {
	b, w := idleUnloadBridge(t, 1)
	addIdle(b, "running", 10, 1)
	addIdle(b, "idle", 5, 1)
	b.mu.Lock()
	b.queueSnapshots = map[string]map[string]any{
		"running": {"sessionId": "running", "entries": []any{}, "runningPromptId": "p1"},
	}
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan int, 1)
	go func() { done <- b.SweepIdleUnload(ctx) }()
	waitLineCount(t, w, 1)
	resolveNext(t, b, w, map[string]any{"ok": true})
	if n := <-done; n != 1 {
		t.Fatalf("unloaded %d, want 1 (idle goes, running stays)", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sessions["idle"].unloaded {
		t.Fatal("idle session should be unloaded")
	}
	if b.sessions["running"].unloaded {
		t.Fatal("session with runningPromptId must stay resident")
	}
}

func TestPromptRehydratesUnloadedSession(t *testing.T) {
	b, w := idleUnloadBridge(t, 4)
	addIdle(b, "s1", 1, 1)
	b.mu.Lock()
	b.sessions["s1"].unloaded = true
	b.activeSessionID = "s1"
	b.mu.Unlock()

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		_, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, PromptOpts{})
		done <- err
	}()
	waitLineCount(t, w, 1)
	resolveLine(t, b, w, 0, map[string]any{"sessionId": "s1"})
	waitLineCount(t, w, 2)
	resolveLine(t, b, w, 1, map[string]any{"stopReason": "end_turn"})
	if err := <-done; err != nil {
		t.Fatalf("PromptWithOpts: %v", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.lines) < 2 {
		t.Fatalf("wrote %d RPCs, want load then prompt", len(w.lines))
	}
	var load, prompt map[string]any
	if err := json.Unmarshal(w.lines[0], &load); err != nil {
		t.Fatal(err)
	}
	if load["method"] != "session/load" {
		t.Fatalf("first method = %v, want session/load", load["method"])
	}
	if err := json.Unmarshal(w.lines[1], &prompt); err != nil {
		t.Fatal(err)
	}
	if prompt["method"] != "session/prompt" {
		t.Fatalf("second method = %v, want session/prompt", prompt["method"])
	}
}
