package acp

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockedClientResponseWriter struct {
	recordingStdin
	entered  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (w *blockedClientResponseWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	n, err := w.recordingStdin.Write(p)
	close(w.finished)
	return n, err
}

func TestBlockedOldResponseDoesNotBlockResetOrReachNewProcess(t *testing.T) {
	b, _ := metaReadyBridge(t)
	old := &blockedClientResponseWriter{entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(old.release) })
	b.mu.Lock()
	b.stdin = old
	b.mu.Unlock()
	b.forwardPermission(float64(7), "session/request_permission", permissionParams(allowAlwaysOption()))
	if err := b.RespondPermissionWithMeta("acp_cr_2", "allow-always-command", false, nil, ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.entered:
	case <-time.After(time.Second):
		t.Fatal("old response did not start")
	}
	reset := make(chan struct{})
	go func() { b.resetRoster("blocked-pipe"); close(reset) }()
	select {
	case <-reset:
	case <-time.After(time.Second):
		t.Fatal("blocked write prevented process retirement")
	}
	replacement := &recordingStdin{}
	b.mu.Lock()
	b.stdin = replacement
	b.mu.Unlock()
	// The write already selected the old pipe before process retirement.
	release.Do(func() { close(old.release) })
	select {
	case <-old.finished:
	case <-time.After(time.Second):
		t.Fatal("old write did not finish")
	}
	if replacement.last() != nil {
		t.Fatal("old response reached replacement")
	}
}

func TestClientRequestTimeoutRejectsLateAnswer(t *testing.T) {
	for _, permission := range []bool{true, false} {
		b, w := metaReadyBridge(t)
		if permission {
			b.forwardPermission(float64(7), "session/request_permission", permissionParams(allowAlwaysOption()))
		} else {
			b.forwardXaiRequest(float64(7), "x.ai/ask_user_question", map[string]any{"sessionId": "s1"})
		}
		value, _ := b.clientReqs.Load("acp_cr_2")
		cr := value.(*clientRequest)
		b.expireClientRequest("acp_cr_2", cr)
		msg := w.last()
		if msg == nil {
			t.Fatal("timeout did not write a response")
		}
		if permission {
			outcome := msg["result"].(map[string]any)["outcome"].(map[string]any)
			if outcome["outcome"] != "cancelled" {
				t.Fatalf("timeout outcome: %v", outcome)
			}
		} else if msg["error"].(map[string]any)["code"] != float64(-32002) {
			t.Fatalf("timeout error: %v", msg)
		}
		if err := b.RespondClientRequest("acp_cr_2", map[string]any{"answer": "late"}, ""); err == nil {
			t.Fatal("late answer accepted")
		}
		b.expireClientRequest("acp_cr_2", cr)
		w.mu.Lock()
		n := len(w.lines)
		w.mu.Unlock()
		if n != 1 {
			t.Fatalf("timeout responded %d times", n)
		}
	}
}

func TestOnlyResponsesConsumePending(t *testing.T) {
	for _, msg := range []map[string]any{
		{"id": float64(7)},
		{"id": float64(7), "method": ""},
		{"id": float64(7), "method": 123, "result": nil},
		{"id": float64(7), "error": nil},
		{"id": float64(7), "result": nil, "error": map[string]any{}},
		{"method": "unknown_notification"},
	} {
		b, _ := metaReadyBridge(t)
		b.pending.Store("7", make(chan rpcResult, 1))
		b.onAgentMessage(msg)
		if _, ok := b.pending.Load("7"); !ok {
			t.Fatalf("non-response consumed pending: %v", msg)
		}
	}
	b, _ := metaReadyBridge(t)
	ch := make(chan rpcResult, 1)
	b.pending.Store("7", ch)
	b.onAgentMessage(map[string]any{"id": float64(7), "result": nil})
	select {
	case <-ch:
	default:
		t.Fatal("null result is a valid response")
	}
}

func TestRetiredRequestsCannotRespondToReplacementProcess(t *testing.T) {
	b, w := metaReadyBridge(t)
	tabs := make([]chan Event, 2)
	for i := range tabs {
		ch, unsub := b.Subscribe()
		tabs[i] = ch
		defer unsub()
	}
	id := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
	v, _ := b.clientReqs.Load(id)
	old := v.(*clientRequest)
	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	b.cancelRd = cancel
	b.mu.Unlock()
	b.resetRoster("test-restart")
	for _, tab := range tabs {
		if n := countEvents(tab, "client_request_resolved"); n != 1 {
			t.Fatalf("resolved count = %d", n)
		}
	}
	if n := len(b.Snapshot().PendingRequests); n != 0 {
		t.Fatalf("snapshot retains %d requests", n)
	}
	replacement := &recordingStdin{}
	b.mu.Lock()
	b.stdin = replacement
	b.sessions["s1"] = &SessionState{SessionID: "s1"}
	b.activeSessionID = "s1"
	b.mu.Unlock()
	newID := pushPermission(t, b, replacement, permissionParams(allowAlwaysOption()), 7)
	if id == newID {
		t.Fatal("browser request ID reused across processes")
	}
	// Exercise every delayed path after a new agent reuses the same wire ID.
	b.resolveClientRequest(id, old)
	b.expireClientRequest(id, old)
	if err := b.RespondPermissionWithMeta(id, "allow-always-command", false, nil, ""); err == nil {
		t.Fatal("late click accepted")
	}
	if err := b.RespondClientRequest(id, map[string]any{"approved": true}, ""); err == nil {
		t.Fatal("late generic answer accepted")
	}
	b.handleStdoutLineContext(ctx, []byte(`{"id":8,"method":"session/request_permission","params":{"sessionId":"s1"}}`))
	if replacement.last() != nil {
		t.Fatal("old response was written to replacement process")
	}
	if w.last() != nil {
		t.Fatal("retired request wrote a response")
	}
	if len(b.Snapshot().PendingRequests) != 1 {
		t.Fatal("old stdout revived a retired request")
	}
	b.mu.Lock()
	awaiting := b.sessions["s1"].AwaitingInput
	b.mu.Unlock()
	if !awaiting {
		t.Fatal("old waiter cleared new request's awaiting state")
	}
	if err := b.RespondPermissionWithMeta(newID, "allow-always-command", false, nil, ""); err != nil {
		t.Fatal(err)
	}
	waitForWireResponse(t, b, replacement, 7)
}

func TestClientRequestConcurrentCompletion(t *testing.T) {
	for _, permission := range []bool{true, false} {
		b, w := metaReadyBridge(t)
		ch, unsub := b.Subscribe()
		var id string
		if permission {
			id = pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
		} else {
			b.forwardXaiRequest(float64(7), "x.ai/ask_user_question", map[string]any{"sessionId": "s1"})
			id = "acp_cr_2"
		}
		v, ok := b.clientReqs.Load(id)
		if !ok {
			t.Fatal("missing request")
		}
		cr := v.(*clientRequest)
		var wg sync.WaitGroup
		var accepted atomic.Int32
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err error
				if permission {
					err = b.RespondPermissionWithMeta(id, "allow-always-command", false, nil, "")
				} else {
					err = b.RespondClientRequest(id, map[string]any{"answer": "yes"}, "")
				}
				if err == nil {
					accepted.Add(1)
				}
			}()
		}
		wg.Wait()
		b.resolveClientRequest(id, cr)
		waitForWireResponse(t, b, w, 7)
		b.expireClientRequest(id, cr)
		b.Cancel("s1")
		if accepted.Load() != 1 {
			t.Fatalf("accepted %d answers", accepted.Load())
		}
		w.mu.Lock()
		replies := 0
		for _, line := range w.lines {
			var m map[string]any
			_ = json.Unmarshal(line, &m)
			if m["id"] == float64(7) {
				replies++
			}
		}
		w.mu.Unlock()
		if replies != 1 {
			t.Fatalf("wrote %d responses", replies)
		}
		if n := countEvents(ch, "client_request_resolved"); n != 1 {
			t.Fatalf("broadcast %d completions", n)
		}
		unsub()
	}
}

func TestResetRacesAnswerCancelAndTimeout(t *testing.T) {
	for i := 0; i < 30; i++ {
		b, w := metaReadyBridge(t)
		ch, unsub := b.Subscribe()
		id := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
		v, _ := b.clientReqs.Load(id)
		cr := v.(*clientRequest)
		replacement := &recordingStdin{}
		var wg sync.WaitGroup
		actions := []func(){
			func() { _ = b.RespondPermissionWithMeta(id, "allow-always-command", false, nil, "") },
			func() { b.Cancel("s1") },
			func() { b.expireClientRequest(id, cr) },
			func() { b.resetRoster("test-race"); b.mu.Lock(); b.stdin = replacement; b.mu.Unlock() },
		}
		for _, action := range actions {
			wg.Add(1)
			go func(f func()) { defer wg.Done(); f() }(action)
		}
		wg.Wait()
		b.resolveClientRequest(id, cr)
		if replacement.last() != nil {
			t.Fatal("stale response reached new process")
		}
		if n := countEvents(ch, "client_request_resolved"); n != 1 {
			t.Fatalf("completion count %d", n)
		}
		unsub()
	}
}

func TestCancelTargetsOnlyValidSession(t *testing.T) {
	b, w := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s2"] = &SessionState{SessionID: "s2"}
	b.mu.Unlock()
	id1 := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
	params := permissionParams(allowAlwaysOption())
	params["sessionId"] = "s2"
	id2 := pushPermission(t, b, w, params, 8)
	defer func() { _ = b.RespondPermissionWithMeta(id2, "", true, nil, "") }()
	b.Cancel("missing")
	b.mu.Lock()
	b.activeSessionID = ""
	b.mu.Unlock()
	b.Cancel("")
	if len(b.Snapshot().PendingRequests) != 2 {
		t.Fatal("invalid target cleared requests")
	}
	b.mu.Lock()
	b.activeSessionID = "s1"
	b.mu.Unlock()
	b.Cancel("")
	if _, ok := b.clientReqs.Load(id1); ok {
		t.Fatal("active session request was not cancelled")
	}
	if _, ok := b.clientReqs.Load(id2); !ok {
		t.Fatal("other session request was cancelled")
	}
}
