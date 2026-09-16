package acp

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// client_request_budget_test.go — 客户端请求的等待预算。
//
// 原始问题：host 对所有转发请求一律用 approvalTimeout（15 分钟）计时，
// 而 x.ai/ask_user_question 的预算由 [toolset.ask_user_question] 决定
// （agent 的 RESPONSE_TIMEOUT 与 FE 倒计时都用它，默认 1800s）。于是提问
// 卡片会在用户还在看的时候被 host 提前摘掉，把 agent 侧还合法的等待判成
// 「提问超时」。

// newBridgeWithToolset 建一个 GrokHome 指向临时目录的 bridge，并按给定
// 内容写入 config.toml（"" = 不建文件）。
func newBridgeWithToolset(t *testing.T, config string) *Bridge {
	t.Helper()
	home := t.TempDir()
	if config != "" {
		if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(config), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        home,
	})
	t.Cleanup(b.Shutdown)
	return b
}

func TestClientRequestBudget(t *testing.T) {
	const cfg1800 = `
[toolset.ask_user_question]
timeout_secs = 1800
`
	for _, tc := range []struct {
		name     string
		config   string
		method   string
		want     time.Duration
		deadline bool
	}{
		{"permission keeps the host constant", cfg1800, "session/request_permission", approvalTimeout, true},
		{"question uses the configured budget", cfg1800, methodAskUserQuestion, 30 * time.Minute, true},
		{"custom budget", "[toolset.ask_user_question]\ntimeout_secs = 60\n", methodAskUserQuestion, time.Minute, true},
		{"no config falls back to the agent default", "", methodAskUserQuestion, askUserQuestionTimeoutDefault, true},
		{"unparsable config falls back", "this is not toml =", methodAskUserQuestion, askUserQuestionTimeoutDefault, true},
		{"disabled timer means no host deadline", "[toolset.ask_user_question]\ntimeout_enabled = false\ntimeout_secs = 60\n", methodAskUserQuestion, 0, false},
		{"zero budget falls back", "[toolset.ask_user_question]\ntimeout_secs = 0\n", methodAskUserQuestion, askUserQuestionTimeoutDefault, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBridgeWithToolset(t, tc.config)
			got, deadline := b.clientRequestBudget(&clientRequest{Method: tc.method})
			if deadline != tc.deadline || got != tc.want {
				t.Fatalf("budget(%s) = (%v, deadline=%v), want (%v, %v)", tc.method, got, deadline, tc.want, tc.deadline)
			}
		})
	}
}

// 提问预算必须真的驱动计时器：配置成很短时 host 到点就收口，且错误文案是
// 「提问超时」（不是审批超时——它会让用户去查错误的设置项）。
func TestQuestionBudgetDrivesTheTimer(t *testing.T) {
	b := newBridgeWithToolset(t, "[toolset.ask_user_question]\ntimeout_secs = 1\n")
	w := &recordingStdin{}
	b.mu.Lock()
	b.ready = true
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.activeSessionID = "s1"
	b.stdin = w
	b.mu.Unlock()

	ch, unsub := b.Subscribe()
	defer unsub()

	b.forwardXaiRequest(float64(7), methodAskUserQuestion, map[string]any{"sessionId": "s1"})
	b.mu.Lock()
	awaiting := b.sessions["s1"].AwaitingInput
	b.mu.Unlock()
	if !awaiting {
		t.Fatal("question did not mark the session awaiting input")
	}

	// 1 秒预算 + 富余：超时后 host 回错误响应并清掉等待态、摘掉卡片。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		awaiting = b.sessions["s1"].AwaitingInput
		b.mu.Unlock()
		if !awaiting {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if awaiting {
		t.Fatal("question was never expired by its configured budget")
	}
	if n := len(b.Snapshot().PendingRequests); n != 0 {
		t.Fatalf("snapshot retains %d requests after expiry", n)
	}
	if n := countEvents(ch, "client_request_resolved"); n != 1 {
		t.Fatalf("resolved events = %d, want 1", n)
	}
	res := waitForWireError(t, w, 7)
	if msg, _ := res["message"].(string); msg != "提问超时" {
		t.Fatalf("expiry message = %q, want 提问超时 (a permission-only wording sends the user to the wrong setting)", msg)
	}
}

// waitForWireError polls the recording stdin until a JSON-RPC error response
// with the given id appears (waitForWireResponse only reads successful
// results, so it cannot see an expiry).
func waitForWireError(t *testing.T, w *recordingStdin, id float64) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if msg := w.last(); msg != nil {
			if mid, ok := msg["id"].(float64); ok && mid == id {
				if errObj, ok := msg["error"].(map[string]any); ok {
					return errObj
				}
				t.Fatalf("response for id=%v is not an error: %#v", id, msg)
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("bridge never wrote an error response (id=%v) to stdin", id)
	return nil
}

// 权限请求不受 [toolset.ask_user_question] 影响：配置一个极短的提问预算
// 也不能把权限卡片的等待时间缩短。
func TestPermissionUnaffectedByQuestionBudget(t *testing.T) {
	b := newBridgeWithToolset(t, "[toolset.ask_user_question]\ntimeout_secs = 1\n")
	got, deadline := b.clientRequestBudget(&clientRequest{Method: "session/request_permission", isPermission: true})
	if !deadline || got != approvalTimeout {
		t.Fatalf("permission budget = (%v, %v), want (%v, true)", got, deadline, approvalTimeout)
	}
}

// timeout_enabled = false：host 不设截止时间，等待只由浏览器答复或 agent
// 自己的超时结束（不得凭空造一个 15 分钟的期限）。
func TestDisabledQuestionTimerWaitsWithoutDeadline(t *testing.T) {
	b := newBridgeWithToolset(t, "[toolset.ask_user_question]\ntimeout_enabled = false\ntimeout_secs = 1\n")
	w := &recordingStdin{}
	b.mu.Lock()
	b.ready = true
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.activeSessionID = "s1"
	b.stdin = w
	b.mu.Unlock()

	b.forwardXaiRequest(float64(7), methodAskUserQuestion, map[string]any{"sessionId": "s1"})
	// 远长于被禁用前的 1 秒预算：请求必须仍然挂着（既没超时也没被收口）。
	time.Sleep(1500 * time.Millisecond)
	if n := len(b.Snapshot().PendingRequests); n != 1 {
		t.Fatalf("pending = %d, want the question still open", n)
	}
	b.mu.Lock()
	awaiting := b.sessions["s1"].AwaitingInput
	b.mu.Unlock()
	if !awaiting {
		t.Fatal("session left awaiting-input although no deadline applies")
	}
	if w.last() != nil {
		t.Fatalf("host answered a question it had no deadline for: %s", w.last())
	}
}
