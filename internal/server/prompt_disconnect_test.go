package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
)

// prompt_disconnect_test.go — 受理即返回（async accept）语义：
// POST /api/prompt 只确认"已受理"，回合在后台跑且与客户端连接解耦——
// 客户端断开绝不取消回合；进度与结果经 live 通道送达。

// TestPromptDisconnectKeepsTurnRunning verifies the async-accept contract:
// the POST returns as soon as the prompt is accepted (before the turn
// ends), and the turn runs on a context independent of the request — a
// client disconnect never cancels it; progress/completion ride the live
// channel only.
func TestPromptDisconnectKeepsTurnRunning(t *testing.T) {
	b := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h", HostName: "host"})
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)

	called := make(chan struct{})
	turnCtx := make(chan context.Context, 1)
	released := make(chan struct{})
	s.promptFn = func(ctx context.Context, _ string, _ []acp.ContentBlock) (string, error) {
		turnCtx <- ctx
		close(called)
		<-released // simulate a long-running turn; we control when it ends
		return "completed", nil
	}

	req := httptest.NewRequest("POST", "/api/prompt",
		strings.NewReader(`{"blocks":[{"type":"text","text":"hi"}]}`))
	reqCtx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(reqCtx)
	w := httptest.NewRecorder()

	// The handler returns immediately with the accept confirmation, even
	// though the turn is still running (promptFn blocks on <-released).
	s.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/prompt status = %d, body=%s", w.Code, w.Body.String())
	}
	if body := w.Body.String(); !strings.Contains(body, `"ok":true`) {
		t.Fatalf("accepted body = %s, want {ok:true}", body)
	}

	// The turn must run on a context independent of the request.
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("promptFn was not invoked")
	}
	ctx := <-turnCtx

	// Simulate the browser crashing: cancel the request context. The
	// handler already returned; the turn must still be alive.
	cancel()
	select {
	case <-ctx.Done():
		t.Fatal("turn context was cancelled on client disconnect — the turn would be killed")
	default:
	}

	// Let the turn finish; it must complete normally (no panic / no hang).
	close(released)
}

// TestPromptConnectedReturnsAccepted — happy path: a connected client gets
// the accept confirmation {ok:true} immediately; the turn result no longer
// rides the HTTP response (it arrives via the live done event).
func TestPromptConnectedReturnsAccepted(t *testing.T) {
	b := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h", HostName: "host"})
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)

	ran := make(chan struct{})
	s.promptFn = func(_ context.Context, _ string, _ []acp.ContentBlock) (string, error) {
		close(ran)
		return "completed", nil
	}

	req := httptest.NewRequest("POST", "/api/prompt",
		strings.NewReader(`{"blocks":[{"type":"text","text":"hi"}]}`))
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"ok":true`) {
		t.Fatalf("body = %s, want {ok:true}", body)
	}
	if strings.Contains(body, "stopReason") {
		t.Fatalf("accept response must not carry stopReason: %s", body)
	}
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("promptFn was not invoked")
	}
}
