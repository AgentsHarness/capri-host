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

// TestPromptDisconnectKeepsTurnRunning verifies the fix: when the client
// (browser) dies mid-turn, the running turn must NOT be cancelled — only the
// HTTP handler returns. The turn's context is decoupled from the request, so
// a disconnect no longer aborts the grok agent's work.
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

	handlerDone := make(chan struct{})
	go func() {
		s.http.Handler.ServeHTTP(w, req)
		close(handlerDone)
	}()

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("promptFn was not invoked")
	}

	// The turn must run on a context independent of the request.
	ctx := <-turnCtx

	// Simulate the browser crashing: cancel the request context.
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after client disconnect")
	}

	// The turn must still be alive — its context must not have been cancelled.
	select {
	case <-ctx.Done():
		t.Fatal("turn context was cancelled on client disconnect — the turn would be killed")
	default:
	}

	// Let the turn finish; it must complete normally (no panic / no hang).
	close(released)
}

// TestPromptDisconnectReusesOriginalContext ensures the handler still serves
// the happy path: a connected client gets the stopReason once the turn ends.
func TestPromptConnectedReturnsStopReason(t *testing.T) {
	b := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h", HostName: "host"})
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)

	s.promptFn = func(_ context.Context, _ string, _ []acp.ContentBlock) (string, error) {
		return "completed", nil
	}

	req := httptest.NewRequest("POST", "/api/prompt",
		strings.NewReader(`{"blocks":[{"type":"text","text":"hi"}]}`))
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if body := w.Body.String(); !strings.Contains(body, `"stopReason":"completed"`) {
		t.Fatalf("body = %s, want stopReason=completed", body)
	}
}
