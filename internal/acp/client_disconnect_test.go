package acp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// discardWriteCloser accepts writes without a reader, standing in for the
// grok process stdin in tests that must not spawn a real process.
type discardWriteCloser struct{}

func (discardWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (discardWriteCloser) Close() error                { return nil }

// A canceled caller context must surface as context.Canceled (so callers
// can tell a dead client apart from a dead agent), and must not leak the
// pending RPC entry.
func TestRequestWrapsClientCancel(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.request(ctx, "session/prompt", map[string]any{}, time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("request err = %v, want wrapped context.Canceled", err)
	}
	if _, ok := b.pending.Load("1"); ok {
		t.Error("pending entry leaked after canceled request")
	}
}

// A plain timeout must stay a timeout — not be misclassified as a client
// disconnect.
func TestRequestTimeoutStaysTimeout(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()

	_, err := b.request(context.Background(), "session/prompt", map[string]any{}, 10*time.Millisecond)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a timeout error, not client cancel", err)
	}
}

// The core no-false-kill guarantee: a client disconnect mid-prompt must
// cancel the orphaned turn WITHOUT killing the process, wiping the roster,
// or spawning anything new.
func TestPromptClientDisconnectDoesNotKillProcess(t *testing.T) {
	dir := t.TempDir()
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: filepath.Join(dir, "last-session.json"),
	})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.activeSessionID = "s1"
	b.ready = true
	b.stdin = discardWriteCloser{}
	b.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.Prompt(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Prompt err = %v, want context.Canceled", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cmd != nil {
		t.Error("a process was spawned/killed on client disconnect — cmd must stay untouched")
	}
	if !b.ready {
		t.Error("ready flipped to false — process state was reset on client disconnect")
	}
	if len(b.sessions) != 1 || b.sessions["s1"] == nil {
		t.Errorf("roster wiped on client disconnect: %d sessions", len(b.sessions))
	}
	if b.sessions["s1"].Busy {
		t.Error("session still marked busy after the orphaned turn was canceled")
	}
}

// A second caller during an in-flight boot must wait for the boot outcome
// instead of racing it (which previously produced a bogus "grok 进程未运行"
// failure that killed the just-spawned process). Simulated with a
// pre-closed bootDone so no real process and no goroutine scheduling is
// involved; Bin points at a non-existent binary so an accidental boot
// fails fast instead of spawning a real agent.
func TestEnsureBootedWaitsForInFlightBoot(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.booting = true
	b.bootDone = make(chan struct{})
	b.ready = true
	close(b.bootDone) // the in-flight boot finished successfully
	b.mu.Unlock()

	if err := b.ensureBooted(context.Background()); err != nil {
		t.Fatalf("ensureBooted = %v, want nil", err)
	}
}

// A waiter must observe a failed boot's error, not invent its own.
func TestEnsureBootedWaiterSeesBootFailure(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "/nonexistent/grok"})
	b.mu.Lock()
	b.booting = true
	b.bootDone = make(chan struct{})
	b.bootError = "boom"
	close(b.bootDone) // the in-flight boot finished with an error
	b.mu.Unlock()

	err := b.ensureBooted(context.Background())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("ensureBooted = %v, want 'boom'", err)
	}
}
