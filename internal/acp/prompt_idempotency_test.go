package acp

import (
	"context"
	"testing"
)

// 同一个 promptId 的第二次 PromptWithOpts 必须不再向 agent 发 session/prompt。
// 这是「消息可能已送达」路径的幂等保障：FE 判未送达后原样重发，host 若
// 把两条都转下去，用户就会看到同一句话跑了两个回合。
func TestPromptIdDuplicateIsIgnored(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	opts := PromptOpts{Meta: map[string]any{"promptId": "p-1"}}

	done := make(chan callResult, 1)
	go func() {
		sr, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, opts)
		done <- callResult{map[string]any{"stopReason": sr}, err}
	}()
	resolveNext(t, b, w, map[string]any{"stopReason": "end_turn"})
	if cr := <-done; cr.err != nil {
		t.Fatalf("first prompt: %v", cr.err)
	}

	// Second call with the same id: no wire traffic, no error.
	framesBefore := w.frameCount()
	sr, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, opts)
	if err != nil {
		t.Fatalf("duplicate prompt returned error: %v", err)
	}
	if sr != StopReasonDuplicate {
		t.Errorf("duplicate stopReason = %q, want %q", sr, StopReasonDuplicate)
	}
	if got := w.frameCount() - framesBefore; got != 0 {
		t.Errorf("duplicate sent %d frames to the agent, want 0", got)
	}
}

// messageId is the fallback id source; a duplicate through it is reported
// as a duplicate too.
func TestPromptIdDuplicateReportsSentinel(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	opts := PromptOpts{MessageID: "m-1"}

	done := make(chan error, 1)
	go func() {
		_, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, opts)
		done <- err
	}()
	resolveNext(t, b, w, map[string]any{"stopReason": "end_turn"})
	if err := <-done; err != nil {
		t.Fatalf("first prompt: %v", err)
	}

	before := w.frameCount()
	sr, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, opts)
	if err != nil {
		t.Fatalf("duplicate prompt returned error: %v", err)
	}
	if sr != StopReasonDuplicate {
		t.Errorf("duplicate stopReason = %q, want %q", sr, StopReasonDuplicate)
	}
	if got := w.frameCount() - before; got != 0 {
		t.Errorf("duplicate sent %d frames to the agent, want 0", got)
	}
}

// A distinct id still runs: the dedupe must not collapse legitimately
// separate turns that happen to carry the same text.
func TestPromptIdDistinctStillRuns(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()

	for _, pid := range []string{"p-1", "p-2"} {
		done := make(chan callResult, 1)
		go func() {
			sr, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}},
				PromptOpts{Meta: map[string]any{"promptId": pid}})
			done <- callResult{map[string]any{"stopReason": sr}, err}
		}()
		resolveNext(t, b, w, map[string]any{"stopReason": "end_turn"})
		if cr := <-done; cr.err != nil {
			t.Fatalf("prompt %s: %v", pid, cr.err)
		}
	}
	if got := w.frameCount(); got != 2 {
		t.Errorf("frames sent = %d, want 2 (one per distinct promptId)", got)
	}
}

// A call that dies before reaching the agent must not consume the id:
// otherwise the FE's retry would be silently swallowed even though the
// first attempt never ran.
func TestPromptIdReleasedWhenNeverIssued(t *testing.T) {
	b, _ := metaReadyBridge(t)
	ctx := context.Background()
	// No stdin write target / no resolver: point at a dead agent so
	// ensureBooted fails before session/prompt is written.
	b.mu.Lock()
	b.ready = false
	b.bootOK = false
	b.mu.Unlock()

	opts := PromptOpts{Meta: map[string]any{"promptId": "p-retry"}}
	if _, _, err := b.PromptWithOpts(ctx, "s1", []ContentBlock{{"type": "text", "text": "hi"}}, opts); err == nil {
		t.Fatal("expected an error from an unbooted bridge")
	}

	b.mu.Lock()
	_, still := b.promptedIDs["p-retry"]
	b.mu.Unlock()
	if still {
		t.Error("promptId stayed claimed after the prompt never reached the agent")
	}
}

// frameCount is the number of JSON-RPC frames written to the agent so far.
func (w *recordingStdin) frameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.lines)
}
