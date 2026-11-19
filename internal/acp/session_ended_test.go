package acp

import "testing"

// TestSessionEndedClosesObservedTurn pins the grok 1.0.89 SessionUpdate::
// SessionEnded handling: a session that disappears mid-turn must clear the
// agent-observed busy leg immediately, because no turn_completed can follow.
func TestSessionEndedClosesObservedTurn(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "grok"})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.mu.Unlock()

	b.observeUpdateKind("s1", "agent_message_chunk", map[string]any{})
	b.mu.Lock()
	if w := b.turns["s1"]; w == nil || !w.open {
		b.mu.Unlock()
		t.Fatal("agent_message_chunk did not open the observed turn")
	}
	if !b.sessions["s1"].Busy {
		b.mu.Unlock()
		t.Fatal("observed turn did not project busy=true")
	}
	b.mu.Unlock()

	b.observeUpdateKind("s1", "session_ended", map[string]any{})
	b.mu.Lock()
	defer b.mu.Unlock()
	if w := b.turns["s1"]; w != nil && w.open {
		t.Fatal("session_ended left the observed turn open")
	}
	if b.sessions["s1"].Busy {
		t.Fatal("session_ended left the session busy")
	}
}
