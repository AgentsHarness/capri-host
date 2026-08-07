package acp

import (
	"encoding/json"
	"testing"
)

func TestSessionStateClassification(t *testing.T) {
	cases := []struct {
		name   string
		busy   bool
		await  bool
		expect string
	}{
		{"idle", false, false, "idle"},
		{"active", true, false, "active"},
		{"awaiting", true, true, "awaiting"},
		// Awaiting without a turn is not a real state — idle wins.
		{"await-without-turn", false, true, "idle"},
	}
	for _, c := range cases {
		s := &SessionState{Busy: c.busy, AwaitingInput: c.await}
		if got := s.State(); got != c.expect {
			t.Errorf("%s: State() = %q, want %q", c.name, got, c.expect)
		}
	}
}

func TestSessionStateMarshalIncludesState(t *testing.T) {
	s := SessionState{SessionID: "s1", Busy: true}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if m["state"] != "active" {
		t.Errorf("marshal state = %v, want active", m["state"])
	}
	if m["sessionId"] != "s1" {
		t.Errorf("marshal sessionId = %v, want s1", m["sessionId"])
	}
	if m["busy"] != true {
		t.Errorf("marshal busy = %v, want true", m["busy"])
	}
}

func TestStatusRosterOrdering(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host"})
	b.mu.Lock()
	b.sessions["a"] = &SessionState{SessionID: "a", CreatedAt: 100, LastActiveAt: 500}
	b.sessions["b"] = &SessionState{SessionID: "b", CreatedAt: 200, LastActiveAt: 900}
	b.sessions["c"] = &SessionState{SessionID: "c", CreatedAt: 300, LastActiveAt: 500}
	b.mu.Unlock()

	snap := b.Snapshot()
	if len(snap.Roster) != 3 {
		t.Fatalf("roster len = %d, want 3", len(snap.Roster))
	}
	want := []string{"b", "c", "a"} // LastActiveAt desc, CreatedAt desc tiebreak
	for i, sid := range want {
		if snap.Roster[i].SessionID != sid {
			t.Errorf("roster[%d] = %s, want %s", i, snap.Roster[i].SessionID, sid)
		}
	}
	if snap.Busy {
		t.Error("Busy should be false with no active turns")
	}

	// Busy is derived from any session having a turn in flight.
	b.mu.Lock()
	b.sessions["c"].Busy = true
	b.mu.Unlock()
	if !b.Snapshot().Busy {
		t.Error("Busy should be true when any session is busy")
	}
}
