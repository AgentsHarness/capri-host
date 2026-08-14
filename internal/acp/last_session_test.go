package acp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRememberSessionPersistsAndSurvivesKill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "last-session.json")
	b := NewBridge(GrokConfig{
		Bin:             "grok",
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: path,
	})

	b.mu.Lock()
	b.sessions["sess-abc"] = &SessionState{SessionID: "sess-abc", Cwd: "/tmp/ws"}
	b.activeSessionID = "sess-abc"
	b.rememberSessionLocked("sess-abc", "/tmp/ws")
	b.mu.Unlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("persist file missing: %v", err)
	}
	var st lastSessionFile
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if st.SessionID != "sess-abc" || st.Cwd != "/tmp/ws" {
		t.Fatalf("persisted = %+v, want sess-abc /tmp/ws", st)
	}

	// resetRoster must wipe the in-memory roster but keep the last-session
	// pointer so restoreLastSession can session/load it.
	b.resetRoster("test")
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.sessions) != 0 {
		t.Errorf("sessions should be empty after kill, got %d", len(b.sessions))
	}
	if b.activeSessionID != "" {
		t.Errorf("activeSessionID should be empty after kill, got %q", b.activeSessionID)
	}
	if b.lastSessionID != "sess-abc" || b.lastSessionCwd != "/tmp/ws" {
		t.Errorf("last session lost after kill: id=%q cwd=%q", b.lastSessionID, b.lastSessionCwd)
	}
}

func TestNewBridgeLoadsLastSessionFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "last-session.json")
	raw, _ := json.Marshal(lastSessionFile{SessionID: "from-disk", Cwd: "/home/u/proj"})
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	b := NewBridge(GrokConfig{
		Bin:             "grok",
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: path,
	})
	if b.lastSessionID != "from-disk" || b.lastSessionCwd != "/home/u/proj" {
		t.Errorf("loaded last = %q %q, want from-disk /home/u/proj", b.lastSessionID, b.lastSessionCwd)
	}
}

func TestRememberSessionNoopOnEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "last-session.json")
	b := NewBridge(GrokConfig{LastSessionFile: path})
	b.mu.Lock()
	b.rememberSessionLocked("", "/tmp")
	b.mu.Unlock()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("empty sid should not write file, err=%v", err)
	}
}

func TestWaitProcessPreservesLastSession(t *testing.T) {
	// Simulate the roster wipe half of waitProcess without spawning a process.
	dir := t.TempDir()
	path := filepath.Join(dir, "last-session.json")
	b := NewBridge(GrokConfig{LastSessionFile: path})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.activeSessionID = "s1"
	// Mimic waitProcess snapshot + wipe:
	if act := b.activeSessionLocked(); act != nil {
		b.rememberSessionLocked(act.SessionID, act.Cwd)
	}
	b.sessions = make(map[string]*SessionState)
	b.activeSessionID = ""
	b.ready = false
	lastID, lastCwd := b.lastSessionID, b.lastSessionCwd
	b.mu.Unlock()

	if lastID != "s1" || lastCwd != "/ws" {
		t.Fatalf("last after wipe = %q %q, want s1 /ws", lastID, lastCwd)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var st lastSessionFile
	_ = json.Unmarshal(raw, &st)
	if st.SessionID != "s1" {
		t.Errorf("disk last = %q, want s1", st.SessionID)
	}
}
