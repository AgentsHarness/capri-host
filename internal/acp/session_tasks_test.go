package acp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeCwdDirname(t *testing.T) {
	cases := map[string]string{
		"/Users/benin/ccwork":     "%2FUsers%2Fbenin%2Fccwork",
		"/tmp":                    "%2Ftmp",
		"/Volumes/SD10/Learning":  "%2FVolumes%2FSD10%2FLearning",
		"/a b/c+d":                "%2Fa%20b%2Fc%2Bd",
		"/私有 目录":                  "%2F%E7%A7%81%E6%9C%89%20%E7%9B%AE%E5%BD%95",
		"/plain-dots_~tilde/name": "%2Fplain-dots_~tilde%2Fname",
	}
	for in, want := range cases {
		if got := EncodeCwdDirname(in); got != want {
			t.Errorf("EncodeCwdDirname(%q) = %q, want %q", in, got, want)
		}
	}
}

// writeSessionFile builds a fake grok home with one session's updates.
func writeSessionFile(t *testing.T, grokHome, cwd, sessionID string, lines []string) string {
	t.Helper()
	dir := filepath.Join(grokHome, "sessions", EncodeCwdDirname(cwd), sessionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "updates.jsonl")
	var sb []byte
	for _, l := range lines {
		sb = append(sb, []byte(l)...)
		sb = append(sb, '\n')
	}
	if err := os.WriteFile(path, sb, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func envLine(ts int64, update any) string {
	raw, _ := json.Marshal(map[string]any{
		"timestamp": ts,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": "sess-1",
			"update":    update,
		},
	})
	return string(raw)
}

func bgUpdate(taskID, command string) map[string]any {
	return bgUpdateWithLog(taskID, command, "/tmp/out.log")
}

func bgUpdateWithLog(taskID, command, outputFile string) map[string]any {
	return map[string]any{
		"sessionUpdate": "task_backgrounded",
		"task_id":       taskID,
		"command":       command,
		"description":   "desc " + taskID,
		"cwd":           "/tmp/ws",
		"output_file":   outputFile,
	}
}

func doneUpdate(taskID string, exit int) map[string]any {
	return map[string]any{
		"sessionUpdate": "task_completed",
		"task_snapshot": map[string]any{
			"task_id":   taskID,
			"exit_code": exit,
			"completed": true,
		},
	}
}

func doneUpdateWithOutput(taskID string, exit int, output string) map[string]any {
	return map[string]any{
		"sessionUpdate": "task_completed",
		"task_snapshot": map[string]any{
			"task_id":   taskID,
			"exit_code": exit,
			"completed": true,
			"output":    output,
		},
	}
}

func monitorUpdate(taskID, eventText string) map[string]any {
	return map[string]any{
		"sessionUpdate": "monitor_event",
		"task_id":       taskID,
		"event_text":    eventText,
	}
}

func schedUpdate(kind, taskID, prompt string) map[string]any {
	return map[string]any{
		"sessionUpdate":  kind,
		"task_id":        taskID,
		"prompt":         prompt,
		"human_schedule": "every 5m",
	}
}

func userChunk(ts int64, promptIdx int) string {
	raw, _ := json.Marshal(map[string]any{
		"timestamp": ts,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": "sess-1",
			"update": map[string]any{
				"sessionUpdate": "user_message_chunk",
				"content":       map[string]any{"type": "text", "text": "hi"},
			},
			"_meta": map[string]any{"promptIndex": promptIdx},
		},
	})
	return string(raw)
}

func rewindMarkerLine(target int) string {
	raw, _ := json.Marshal(map[string]any{
		"timestamp": 1,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": "sess-1",
			"update": map[string]any{
				"sessionUpdate":       "rewind_marker",
				"target_prompt_index": target,
			},
		},
	})
	return string(raw)
}

func TestParseTaskEventsOrderAndFields(t *testing.T) {
	home := t.TempDir()
	lines := []string{
		envLine(100, bgUpdate("t-1", "npm run dev")),
		envLine(101, userChunk(101, 0)),
		envLine(102, doneUpdate("t-1", 0)),
		envLine(103, schedUpdate("scheduled_task_created", "loop-1", "check status")),
		envLine(104, bgUpdate("t-2", "sleep 99")), // still running (orphan)
	}
	path := writeSessionFile(t, home, "/Users/benin/ccwork", "sess-1", lines)

	events, err := parseTaskEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("events = %d, want 4: %+v", len(events), events)
	}
	bg := events[0]
	if bg.Kind != "task_backgrounded" || bg.TaskID != "t-1" || bg.Command != "npm run dev" ||
		bg.Description != "desc t-1" || bg.Cwd != "/tmp/ws" || bg.OutputFile != "/tmp/out.log" ||
		bg.Timestamp != 100 {
		t.Errorf("bg event = %+v", bg)
	}
	if events[1].Kind != "task_completed" || events[1].TaskID != "t-1" ||
		events[1].ExitCode == nil || *events[1].ExitCode != 0 || !events[1].Completed {
		t.Errorf("done event = %+v", events[1])
	}
	if events[2].Kind != "scheduled_task_created" || events[2].TaskID != "loop-1" ||
		events[2].HumanSchedule != "every 5m" {
		t.Errorf("sched event = %+v", events[2])
	}
	if events[3].TaskID != "t-2" || events[3].Kind != "task_backgrounded" {
		t.Errorf("orphan event = %+v", events[3])
	}
}

func TestParseTaskEventsRewindDeadBranch(t *testing.T) {
	home := t.TempDir()
	// p1 → t-1 bg → p2 → t-2 bg → rewind to prompt 1 (keeps p1..t-1,
	// drops t-2) → p3 → t-3 bg. Surviving tasks: t-1, t-3 — mirrors the
	// agent's filter_rewind_lines (rewind target N truncates at prompt N's
	// start boundary).
	lines := []string{
		userChunk(1, 0),
		envLine(2, bgUpdate("t-1", "keep me")),
		userChunk(3, 1),
		envLine(4, bgUpdate("t-2", "dead branch")),
		rewindMarkerLine(1),
		userChunk(5, 2),
		envLine(6, bgUpdate("t-3", "keep me too")),
	}
	path := writeSessionFile(t, home, "/tmp/ws", "sess-1", lines)
	events, err := parseTaskEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, e := range events {
		ids = append(ids, e.TaskID)
	}
	want := []string{"t-1", "t-3"}
	if len(ids) != len(want) || ids[0] != want[0] || ids[1] != want[1] {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
}

func TestScanTaskSummaryCounts(t *testing.T) {
	home := t.TempDir()
	// Include a message whose TEXT mentions task_backgrounded — escaped
	// quotes mean the raw tag cannot appear inside a JSON string, so the
	// count must ignore it.
	msg, _ := json.Marshal(map[string]any{
		"timestamp": 1,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": "sess-1",
			"update": map[string]any{
				"sessionUpdate": "agent_message_chunk",
				"content":       map[string]any{"type": "text", "text": `handle_task_backgrounded says "sessionUpdate":"task_backgrounded"`},
			},
		},
	})
	// t-1 completed; t-2 orphan whose log is HELD OPEN by this test
	// process (kernel-level liveness → running); t-3 orphan whose log
	// exists but no process holds it (dead).
	freshLog := filepath.Join(home, "held.log")
	held, err := os.OpenFile(freshLog, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	staleLog := filepath.Join(home, "closed.log")
	if err := os.WriteFile(staleLog, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		envLine(1, bgUpdateWithLog("t-1", "a", freshLog)),
		envLine(2, bgUpdateWithLog("t-2", "b", freshLog)),
		envLine(3, doneUpdate("t-1", 0)),
		envLine(4, bgUpdateWithLog("t-3", "c", staleLog)),
		envLine(5, schedUpdate("scheduled_task_created", "loop-1", "x")),
		string(msg),
	}
	path := writeSessionFile(t, home, "/tmp/ws", "sess-1", lines)

	sum, err := scanTaskSummary(path)
	if err != nil {
		t.Fatal(err)
	}
	if !sum.HasTasks {
		t.Error("HasTasks = false, want true")
	}
	if sum.BgCount != 3 {
		t.Errorf("BgCount = %d, want 3", sum.BgCount)
	}
	if sum.BgRunning != 1 {
		t.Errorf("BgRunning = %d, want 1 (only t-2 passes the open-fd probe)", sum.BgRunning)
	}
}

func TestRunningTasksLivenessProbe(t *testing.T) {
	home := t.TempDir()
	// The liveness signal is the open file handle, NOT log writes: the
	// held file was written ONCE (a silent task writes nothing more), the
	// closed file was written at the same time — only the held one counts.
	freshLog := filepath.Join(home, "held.log")
	held, err := os.OpenFile(freshLog, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := held.WriteString("x"); err != nil {
		t.Fatal(err)
	}
	staleLog := filepath.Join(home, "closed.log")
	if err := os.WriteFile(staleLog, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		envLine(1, bgUpdateWithLog("t-alive", "npm run dev", freshLog)),
		envLine(2, bgUpdateWithLog("t-dead", "go run .", staleLog)),
		envLine(3, bgUpdateWithLog("t-done", "short task", freshLog)),
		envLine(4, doneUpdate("t-done", 0)),
	}
	path := writeSessionFile(t, home, "/Users/benin/ccwork", "sess-abc", lines)

	events, err := parseTaskEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	running := runningTasks(events)
	if len(running) != 1 {
		t.Fatalf("running = %d, want 1: %+v", len(running), running)
	}
	if running[0].TaskID != "t-alive" || !running[0].Running {
		t.Errorf("running[0] = %+v, want t-alive with Running=true", running[0])
	}
}

func TestSessionsCwdDirHashFallback(t *testing.T) {
	home := t.TempDir()
	// Simulate a long-path slug-hash dir with a .cwd metadata file.
	hashDir := filepath.Join(home, "sessions", "myproject-abcdef0123456789")
	if err := os.MkdirAll(hashDir, 0o755); err != nil {
		t.Fatal(err)
	}
	longCwd := "/very/long/workspace/path/that/exceeds/the/url/encoded/dirname/limit/on/disc" +
		"/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
		"/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := os.WriteFile(filepath.Join(hashDir, ".cwd"), []byte(longCwd), 0o600); err != nil {
		t.Fatal(err)
	}
	got := sessionsCwdDir(home, longCwd)
	if got != hashDir {
		t.Errorf("sessionsCwdDir = %q, want %q", got, hashDir)
	}
}

func TestBridgeSessionRunningTasks(t *testing.T) {
	home := t.TempDir()
	// t-alive: orphan whose log is HELD OPEN by this test process →
	// returned. t-done: completed → not returned.
	freshLog := filepath.Join(home, "held.log")
	held, err := os.OpenFile(freshLog, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	lines := []string{
		envLine(1, bgUpdateWithLog("t-alive", "npm run dev", freshLog)),
		envLine(2, bgUpdateWithLog("t-done", "short task", freshLog)),
		envLine(3, doneUpdate("t-done", 0)),
	}
	writeSessionFile(t, home, "/Users/benin/ccwork", "sess-abc", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	events, err := b.SessionRunningTasks("sess-abc", "/Users/benin/ccwork")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].TaskID != "t-alive" || !events[0].Running {
		t.Fatalf("running events = %+v, want [t-alive Running]", events)
	}
	// Unknown session → empty list, no error.
	events, err = b.SessionRunningTasks("nope", "/Users/benin/ccwork")
	if err != nil {
		t.Fatalf("missing session should be empty, got error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("missing session events = %d, want 0", len(events))
	}
}

func TestBridgeTaskLogFileAuthoritative(t *testing.T) {
	home := t.TempDir()
	// The on-disk log is the authoritative full stdout (TUI pager
	// writes it) — even when the snapshot carries a shorter output.
	logPath := filepath.Join(home, "task.log")
	if err := os.WriteFile(logPath, []byte("line1\nline2\nline3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		envLine(1, bgUpdateWithLog("t-1", "npm run dev", logPath)),
		envLine(2, monitorUpdate("t-1", "line1\n")),
		envLine(3, monitorUpdate("t-1", "line2\n")),
		envLine(4, doneUpdateWithOutput("t-1", 0, "line1\nline2\n")),
	}
	writeSessionFile(t, home, "/Users/benin/ccwork", "sess-abc", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	tl, err := b.TaskLog("sess-abc", "/Users/benin/ccwork", "t-1")
	if err != nil {
		t.Fatal(err)
	}
	if tl.Command != "npm run dev" || tl.OutputFile != logPath {
		t.Errorf("TaskLog header = %+v", tl)
	}
	if tl.Output != "line1\nline2\nline3\n" {
		t.Errorf("Output = %q, want the on-disk log", tl.Output)
	}
	if !tl.Completed || tl.Failed || tl.Running || tl.Truncated {
		t.Errorf("flags = completed=%v failed=%v running=%v truncated=%v", tl.Completed, tl.Failed, tl.Running, tl.Truncated)
	}
}

func TestBridgeTaskLogTimelineFallback(t *testing.T) {
	home := t.TempDir()
	// No output file on disk (workspace cleaned / different host): the
	// log is reconstructed from monitor_event chunks + the completion
	// snapshot. Snapshot wins when longer (FE rule).
	lines := []string{
		envLine(1, bgUpdateWithLog("t-1", "serve", filepath.Join(home, "gone.log"))),
		envLine(2, monitorUpdate("t-1", "boot ok\n")),
		envLine(3, doneUpdateWithOutput("t-1", 1, "boot ok\nfatal: boom\n")),
	}
	writeSessionFile(t, home, "/tmp/ws", "sess-abc", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	tl, err := b.TaskLog("sess-abc", "/tmp/ws", "t-1")
	if err != nil {
		t.Fatal(err)
	}
	if tl.Output != "boot ok\nfatal: boom\n" {
		t.Errorf("Output = %q, want merged snapshot (longer)", tl.Output)
	}
	if !tl.Completed || !tl.Failed {
		t.Errorf("flags = completed=%v failed=%v, want true/true", tl.Completed, tl.Failed)
	}
	if tl.Running {
		t.Error("Running = true, want false (completed)")
	}
}

func TestBridgeTaskLogRunningAndNotFound(t *testing.T) {
	home := t.TempDir()
	// Still-running task: no completion in the timeline + log held open
	// by this test process → Running=true.
	heldLog := filepath.Join(home, "held.log")
	held, err := os.OpenFile(heldLog, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := held.WriteString("streaming...\n"); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		envLine(1, bgUpdateWithLog("t-run", "dev server", heldLog)),
		envLine(2, monitorUpdate("t-run", "streaming...\n")),
	}
	writeSessionFile(t, home, "/tmp/ws", "sess-abc", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	tl, err := b.TaskLog("sess-abc", "/tmp/ws", "t-run")
	if err != nil {
		t.Fatal(err)
	}
	if !tl.Running || tl.Completed {
		t.Errorf("flags = running=%v completed=%v, want running only", tl.Running, tl.Completed)
	}
	if tl.Output != "streaming...\n" {
		t.Errorf("Output = %q, want the held log content", tl.Output)
	}

	// Unknown task / unknown session → ErrTaskLogNotFound.
	if _, err := b.TaskLog("sess-abc", "/tmp/ws", "nope"); !errors.Is(err, ErrTaskLogNotFound) {
		t.Errorf("unknown task err = %v, want ErrTaskLogNotFound", err)
	}
	if _, err := b.TaskLog("nope", "/tmp/ws", "t-run"); !errors.Is(err, ErrTaskLogNotFound) {
		t.Errorf("unknown session err = %v, want ErrTaskLogNotFound", err)
	}
}
