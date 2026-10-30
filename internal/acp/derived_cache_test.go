package acp

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// buildUpdateLine returns one session/update envelope line (no trailing
// newline) whose JSON carries an extra `pad` string. pad may be negative:
// the surviving payload keeps a longer `content`, which is trimmed to make
// the whole line exactly the same byte length as the line it replaces. The
// cache tests need that — they rewrite (size, mtime)-identical files.
func buildUpdateLine(t *testing.T, ts int64, update map[string]any, pad int) string {
	t.Helper()
	if pad >= 0 {
		update["pad"] = strings.Repeat("p", pad)
	} else {
		content, _ := update["content"].(string)
		trim := -pad
		if len(content) < trim {
			t.Fatalf("cannot shave %d bytes off content %q", trim, content)
		}
		update["content"] = content[:len(content)-trim]
	}
	line, err := json.Marshal(map[string]any{
		"timestamp": ts,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": "sess-1",
			"update":    update,
			"_meta":     map[string]any{"agentTimestampMs": ts},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(line)
}

// writePaddedSession rewrites the session file with the given lines, then
// restores the file's mtime so a subsequent call sees the same cache key.
func writePaddedSession(t *testing.T, path string, lines []string, mod time.Time) {
	t.Helper()
	body := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
}

// TestSessionTaskCensusCacheSkipsRescan proves the census cache actually
// short-circuits the file scan. The cache key is (size, mtime), so the test
// rewrites the file to the SAME size and mtime with a payload that would
// scan to a different census: a hit answers with the pre-rewrite values, a
// miss reports the new (empty) ones.
func TestSessionTaskCensusCacheSkipsRescan(t *testing.T) {
	home := t.TempDir()
	task := map[string]any{
		"sessionUpdate": "task_backgrounded",
		"task_id":       "t1",
		"command":       "sleep 100",
		// content 给两侧留出可裁剪的余量：替换后的一行更长，靠它补齐长度。
		"content": strings.Repeat("x", 40),
	}
	firstLine := buildUpdateLine(t, 10, task, 0)
	path := writeSessionFile(t, home, "/ws", "sess-1", []string{firstLine})
	b := NewBridge(GrokConfig{Bin: "grok", GrokHome: home})

	sum, _ := b.sessionTaskCensusCached(home, "/ws", "sess-1")
	if sum.BgCount != 1 || !sum.HasTasks {
		t.Fatalf("cold census = %+v, want BgCount=1 HasTasks=true", sum)
	}

	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 同长度、不同语义：kind 换成 agent_message_chunk，长度差额用 pad 补足。
	blank := map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"task_id":       "t1",
		"command":       "sleep 100",
		"content":       strings.Repeat("x", 40),
	}
	secondLine := buildUpdateLine(t, 10, blank, len(firstLine)-len(buildUpdateLine(t, 10, blank, 0)))
	if len(secondLine) != len(firstLine) {
		t.Fatalf("test setup: lengths differ (%d vs %d)", len(secondLine), len(firstLine))
	}
	writePaddedSession(t, path, []string{secondLine}, original.ModTime())

	if st, err := os.Stat(path); err != nil || st.Size() != original.Size() {
		t.Fatalf("test setup: size changed (%v, %v) — the cache key would miss", st, err)
	}

	warm, _ := b.sessionTaskCensusCached(home, "/ws", "sess-1")
	if warm.BgCount != 1 {
		t.Fatalf("warm census = %+v, want the cached BgCount=1 (scan was not skipped)", warm)
	}
}

// TestSessionTaskCensusCacheInvalidatesOnAppend: appending to the source
// must produce a fresh scan (the badge has to follow the file).
func TestSessionTaskCensusCacheInvalidatesOnAppend(t *testing.T) {
	home := t.TempDir()
	path := writeSessionFile(t, home, "/ws", "sess-1", []string{
		buildUpdateLine(t, 10, map[string]any{
			"sessionUpdate": "task_backgrounded",
			"task_id":       "t1",
			"command":       "sleep 100",
		}, 0),
	})
	b := NewBridge(GrokConfig{Bin: "grok", GrokHome: home})
	if sum, _ := b.sessionTaskCensusCached(home, "/ws", "sess-1"); sum.BgCount != 1 {
		t.Fatalf("cold census = %+v, want BgCount=1", sum)
	}

	line := buildUpdateLine(t, 20, map[string]any{
		"sessionUpdate": "task_backgrounded",
		"task_id":       "t2",
		"command":       "sleep 200",
	}, 0) + "\n"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(line); err != nil {
		t.Fatal(err)
	}
	f.Close()
	// 保证 mtime 前进（部分文件系统的时间戳粒度是秒）。
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}

	sum, _ := b.sessionTaskCensusCached(home, "/ws", "sess-1")
	if sum.BgCount != 2 {
		t.Fatalf("census after append = %+v, want BgCount=2", sum)
	}
}

// TestSessionStatsCacheSkipsRescan mirrors the census test for the composer
// stats: a same-length, same-mtime rewrite that would drop the turn must
// still report the cached turn count.
func TestSessionStatsCacheSkipsRescan(t *testing.T) {
	home := t.TempDir()
	userLine := buildUpdateLine(t, 10, map[string]any{"sessionUpdate": "user_message_chunk", "content": "A"}, 0)
	agentLine := buildUpdateLine(t, 20, map[string]any{"sessionUpdate": "agent_message_chunk", "content": "hello"}, 0)
	path := writeSessionFile(t, home, "/ws", "sess-1", []string{userLine, agentLine})
	b := NewBridge(GrokConfig{Bin: "grok", GrokHome: home})

	first, err := b.SessionStats(context.Background(), "/ws", "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Turns != 1 {
		t.Fatalf("cold stats turns = %d, want 1", first.Turns)
	}

	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 同长度改写：把 user 回合换成一段思想流，重扫应当得到 Turns=0。
	noTurn := map[string]any{"sessionUpdate": "agent_thought_chunk", "content": "A"}
	blanked := buildUpdateLine(t, 10, noTurn, len(userLine)-len(buildUpdateLine(t, 10, noTurn, 0)))
	if len(blanked) != len(userLine) {
		t.Fatalf("test setup: lengths differ (%d vs %d)", len(blanked), len(userLine))
	}
	writePaddedSession(t, path, []string{blanked, agentLine}, original.ModTime())

	warm, err := b.SessionStats(context.Background(), "/ws", "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if warm.Turns != 1 {
		t.Fatalf("warm stats turns = %d, want the cached 1 (scan was not skipped)", warm.Turns)
	}
	// 返回的必须是副本：调用方改写不能污染缓存。
	warm.Turns = 99
	again, err := b.SessionStats(context.Background(), "/ws", "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Turns != 1 {
		t.Fatalf("cache poisoned by caller mutation: turns = %d, want 1", again.Turns)
	}
}

// TestRosterRowsCarryLiveState: the busy-flip push must carry the row, so a
// client can patch instead of re-fetching the whole list.
func TestRosterRowsCarryLiveState(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "grok"})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{
		SessionID:     "s1",
		Cwd:           "/ws",
		Busy:          true,
		AwaitingInput: true,
	}
	b.mu.Unlock()

	row := b.rosterRow("s1")
	if row == nil {
		t.Fatal("rosterRow returned nil for a live session")
	}
	if row["sessionId"] != "s1" {
		t.Fatalf("row sessionId = %v", row["sessionId"])
	}
	status, _ := row["status"].(map[string]any)
	if status["state"] != "awaiting" || status["busy"] != true {
		t.Fatalf("row status = %+v, want awaiting/busy", status)
	}
	// census 字段不在行里：客户端合并时必须保留旧值。
	for _, k := range []string{"hasTasks", "bgCount", "bgRunning"} {
		if _, ok := row[k]; ok {
			t.Errorf("row must not carry %q (absent ≠ zero)", k)
		}
	}
	if b.rosterRow("missing") != nil {
		t.Fatal("rosterRow must return nil for an unknown session")
	}
}

// TestBusyFlipBroadcastsRow drives the real event stream path: an agent
// update that flips the session busy must broadcast sessions_changed WITH
// the row (that is what lets the browser skip the list fetch). A structural
// change (roster rebuilt) must stay payload-free.
func TestBusyFlipBroadcastsRow(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "grok"})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{SessionID: "s1", Cwd: "/ws"}
	b.mu.Unlock()

	ch, unsub := b.bus.subscribe()
	defer unsub()

	// 一条能证明在跑回合的 update → Busy 翻转 → 必须带行。
	b.observeUpdateKind("s1", "agent_message_chunk", map[string]any{})

	var got Event
	deadline := time.After(2 * time.Second)
	for got == nil {
		select {
		case ev := <-ch:
			if ev["type"] == "sessions_changed" {
				got = ev
			}
		case <-deadline:
			t.Fatal("no sessions_changed broadcast for the busy flip")
		}
	}
	params, _ := got["params"].(map[string]any)
	upserted, _ := params["upserted"].([]any)
	if len(upserted) != 1 {
		t.Fatalf("busy flip params = %+v, want one upserted row", params)
	}
	row, _ := upserted[0].(map[string]any)
	if row["sessionId"] != "s1" {
		t.Fatalf("upserted row sessionId = %v, want s1", row["sessionId"])
	}
	if st, _ := row["status"].(map[string]any); st["busy"] != true {
		t.Fatalf("upserted status = %+v, want busy=true", st)
	}

	// 结构变化（无从列举受影响的行）仍走无载荷广播。
	b.broadcastRosterChange()
	for {
		select {
		case ev := <-ch:
			if ev["type"] != "sessions_changed" {
				continue
			}
			if p, ok := ev["params"].(map[string]any); ok && len(p) > 0 {
				t.Fatalf("structural change must stay payload-free, got %+v", p)
			}
			return
		case <-time.After(2 * time.Second):
			t.Fatal("no sessions_changed for the structural change")
		}
	}
}

// TestSessionStatsContextCancel is a guard on the refactor: the accumulated
// loop still honours ctx cancellation.
func TestSessionStatsContextCancel(t *testing.T) {
	home := t.TempDir()
	lines := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		lines = append(lines, buildUpdateLine(t, int64(10+i),
			map[string]any{"sessionUpdate": "agent_message_chunk", "content": strings.Repeat("x", 32)}, 0))
	}
	writeSessionFile(t, home, "/ws", "sess-1", lines)
	b := NewBridge(GrokConfig{Bin: "grok", GrokHome: home})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.SessionStats(ctx, "/ws", "sess-1"); err == nil {
		t.Fatal("expected a context error from a cancelled context on a cache miss")
	}
}
