package acp

import (
	"context"
	"os"
	"testing"
)

func TestSyntheticToolCallIDInjection(t *testing.T) {
	t.Run("serial missing call id", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 1000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "ls -la"}},
			{agentTsMs: 1001, tool: &toolLineMeta{sessionUpdate: "tool_call_update", kind: "execute", command: "ls -la"}},
			{agentTsMs: 1002, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "Bash", outputCmd: "ls -la"}},
		}
		order := []int{0, 1, 2}
		synthIDs := resolveSyntheticToolCallIDs(order, lines)
		want := formatSynthCallID(1000, 0)
		for seq := 0; seq < 3; seq++ {
			if synthIDs[seq] != want {
				t.Errorf("msgSeq %d synthID = %v, want %v", seq, synthIDs[seq], want)
			}
		}
	})

	t.Run("parallel same family out of order completion", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 1000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/a.rs", offset: 1080, hasOffset: true}},
			{agentTsMs: 1001, tool: &toolLineMeta{sessionUpdate: "tool_call_update", kind: "read", path: "src/a.rs"}},
			{agentTsMs: 1002, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/b.rs"}},
			{agentTsMs: 1003, tool: &toolLineMeta{sessionUpdate: "tool_call_update", kind: "read", path: "src/b.rs"}},
			{agentTsMs: 1004, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "ReadFile", contentPrefix: "1→use std::path::Path;\n"}},
			{agentTsMs: 1005, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "ReadFile", contentPrefix: "1080→tracing::debug!(\"hello\");\n"}},
		}
		order := []int{0, 1, 2, 3, 4, 5}
		synthIDs := resolveSyntheticToolCallIDs(order, lines)
		idA := formatSynthCallID(1000, 0)
		idB := formatSynthCallID(1002, 0)
		if synthIDs[0] != idA || synthIDs[1] != idA {
			t.Errorf("A starts/updates: %v %v, want %v", synthIDs[0], synthIDs[1], idA)
		}
		if synthIDs[2] != idB || synthIDs[3] != idB {
			t.Errorf("B starts/updates: %v %v, want %v", synthIDs[2], synthIDs[3], idB)
		}
		if synthIDs[4] != idB {
			t.Errorf("seq 4 (completed B) got %v, want %v", synthIDs[4], idB)
		}
		if synthIDs[5] != idA {
			t.Errorf("seq 5 (completed A) got %v, want %v", synthIDs[5], idA)
		}
	})

	t.Run("same file different offsets distinguished by line prefix not contains", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 2000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/a.rs", offset: 80, hasOffset: true}},
			{agentTsMs: 2001, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/a.rs", offset: 1080, hasOffset: true}},
			{agentTsMs: 2002, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "ReadFile", contentPrefix: "1080→fn main() {\n"}},
			{agentTsMs: 2003, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "ReadFile", contentPrefix: "80→use foo;\n"}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1, 2, 3}, lines)
		id80 := formatSynthCallID(2000, 0)
		id1080 := formatSynthCallID(2001, 0)
		if synthIDs[2] != id1080 {
			t.Errorf("1080→ completion got %v, want %v (must not match offset 80 via Contains)", synthIDs[2], id1080)
		}
		if synthIDs[3] != id80 {
			t.Errorf("80→ completion got %v, want %v", synthIDs[3], id80)
		}
	})

	t.Run("no-offset read does not steal 21→ as 1→", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 3000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/a.rs"}},
			{agentTsMs: 3001, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/b.rs", offset: 21, hasOffset: true}},
			{agentTsMs: 3002, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "ReadFile", contentPrefix: "21→let x = 1;\n"}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1, 2}, lines)
		idB := formatSynthCallID(3001, 0)
		if synthIDs[2] != idB {
			t.Errorf("21→ completion got %v, want %v (1→ substring must not win)", synthIDs[2], idB)
		}
	})

	t.Run("parallel web_fetch and bash with error", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 1000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "web_fetch", url: "https://github.com/foo/bar"}},
			{agentTsMs: 1001, tool: &toolLineMeta{sessionUpdate: "tool_call_update", kind: "fetch", url: "https://github.com/foo/bar"}},
			{agentTsMs: 1002, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "uname -m"}},
			{agentTsMs: 1003, tool: &toolLineMeta{sessionUpdate: "tool_call_update", kind: "execute", command: "uname -m"}},
			{agentTsMs: 1004, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "failed", errorMsg: "SSRF blocked: github.com resolves to private IP", contentText: "Tool `web_fetch` failed: SSRF blocked"}},
			{agentTsMs: 1005, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "Bash", outputCmd: "uname -m"}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1, 2, 3, 4, 5}, lines)
		idFetch := formatSynthCallID(1000, 0)
		idBash := formatSynthCallID(1002, 0)
		if synthIDs[0] != idFetch || synthIDs[1] != idFetch || synthIDs[4] != idFetch {
			t.Errorf("fetch events mismatch: %v %v %v", synthIDs[0], synthIDs[1], synthIDs[4])
		}
		if synthIDs[2] != idBash || synthIDs[3] != idBash || synthIDs[5] != idBash {
			t.Errorf("bash events mismatch: %v %v %v", synthIDs[2], synthIDs[3], synthIDs[5])
		}
	})

	t.Run("parallel same command does not guess", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 4000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "git status"}},
			{agentTsMs: 4001, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "git status"}},
			{agentTsMs: 4002, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "Bash", outputCmd: "git status"}},
			{agentTsMs: 4003, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "Bash", outputCmd: "git status"}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1, 2, 3}, lines)
		if synthIDs[0] == "" || synthIDs[1] == "" || synthIDs[0] == synthIDs[1] {
			t.Errorf("starts must get distinct ids, got %v %v", synthIDs[0], synthIDs[1])
		}
		if _, ok := synthIDs[2]; ok {
			t.Errorf("ambiguous bash completion must not be assigned, got %v", synthIDs[2])
		}
		if _, ok := synthIDs[3]; ok {
			t.Errorf("second ambiguous bash completion must not be assigned, got %v", synthIDs[3])
		}
	})

	t.Run("bash output mentioning a read path does not steal the read", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 6000, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "git status"}},
			{agentTsMs: 6001, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "git status"}},
			{agentTsMs: 6002, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "read_file", path: "src/index.ts"}},
			{agentTsMs: 6003, tool: &toolLineMeta{
				sessionUpdate: "tool_call_update",
				status:        "completed",
				outputType:    "Bash",
				outputCmd:     "git status",
				contentText:   "src/index.ts: modified",
			}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1, 2, 3}, lines)
		readID := formatSynthCallID(6002, 0)
		if synthIDs[2] != readID {
			t.Fatalf("read start id = %v, want %v", synthIDs[2], readID)
		}
		if id, ok := synthIDs[3]; ok {
			t.Errorf("ambiguous bash completion must not attach via path-in-text, got %v (read id %v)", id, readID)
		}
	})

	t.Run("named calls stay out of anon open set", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 5000, tool: &toolLineMeta{sessionUpdate: "tool_call", toolCallID: "real-1", name: "run_terminal_command", command: "ls"}},
			{agentTsMs: 5001, tool: &toolLineMeta{sessionUpdate: "tool_call_update", toolCallID: "real-1", status: "completed", outputType: "Bash", outputCmd: "ls"}},
			{agentTsMs: 5002, tool: &toolLineMeta{sessionUpdate: "tool_call", name: "run_terminal_command", command: "ls"}},
			{agentTsMs: 5003, tool: &toolLineMeta{sessionUpdate: "tool_call_update", status: "completed", outputType: "Bash", outputCmd: "ls"}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1, 2, 3}, lines)
		if _, ok := synthIDs[0]; ok {
			t.Errorf("named start must not enter synth map, got %v", synthIDs[0])
		}
		if _, ok := synthIDs[1]; ok {
			t.Errorf("named update must not enter synth map, got %v", synthIDs[1])
		}
		want := formatSynthCallID(5002, 0)
		if synthIDs[2] != want {
			t.Errorf("anon start = %v, want %v", synthIDs[2], want)
		}
		if synthIDs[3] != want {
			t.Errorf("anon completion captured by named call? got %v, want %v", synthIDs[3], want)
		}
	})

	t.Run("preserves existing toolCallId on inject", func(t *testing.T) {
		lines := []updateLineMeta{
			{agentTsMs: 1000, tool: &toolLineMeta{sessionUpdate: "tool_call", toolCallID: "real-call-123", name: "bash"}},
			{agentTsMs: 1001, tool: &toolLineMeta{sessionUpdate: "tool_call_update", toolCallID: "real-call-123", status: "completed"}},
		}
		synthIDs := resolveSyntheticToolCallIDs([]int{0, 1}, lines)
		if len(synthIDs) != 0 {
			t.Errorf("synthIDs should be empty for existing call IDs, got %v", synthIDs)
		}

		obj := map[string]any{
			kParams: map[string]any{
				kUpdate: map[string]any{
					"sessionUpdate": "tool_call",
					"toolCallId":    "real-call-123",
				},
			},
		}
		injectSynthToolCallID(obj, "synth:call:999:0")
		upd := obj[kParams].(map[string]any)[kUpdate].(map[string]any)
		if upd["toolCallId"] != "real-call-123" {
			t.Errorf("existing toolCallId overwritten! got %v", upd["toolCallId"])
		}
	})
}

func TestParseLeadingLineNum(t *testing.T) {
	if n, ok := parseLeadingLineNum("1080→tracing"); !ok || n != 1080 {
		t.Errorf("1080→ got %d %v", n, ok)
	}
	if n, ok := parseLeadingLineNum("80→use"); !ok || n != 80 {
		t.Errorf("80→ got %d %v", n, ok)
	}
	if n, ok := parseLeadingLineNum("21→let"); !ok || n != 21 {
		t.Errorf("21→ got %d %v", n, ok)
	}
	if _, ok := parseLeadingLineNum("no line"); ok {
		t.Error("plain text should not parse")
	}
}

func TestRealSession01a0630cSyntheticInjection(t *testing.T) {
	sessionPath := "/Users/benin/.grok/sessions/%2FUsers%2Fbenin%2Fccwork/01a0630c-9370-72e0-87ad-ded5d1af3198/updates.jsonl"
	if _, err := os.Stat(sessionPath); err != nil {
		t.Skip("local session 01a0630c not found on this machine, skipping")
	}

	view, err := buildNormalizedHistory(sessionPath)
	if err != nil {
		t.Fatalf("buildNormalizedHistory failed: %v", err)
	}

	toolCalls := 0
	missingStarts := 0
	for seq, lineIdx := range view.order {
		m := &view.lines[lineIdx]
		if m.tool == nil || m.tool.sessionUpdate != "tool_call" {
			continue
		}
		toolCalls++
		id, hasSynth := view.synthToolCallIDs[seq]
		if m.tool.toolCallID != "" {
			id = m.tool.toolCallID
		}
		if id == "" && !hasSynth {
			missingStarts++
		}
	}
	if toolCalls < 180 {
		t.Errorf("toolCallsCount = %d, expected >= 180", toolCalls)
	}
	if missingStarts != 0 {
		t.Errorf("%d tool_call starts still have empty ids", missingStarts)
	}
}

func sliceEnv(ts int64, eventID string, update map[string]any) map[string]any {
	return map[string]any{
		"timestamp": ts,
		kParams: map[string]any{
			kUpdate: update,
			kMeta: map[string]any{
				"agentTimestampMs": ts,
				"eventId":          eventID,
			},
		},
	}
}

func TestSyntheticInjectionInSlice(t *testing.T) {
	raw := []any{
		sliceEnv(1000, "e0", map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "run_terminal_command",
			"rawInput":      map[string]any{"command": "echo hello"},
		}),
		sliceEnv(1001, "e1", map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "",
			"kind":          "execute",
			"rawInput":      map[string]any{"command": "echo hello"},
		}),
		sliceEnv(1002, "e2", map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "",
			"status":        "completed",
			"rawOutput":     map[string]any{"type": "Bash", "command": "echo hello"},
		}),
	}
	normalizeSyntheticToolCallsInSlice(raw)
	want := formatSynthCallID(1000, 0)
	for i, item := range raw {
		upd := item.(map[string]any)[kParams].(map[string]any)[kUpdate].(map[string]any)
		if upd["toolCallId"] != want {
			t.Errorf("item %d toolCallId = %v, want %v", i, upd["toolCallId"], want)
		}
	}
}

func TestPassthroughPagesDoNotCollideIDs(t *testing.T) {
	page1 := []any{
		sliceEnv(1000, "e-a", map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "run_terminal_command",
			"rawInput":      map[string]any{"command": "echo a"},
		}),
	}
	page2 := []any{
		sliceEnv(5000, "e-b", map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "run_terminal_command",
			"rawInput":      map[string]any{"command": "echo b"},
		}),
	}
	normalizeSyntheticToolCallsInSlice(page1)
	normalizeSyntheticToolCallsInSlice(page2)
	id1 := page1[0].(map[string]any)[kParams].(map[string]any)[kUpdate].(map[string]any)["toolCallId"]
	id2 := page2[0].(map[string]any)[kParams].(map[string]any)[kUpdate].(map[string]any)["toolCallId"]
	if id1 == id2 {
		t.Errorf("two pages both got %v (page-local index collision)", id1)
	}
	if id1 != formatSynthCallID(1000, 0) || id2 != formatSynthCallID(5000, 0) {
		t.Errorf("ids = %v %v", id1, id2)
	}
}

func TestPassthroughFallsBackToEventIDWithoutTimestamp(t *testing.T) {
	raw := []any{
		map[string]any{
			kParams: map[string]any{
				kUpdate: map[string]any{
					"sessionUpdate": "tool_call",
					"toolCallId":    "",
					"title":         "run_terminal_command",
					"rawInput":      map[string]any{"command": "echo z"},
				},
				kMeta: map[string]any{"eventId": "evt-z"},
			},
		},
	}
	normalizeSyntheticToolCallsInSlice(raw)
	id := raw[0].(map[string]any)[kParams].(map[string]any)[kUpdate].(map[string]any)["toolCallId"]
	if id != "synth:call:e:evt-z" {
		t.Errorf("toolCallId = %v, want synth:call:e:evt-z", id)
	}
}

func TestLiveResolverSeedsHistoryIDs(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	const cwd = "/ws"
	const T = int64(1_700_000_000_000)
	writeSessionFile(t, home, cwd, sid, []string{
		histEnvelope(sid, 0, T, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "run_terminal_command",
			"rawInput":      map[string]any{"command": "uname -m"},
		}),
		histEnvelope(sid, 1, T+1, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "",
			"kind":          "execute",
			"rawInput":      map[string]any{"command": "uname -m"},
		}),
	})
	path := sessionUpdatesFile(home, cwd, sid)
	view, err := buildNormalizedHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	want := formatSynthCallID(T, 0)
	if view.synthToolCallIDs[0] != want {
		t.Fatalf("history start id = %v, want %v", view.synthToolCallIDs[0], want)
	}

	r := &liveToolResolver{}
	r.seedFrom(view)
	if len(r.openCalls) != 1 {
		t.Fatalf("seeded openCalls = %d, want 1", len(r.openCalls))
	}
	start := map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "",
		"title":         "run_terminal_command",
		"rawInput":      map[string]any{"command": "uname -m"},
	}
	r.handleLive("tool_call", start, T, "")
	if start["toolCallId"] != want {
		t.Fatalf("live start reused id = %v, want %v", start["toolCallId"], want)
	}
	if len(r.openCalls) != 1 {
		t.Fatalf("reused start must not append a second open call, got %d", len(r.openCalls))
	}
	upd := map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    "",
		"status":        "completed",
		"rawOutput":     map[string]any{"type": "Bash", "command": "uname -m"},
	}
	r.handleLive("tool_call_update", upd, T+2, "")
	if upd["toolCallId"] != want {
		t.Errorf("live completion id = %v, want history id %v", upd["toolCallId"], want)
	}
}

func TestLiveToolResolveSkipsReplayAndJoinsHistory(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	const cwd = "/ws"
	const T = int64(1_700_000_000_000)
	writeSessionFile(t, home, cwd, sid, []string{
		histEnvelope(sid, 0, T, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "run_terminal_command",
			"rawInput":      map[string]any{"command": "uname -m"},
		}),
	})
	b, _ := historyBridge(t, home)

	start := map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "",
		"title":         "run_terminal_command",
		"rawInput":      map[string]any{"command": "uname -m"},
	}
	params := map[string]any{kMeta: map[string]any{"agentTimestampMs": T, "eventId": sid + "-0"}}
	b.liveToolResolve(sid, "tool_call", start, params, true)
	if id, _ := start["toolCallId"].(string); id != "" {
		t.Fatalf("replay must not inject, got %q", id)
	}

	done := map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    "",
		"status":        "completed",
		"rawOutput":     map[string]any{"type": "Bash", "command": "uname -m"},
	}
	b.liveToolResolve(sid, "tool_call_update", done, map[string]any{
		kMeta: map[string]any{"agentTimestampMs": T + 2},
	}, false)
	want := formatSynthCallID(T, 0)
	if done["toolCallId"] != want {
		t.Fatalf("live completion id = %v, want %v", done["toolCallId"], want)
	}
}

func TestLiveTurnCompletedClearsPendingStarts(t *testing.T) {
	r := &liveToolResolver{
		openCalls:     []openCall{{id: "synth:call:1:0"}},
		pendingStarts: []openCall{{id: "synth:call:1:0", tsMs: 1}},
	}
	r.handleLive("turn_completed", nil, 0, "")
	if len(r.openCalls) != 0 {
		t.Errorf("openCalls after turn_completed = %d, want 0", len(r.openCalls))
	}
	if len(r.pendingStarts) != 0 {
		t.Errorf("pendingStarts after turn_completed = %d, want 0", len(r.pendingStarts))
	}
}

func TestLiveNewStartUsesTimestampID(t *testing.T) {
	r := &liveToolResolver{}
	r.seedFrom(nil)
	upd := map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "",
		"title":         "run_terminal_command",
		"rawInput":      map[string]any{"command": "echo hi"},
	}
	r.handleLive("tool_call", upd, 42, "")
	if upd["toolCallId"] != formatSynthCallID(42, 0) {
		t.Errorf("new live start id = %v, want %v", upd["toolCallId"], formatSynthCallID(42, 0))
	}
}

func TestSessionUpdatesInjectsTimestampSynthIDs(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	const T = int64(1_000_000)
	writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, T, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "",
			"title":         "run_terminal_command",
			"rawInput":      map[string]any{"command": "echo hi"},
		}),
		histEnvelope(sid, 1, T+1, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "",
			"status":        "completed",
			"rawOutput":     map[string]any{"type": "Bash", "command": "echo hi"},
		}),
	})
	b, _ := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Updates) != 2 {
		t.Fatalf("updates = %d, want 2", len(page.Updates))
	}
	want := formatSynthCallID(T, 0)
	for i, raw := range page.Updates {
		upd := raw.(map[string]any)[kParams].(map[string]any)[kUpdate].(map[string]any)
		if upd["toolCallId"] != want {
			t.Errorf("item %d toolCallId = %v, want %v", i, upd["toolCallId"], want)
		}
	}
}
