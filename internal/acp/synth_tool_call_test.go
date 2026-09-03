package acp

import (
	"os"
	"testing"
)

func TestSyntheticToolCallIDInjection(t *testing.T) {
	// 测试场景 1：串行调用，toolCallId 为空，注入正确的 synth:call:<msgSeq>
	t.Run("serial missing call id", func(t *testing.T) {
		lines := []updateLineMeta{
			{
				agentTsMs: 1000,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call",
					name:          "run_terminal_command",
					command:       "ls -la",
				},
			},
			{
				agentTsMs: 1001,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					kind:          "execute",
					command:       "ls -la",
				},
			},
			{
				agentTsMs: 1002,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					status:        "completed",
					outputType:    "Bash",
					outputCmd:     "ls -la",
				},
			},
		}
		order := []int{0, 1, 2}
		synthIDs := resolveSyntheticToolCallIDs(order, lines)

		wantID := "synth:call:0"
		if synthIDs[0] != wantID {
			t.Errorf("msgSeq 0 synthID = %v, want %v", synthIDs[0], wantID)
		}
		if synthIDs[1] != wantID {
			t.Errorf("msgSeq 1 synthID = %v, want %v", synthIDs[1], wantID)
		}
		if synthIDs[2] != wantID {
			t.Errorf("msgSeq 2 synthID = %v, want %v", synthIDs[2], wantID)
		}
	})

	// 测试场景 2：并发两个同族调用（read_file 读不同文件不同 offset），乱序完成，各归各的合成 ID
	t.Run("parallel same family out of order completion", func(t *testing.T) {
		lines := []updateLineMeta{
			// seq 0: start read A (offset 1080)
			{
				agentTsMs: 1000,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call",
					name:          "read_file",
					path:          "src/a.rs",
					offset:        1080,
					hasOffset:     true,
				},
			},
			// seq 1: update read A
			{
				agentTsMs: 1001,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					kind:          "read",
					path:          "src/a.rs",
				},
			},
			// seq 2: start read B (no offset)
			{
				agentTsMs: 1002,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call",
					name:          "read_file",
					path:          "src/b.rs",
					offset:        0,
					hasOffset:     false,
				},
			},
			// seq 3: update read B
			{
				agentTsMs: 1003,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					kind:          "read",
					path:          "src/b.rs",
				},
			},
			// seq 4: B 完成了！(带 1→ content 前缀，无路径)
			{
				agentTsMs: 1004,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					status:        "completed",
					outputType:    "ReadFile",
					contentPrefix: "1→use std::path::Path;\n",
				},
			},
			// seq 5: A 完成了！(带 1080→ content 前缀，无路径)
			{
				agentTsMs: 1005,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					status:        "completed",
					outputType:    "ReadFile",
					contentPrefix: "1080→tracing::debug!(\"hello\");\n",
				},
			},
		}
		order := []int{0, 1, 2, 3, 4, 5}
		synthIDs := resolveSyntheticToolCallIDs(order, lines)

		idA := "synth:call:0"
		idB := "synth:call:2"

		if synthIDs[0] != idA || synthIDs[1] != idA {
			t.Errorf("A starts/updates should have idA: %v, %v", synthIDs[0], synthIDs[1])
		}
		if synthIDs[2] != idB || synthIDs[3] != idB {
			t.Errorf("B starts/updates should have idB: %v, %v", synthIDs[2], synthIDs[3])
		}
		// 验证 seq 4 是 B 的完成，seq 5 是 A 的完成！
		if synthIDs[4] != idB {
			t.Errorf("seq 4 (completed B) got %v, want %v", synthIDs[4], idB)
		}
		if synthIDs[5] != idA {
			t.Errorf("seq 5 (completed A) got %v, want %v", synthIDs[5], idA)
		}
	})

	// 测试场景 3：并发 web_fetch 与 bash，web_fetch 抛 SSRF 错误快速失败
	t.Run("parallel web_fetch and bash with error", func(t *testing.T) {
		lines := []updateLineMeta{
			// seq 0: web_fetch start
			{
				agentTsMs: 1000,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call",
					name:          "web_fetch",
					url:           "https://github.com/foo/bar",
				},
			},
			// seq 1: web_fetch update
			{
				agentTsMs: 1001,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					kind:          "fetch",
					url:           "https://github.com/foo/bar",
				},
			},
			// seq 2: bash start
			{
				agentTsMs: 1002,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call",
					name:          "run_terminal_command",
					command:       "uname -m",
				},
			},
			// seq 3: bash update
			{
				agentTsMs: 1003,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					kind:          "execute",
					command:       "uname -m",
				},
			},
			// seq 4: web_fetch failed with SSRF error
			{
				agentTsMs: 1004,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					status:        "failed",
					errorMsg:      "SSRF blocked: github.com resolves to private IP",
					contentText:   "Tool `web_fetch` failed: SSRF blocked",
				},
			},
			// seq 5: bash completed
			{
				agentTsMs: 1005,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					status:        "completed",
					outputType:    "Bash",
					outputCmd:     "uname -m",
				},
			},
		}
		order := []int{0, 1, 2, 3, 4, 5}
		synthIDs := resolveSyntheticToolCallIDs(order, lines)

		idFetch := "synth:call:0"
		idBash := "synth:call:2"

		if synthIDs[0] != idFetch || synthIDs[1] != idFetch || synthIDs[4] != idFetch {
			t.Errorf("fetch events mismatch: seq0=%v, seq1=%v, seq4=%v, want %v", synthIDs[0], synthIDs[1], synthIDs[4], idFetch)
		}
		if synthIDs[2] != idBash || synthIDs[3] != idBash || synthIDs[5] != idBash {
			t.Errorf("bash events mismatch: seq2=%v, seq3=%v, seq5=%v, want %v", synthIDs[2], synthIDs[3], synthIDs[5], idBash)
		}
	})

	// 测试场景 4：原有非空 toolCallId 保持不变，不被覆盖
	t.Run("preserves existing toolCallId", func(t *testing.T) {
		lines := []updateLineMeta{
			{
				agentTsMs: 1000,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call",
					toolCallID:    "real-call-123",
					name:          "bash",
				},
			},
			{
				agentTsMs: 1001,
				tool: &toolLineMeta{
					sessionUpdate: "tool_call_update",
					toolCallID:    "real-call-123",
					status:        "completed",
				},
			},
		}
		order := []int{0, 1}
		synthIDs := resolveSyntheticToolCallIDs(order, lines)
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
		injectSynthToolCallID(obj, "synth:call:999")
		upd := obj[kParams].(map[string]any)[kUpdate].(map[string]any)
		if upd["toolCallId"] != "real-call-123" {
			t.Errorf("existing toolCallId overwritten! got %v", upd["toolCallId"])
		}
	})
}

// TestRealSession01a0630cSyntheticInjection 对真实会话 01a0630c 进行端到端测试
func TestRealSession01a0630cSyntheticInjection(t *testing.T) {
	sessionPath := "/Users/benin/.grok/sessions/%2FUsers%2Fbenin%2Fccwork/01a0630c-9370-72e0-87ad-ded5d1af3198/updates.jsonl"
	if _, err := os.Stat(sessionPath); err != nil {
		t.Skip("local session 01a0630c not found on this machine, skipping")
	}

	view, err := buildNormalizedHistory(sessionPath)
	if err != nil {
		t.Fatalf("buildNormalizedHistory failed: %v", err)
	}

	if len(view.synthToolCallIDs) == 0 {
		t.Fatal("expected synthToolCallIDs to be populated for session 01a0630c")
	}

	// 验证会话中所有 tool_call 和 tool_call_update 是否都获得了非空 ID
	toolCallsCount := 0
	toolUpdatesCount := 0
	for seq, lineIdx := range view.order {
		m := &view.lines[lineIdx]
		if m.tool == nil {
			continue
		}
		id, hasSynth := view.synthToolCallIDs[seq]
		if m.tool.toolCallID != "" {
			id = m.tool.toolCallID
		}
		if id == "" {
			t.Fatalf("msgSeq %d (%s, name=%s, title=%s) missing toolCallId!", seq, m.tool.sessionUpdate, m.tool.name, m.tool.title)
		}
		if m.tool.sessionUpdate == "tool_call" {
			toolCallsCount++
		} else if m.tool.sessionUpdate == "tool_call_update" {
			toolUpdatesCount++
		}
		_ = hasSynth
	}

	if toolCallsCount < 180 {
		t.Errorf("toolCallsCount = %d, expected >= 180", toolCallsCount)
	}
	if toolUpdatesCount < 350 {
		t.Errorf("toolUpdatesCount = %d, expected >= 350", toolUpdatesCount)
	}
	t.Logf("Session 01a0630c: successfully verified %d tool_calls and %d tool_call_updates with 100%% non-empty synth IDs!", toolCallsCount, toolUpdatesCount)
}

// TestSyntheticInjectionInSlice 透传切片单页测试
func TestSyntheticInjectionInSlice(t *testing.T) {
	raw := []any{
		map[string]any{
			kParams: map[string]any{
				kUpdate: map[string]any{
					"sessionUpdate": "tool_call",
					"toolCallId":    "",
					"title":         "run_terminal_command",
					"rawInput":      map[string]any{"command": "echo hello"},
				},
			},
		},
		map[string]any{
			kParams: map[string]any{
				kUpdate: map[string]any{
					"sessionUpdate": "tool_call_update",
					"toolCallId":    "",
					"kind":          "execute",
					"rawInput":      map[string]any{"command": "echo hello"},
				},
			},
		},
		map[string]any{
			kParams: map[string]any{
				kUpdate: map[string]any{
					"sessionUpdate": "tool_call_update",
					"toolCallId":    "",
					"status":        "completed",
					"rawOutput":     map[string]any{"type": "Bash", "command": "echo hello"},
				},
			},
		},
	}

	normalizeSyntheticToolCallsInSlice(raw)

	for i, item := range raw {
		upd := item.(map[string]any)[kParams].(map[string]any)[kUpdate].(map[string]any)
		id, _ := upd["toolCallId"].(string)
		if id != "synth:call:0" {
			t.Errorf("item %d toolCallId = %v, want synth:call:0", i, id)
		}
	}
}
