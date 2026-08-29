package server

import (
	"path/filepath"
	"reflect"
	"testing"
)

// http_ext_terminal_test.go — 终端 / PTY 端点测试。

func TestExtTerminalEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/terminal/create", `{"command":"ls","args":["-la"],"env":{"K":"v"},"cwd":"/ws","outputByteLimit":1000}`},
		{"/api/terminal/kill", `{"terminalId":"t-1"}`},
		{"/api/terminal/output", `{"terminalId":"t-1"}`},
		{"/api/terminal/wait-for-exit", `{"terminalId":"t-1"}`},
		{"/api/terminal/release", `{"terminalId":"t-1"}`},
		{"/api/terminal/background", `{"terminalId":"t-1"}`},
		{"/api/terminal/pty/create", `{"shell":"/bin/zsh","rows":24,"cols":80,"meta":{"x":1}}`},
		{"/api/terminal/pty/load", `{"terminalId":"t-1"}`},
		{"/api/terminal/pty/resize", `{"terminalId":"t-1","rows":30,"cols":100}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}

	// pty/input 是 fire-and-forget 通知（无 JSON-RPC id）→ 200 {ok, result:{ok}}。
	rec := postJSON(t, s, "/api/terminal/pty/input", `{"terminalId":"t-1","data":"aGk="}`)
	m := wantOK(t, rec)
	if res, ok := m["result"].(map[string]any); !ok || res["ok"] != true {
		t.Fatalf("pty/input body = %s, want result.ok:true", rec.Body.String())
	}
}

// ── pty/input 通知录制（无 JSON-RPC id）──────────────────────────────

func TestExtTerminalPtyInputNotification(t *testing.T) {
	notifPath := filepath.Join(t.TempDir(), "notifs.jsonl")
	t.Setenv(ACPHostFakeAgentRecordNotifs, notifPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/terminal/pty/input", `{"terminalId":"t-1","data":"aGk="}`)
	wantOK(t, rec)

	var found map[string]any
	for _, m := range readRecordedLines(t, notifPath) {
		if m["method"] == "_x.ai/terminal/pty/input" {
			found = m
			break
		}
	}
	if found == nil {
		t.Fatalf("no _x.ai/terminal/pty/input notification recorded")
	}
	if id := found["id"]; id != nil {
		t.Errorf("notification must have no JSON-RPC id, got %v", id)
	}
	params, _ := found["params"].(map[string]any)
	want := map[string]any{"terminalId": "t-1", "data": "aGk="}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("pty/input params = %v, want %v", params, want)
	}
}
