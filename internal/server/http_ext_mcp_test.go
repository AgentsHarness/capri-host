package server

import (
	"path/filepath"
	"testing"
)

// http_ext_mcp_test.go — MCP 直通端点测试。

func TestMCPTypedEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	// setup：camelCase wire（sessionId/serverName/values）。
	rec := postJSON(t, s, "/api/mcp/setup", `{"serverName":"fs","values":{"token":"x"}}`)
	wantOK(t, rec)

	// toggle-tool：snake wire（session_id/server_name/tool_name/enabled）。
	rec = postJSON(t, s, "/api/mcp/toggle-tool", `{"serverName":"fs","toolName":"read","enabled":true}`)
	wantOK(t, rec)

	// call：camelCase wire（server/tool/arguments）。
	rec = postJSON(t, s, "/api/mcp/call", `{"server":"fs","tool":"read","arguments":{"path":"/ws"}}`)
	wantOK(t, rec)
}

// 调试调用不带 sessionId 时必须填活动会话，走会话 MCP 池（与 list 同源）。
// 省略该键会被 agent 当成 agent 池，热加载的 HTTP 服务器会报 not found。
func TestMCPCallDefaultsActiveSession(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/mcp/call", `{"server":"jxust-yqlx","tool":"adminConfig"}`)
	wantOK(t, rec)
	var last map[string]any
	for _, m := range readRecordedRequests(t, recordPath) {
		if m["method"] == "_x.ai/mcp/call" {
			last = m
		}
	}
	if last == nil {
		t.Fatal("no _x.ai/mcp/call recorded")
	}
	params, _ := last["params"].(map[string]any)
	if got, _ := params["sessionId"].(string); got != "sess-new" {
		t.Fatalf("sessionId = %v, want sess-new (params=%v)", params["sessionId"], params)
	}
	if got, _ := params["server"].(string); got != "jxust-yqlx" {
		t.Fatalf("server = %v, want jxust-yqlx", params["server"])
	}
}

func TestMCPReadResourceDefaultsActiveSession(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/mcp/read-resource", `{"server":"fs","uri":"file:///a"}`)
	wantOK(t, rec)
	var last map[string]any
	for _, m := range readRecordedRequests(t, recordPath) {
		if m["method"] == "_x.ai/mcp/read_resource" {
			last = m
		}
	}
	if last == nil {
		t.Fatal("no _x.ai/mcp/read_resource recorded")
	}
	params, _ := last["params"].(map[string]any)
	if got, _ := params["sessionId"].(string); got != "sess-new" {
		t.Fatalf("sessionId = %v, want sess-new (params=%v)", params["sessionId"], params)
	}
}
