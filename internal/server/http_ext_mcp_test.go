package server

import (
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
