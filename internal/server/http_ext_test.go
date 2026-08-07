package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// http_ext_test.go — 冒烟测试 x.ai 扩展直通端点（http_ext.go）。
// fake agent 对未知方法默认回 {}，因此 200 冒烟验证路由 + XaiCall 往返 +
// 应答形状 {ok:true, result}；400 用例验证必填字段校验。

// wantOK asserts a 200 {ok:true} response.
func wantOK(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if ok, _ := m["ok"].(bool); !ok {
		t.Fatalf("body = %s, want ok:true", rec.Body.String())
	}
	return m
}

func TestXaiCallPassthrough(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// 无 params：默认空 map，成功返回 {ok:true, result:{}}。
	rec := postJSON(t, s, "/api/xai-call", `{"method":"x.ai/noop"}`)
	m := wantOK(t, rec)
	if _, has := m["result"]; !has {
		t.Fatalf("body = %s, want result key", rec.Body.String())
	}

	// params 带空 sessionId：XaiCall 填活动会话。
	createActiveSession(t, s)
	rec = postJSON(t, s, "/api/xai-call", `{"method":"x.ai/session/state","params":{"sessionId":""}}`)
	wantOK(t, rec)
}

func TestXaiCallRejectsMissingMethod(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	rec := postJSON(t, s, "/api/xai-call", `{"params":{}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestSessionResumeValidationAndOK(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// 缺 cwd → 400。
	rec := postJSON(t, s, "/api/session-resume", `{"sessionId":"s1"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", rec.Code, rec.Body.String())
	}
	// 缺 sessionId → 400。
	rec = postJSON(t, s, "/api/session-resume", `{"cwd":"/ws"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", rec.Code, rec.Body.String())
	}

	// 正常：fake agent 对 session/resume 回 {} → 200 ok。
	rec = postJSON(t, s, "/api/session-resume", `{"sessionId":"s1","cwd":"/ws"}`)
	wantOK(t, rec)
}

func TestSessionCloseDefaultsToActive(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/session-close", `{}`)
	wantOK(t, rec)
}

func TestGitEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// 无 cwd 冒烟：git/status → 200。
	rec := postJSON(t, s, "/api/git/status", `{"cwd":""}`)
	wantOK(t, rec)

	// commit 缺 message → 400。
	rec = postJSON(t, s, "/api/git/commit", `{"cwd":"/ws"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", rec.Code, rec.Body.String())
	}
}

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

func TestExtMiscSmoke(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	// queue/clear 需要活动会话（XaiCall 填 sessionId，否则 404）。
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/skills/list", `{}`},
		{"/api/queue/clear", `{}`},
		{"/api/auth/info", `{}`},
		{"/api/terminal/list", `{}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
