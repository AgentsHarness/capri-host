package server

import (
	"net/http"
	"testing"
)

// http_ext_session_test.go — 会话扩展端点测试（生命周期/状态/摘要/插话）。

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

// 以下覆盖会话域直通端点：fake agent 冒烟（200 {ok:true, result}）、必填校验
// （400）、wire 键断言（经 ACPHostFakeAgentRecordRequests 录制 host→agent 请求
// 逐键核对）、agent 侧失败降级（200 {ok:false}）。

// ── 冒烟：全部新端点 200 ok ──────────────────────────────────────────

func TestExtSessionEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/session/state", `{"cwd":"/ws"}`},
		{"/api/session-import", `{"cwd":"/ws","state":{"summary":{"title":"t"}},"updates":[{"a":1}]}`},
		{"/api/session-repair", `{"dryRun":true}`},
		{"/api/session-update-mcp-servers", `{"mcpServers":[{"url":"stdio://fs"}]}`},
		{"/api/session-add-local-workspace", `{"meta":{"kind":"chat"}}`},
		{"/api/session-resolve-worktree-resume", `{"cwd":"/ws"}`},
		{"/api/session-rehydrate", `{"sourceCwd":"/ws","repoRoot":"/repo","worktreePath":"/wt"}`},
		{"/api/session-summaries/session-list", `{"workspaceDirectory":"/ws"}`},
		{"/api/session-summaries/workspace-list", `{}`},
		{"/api/session-summaries/workspace-list-recent", `{"limit":50}`},
		{"/api/session-load-history", `{"beforeId":"c-1"}`},
		{"/api/session-load-history", `{}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
