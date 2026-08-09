package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	// 无 cwd 冒烟：git/status → 200；缺省 includeUntracked → wire 显式 true
	// （host 默认，不依赖 agent 侧 2026-08-07 起的 false 缺省）。
	rec := postJSON(t, s, "/api/git/status", `{"cwd":""}`)
	wantOK(t, rec)
	params := recordedParams(t, s, recordPath, "/api/git/status", `{"cwd":""}`, "_x.ai/git/status")
	want := map[string]any{"includeUntracked": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("git/status default params = %v, want %v", params, want)
	}

	// 显式 includeUntracked:false → wire false（不依赖 agent 缺省）。
	// findRequest 取第一个匹配，这里直接取最后一个 git/status 请求。
	rec = postJSON(t, s, "/api/git/status", `{"cwd":"/ws","includeUntracked":false}`)
	wantOK(t, rec)
	reqs := readRecordedRequests(t, recordPath)
	var last map[string]any
	for _, m := range reqs {
		if m["method"] == "_x.ai/git/status" {
			last = m
		}
	}
	params, _ = last["params"].(map[string]any)
	want = map[string]any{"gitRoot": "/ws", "includeUntracked": false}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("git/status explicit params = %v, want %v", params, want)
	}

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
	// queue/clear 需要活动会话（XaiNotify 填 sessionId，否则 404）。
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

// 队列端点必须把改动以 fire-and-forget 通知（无 JSON-RPC id）发给 agent：
// grok 侧只在 ext_notification 处理 x.ai/queue/*（ext_method 无对应分支，
// request 型会回 -32601 → 宿主降级 ok:false，队列操作静默失效）。TUI 同款
// （xai-grok-pager app/effects/mod.rs 全以 ExtNotification 发送）；wire
// 键按 shell 侧 parse_queue_edit_command 的约定（id/orderedIds/newText/
// expectedVersion），sessionId 由宿主解析为活动会话 —— 请求体带可选
// sessionId 时显式指定目标会话（直通，不经活动会话解析）。
func TestQueueEndpointsSendNotifications(t *testing.T) {
	notifPath := filepath.Join(t.TempDir(), "notifs.jsonl")
	t.Setenv(ACPHostFakeAgentRecordNotifs, notifPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	calls := []struct {
		path string
		body string
		want map[string]any
	}{
		{"/api/queue/remove", `{"id":"p1"}`,
			map[string]any{"sessionId": "sess-new", "id": "p1"}},
		{"/api/queue/remove", `{"id":"p2","expectedVersion":3}`,
			map[string]any{"sessionId": "sess-new", "id": "p2", "expectedVersion": float64(3)}},
		{"/api/queue/clear", `{}`,
			map[string]any{"sessionId": "sess-new"}},
		{"/api/queue/reorder", `{"ids":["a","b"]}`,
			map[string]any{"sessionId": "sess-new", "orderedIds": []any{"a", "b"}}},
		{"/api/queue/edit", `{"id":"p3","newText":"x"}`,
			map[string]any{"sessionId": "sess-new", "id": "p3", "newText": "x"}},
		{"/api/queue/interject", `{"id":"p4"}`,
			map[string]any{"sessionId": "sess-new", "id": "p4"}},
		{"/api/queue/interject", `{"id":"p4","newText":"y","expectedVersion":1}`,
			map[string]any{"sessionId": "sess-new", "id": "p4", "newText": "y", "expectedVersion": float64(1)}},
		{"/api/queue/hold-edit", `{"id":"p5"}`,
			map[string]any{"sessionId": "sess-new", "id": "p5"}},
		{"/api/queue/release-edit", `{"id":"p5"}`,
			map[string]any{"sessionId": "sess-new", "id": "p5"}},
		// 可选 sessionId：显式指定时直达该会话（不经活动会话解析）。
		{"/api/queue/remove", `{"id":"p6","sessionId":"sess-B"}`,
			map[string]any{"sessionId": "sess-B", "id": "p6"}},
		{"/api/queue/clear", `{"sessionId":"sess-B"}`,
			map[string]any{"sessionId": "sess-B"}},
		{"/api/queue/interject", `{"id":"p7","sessionId":"sess-B","newText":"z"}`,
			map[string]any{"sessionId": "sess-B", "id": "p7", "newText": "z"}},
		{"/api/queue/reorder", `{"ids":["c"],"sessionId":"sess-B"}`,
			map[string]any{"sessionId": "sess-B", "orderedIds": []any{"c"}}},
		{"/api/queue/hold-edit", `{"id":"p8","sessionId":"sess-B"}`,
			map[string]any{"sessionId": "sess-B", "id": "p8"}},
		{"/api/queue/release-edit", `{"id":"p8","sessionId":"sess-B"}`,
			map[string]any{"sessionId": "sess-B", "id": "p8"}},
		{"/api/queue/edit", `{"id":"p9","newText":"w","sessionId":"sess-B"}`,
			map[string]any{"sessionId": "sess-B", "id": "p9", "newText": "w"}},
	}
	wantMethods := []string{
		"_x.ai/queue/remove", "_x.ai/queue/remove", "_x.ai/queue/clear",
		"_x.ai/queue/reorder", "_x.ai/queue/edit", "_x.ai/queue/interject",
		"_x.ai/queue/interject", "_x.ai/queue/hold_edit", "_x.ai/queue/release_edit",
		"_x.ai/queue/remove", "_x.ai/queue/clear", "_x.ai/queue/interject",
		"_x.ai/queue/reorder", "_x.ai/queue/hold_edit", "_x.ai/queue/release_edit",
		"_x.ai/queue/edit",
	}
	for _, c := range calls {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
	notifs := readRecordedNotifs(t, notifPath, len(calls))
	for i, m := range notifs {
		if m["method"] != wantMethods[i] {
			t.Fatalf("notif %d method = %v, want %s", i, m["method"], wantMethods[i])
		}
		if m["id"] != nil {
			t.Fatalf("notif %d has id %v — queue edits must be fire-and-forget notifications", i, m["id"])
		}
		params, _ := m["params"].(map[string]any)
		if !reflect.DeepEqual(params, calls[i].want) {
			t.Errorf("notif %d (%s) params = %v, want %v", i, wantMethods[i], params, calls[i].want)
		}
	}
}

// 队列端点校验：必填字段 400；无活动会话 404（通知无响应，sessionId 必须
// 由宿主解析，不能像 request 那样靠 agent 侧兜底）。
func TestQueueEndpointsValidation(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	bad := []struct {
		path string
		body string
	}{
		{"/api/queue/remove", `{}`},
		{"/api/queue/edit", `{}`},
		{"/api/queue/edit", `{"id":"p1"}`},
		{"/api/queue/interject", `{}`},
		{"/api/queue/hold-edit", `{}`},
		{"/api/queue/release-edit", `{}`},
	}
	for _, c := range bad {
		rec := postJSON(t, s, c.path, c.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400 (body=%s)", c.path, c.body, rec.Code, rec.Body.String())
		}
	}

	// 无活动会话：404（XaiNotify 解析不到 sessionId）。
	s2, _ := newFakeAgentServer(t)
	rec := postJSON(t, s2, "/api/queue/clear", `{}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, want 404 (body=%s)", rec.Code, rec.Body.String())
	}

	// 无活动会话 + 显式 sessionId：通知带显式 id 直达，无需活动会话。
	s3, _ := newFakeAgentServer(t)
	rec = postJSON(t, s3, "/api/queue/clear", `{"sessionId":"sess-B"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("no-session-with-explicit-sid status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
}
