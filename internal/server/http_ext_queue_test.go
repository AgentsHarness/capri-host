package server

import (
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
)

// http_ext_queue_test.go — 队列端点测试（通知发送与参数校验）。

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
