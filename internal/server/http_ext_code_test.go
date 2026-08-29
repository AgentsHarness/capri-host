package server

import (
	"testing"
)

// http_ext_code_test.go — 代码智能 / hunk tracker / review 端点测试。

func TestExtHunkTrackerEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/hunk-tracker/files", `{}`},
		{"/api/hunk-tracker/file-contents", `{}`},
		{"/api/hunk-tracker/summary", `{}`},
		{"/api/hunk-tracker/hunk-action", `{"hunkId":"h-1","action":"accept"}`},
		{"/api/hunk-tracker/file-action", `{"path":"a.go","action":"reject"}`},
		{"/api/hunk-tracker/turn-action", `{"promptIndex":2,"action":"accept"}`},
		{"/api/hunk-tracker/all-action", `{"action":"reject"}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}

func TestExtCodeEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/code/goto-definition", `{"cwd":"/ws","path":"a.go","row":10,"column":3}`},
		{"/api/code/goto-references", `{"cwd":"/ws","path":"a.go","row":10,"column":3}`},
		{"/api/code/find-definitions", `{"cwd":"/ws","symbol":"Foo","contextPath":"a.go"}`},
		{"/api/code/find-references", `{"cwd":"/ws","symbol":"Foo"}`},
		{"/api/code/status", `{"cwd":"/ws"}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}

func TestExtReviewDebugEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/review/comment", `{"promptIndex":1,"comment":"nit","citation":{"path":"a.go","startLine":1,"endLine":2,"text":"x","side":"left"}}`},
		{"/api/review/comment-delete", `{"commentId":"c-1"}`},
		{"/api/debug/trigger-feedback", `{"tier":"tier2","mode":"stars"}`},
		{"/api/debug/arm-auto-compact", `{}`},
		{"/api/debug/agent", `{}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
