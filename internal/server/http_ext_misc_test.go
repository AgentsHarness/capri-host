package server

import (
	"testing"
)

// http_ext_misc_test.go — 能力声明等杂项端点冒烟。

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
