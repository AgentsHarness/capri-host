package server

import (
	"testing"
)

// http_ext_auth_test.go — 认证 / 账号端点测试。

func TestExtAuthEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	cases := []struct {
		path string
		body string
	}{
		{"/api/api-key-get", `{}`},
		{"/api/api-key-set", `{"key":"xai-123"}`},
		{"/api/auth/get-bearer-token", `{}`},
		{"/api/auth/cancel", `{"requestSeq":7}`},
		{"/api/auth/check-subscription", `{}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}

func TestExtPrivacyRolloutEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/privacy/set-coding-data-retention", `{"codingDataRetentionOptOut":true}`)
	wantOK(t, rec)
	rec = postJSON(t, s, "/api/rollout/survey", `{"preferences":["fast"],"feedback":"great"}`)
	wantOK(t, rec)
}
