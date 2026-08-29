package server

import (
	"testing"
)

// http_ext_ecosystem_test.go — skills / plugins / subagent 端点测试。

func TestExtSkillsPluginsSubagentEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/skills/reset", `{"cwd":"/ws"}`},
		{"/api/skills/config", `{"cwd":"/ws"}`},
		{"/api/plugins/notify-updates", `{"updates":[["p","1.0","1.1"]]}`},
		{"/api/subagent/get", `{"subagentId":"sub-1","block":true,"timeoutMs":5000}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
