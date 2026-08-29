package server

import (
	"testing"
)

// http_ext_cloud_test.go — 云端环境端点测试（fake agent 冒烟：200 {ok:true}）。

func TestExtCloudEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	cases := []struct {
		path string
		body string
	}{
		{"/api/cloud/terminate", `{"sandboxId":"sb-1"}`},
		{"/api/cloud/env/create", `{"name":"dev","defaultBranch":"main","containerImage":"img","setupScript":"echo hi"}`},
		{"/api/cloud/env/update", `{"environmentId":"env-1","name":"prod","description":"d"}`},
		{"/api/cloud/env/delete", `{"environmentId":"env-1"}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
