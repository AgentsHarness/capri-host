package server

import (
	"testing"
)

// http_ext_fs_test.go — 文件系统 / 检索端点测试。

func TestExtFSSearchBundleEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/fs/write-file", `{"path":"/ws/a.txt","content":"hi","createDirs":true}`},
		{"/api/fs/delete-file", `{"path":"/ws/a.txt"}`},
		{"/api/search/fuzzy/open", `{"cwd":"/ws","root":"src","hidden":true,"meta":{"routing":1}}`},
		{"/api/search/fuzzy/change", `{"searchId":"sr-1","query":"foo","dirsOnly":true,"limit":10}`},
		{"/api/search/fuzzy/close", `{"searchId":"sr-1"}`},
		{"/api/bundle/sync", `{"force":true}`},
		{"/api/bundle/entry-get", `{"kind":"persona","name":"default"}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
