package server

import (
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
)

// http_ext_git_test.go — git / worktree 端点测试。

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

func TestExtGitEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/git/files", `{"cwd":"/ws","paths":["a.go"],"version":"HEAD~1"}`},
		{"/api/git/stage-content", `{"cwd":"/ws","path":"a.go","content":"package main"}`},
		{"/api/git/checkout-session-head", `{"cwd":"/ws","stashIfDirty":true}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}

func TestExtWorktreeEndpoints(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	cases := []struct {
		path string
		body string
	}{
		{"/api/git/worktree/create", `{"sourcePath":"/ws","copyMode":"dirty","ignoredSkipPatterns":["*.log"]}`},
		{"/api/git/worktree/remove", `{"idOrPath":"wt-1","force":true}`},
		{"/api/git/worktree/apply", `{"worktreePath":"/wt","mode":"merge"}`},
		{"/api/git/worktree/create-from-worktree", `{"sourceWorktreePath":"/wt","newSessionId":"s-2","label":"fork"}`},
		{"/api/git/worktree/create-from-worktree-sync", `{"sourceWorktreePath":"/wt","newSessionId":"s-2"}`},
		{"/api/git/worktree/resume-session", `{"sourceCwd":"/ws","restoreCode":true,"gitRef":"main"}`},
		{"/api/git/worktree/list", `{"repo":"/repo","type":["linked"],"includeAll":true}`},
		{"/api/git/worktree/show", `{"idOrPath":"wt-1"}`},
		{"/api/git/worktree/gc", `{"dryRun":true,"maxAge":"7d"}`},
		{"/api/git/worktree/db/stats", `{}`},
		{"/api/git/worktree/db/rebuild", `{}`},
		{"/api/git/worktree/db/path", `{}`},
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		wantOK(t, rec)
	}
}
