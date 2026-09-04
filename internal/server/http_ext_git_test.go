package server

import (
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
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

	// git/diffs 缺省 includePatch 为 true
	recDiff := postJSON(t, s, "/api/git/diffs", `{"cwd":"/ws","from":"HEAD","to":"working"}`)
	wantOK(t, recDiff)
	diffParams := recordedParams(t, s, recordPath, "/api/git/diffs", `{"cwd":"/ws","from":"HEAD","to":"working"}`, "_x.ai/git/diffs")
	if diffParams["includePatch"] != true {
		t.Errorf("git/diffs default includePatch = %v, want true", diffParams["includePatch"])
	}

	// commit 缺 message → 400。
	rec = postJSON(t, s, "/api/git/commit", `{"cwd":"/ws"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", rec.Code, rec.Body.String())
	}
}

// TestGitRepoRootWireKey — /api/git/repo-root 的 wire 键必须是 agent 侧
// GitRepoRequest 的 currentWorkingDirectory（serde 必填）。该端点历史上发的
// 是其它 git 方法用的 gitRoot，agent 判缺字段直接 -32602 "Invalid params"，
// FE home 空状态的「在新 worktree 中开始」门控因此恒为置灰。
func TestGitRepoRootWireKey(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	// 无活动会话也要能发出去（home 场景本来就没有会话）。
	params := recordedParams(t, s, recordPath, "/api/git/repo-root",
		`{"cwd":"/ws"}`, "_x.ai/git/git_repo_root")
	want := map[string]any{"currentWorkingDirectory": "/ws"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("git_repo_root params = %v, want %v", params, want)
	}

	// 空 cwd → 400，且不下发 wire 请求（该字段没有活动会话兜底）。
	before := len(readRecordedRequests(t, recordPath))
	rec := postJSON(t, s, "/api/git/repo-root", `{"cwd":""}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", rec.Code, rec.Body.String())
	}
	if after := len(readRecordedRequests(t, recordPath)); after != before {
		t.Errorf("空 cwd 不应发出 wire 请求（%d → %d 条）", before, after)
	}
}

// TestWorktreeCreateSessionSemantics — worktree/create 的 sessionId 语义：
// 有活动会话时回落活动会话（原行为）；home 空状态无会话时也必须能发出
// 去——agent 侧 CreateWorktreeRequest.session_id 是 serde 必填字段，占位
// 非空且唯一，避免 XaiCall 把空串解析成活动会话而 404（该场景创建根本
// 不需要真实会话）。
func TestWorktreeCreateSessionSemantics(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	// 无活动会话：200，wire sessionId 是非空占位（前缀 nosession-），
	// sourcePath 原样透传。
	params := recordedParams(t, s, recordPath, "/api/git/worktree/create",
		`{"sourcePath":"/home/repo"}`, "_x.ai/git/worktree/create")
	sid, _ := params["sessionId"].(string)
	if !strings.HasPrefix(sid, "nosession-") {
		t.Errorf("sessionless create sessionId = %q, want nosession-* placeholder", sid)
	}
	if params["sourcePath"] != "/home/repo" {
		t.Errorf("sourcePath = %v, want /home/repo", params["sourcePath"])
	}

	// 有活动会话：回落活动会话 id（sess-new），不再用占位。
	createActiveSession(t, s)
	rec := postJSON(t, s, "/api/git/worktree/create", `{"sourcePath":"/ws"}`)
	wantOK(t, rec)
	var last map[string]any
	for _, m := range readRecordedRequests(t, recordPath) {
		if m["method"] == "_x.ai/git/worktree/create" {
			last = m
		}
	}
	if last == nil {
		t.Fatal("no recorded _x.ai/git/worktree/create request")
	}
	params, _ = last["params"].(map[string]any)
	if params["sessionId"] != "sess-new" {
		t.Errorf("active-session create sessionId = %v, want sess-new", params["sessionId"])
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
