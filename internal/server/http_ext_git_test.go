package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
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

// TestGitStateEndpoint — /api/git/state 用真实临时仓库验证：干净仓库
// mergeInProgress=false；制造合并冲突后 mergeInProgress=true 并列出冲突文件。
// 非仓库目录返回 ok:false。
func TestGitStateEndpoint(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git 不可用")
	}
	s, _ := newFakeAgentServer(t)
	dir := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, strings.TrimSpace(out.String()))
		}
	}
	writeFile := func(content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	getState := func() GitRepoState {
		t.Helper()
		rec := postJSON(t, s, "/api/git/state", fmt.Sprintf(`{"cwd":%q}`, dir))
		wantOK(t, rec)
		var body struct {
			Ok    bool         `json:"ok"`
			State GitRepoState `json:"state"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode /api/git/state response: %v", err)
		}
		return body.State
	}

	git("init")
	git("config", "user.email", "t@example.com")
	git("config", "user.name", "T")
	writeFile("base\n")
	git("add", ".")
	git("commit", "-m", "base")
	git("checkout", "-b", "feature")
	writeFile("feature\n")
	git("commit", "-am", "feature")
	git("checkout", "-")
	writeFile("main\n")
	git("commit", "-am", "main")

	clean := getState()
	if clean.MergeInProgress || clean.RebaseInProgress || clean.CherryPickInProgress {
		t.Errorf("clean repo state = %+v, want no in-progress operation", clean)
	}
	if clean.ConflictCount != 0 || len(clean.Conflicts) != 0 {
		t.Errorf("clean repo conflicts = %+v, want none", clean)
	}

	// 合并 feature → 冲突（merge 失败是预期，忽略错误）。
	_ = exec.Command("git", "-C", dir, "merge", "feature").Run()
	state := getState()
	if !state.MergeInProgress {
		t.Errorf("conflicted repo mergeInProgress = false, want true (%+v)", state)
	}
	if state.ConflictCount != 1 || len(state.Conflicts) != 1 || state.Conflicts[0] != "f.txt" {
		t.Errorf("conflicted repo conflicts = %+v, want [f.txt]", state.Conflicts)
	}

	// 非 git 目录 → HTTP 200 但 ok:false。
	rec := postJSON(t, s, "/api/git/state", fmt.Sprintf(`{"cwd":%q}`, t.TempDir()))
	if rec.Code != http.StatusOK {
		t.Fatalf("non-repo status = %d, want 200", rec.Code)
	}
	var nonRepo struct {
		Ok    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &nonRepo); err != nil {
		t.Fatalf("decode non-repo response: %v", err)
	}
	if nonRepo.Ok || nonRepo.Error == "" {
		t.Errorf("non-repo /api/git/state = %s, want ok:false with error", rec.Body.String())
	}

	// 缺 cwd → 400。
	if rec := postJSON(t, s, "/api/git/state", `{}`); rec.Code != http.StatusBadRequest {
		t.Errorf("missing cwd status = %d, want 400", rec.Code)
	}
}
