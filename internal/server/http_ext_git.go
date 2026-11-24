package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

// http_ext_git.go — git 与 worktree 端点（状态/暂存/提交/stash、worktree CRUD、PR 状态）。

// ── Git（camelCase 键；cwd 空则不带 gitRoot）──────────────────────────

func (s *Server) handleGitStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd              string `json:"cwd"`
		IncludeUntracked *bool  `json:"includeUntracked"`
	}
	if !readBody(w, r, &body) {
		return
	}
	// Agent 侧 x.ai/git/status 的 include_untracked 缺省自 2026-08-07 起为
	// false（此前为 true）；host 这里显式解析默认值并总是发送该键，保证
	// 行为不随 agent 版本漂移。默认 true 维持本端点历史语义（含 untracked）。
	includeUntracked := true
	if body.IncludeUntracked != nil {
		includeUntracked = *body.IncludeUntracked
	}
	params := gitRootParams(body.Cwd)
	params["includeUntracked"] = includeUntracked
	s.xaiCall(w, r, "x.ai/git/status", params)
}

func (s *Server) handleGitDiffs(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd          string   `json:"cwd"`
		From         string   `json:"from"`
		To           string   `json:"to"`
		Paths        []string `json:"paths"`
		IncludePatch *bool    `json:"includePatch"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.From == "" || body.To == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 from 和 to"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["from"] = body.From
	params["to"] = body.To
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	includePatch := true
	if body.IncludePatch != nil {
		includePatch = *body.IncludePatch
	}
	params["includePatch"] = includePatch
	s.xaiCall(w, r, "x.ai/git/diffs", params)
}

func (s *Server) handleGitStage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string   `json:"cwd"`
		Paths []string `json:"paths"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	s.xaiCall(w, r, "x.ai/git/stage", params)
}

func (s *Server) handleGitUnstage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string   `json:"cwd"`
		Paths []string `json:"paths"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	s.xaiCall(w, r, "x.ai/git/unstage", params)
}

func (s *Server) handleGitDiscard(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd              string   `json:"cwd"`
		Paths            []string `json:"paths"`
		IncludeUntracked *bool    `json:"includeUntracked"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if len(body.Paths) > 0 {
		params["paths"] = body.Paths
	}
	if body.IncludeUntracked != nil {
		params["includeUntracked"] = *body.IncludeUntracked
	}
	s.xaiCall(w, r, "x.ai/git/discard", params)
}

func (s *Server) handleGitCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string `json:"cwd"`
		Message string `json:"message"`
		Amend   *bool  `json:"amend"`
		Signoff *bool  `json:"signoff"`
		Push    *bool  `json:"push"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Message == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 message"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["message"] = body.Message
	if body.Amend != nil {
		params["amend"] = *body.Amend
	}
	if body.Signoff != nil {
		params["signoff"] = *body.Signoff
	}
	if body.Push != nil {
		params["push"] = *body.Push
	}
	s.xaiCall(w, r, "x.ai/git/commit", params)
}

func (s *Server) handleGitCheckout(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Branch string `json:"branch"`
		Create *bool  `json:"create"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Branch == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 branch"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["branch"] = body.Branch
	if body.Create != nil {
		params["create"] = *body.Create
	}
	s.xaiCall(w, r, "x.ai/git/checkout", params)
}

func (s *Server) handleGitCheckoutCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd          string `json:"cwd"`
		Commit       string `json:"commit"`
		StashIfDirty *bool  `json:"stashIfDirty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Commit == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 commit"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["commit"] = body.Commit
	if body.StashIfDirty != nil {
		params["stashIfDirty"] = *body.StashIfDirty
	}
	s.xaiCall(w, r, "x.ai/git/checkout_commit", params)
}

func (s *Server) handleGitStash(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd              string `json:"cwd"`
		IncludeUntracked *bool  `json:"includeUntracked"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	if body.IncludeUntracked != nil {
		params["includeUntracked"] = *body.IncludeUntracked
	}
	s.xaiCall(w, r, "x.ai/git/stash", params)
}

func (s *Server) handleGitBranches(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/git/branches", gitRootParams(body.Cwd))
}

func (s *Server) handleGitCurrentCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/git/current_commit", gitRootParams(body.Cwd))
}

// handleGitRepoRoot — POST /api/git/repo-root {cwd} → x.ai/git/git_repo_root。
// wire 键注意：agent 侧 GitRepoRequest 只有必填的 currentWorkingDirectory
// （不是其它 git 方法用的 gitRoot）——发 gitRoot 会被 serde 判成缺字段，
// home 空状态的「在新 worktree 中开始」门控于是永远拿到 -32602
// "Invalid params"，入口恒为置灰。响应是 GitRepoResponse：仓库根
// {"GitRepo":{"gitRoot":…}}，非仓库 "NotGitRepo"。
func (s *Server) handleGitRepoRoot(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	s.xaiCall(w, r, "x.ai/git/git_repo_root", map[string]any{"currentWorkingDirectory": body.Cwd})
}

func (s *Server) handlePRStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Branch string `json:"branch"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" || body.Branch == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd 和 branch"})
		return
	}
	s.xaiCall(w, r, "x.ai/pr/status", map[string]any{"cwd": body.Cwd, "branch": body.Branch})
}

// ── Git（x.ai/git/*，camelCase；cwd 空则不带 gitRoot）────────────────

// handleGitFiles — POST /api/git/files {cwd?, paths, version?} →
// x.ai/git/files {gitRoot?, paths, version?}（paths 必填）。
func (s *Server) handleGitFiles(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string   `json:"cwd"`
		Paths   []string `json:"paths"`
		Version string   `json:"version,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if len(body.Paths) == 0 {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 paths"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["paths"] = body.Paths
	if body.Version != "" {
		params["version"] = body.Version
	}
	s.xaiCall(w, r, "x.ai/git/files", params)
}

// handleGitStageContent — POST /api/git/stage-content {cwd?, path, content}
// → x.ai/git/stage/content {gitRoot?, path, content}（path/content 必填）。
func (s *Server) handleGitStageContent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd     string `json:"cwd"`
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	params := gitRootParams(body.Cwd)
	params["path"] = body.Path
	params["content"] = body.Content
	s.xaiCall(w, r, "x.ai/git/stage/content", params)
}

// handleGitCheckoutSessionHead — POST /api/git/checkout-session-head {cwd?,
// stashIfDirty?} → x.ai/git/checkout_session_head {sessionId, gitRoot?,
// stashIfDirty?}（sessionId 必填 → 填活动会话）。
func (s *Server) handleGitCheckoutSessionHead(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd          string `json:"cwd"`
		StashIfDirty *bool  `json:"stashIfDirty,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := gitRootParams(body.Cwd)
	params["sessionId"] = ""
	if body.StashIfDirty != nil {
		params["stashIfDirty"] = *body.StashIfDirty
	}
	s.xaiCall(w, r, "x.ai/git/checkout_session_head", params)
}

// runGitCmd executes git plumbing or commands in cwd and returns trimmed stdout/stderr.
func runGitCmd(ctx context.Context, cwd string, args ...string) (string, error) {
	if cwd == "" {
		return "", fmt.Errorf("cwd 不能为空")
	}
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", cwd}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	outStr := strings.TrimSpace(stdout.String())
	errStr := strings.TrimSpace(stderr.String())
	if err != nil {
		if errStr != "" {
			return "", fmt.Errorf("%s", errStr)
		}
		return "", err
	}
	if outStr != "" {
		return outStr, nil
	}
	return errStr, nil
}

// registerExtGitRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtGitRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/git/status", s.handleGitStatus)
	mux.HandleFunc("POST /api/git/diffs", s.handleGitDiffs)
	mux.HandleFunc("POST /api/git/stage", s.handleGitStage)
	mux.HandleFunc("POST /api/git/unstage", s.handleGitUnstage)
	mux.HandleFunc("POST /api/git/discard", s.handleGitDiscard)
	mux.HandleFunc("POST /api/git/commit", s.handleGitCommit)
	mux.HandleFunc("POST /api/git/checkout", s.handleGitCheckout)
	mux.HandleFunc("POST /api/git/checkout-commit", s.handleGitCheckoutCommit)
	mux.HandleFunc("POST /api/git/stash", s.handleGitStash)
	mux.HandleFunc("POST /api/git/branches", s.handleGitBranches)
	mux.HandleFunc("POST /api/git/current-commit", s.handleGitCurrentCommit)
	mux.HandleFunc("POST /api/git/repo-root", s.handleGitRepoRoot)
	mux.HandleFunc("POST /api/pr/status", s.handlePRStatus)
	mux.HandleFunc("POST /api/git/files", s.handleGitFiles)
	mux.HandleFunc("POST /api/git/stage-content", s.handleGitStageContent)
	mux.HandleFunc("POST /api/git/checkout-session-head", s.handleGitCheckoutSessionHead)
	mux.HandleFunc("POST /api/git/worktree/create", s.handleWorktreeCreate)
	mux.HandleFunc("POST /api/git/worktree/remove", s.handleWorktreeRemove)
	mux.HandleFunc("POST /api/git/worktree/apply", s.handleWorktreeApply)
	mux.HandleFunc("POST /api/git/worktree/create-from-worktree", s.handleWorktreeCreateFromWorktree)
	mux.HandleFunc("POST /api/git/worktree/create-from-worktree-sync", s.handleWorktreeCreateFromWorktreeSync)
	mux.HandleFunc("POST /api/git/worktree/resume-session", s.handleWorktreeResumeSession)
	mux.HandleFunc("POST /api/git/worktree/list", s.handleWorktreeList)
	mux.HandleFunc("POST /api/git/worktree/show", s.handleWorktreeShow)
	mux.HandleFunc("POST /api/git/worktree/detach", s.handleWorktreeDetach)
	mux.HandleFunc("POST /api/git/worktree/salvage", s.handleWorktreeSalvage)
	mux.HandleFunc("POST /api/git/worktree/clean-artifacts", s.handleWorktreeCleanArtifacts)
	mux.HandleFunc("POST /api/git/worktree/gc", s.handleWorktreeGc)
	mux.HandleFunc("POST /api/git/worktree/db/stats", s.handleWorktreeDbStats)
	mux.HandleFunc("POST /api/git/worktree/db/rebuild", s.handleWorktreeDbRebuild)
	mux.HandleFunc("POST /api/git/worktree/db/path", s.handleWorktreeDbPath)
	mux.HandleFunc("POST /api/git/init", s.handleGitInit)
	mux.HandleFunc("POST /api/git/push", s.handleGitPush)
	mux.HandleFunc("POST /api/git/pull", s.handleGitPull)
	mux.HandleFunc("POST /api/git/fetch", s.handleGitFetch)
	mux.HandleFunc("POST /api/git/state", s.handleGitState)
	mux.HandleFunc("POST /api/git/log", s.handleGitLog)
	mux.HandleFunc("POST /api/git/stash/list", s.handleGitStashList)
	mux.HandleFunc("POST /api/git/stash/pop", s.handleGitStashPop)
	mux.HandleFunc("POST /api/git/stash/drop", s.handleGitStashDrop)
	mux.HandleFunc("POST /api/git/branch/create", s.handleGitBranchCreate)
	mux.HandleFunc("POST /api/git/branch/delete", s.handleGitBranchDelete)
}
