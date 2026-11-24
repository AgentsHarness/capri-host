package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (s *Server) handleGitInit(w http.ResponseWriter, r *http.Request) {
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
	out, err := runGitCmd(r.Context(), body.Cwd, "init")
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

func (s *Server) handleGitPush(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd         string `json:"cwd"`
		Remote      string `json:"remote,omitempty"`
		Branch      string `json:"branch,omitempty"`
		Force       bool   `json:"force,omitempty"`
		SetUpstream bool   `json:"setUpstream,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	remote := body.Remote
	if remote == "" {
		remote = "origin"
	}
	args := []string{"push"}
	if body.Force {
		args = append(args, "--force-with-lease")
	}
	if body.SetUpstream {
		args = append(args, "-u")
	}
	args = append(args, remote)
	if body.Branch != "" {
		args = append(args, body.Branch)
	} else {
		args = append(args, "HEAD")
	}
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

func (s *Server) handleGitPull(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Remote string `json:"remote,omitempty"`
		Branch string `json:"branch,omitempty"`
		Rebase bool   `json:"rebase,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	remote := body.Remote
	if remote == "" {
		remote = "origin"
	}
	args := []string{"pull"}
	if body.Rebase {
		args = append(args, "--rebase")
	}
	args = append(args, remote)
	if body.Branch != "" {
		args = append(args, body.Branch)
	}
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

func (s *Server) handleGitFetch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Remote string `json:"remote,omitempty"`
		Prune  bool   `json:"prune,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	remote := body.Remote
	if remote == "" {
		remote = "origin"
	}
	args := []string{"fetch"}
	if body.Prune {
		args = append(args, "--prune")
	}
	args = append(args, remote)
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

// GitRepoState — 仓库进行中状态（merge/rebase/cherry-pick）与冲突文件清单，
// 只读信息，FE git 面板横幅用。
type GitRepoState struct {
	MergeInProgress      bool     `json:"mergeInProgress"`
	RebaseInProgress     bool     `json:"rebaseInProgress"`
	CherryPickInProgress bool     `json:"cherryPickInProgress"`
	Conflicts            []string `json:"conflicts"`
	ConflictCount        int      `json:"conflictCount"`
}

// handleGitState — POST /api/git/state → 仓库进行中状态。直接跑 git
// （同 push/pull/fetch），不依赖 agent 会话；非 git 目录返回 ok:false。
// 进行中状态以 gitDir 下的标记文件/目录存在性为准（worktree 下 rev-parse
// --git-dir 会返回链接后的 gitdir，路径已正确解析）；冲突清单取未合并条目。
func (s *Server) handleGitState(w http.ResponseWriter, r *http.Request) {
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
	gitDir, err := runGitCmd(r.Context(), body.Cwd, "rev-parse", "--git-dir")
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if !filepath.IsAbs(gitDir) {
		gitDir = filepath.Join(body.Cwd, gitDir)
	}
	markerExists := func(name string) bool {
		_, statErr := os.Stat(filepath.Join(gitDir, name))
		return statErr == nil
	}
	state := GitRepoState{Conflicts: []string{}}
	state.MergeInProgress = markerExists("MERGE_HEAD")
	state.RebaseInProgress = markerExists("rebase-merge") || markerExists("rebase-apply")
	state.CherryPickInProgress = markerExists("CHERRY_PICK_HEAD")
	if out, conflictErr := runGitCmd(r.Context(), body.Cwd, "diff", "--name-only", "--diff-filter=U"); conflictErr == nil && out != "" {
		for _, line := range strings.Split(out, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				state.Conflicts = append(state.Conflicts, line)
			}
		}
	}
	state.ConflictCount = len(state.Conflicts)
	writeJSON(w, 200, map[string]any{"ok": true, "state": state})
}

type GitLogEntry struct {
	Hash      string `json:"hash"`
	ShortHash string `json:"shortHash"`
	Author    string `json:"author"`
	Email     string `json:"email"`
	Timestamp int64  `json:"timestamp"`
	Date      string `json:"date"`
	Message   string `json:"message"`
	Refs      string `json:"refs,omitempty"`
}

func (s *Server) handleGitLog(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd      string `json:"cwd"`
		MaxCount int    `json:"maxCount,omitempty"`
		Skip     int    `json:"skip,omitempty"`
		Branch   string `json:"branch,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	maxCount := body.MaxCount
	if maxCount <= 0 {
		maxCount = 30
	} else if maxCount > 100 {
		maxCount = 100
	}
	args := []string{
		"log",
		fmt.Sprintf("-n%d", maxCount),
		fmt.Sprintf("--skip=%d", body.Skip),
		"--pretty=format:%H%x00%h%x00%an%x00%ae%x00%at%x00%ad%x00%s%x00%d",
		"--date=relative",
	}
	if body.Branch != "" {
		args = append(args, body.Branch)
	}
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error(), "commits": []GitLogEntry{}})
		return
	}
	var entries []GitLogEntry
	if out != "" {
		lines := strings.Split(out, "\n")
		for _, line := range lines {
			parts := strings.Split(line, "\x00")
			if len(parts) >= 7 {
				var ts int64
				fmt.Sscanf(parts[4], "%d", &ts)
				refs := ""
				if len(parts) > 7 {
					refs = strings.TrimSpace(parts[7])
					refs = strings.TrimPrefix(refs, "(")
					refs = strings.TrimSuffix(refs, ")")
				}
				entries = append(entries, GitLogEntry{
					Hash:      parts[0],
					ShortHash: parts[1],
					Author:    parts[2],
					Email:     parts[3],
					Timestamp: ts,
					Date:      parts[5],
					Message:   parts[6],
					Refs:      refs,
				})
			}
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true, "commits": entries})
}

type GitStashItem struct {
	Index   int    `json:"index"`
	Ref     string `json:"ref"`
	Hash    string `json:"hash"`
	Date    string `json:"date"`
	Message string `json:"message"`
}

func (s *Server) handleGitStashList(w http.ResponseWriter, r *http.Request) {
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
	// %cI 严格 ISO 8601，前端 GitPanel 统一格式化为本地 "YYYY-MM-DD HH:mm"。
	out, err := runGitCmd(r.Context(), body.Cwd, "stash", "list", "--pretty=format:%gd%x00%h%x00%cI%x00%gs")
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error(), "stashes": []GitStashItem{}})
		return
	}
	var list []GitStashItem
	if out != "" {
		lines := strings.Split(out, "\n")
		for i, line := range lines {
			parts := strings.Split(line, "\x00")
			if len(parts) >= 4 {
				list = append(list, GitStashItem{
					Index:   i,
					Ref:     parts[0],
					Hash:    parts[1],
					Date:    parts[2],
					Message: parts[3],
				})
			}
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true, "stashes": list})
}

func (s *Server) handleGitStashPop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string `json:"cwd"`
		Index string `json:"index,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	args := []string{"stash", "pop"}
	if body.Index != "" {
		ref := body.Index
		if !strings.HasPrefix(ref, "stash@{") {
			ref = fmt.Sprintf("stash@{%s}", ref)
		}
		args = append(args, ref)
	}
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

func (s *Server) handleGitStashDrop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd   string `json:"cwd"`
		Index string `json:"index,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	args := []string{"stash", "drop"}
	if body.Index != "" {
		ref := body.Index
		if !strings.HasPrefix(ref, "stash@{") {
			ref = fmt.Sprintf("stash@{%s}", ref)
		}
		args = append(args, ref)
	}
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

func (s *Server) handleGitBranchCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd      string `json:"cwd"`
		Branch   string `json:"branch"`
		Checkout bool   `json:"checkout,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" || body.Branch == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd 和 branch"})
		return
	}
	var args []string
	if body.Checkout {
		args = []string{"checkout", "-b", body.Branch}
	} else {
		args = []string{"branch", body.Branch}
	}
	out, err := runGitCmd(r.Context(), body.Cwd, args...)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}

func (s *Server) handleGitBranchDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd    string `json:"cwd"`
		Branch string `json:"branch"`
		Force  bool   `json:"force,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Cwd == "" || body.Branch == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd 和 branch"})
		return
	}
	flag := "-d"
	if body.Force {
		flag = "-D"
	}
	out, err := runGitCmd(r.Context(), body.Cwd, "branch", flag, body.Branch)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": out})
}
