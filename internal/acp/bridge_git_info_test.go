package acp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── grokMarkerMainRepo（standalone grok clone 背指针扫描）──────────────

func TestGrokMarkerMainRepoPlainDir(t *testing.T) {
	dir := t.TempDir()
	if main, ok := grokMarkerMainRepo(dir); ok {
		t.Fatalf("plain dir reported marker main = %q, want none", main)
	}
}

func TestGrokMarkerMainRepoFindsMarker(t *testing.T) {
	main := "/Users/benin/src/main-repo"
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clone, ".git", "grok-worktree-source"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
	got, ok := grokMarkerMainRepo(clone)
	if !ok || got != main {
		t.Fatalf("marker lookup = (%q, %v), want (%q, true)", got, ok, main)
	}
}

func TestGrokMarkerMainRepoFromNestedCwd(t *testing.T) {
	main := "/Users/benin/src/main-repo"
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clone, ".git", "grok-worktree-source"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(clone, "sub", "dir")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	got, ok := grokMarkerMainRepo(nested)
	if !ok || got != main {
		t.Fatalf("nested marker lookup = (%q, %v), want (%q, true)", got, ok, main)
	}
}

func TestGrokMarkerMainRepoStopsAtNestedRepo(t *testing.T) {
	main := "/Users/benin/src/main-repo"
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clone, ".git", "grok-worktree-source"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
	// Nested independent checkout: own .git without a marker — must NOT
	// inherit the parent clone's marker (TUI parity).
	nested := filepath.Join(clone, "vendor", "dep")
	if err := os.MkdirAll(filepath.Join(nested, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, ok := grokMarkerMainRepo(nested); ok {
		t.Fatal("nested repo inherited the parent marker, want none")
	}
}

func TestGrokMarkerMainRepoStopsAtLinkedWorktreeDotGitFile(t *testing.T) {
	// A linked worktree's `.git` is a FILE (gitdir: …), so the marker read
	// fails and the walk must stop there instead of inheriting an ancestor's
	// marker.
	main := "/Users/benin/src/main-repo"
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clone, ".git", "grok-worktree-source"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(clone, "linked-wt")
	if err := os.MkdirAll(linked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: /elsewhere/.git/worktrees/linked-wt\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := grokMarkerMainRepo(linked); ok {
		t.Fatal("linked worktree (.git file) inherited the ancestor marker, want none")
	}
}

// ── probeWorktree（marker 优先 + rev-parse linked 兜底）────────────────

func TestProbeWorktreeStandaloneCloneMarker(t *testing.T) {
	// Outside $HOME so the expected mainRepo stays verbatim (tilde-collapse
	// is covered by TestShortenHome).
	main := filepath.Join(t.TempDir(), "main-repo")
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clone, ".git", "grok-worktree-source"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
	// No real git repo needed: the marker check runs before rev-parse.
	isWt, got := probeWorktree(clone)
	if !isWt || got != main {
		t.Fatalf("probeWorktree(clone) = (%v, %q), want (true, %q)", isWt, got, main)
	}
}

func TestProbeWorktreeLinkedGitWorktree(t *testing.T) {
	requireGit(t)
	// `git -C` cannot chdir into a not-yet-existing dir, so create the
	// repo dirs first, then init inside them.
	main := filepath.Join(t.TempDir(), "main")
	wt := filepath.Join(t.TempDir(), "wt")
	if err := os.MkdirAll(main, 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(main, "init", "-q", "-b", "main")
	runGit(main, "worktree", "add", "-q", "-b", "wt-branch", wt)

	isWt, got := probeWorktree(wt)
	if !isWt {
		t.Fatal("linked worktree not detected")
	}
	// git resolves symlinked prefixes (/var → /private/var on macOS), so
	// compare against the symlink-resolved main path.
	want, err := filepath.EvalSymlinks(main)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("mainRepo = %q, want %q", got, want)
	}
	// The main checkout itself is NOT a worktree.
	if isWt, _ := probeWorktree(main); isWt {
		t.Fatal("main checkout reported as worktree")
	}
}

func TestProbeWorktreePlainRepo(t *testing.T) {
	requireGit(t)
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(repo, "init", "-q", "-b", "main")
	if isWt, main := probeWorktree(repo); isWt || main != "" {
		t.Fatalf("plain repo = (%v, %q), want (false, \"\")", isWt, main)
	}
}

// ── stashedWorktree（agent 三路 stash 命中规则）────────────────────────

func TestStashedWorktree(t *testing.T) {
	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host"})
	b.mu.Lock()
	b.sessions["s1"] = &SessionState{
		SessionID:   "s1",
		Cwd:         "/repo/wt",
		gitWorktree: true,
		gitMainRepo: "/repo/main",
	}
	b.mu.Unlock()

	// Exact cwd match → stash wins.
	if isWt, main := b.stashedWorktree("s1", "/repo/wt"); !isWt || main != "/repo/main" {
		t.Fatalf("matching session = (%v, %q), want (true, /repo/main)", isWt, main)
	}
	// Trailing-slash normalization (normCwd) still matches.
	if isWt, main := b.stashedWorktree("s1", "/repo/wt/"); !isWt || main != "/repo/main" {
		t.Fatalf("trailing-slash cwd = (%v, %q), want (true, /repo/main)", isWt, main)
	}
	// Session moved to another cwd → stash is stale, must not leak.
	if isWt, main := b.stashedWorktree("s1", "/elsewhere"); isWt || main != "" {
		t.Fatalf("moved session = (%v, %q), want (false, \"\")", isWt, main)
	}
	// Unknown session / empty sessionId → no stash.
	if isWt, main := b.stashedWorktree("nope", "/repo/wt"); isWt || main != "" {
		t.Fatalf("unknown session = (%v, %q), want (false, \"\")", isWt, main)
	}
	if isWt, main := b.stashedWorktree("", "/repo/wt"); isWt || main != "" {
		t.Fatalf("empty sessionId = (%v, %q), want (false, \"\")", isWt, main)
	}
}

// ── shortenHome ─────────────────────────────────────────────────────────

func TestShortenHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		t.Skip("no home dir")
	}
	if got := shortenHome(filepath.Join(home, "src", "repo")); got != "~/src/repo" {
		t.Fatalf("shortenHome under home = %q, want ~/src/repo", got)
	}
	if got := shortenHome("/elsewhere/repo"); got != "/elsewhere/repo" {
		t.Fatalf("shortenHome outside home = %q, want verbatim", got)
	}
	// Prefix guard: /Users/benin2 must not collapse under /Users/benin.
	prefixSibling := home + "2"
	if strings.HasPrefix(prefixSibling, home+string(filepath.Separator)) {
		t.Skip("home prefix sibling construction unexpected")
	}
	if got := shortenHome(prefixSibling + "/repo"); got != prefixSibling+"/repo" {
		t.Fatalf("shortenHome prefix sibling = %q, want verbatim", got)
	}
}

func requireGit(t *testing.T) {
	t.Helper()
	if out := runGit(".", "--version"); out == "" {
		t.Skip("git not available")
	}
}
