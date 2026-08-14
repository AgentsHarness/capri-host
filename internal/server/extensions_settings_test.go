package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// newLocalServer builds a Server whose bridge never talks to a real
// process — enough for the local-disk endpoints (/api/extensions,
// /api/settings), which never touch the agent.
func newLocalServer(t *testing.T) *Server {
	t.Helper()
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	t.Cleanup(b.Shutdown)
	return New(config.Config{Port: 0}, b)
}

// getJSON issues a GET against the server's mux and returns the recorder.
func getJSON(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

// withFakeGrokHome redirects $HOME to a temp dir containing a .grok
// skeleton and returns the .grok path. os.UserHomeDir() reads $HOME on
// unix, so the handlers pick it up at request time.
func withFakeGrokHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	grok := filepath.Join(home, ".grok")
	if err := os.MkdirAll(grok, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	return grok
}

// ── GET /api/extensions ─────────────────────────────────────────────

// An empty (or absent) ~/.grok yields 200 with empty arrays — never an
// error and never null lists.
func TestExtensionsEmptyHome(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // no .grok at all
	s := newLocalServer(t)

	rec := getJSON(t, s, "/api/extensions")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	for _, k := range []string{"hooks", "plugins", "skills"} {
		arr, ok := m[k].([]any)
		if !ok || len(arr) != 0 {
			t.Fatalf("%s = %v, want empty array", k, m[k])
		}
	}
}

// A populated ~/.grok: config.toml [hooks.*] sections, hook dirs,
// installed plugins, and skills (user + bundled) with SKILL.md
// frontmatter names.
func TestExtensionsPopulatedHome(t *testing.T) {
	grok := withFakeGrokHome(t)
	s := newLocalServer(t)

	// config.toml with [hooks.<name>] sections (one enabled, one default).
	os.WriteFile(filepath.Join(grok, "config.toml"), []byte(`
[hooks.post-prompt]
command = "echo hi"
enabled = true

[hooks.other]
command = "echo bye"
`), 0o644)
	// SKILL.md-style hook dirs.
	os.MkdirAll(filepath.Join(grok, "hooks", "user-hook"), 0o755)
	// Installed plugins (marketplace-cache must NOT appear).
	os.MkdirAll(filepath.Join(grok, "installed-plugins", "myplug"), 0o755)
	os.MkdirAll(filepath.Join(grok, "installed-plugins", "marketplace-cache"), 0o755)
	// User skill with a frontmatter name.
	os.MkdirAll(filepath.Join(grok, "skills", "coder"), 0o755)
	os.WriteFile(filepath.Join(grok, "skills", "coder", "SKILL.md"),
		[]byte("---\nname: Coder Skill\ndescription: x\n---\nbody"), 0o644)
	// Bundled skill without a frontmatter name → dir name fallback.
	os.MkdirAll(filepath.Join(grok, "bundled", "skills", "bundled-skill"), 0o755)
	os.WriteFile(filepath.Join(grok, "bundled", "skills", "bundled-skill", "SKILL.md"),
		[]byte("# no frontmatter\n"), 0o644)

	rec := getJSON(t, s, "/api/extensions")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)

	// hooks: [hooks.*] config sections first (alphabetical), then
	// user-dir hook dirs.
	hooks, _ := m["hooks"].([]any)
	if len(hooks) != 3 {
		t.Fatalf("hooks = %v, want 3 entries", m["hooks"])
	}
	other := hooks[0].(map[string]any)
	if other["name"] != "other" || other["command"] != "echo bye" || other["enabled"] != true {
		t.Errorf("hooks[0] = %v, want enabled defaults to true", other)
	}
	post := hooks[1].(map[string]any)
	if post["name"] != "post-prompt" || post["command"] != "echo hi" || post["enabled"] != true {
		t.Errorf("hooks[1] = %v, want {name:post-prompt command:echo hi enabled:true}", post)
	}
	dirHook := hooks[2].(map[string]any)
	if dirHook["name"] != "user-hook" || dirHook["source"] != "user-dir" {
		t.Errorf("hooks[2] = %v, want {name:user-hook source:user-dir}", dirHook)
	}

	// plugins: installed only — marketplace-cache is excluded.
	plugins, _ := m["plugins"].([]any)
	if len(plugins) != 1 {
		t.Fatalf("plugins = %v, want 1 entry (marketplace-cache excluded)", m["plugins"])
	}
	plug := plugins[0].(map[string]any)
	if plug["name"] != "myplug" || plug["enabled"] != true || plug["source"] != "installed" {
		t.Errorf("plugins[0] = %v, want {name:myplug enabled:true source:installed}", plug)
	}

	// skills: frontmatter name for user scope, dir-name fallback for
	// bundled scope.
	skills, _ := m["skills"].([]any)
	if len(skills) != 2 {
		t.Fatalf("skills = %v, want 2 entries", m["skills"])
	}
	coder := skills[0].(map[string]any)
	if coder["name"] != "Coder Skill" || coder["scope"] != "user" {
		t.Errorf("skills[0] = %v, want {name:Coder Skill scope:user}", coder)
	}
	bs := skills[1].(map[string]any)
	if bs["name"] != "bundled-skill" || bs["scope"] != "bundled" {
		t.Errorf("skills[1] = %v, want {name:bundled-skill scope:bundled}", bs)
	}
}

// ── GET /api/settings ───────────────────────────────────────────────

// Missing config.toml → 200 with {} (never an error, never written).
func TestSettingsMissingFile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s := newLocalServer(t)

	rec := getJSON(t, s, "/api/settings")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); len(m) != 0 {
		t.Fatalf("resp = %s, want {}", rec.Body.String())
	}
}

// A config.toml with the known sections returns the scalar subset under
// ui/session/models/cli; unknown sections are dropped.
func TestSettingsEndpoint(t *testing.T) {
	grok := withFakeGrokHome(t)
	s := newLocalServer(t)

	os.WriteFile(filepath.Join(grok, "config.toml"), []byte(`
[ui]
yolo = true
compact_mode = false
permission_mode = "ask"
theme = "dark"          # inline comment stripped

[models]
default = "grok-4"
default_reasoning_effort = "high"

[session]
keepalive = 30

[cli]
color = "always"

[secret-stuff]
token = "never-exposed"
`), 0o644)

	rec := getJSON(t, s, "/api/settings")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)

	ui, _ := m["ui"].(map[string]any)
	if ui["yolo"] != true || ui["compact_mode"] != false ||
		ui["permission_mode"] != "ask" || ui["theme"] != "dark" {
		t.Errorf("ui = %v", m["ui"])
	}
	models, _ := m["models"].(map[string]any)
	if models["default"] != "grok-4" || models["default_reasoning_effort"] != "high" {
		t.Errorf("models = %v", m["models"])
	}
	sess, _ := m["session"].(map[string]any)
	if sess["keepalive"] != float64(30) {
		t.Errorf("session = %v, want keepalive 30 (number)", m["session"])
	}
	cli, _ := m["cli"].(map[string]any)
	if cli["color"] != "always" {
		t.Errorf("cli = %v", m["cli"])
	}
	// Unknown sections must not leak into the response.
	if _, has := m["secret-stuff"]; has {
		t.Errorf("resp = %s, want no unknown sections", rec.Body.String())
	}
}

// ── unit: hand-rolled parsers ───────────────────────────────────────

func TestParseConfigTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	os.WriteFile(path, []byte(`
bad = "no section, dropped"
# comment
[ui]
yolo = true
permission_mode = 'single-quoted'
ratio = 0.5

[hooks.post-prompt]
command = "echo \"hi\""
enabled = false

[models]
default = "grok-4"
list = [1, 2]            # arrays dropped
`), 0o644)

	got := parseConfigTOML(path)
	ui := got["ui"]
	if ui["yolo"] != true || ui["permission_mode"] != "single-quoted" || ui["ratio"] != 0.5 {
		t.Errorf("ui = %v", ui)
	}
	hooks := got["hooks.post-prompt"]
	if hooks["command"] != `echo "hi"` || hooks["enabled"] != false {
		t.Errorf("hooks.post-prompt = %v", hooks)
	}
	if _, has := got[""]["bad"]; has {
		t.Error("key before any section header must be dropped")
	}
	if _, has := got["models"]["list"]; has {
		t.Error("array values must be dropped")
	}
	if len(got) != 3 { // ui, hooks.post-prompt, models
		t.Errorf("sections = %v, want ui/hooks.post-prompt/models", got)
	}
}

func TestParseConfigTOMLMissingFile(t *testing.T) {
	if got := parseConfigTOML(filepath.Join(t.TempDir(), "nope.toml")); len(got) != 0 {
		t.Errorf("got %v, want empty map", got)
	}
}

func TestFrontmatterName(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "a.md"), []byte("---\nname: My Skill\ndescription: x\n---\nbody"), 0o644)
	if name, ok := frontmatterName(filepath.Join(dir, "a.md")); !ok || name != "My Skill" {
		t.Errorf("a.md: got %q ok=%v, want My Skill", name, ok)
	}

	os.WriteFile(filepath.Join(dir, "b.md"), []byte("---\nname: \"Quoted\"\n---\n"), 0o644)
	if name, ok := frontmatterName(filepath.Join(dir, "b.md")); !ok || name != "Quoted" {
		t.Errorf("b.md: got %q ok=%v, want Quoted", name, ok)
	}

	// No frontmatter → not ok.
	os.WriteFile(filepath.Join(dir, "c.md"), []byte("plain body"), 0o644)
	if _, ok := frontmatterName(filepath.Join(dir, "c.md")); ok {
		t.Error("c.md: no frontmatter, want ok=false")
	}

	// Frontmatter without a name field → not ok.
	os.WriteFile(filepath.Join(dir, "d.md"), []byte("---\ndescription: x\n---\n"), 0o644)
	if _, ok := frontmatterName(filepath.Join(dir, "d.md")); ok {
		t.Error("d.md: no name field, want ok=false")
	}

	// Missing file → not ok.
	if _, ok := frontmatterName(filepath.Join(dir, "missing.md")); ok {
		t.Error("missing.md: want ok=false")
	}
}
