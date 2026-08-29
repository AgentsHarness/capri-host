package acp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestSetUiSettingsWhitelistAndPreserve(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "config.toml")
	if err := os.WriteFile(path, []byte(`
[ui]
theme = "dark"
yolo = true

[models]
default = "grok-4"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        home,
	})
	t.Cleanup(b.Shutdown)

	if err := b.SetUiSettings(map[string]any{
		"collapsed_edit_blocks":   true,
		"page_flip_on_send":       false,
		"remember_tool_approvals": true,
		"permission_mode":         "auto",
		"follow_up_behavior":      "steer",
	}); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var table map[string]any
	if err := toml.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	ui, _ := table["ui"].(map[string]any)
	if ui["collapsed_edit_blocks"] != true ||
		ui["page_flip_on_send"] != false ||
		ui["remember_tool_approvals"] != true ||
		ui["permission_mode"] != "auto" ||
		ui["follow_up_behavior"] != "steer" ||
		ui["theme"] != "dark" ||
		ui["yolo"] != true {
		t.Fatalf("ui = %#v", ui)
	}
	models, _ := table["models"].(map[string]any)
	if models["default"] != "grok-4" {
		t.Fatalf("models = %#v", models)
	}

	if err := b.SetUiSettings(nil); err == nil {
		t.Fatal("empty patch must fail")
	}
	if err := b.SetUiSettings(map[string]any{"theme": "light"}); err == nil {
		t.Fatal("unknown key must fail")
	}
	if err := b.SetUiSettings(map[string]any{"permission_mode": "yolo"}); err == nil {
		t.Fatal("bad permission_mode must fail")
	}
	if err := b.SetUiSettings(map[string]any{"follow_up_behavior": "yolo"}); err == nil {
		t.Fatal("bad follow_up_behavior must fail")
	}
	if err := b.SetUiSettings(map[string]any{"follow_up_behavior": 7}); err == nil {
		t.Fatal("non-string follow_up_behavior must fail")
	}
}

// SetToolsetSettings writes only the [toolset.ask_user_question] timeout
// pair; the rest of [toolset] and other sections survive untouched.
func TestSetToolsetSettingsWhitelistAndPreserve(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "config.toml")
	if err := os.WriteFile(path, []byte(`
[ui]
theme = "dark"

[toolset.bash]
find_bfs = true
`), 0o644); err != nil {
		t.Fatal(err)
	}
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        home,
	})
	t.Cleanup(b.Shutdown)

	if err := b.SetToolsetSettings(map[string]any{
		"timeout_enabled": false,
		"timeout_secs":    int64(45),
	}); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var table map[string]any
	if err := toml.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	toolset, _ := table["toolset"].(map[string]any)
	ask, _ := toolset["ask_user_question"].(map[string]any)
	if ask["timeout_enabled"] != false || ask["timeout_secs"] != int64(45) {
		t.Fatalf("toolset.ask_user_question = %#v", ask)
	}
	bash, _ := toolset["bash"].(map[string]any)
	if bash["find_bfs"] != true {
		t.Fatalf("toolset.bash = %#v, want preserved", bash)
	}
	ui, _ := table["ui"].(map[string]any)
	if ui["theme"] != "dark" {
		t.Fatalf("ui = %#v, want preserved", ui)
	}

	if err := b.SetToolsetSettings(nil); err == nil {
		t.Fatal("empty patch must fail")
	}
	if err := b.SetToolsetSettings(map[string]any{"timeout_secs": 60, "nope": 1}); err == nil {
		t.Fatal("unknown key must fail")
	}
	if err := b.SetToolsetSettings(map[string]any{"timeout_enabled": "yes"}); err == nil {
		t.Fatal("non-bool timeout_enabled must fail")
	}
	for _, bad := range []any{int64(0), int64(-5), int64(86401), "30", 3.5, true} {
		if err := b.SetToolsetSettings(map[string]any{"timeout_secs": bad}); err == nil {
			t.Fatalf("timeout_secs = %#v must fail", bad)
		}
	}
}

// SetToolsetSettings with only one key leaves the other sub-key absent.
func TestSetToolsetSettingsPartialPatch(t *testing.T) {
	home := t.TempDir()
	b := NewBridge(GrokConfig{
		Bin:             "/nonexistent/grok",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        home,
	})
	t.Cleanup(b.Shutdown)

	if err := b.SetToolsetSettings(map[string]any{"timeout_secs": float64(90)}); err != nil {
		t.Fatal(err)
	}
	path, err := b.ConfigTOMLPath()
	if err != nil {
		t.Fatal(err)
	}
	var table map[string]any
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := toml.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	ask, _ := table["toolset"].(map[string]any)["ask_user_question"].(map[string]any)
	if ask["timeout_secs"] != int64(90) {
		t.Fatalf("toolset.ask_user_question = %#v", ask)
	}
	if _, has := ask["timeout_enabled"]; has {
		t.Fatalf("timeout_enabled must be absent, got %#v", ask)
	}
}
