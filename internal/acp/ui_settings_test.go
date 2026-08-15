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
}
