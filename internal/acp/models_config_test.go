package acp

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// tempGrokBridge returns a bridge whose grok home is a temp dir, plus the
// path of the (initially absent) config.toml inside it.
func tempGrokBridge(t *testing.T) (*Bridge, string) {
	t.Helper()
	home := t.TempDir()
	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	return b, filepath.Join(home, "config.toml")
}

func writeTestConfig(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSetDefaultModelConfig(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, `
[models]
default = "grok-4.5"
web_search = "grok-4.5"

[ui]
theme = "groknight"
`)
	if err := b.SetDefaultModelConfig("deepseek-v4-flash-go", "max"); err != nil {
		t.Fatalf("SetDefaultModelConfig: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	for _, want := range []string{
		`default = "deepseek-v4-flash-go"`,
		`default_reasoning_effort = "max"`,
		`web_search = "grok-4.5"`, // 其他 [models] 键保留
		`theme = "groknight"`,     // 其他 section 保留
	} {
		if !strings.Contains(out, want) {
			t.Errorf("config.toml missing %q:\n%s", want, out)
		}
	}
}

func TestSetDefaultModelConfigEffortOmitted(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "[models]\ndefault = \"grok-4.5\"\ndefault_reasoning_effort = \"high\"\n")
	if err := b.SetDefaultModelConfig("deepseek-v4-pro", ""); err != nil {
		t.Fatalf("SetDefaultModelConfig: %v", err)
	}
	raw, _ := os.ReadFile(path)
	out := string(raw)
	if !strings.Contains(out, `default_reasoning_effort = "high"`) {
		t.Errorf("empty effort must leave default_reasoning_effort untouched:\n%s", out)
	}
	if !strings.Contains(out, `default = "deepseek-v4-pro"`) {
		t.Errorf("default not updated:\n%s", out)
	}
}

func TestSetDefaultModelConfigCreatesFile(t *testing.T) {
	b, path := tempGrokBridge(t)
	if err := b.SetDefaultModelConfig("grok-4.5", "high"); err != nil {
		t.Fatalf("SetDefaultModelConfig on missing file: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `default = "grok-4.5"`) {
		t.Errorf("created config missing default:\n%s", raw)
	}
}

func TestSetDefaultModelConfigRefusesUnparseable(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "this is [not toml\n")
	if err := b.SetDefaultModelConfig("grok-4.5", ""); err == nil {
		t.Fatal("expected error for unparseable config, got nil")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "this is [not toml\n" {
		t.Errorf("unparseable config must not be overwritten, got:\n%s", raw)
	}
}

func TestCustomModelsRejectsDuplicateRoutingSlug(t *testing.T) {
	b, _ := tempGrokBridge(t)
	if err := b.UpsertCustomModel("m1", map[string]any{
		"model":    "deepseek-v4-flash",
		"base_url": "https://api.example.com/v1",
	}); err != nil {
		t.Fatalf("UpsertCustomModel m1: %v", err)
	}

	// 同 slug 不同 id → 拒绝（grok 目录按 key 组织，同 slug 解析歧义）。
	err := b.UpsertCustomModel("m2", map[string]any{
		"model":    "deepseek-v4-flash",
		"base_url": "https://api.example.com/v2",
	})
	if err == nil {
		t.Fatal("expected duplicate-slug error, got nil")
	}
	if !strings.Contains(err.Error(), "deepseek-v4-flash") {
		t.Errorf("error should mention the slug, got: %v", err)
	}

	// 同 id 更新自身（slug 不变）→ 允许。
	if err := b.UpsertCustomModel("m1", map[string]any{
		"model":    "deepseek-v4-flash",
		"base_url": "https://api.example.com/v1-updated",
	}); err != nil {
		t.Fatalf("updating own entry must be allowed: %v", err)
	}

	// 编辑 m1 把 slug 改成 m2 已占用的 → 拒绝。
	if err := b.UpsertCustomModel("m2", map[string]any{
		"model":    "other-model",
		"base_url": "https://api.example.com/v2",
	}); err != nil {
		t.Fatalf("UpsertCustomModel m2: %v", err)
	}
	if err := b.UpsertCustomModel("m1", map[string]any{
		"model":    "other-model",
		"base_url": "https://api.example.com/v1",
	}); err == nil {
		t.Fatal("expected duplicate-slug error when editing to a taken slug, got nil")
	}

	list, err := b.ListCustomModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("list = %+v, want 2 entries", list)
	}
}

func TestCustomModelsRoundTrip(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "[ui]\ntheme = \"groknight\"\n")

	if err := b.UpsertCustomModel("my-model", map[string]any{
		"model":            "my-model",
		"base_url":         "https://api.example.com/v1",
		"name":             "My Model",
		"context_window":   int64(200000),
		"api_backend":      "chat_completions",
		"reasoning_effort": "high",
	}); err != nil {
		t.Fatalf("UpsertCustomModel: %v", err)
	}

	// 必填校验。
	if err := b.UpsertCustomModel("bad", map[string]any{"base_url": "x"}); err == nil {
		t.Fatal("expected error when routing slug missing")
	}
	if err := b.UpsertCustomModel("bad2", map[string]any{"model": "m"}); err == nil {
		t.Fatal("expected error when base_url missing")
	}

	list, err := b.ListCustomModels()
	if err != nil {
		t.Fatalf("ListCustomModels: %v", err)
	}
	if len(list) != 1 || list[0]["id"] != "my-model" {
		t.Fatalf("list = %+v, want one entry id=my-model", list)
	}
	if got, _ := list[0]["model"].(string); got != "my-model" {
		t.Errorf("model = %v", list[0]["model"])
	}
	if got, _ := list[0]["name"].(string); got != "My Model" {
		t.Errorf("name = %v", list[0]["name"])
	}

	// 更新同一 id = 整节替换。
	if err := b.UpsertCustomModel("my-model", map[string]any{
		"model":    "my-model",
		"base_url": "https://api.example.com/v2",
	}); err != nil {
		t.Fatalf("UpsertCustomModel update: %v", err)
	}
	list, _ = b.ListCustomModels()
	if len(list) != 1 {
		t.Fatalf("after update list = %+v, want 1 entry", list)
	}
	if got, _ := list[0]["base_url"].(string); got != "https://api.example.com/v2" {
		t.Errorf("base_url after update = %v", list[0]["base_url"])
	}
	if _, ok := list[0]["name"]; ok {
		t.Errorf("name should be gone after full-section replace, got %v", list[0]["name"])
	}

	// 删除并验证其他 section 保留。
	cleared, err := b.DeleteCustomModel("my-model")
	if err != nil {
		t.Fatalf("DeleteCustomModel: %v", err)
	}
	if cleared {
		t.Error("defaultCleared = true, want false")
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), `theme = "groknight"`) {
		t.Errorf("unrelated section lost:\n%s", raw)
	}
	list, _ = b.ListCustomModels()
	if len(list) != 0 {
		t.Errorf("after delete list = %+v, want empty", list)
	}
}

func TestDeleteCustomModelClearsDefault(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, `
[models]
default = "my-model"
default_reasoning_effort = "max"

[model.my-model]
model = "my-model"
base_url = "https://api.example.com/v1"
`)
	cleared, err := b.DeleteCustomModel("my-model")
	if err != nil {
		t.Fatalf("DeleteCustomModel: %v", err)
	}
	if !cleared {
		t.Error("defaultCleared = false, want true")
	}
	raw, _ := os.ReadFile(path)
	out := string(raw)
	if strings.Contains(out, "default =") || strings.Contains(out, "default_reasoning_effort") {
		t.Errorf("default must be cleared:\n%s", out)
	}
	if strings.Contains(out, "[model.my-model]") {
		t.Errorf("model section must be gone:\n%s", out)
	}
}

// TestCustomModelsWritesIntegerLiterals 回归：FE 经 JSON 提交的整数配置
// （context_window=1000000 等）被 encoding/json 解成 float64，若原样交给
// BurntSushi/toml 会写成 `1e+06`（合法 TOML float，但 agent 侧 schema 是
// 整数类型，热加载会拒绝）。落盘必须还原为整数字面量；temperature / top_p
// 保持浮点（top_p=1 写为 1.0）。
func TestCustomModelsWritesIntegerLiterals(t *testing.T) {
	b, path := tempGrokBridge(t)
	if err := b.UpsertCustomModel("big-ctx", map[string]any{
		"model":                 "big-ctx",
		"base_url":              "https://api.example.com/v1",
		"context_window":        float64(1000000), // 模拟 JSON 数字解码
		"max_completion_tokens": float64(200000),
		"max_retries":           float64(3),
		"temperature":           float64(0.7),
		"top_p":                 float64(1),
	}); err != nil {
		t.Fatalf("UpsertCustomModel: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	for _, want := range []string{
		"context_window = 1000000",
		"max_completion_tokens = 200000",
		"max_retries = 3",
		"temperature = 0.7",
		"top_p = 1.0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("config.toml missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "1e+06") {
		t.Errorf("config.toml must not contain float exponent literal:\n%s", out)
	}
}

// ── [models] 目录过滤（hidden_models / disabled_models）──

func TestModelFiltersRoundTrip(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, `
[models]
default = "cc-ds"

[ui]
theme = "groknight"
`)
	// 去空白 + 去重（同一份名单里重复的内层条目在 grok 侧无意义）。
	if err := b.SetModelFilters(ModelFilters{
		Hidden:   []string{" grok-* ", "", "grok-4.5", "grok-4.5"},
		Disabled: []string{"grok-4.7"},
	}); err != nil {
		t.Fatalf("SetModelFilters: %v", err)
	}
	out := readTestConfig(t, path)
	for _, want := range []string{
		`hidden_models = ["grok-*", "grok-4.5"]`,
		`disabled_models = ["grok-4.7"]`,
		`default = "cc-ds"`,   // [models] 其它键保留
		`theme = "groknight"`, // 其它 section 保留
	} {
		if !strings.Contains(out, want) {
			t.Errorf("config.toml missing %q:\n%s", want, out)
		}
	}

	got, err := b.ListModelFilters()
	if err != nil {
		t.Fatalf("ListModelFilters: %v", err)
	}
	if !reflect.DeepEqual(got.Hidden, []string{"grok-*", "grok-4.5"}) {
		t.Errorf("hidden = %#v, want [grok-* grok-4.5]", got.Hidden)
	}
	if !reflect.DeepEqual(got.Disabled, []string{"grok-4.7"}) {
		t.Errorf("disabled = %#v, want [grok-4.7]", got.Disabled)
	}

	// 清空一份名单：删除键，而不是留 `hidden_models = []` 的空壳。
	if err := b.SetModelFilters(ModelFilters{Disabled: []string{"grok-4.7"}}); err != nil {
		t.Fatalf("SetModelFilters (clear hidden): %v", err)
	}
	if out = readTestConfig(t, path); strings.Contains(out, "hidden_models") {
		t.Errorf("清空的名单应删除键:\n%s", out)
	}
}

func TestListModelFiltersEmptyWhenUnset(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "[models]\ndefault = \"cc-ds\"\n")
	got, err := b.ListModelFilters()
	if err != nil {
		t.Fatalf("ListModelFilters: %v", err)
	}
	// 空而非 nil：FE 侧直接当数组用，JSON 里必须是 []。
	if got.Hidden == nil || len(got.Hidden) != 0 {
		t.Errorf("hidden = %#v, want 非 nil 空切片", got.Hidden)
	}
	if got.Disabled == nil || len(got.Disabled) != 0 {
		t.Errorf("disabled = %#v, want 非 nil 空切片", got.Disabled)
	}
}

func TestListModelFiltersRejectsNonStringEntries(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "[models]\nhidden_models = [1, 2]\n")
	// 报错而不是当空名单：否则 FE 保存时会把用户手写的值冲掉。
	if _, err := b.ListModelFilters(); err == nil {
		t.Fatal("want error for non-string entries")
	}
}

func TestSetModelFiltersRejectsUnbalancedGlob(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "[models]\ndefault = \"cc-ds\"\n")
	if err := b.SetModelFilters(ModelFilters{Hidden: []string{"grok-4.5["}}); err == nil {
		t.Fatal("want error for unclosed character class")
	}
	if strings.Contains(readTestConfig(t, path), "hidden_models") {
		t.Errorf("非法模式不能落盘:\n%s", readTestConfig(t, path))
	}
	// `[]]`（类内首个 ] 是字面量）与 `{a,b}` 都是合法 glob，不能被误拒。
	if err := b.SetModelFilters(ModelFilters{Hidden: []string{"x[]]y", "a{b,c}"}}); err != nil {
		t.Fatalf("合法模式被拒: %v", err)
	}
}

func TestSetModelFiltersEmptyWithoutModelsTableWritesNothing(t *testing.T) {
	b, path := tempGrokBridge(t)
	if err := b.SetModelFilters(ModelFilters{}); err != nil {
		t.Fatalf("SetModelFilters: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("两份空名单不该创建 config.toml（stat err = %v）", err)
	}
}

func TestSetModelFiltersRefusesNonTableModels(t *testing.T) {
	b, path := tempGrokBridge(t)
	writeTestConfig(t, path, "models = 5\n")
	// 用户写坏的 `models = 5` 不能被一个空表冲掉。
	if err := b.SetModelFilters(ModelFilters{Hidden: []string{"grok-*"}}); err == nil {
		t.Fatal("want error when [models] is not a table")
	}
	if out := readTestConfig(t, path); out != "models = 5\n" {
		t.Errorf("文件被改写：%q", out)
	}
}

func readTestConfig(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
