package acp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// normalizeConfigValue converts JSON-derived float64s back to integer-typed
// TOML literals when the value is numerically integral. encoding/json has no
// integer type for map[string]any — a FE-sent context_window=1000000 arrives
// as float64, and BurntSushi/toml renders float64 with %g, emitting `1e+06`
// (a valid TOML float, but the agent's schema types context_window /
// max_retries / … as integers, so the watcher rejects the hot reload).
// temperature / top_p stay floats (f64 in the agent schema) — even when the
// user types an integral value, they must keep their float literal — and
// genuinely fractional values keep their float form too.
func normalizeConfigValue(k string, v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for nk, nv := range x {
			out[nk] = normalizeConfigValue(nk, nv)
		}
		return out
	case float64:
		switch k {
		case "temperature", "top_p":
			return v
		}
		if x == math.Trunc(x) && x >= float64(math.MinInt64) && x < float64(math.MaxInt64) {
			return int64(x)
		}
	}
	return v
}

// config.toml 的 `[models]` / `[model.<id>]` 原子读改写 —— FE 侧「设为默认
// 模型」与「自定义模型可视化配置」的落盘层。语义对齐 TUI（xai-grok-shell
// util/config/persist.rs）：读现有文件 → 只改目标小节 → 临时文件 + 原子
// rename（保留权限位）。agent 侧自带 config.toml watcher（ConfigUpdate::
// ModelsChanged），写完后热加载生效，无需重启。

// ConfigTOMLPath returns <grokHome>/config.toml.
func (b *Bridge) ConfigTOMLPath() (string, error) {
	home := b.grokHome()
	if home == "" {
		return "", errors.New("grok home 未知，无法定位 config.toml")
	}
	return filepath.Join(home, "config.toml"), nil
}

// readConfigTable loads config.toml as a generic table; a missing file
// yields an empty table (first write creates the file).
func readConfigTable(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取 %s: %w", path, err)
	}
	var table map[string]any
	if err := toml.Unmarshal(raw, &table); err != nil {
		// 解析失败拒绝覆盖（TUI 同款语义），避免把用户写坏的文件冲掉。
		return nil, fmt.Errorf("解析 %s 失败（拒绝覆盖，请先修复语法）: %w", path, err)
	}
	return table, nil
}

// writeConfigTable atomically writes the table back: temp file + rename,
// preserving the destination mode on unix.
func writeConfigTable(path string, table map[string]any) error {
	raw, err := toml.Marshal(table)
	if err != nil {
		return fmt.Errorf("序列化 %s: %w", path, err)
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("创建目录 %s: %w", dir, err)
		}
	}
	var priorMode os.FileMode
	if st, err := os.Stat(path); err == nil {
		priorMode = st.Mode().Perm()
	} else {
		priorMode = 0o600
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, raw, priorMode); err != nil {
		return fmt.Errorf("写入临时文件 %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("替换 %s: %w", path, err)
	}
	return nil
}

// configSection returns the subtable at `key`, creating it when absent.
func configSection(table map[string]any, key string) map[string]any {
	if m, ok := table[key].(map[string]any); ok {
		return m
	}
	m := map[string]any{}
	table[key] = m
	return m
}

// readConfigSection walks nested tables without creating anything: it is
// the read-only counterpart to configSection, for callers that must not
// mutate the parsed config (asking whether a key exists, not seeding one).
func readConfigSection(table map[string]any, keys ...string) (map[string]any, bool) {
	cur := table
	for _, key := range keys {
		next, ok := cur[key].(map[string]any)
		if !ok {
			return map[string]any{}, false
		}
		cur = next
	}
	return cur, true
}

// SetDefaultModelConfig persists `[models].default` and, when effort is
// non-empty, `[models].default_reasoning_effort`. Other `[models]` keys and
// every other section survive untouched.
func (b *Bridge) SetDefaultModelConfig(modelID, effort string) error {
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return err
	}
	models := configSection(table, "models")
	models["default"] = modelID
	if effort != "" {
		models["default_reasoning_effort"] = effort
	}
	return writeConfigTable(path, table)
}

// ListCustomModels returns every `[model.<id>]` entry as a JSON-friendly
// map (id plus the raw section fields). Only user-defined sections appear —
// built-in catalog models are not in config.toml.
func (b *Bridge) ListCustomModels() ([]map[string]any, error) {
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return nil, err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return nil, err
	}
	models, _ := table["model"].(map[string]any)
	var out []map[string]any
	for id, v := range models {
		entry, ok := v.(map[string]any)
		if !ok {
			continue
		}
		row := map[string]any{"id": id}
		for k, val := range entry {
			row[k] = val
		}
		out = append(out, row)
	}
	return out, nil
}

// UpsertCustomModel replaces the whole `[model.<id>]` section with values.
// The id is the section key (never part of the section body); `model`
// (routing slug) and `base_url` are required — the same mandatory fields
// the TUI's ModelEntryConfig enforces for usable BYOK models.
//
// Uniqueness guard: a routing slug may be configured for ONE model id only.
// grok merges `[model.*]` overrides into a keyed catalog
// (parse_model_overrides → IndexMap insert) where a duplicate id silently
// overwrites the earlier entry ("duplicate id: second overwrites first" —
// config_model_override_parse.rs), and default-model resolution matches by
// slug with first-hit-wins (models/resolution.rs) — two entries sharing a
// slug make `default` / `/model <slug>` resolution ambiguous. Rejecting the
// write keeps config.toml free of such collisions.
func (b *Bridge) UpsertCustomModel(id string, values map[string]any) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("模型 id 不能为空")
	}
	slug, _ := values["model"].(string)
	if strings.TrimSpace(slug) == "" {
		return errors.New("model（路由 slug）必填")
	}
	base, _ := values["base_url"].(string)
	if strings.TrimSpace(base) == "" {
		return errors.New("base_url 必填")
	}
	// 同一 routing slug 只允许一个配置（grok 目录按 key 组织，同 slug 的
	// 第二个条目会让默认模型/`/model <slug>` 解析歧义）。
	if existing, err := b.ListCustomModels(); err == nil {
		for _, row := range existing {
			rowID, _ := row["id"].(string)
			if rowID == id {
				continue
			}
			if other, _ := row["model"].(string); other == slug {
				return fmt.Errorf("路由 slug「%s」已被 [model.%s] 使用，相同模型只能配置一个", slug, rowID)
			}
		}
	}
	section := make(map[string]any, len(values))
	for k, v := range values {
		if k == "id" {
			continue
		}
		section[k] = normalizeConfigValue(k, v)
	}
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return err
	}
	models := configSection(table, "model")
	models[id] = section
	return writeConfigTable(path, table)
}

// DeleteCustomModel removes `[model.<id>]`. When the deleted id is the
// configured default, `models.default` / `default_reasoning_effort` are
// cleared too so model resolution falls back instead of pointing at a
// missing entry. Returns whether the default was cleared.
func (b *Bridge) DeleteCustomModel(id string) (defaultCleared bool, err error) {
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return false, err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return false, err
	}
	if models, ok := table["model"].(map[string]any); ok {
		if _, exists := models[id]; exists {
			delete(models, id)
			if len(models) == 0 {
				delete(table, "model")
			}
		}
	}
	if models, ok := table["models"].(map[string]any); ok {
		if cur, _ := models["default"].(string); cur == id {
			delete(models, "default")
			delete(models, "default_reasoning_effort")
			defaultCleared = true
		}
	}
	return defaultCleared, writeConfigTable(path, table)
}

// ReloadModels asks the agent to re-read config.toml and rebuild its model
// catalog (x.ai/internal/reload_models: re-read → merge → apply_config →
// notify clients). The stdio agent has NO config.toml watcher of its own
// (the ConfigFileWatcher only runs in leader/pager mode), so every
// config.toml write must be followed by this call for new custom models /
// the new default to take effect WITHOUT a restart.
func (b *Bridge) ReloadModels(ctx context.Context) error {
	_, err := b.XaiCall(ctx, "x.ai/internal/reload_models", map[string]any{})
	return err
}

// ── [models] 目录过滤：hidden_models / disabled_models ─────────────────

// ModelFilters 是 config.toml `[models]` 下的两个目录过滤名单，语义对齐
// grok 的 resolve_model_catalog：hidden_models 把命中的条目从模型列表 /
// 选择器隐藏（仍可用 `-m` 指定），disabled_models 把命中的条目从目录里
// 整条移除。两者都按「目录键名或模型 id」做 glob 匹配（grok 侧 globset），
// 所以条目既可写 id（grok-4.6），也可写模式（grok-*）——内置默认与官方
// 拉取的模型不在 config.toml 里，只有这种名单能挡掉它们。
type ModelFilters struct {
	Hidden   []string
	Disabled []string
}

// ListModelFilters 读这两个名单。缺键、缺 [models] 小节都算空名单（返回
// 非 nil 空切片，JSON 里是 [] 而非 null）；值不是字符串数组则报错，而不
// 当成空名单——否则 FE 一保存就把用户手写的值冲掉了。
func (b *Bridge) ListModelFilters() (ModelFilters, error) {
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return ModelFilters{}, err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return ModelFilters{}, err
	}
	models, _ := readConfigSection(table, "models")
	hidden, err := configStringList(models["hidden_models"], "hidden_models")
	if err != nil {
		return ModelFilters{}, err
	}
	disabled, err := configStringList(models["disabled_models"], "disabled_models")
	if err != nil {
		return ModelFilters{}, err
	}
	return ModelFilters{Hidden: hidden, Disabled: disabled}, nil
}

// SetModelFilters 写这两个名单：非空写入，空则删除键（不留 `hidden_models
// = []` 的空壳）。只动这两个键，[models] 其它键与其它小节原样保留。grok 侧
// 没有配置 watcher，写完仍需 ReloadModels 才生效。
func (b *Bridge) SetModelFilters(filters ModelFilters) error {
	hidden, err := normalizeFilterPatterns("hidden_models", filters.Hidden)
	if err != nil {
		return err
	}
	disabled, err := normalizeFilterPatterns("disabled_models", filters.Disabled)
	if err != nil {
		return err
	}
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return err
	}
	models, ok := readConfigSection(table, "models")
	if !ok {
		// 键存在但不是表（用户写坏的 `models = 5`）：拒绝覆盖，别用一个空表
		// 把它冲掉——grok 自己也解析不了这种文件，先修好再改。
		if raw, exists := table["models"]; exists {
			return fmt.Errorf("[models] 不是表（当前是 %T），拒绝覆盖", raw)
		}
		if len(hidden) == 0 && len(disabled) == 0 {
			return nil // 两份名单都清空且本来没有 [models]：不凭空建表
		}
		models = configSection(table, "models")
	}
	applyFilterList(models, "hidden_models", hidden)
	applyFilterList(models, "disabled_models", disabled)
	return writeConfigTable(path, table)
}

func applyFilterList(models map[string]any, key string, patterns []string) {
	if len(patterns) == 0 {
		delete(models, key)
		return
	}
	models[key] = patterns
}

// configStringList 把 `[models]` 里读到的值转成字符串名单。
func configStringList(value any, key string) ([]string, error) {
	out := []string{}
	switch v := value.(type) {
	case nil:
		return out, nil
	case []any:
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s 必须是字符串数组（发现 %T）", key, item)
			}
			out = append(out, s)
		}
	case []string:
		out = append(out, v...)
	default:
		return nil, fmt.Errorf("%s 必须是字符串数组（发现 %T）", key, value)
	}
	return out, nil
}

// normalizeFilterPatterns 去空白、丢弃空项、按首次出现去重，并在写入前挡掉
// grok 侧会静默忽略的模式：globset 编译失败时 resolve_model_catalog 的
// `if let Ok(Some(..))` 会跳过整份名单，用户会以为保存生效了却什么都没发生。
func normalizeFilterPatterns(key string, in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, raw := range in {
		pat := strings.TrimSpace(raw)
		if pat == "" {
			continue
		}
		if strings.ContainsAny(pat, "\n\r") {
			return nil, fmt.Errorf("%s 的条目不能含换行：%q", key, raw)
		}
		if err := checkGlobBalanced(pat); err != nil {
			return nil, fmt.Errorf("%s 的模式 %q %w", key, pat, err)
		}
		if seen[pat] {
			continue
		}
		seen[pat] = true
		out = append(out, pat)
	}
	return out, nil
}

// checkGlobBalanced 只拦「开了没关」的字符类 / 花括号（`grok[4.6`、`{a,b`）。
// 只比较未转义的开启数是否多于闭合数：`[]]` 这类合法写法（类内首个 ] 是字面
// 量）闭合数更多，不会被误拒。
func checkGlobBalanced(pat string) error {
	for _, pair := range [][2]byte{{'[', ']'}, {'{', '}'}} {
		opens, closes := 0, 0
		escaped := false
		for i := 0; i < len(pat); i++ {
			c := pat[i]
			if escaped {
				escaped = false
				continue
			}
			switch c {
			case '\\':
				escaped = true
			case pair[0]:
				opens++
			case pair[1]:
				closes++
			}
		}
		if opens > closes {
			return fmt.Errorf("缺少配对的 %q", string(pair[1]))
		}
	}
	return nil
}
