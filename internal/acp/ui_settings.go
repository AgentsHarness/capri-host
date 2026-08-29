package acp

import (
	"fmt"
	"math"
)

// FE-consumed [ui] scalars that POST /api/settings may write. Everything
// else (theme, notifications table, unknown keys) is rejected so a
// compromised or stale client cannot widen the config.toml surface.
var writableUIKeys = map[string]func(any) (any, error){
	"collapsed_edit_blocks":   requireBool,
	"page_flip_on_send":       requireBool,
	"remember_tool_approvals": requireBool,
	"permission_mode":         requirePermissionMode,
	"follow_up_behavior":      requireFollowUpBehavior,
}

// FE-consumed [toolset.ask_user_question] scalars that POST /api/settings
// may write — mirror of the TUI's `[toolset.ask_user_question] timeout_*`
// pair (ask tool's AskUserQuestionParams). Only these keys of the whole
// [toolset] subtree are exposed; bash/web_search and other tool tables
// stay opaque so the FE cannot widen the config surface.
var writableToolsetKeys = map[string]func(any) (any, error){
	"timeout_enabled": requireBool,
	"timeout_secs":    requireTimeoutSecs,
}

// requireTimeoutSecs validates [toolset.ask_user_question].timeout_secs:
// a positive integer in [1, 86400]. The agent itself accepts any positive
// integer (config layers drop 0/negative with a warning) — this host-side
// ceiling is a conservative guard so a typo cannot arm a multi-year timer.
func requireTimeoutSecs(v any) (any, error) {
	var n int64
	switch x := v.(type) {
	case int64:
		n = x
	case float64:
		if x != math.Trunc(x) || x < math.MinInt64 || x > math.MaxInt64 {
			return nil, fmt.Errorf("必须是 1–86400 的整数（秒）")
		}
		n = int64(x)
	case int:
		n = int64(x)
	default:
		return nil, fmt.Errorf("必须是 1–86400 的整数（秒）")
	}
	if n < 1 || n > 86400 {
		return nil, fmt.Errorf("必须是 1–86400 的整数（秒）")
	}
	return n, nil
}

func requireBool(v any) (any, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, fmt.Errorf("必须是布尔")
	}
	return b, nil
}

func requirePermissionMode(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("必须是字符串")
	}
	switch s {
	case "ask", "auto", "always-approve":
		return s, nil
	default:
		return nil, fmt.Errorf("必须是 ask / auto / always-approve")
	}
}

// requireFollowUpBehavior validates [ui].follow_up_behavior — canonical
// values "queue" (default) / "steer" (mid-turn interjection), matching the
// TUI FollowUpBehavior enum.
func requireFollowUpBehavior(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("必须是字符串")
	}
	switch s {
	case "queue", "steer":
		return s, nil
	default:
		return nil, fmt.Errorf("必须是 queue / steer")
	}
}

// SetUiSettings patches the listed [ui] keys into config.toml and leaves
// every other section / key untouched. Same atomic rewrite as
// SetDefaultModelConfig. Unknown or ill-typed keys are rejected before
// any write. An empty patch is an error.
func (b *Bridge) SetUiSettings(patch map[string]any) error {
	if len(patch) == 0 {
		return fmt.Errorf("需要至少一个设置项")
	}
	normalized := make(map[string]any, len(patch))
	for k, v := range patch {
		check, ok := writableUIKeys[k]
		if !ok {
			return fmt.Errorf("不允许的设置项 %q", k)
		}
		nv, err := check(v)
		if err != nil {
			return fmt.Errorf("%s: %w", k, err)
		}
		normalized[k] = nv
	}
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return err
	}
	ui := configSection(table, "ui")
	for k, v := range normalized {
		ui[k] = v
	}
	return writeConfigTable(path, table)
}

// SetToolsetSettings patches the listed [toolset.ask_user_question] keys
// into config.toml. Only the two whitelisted timeout keys are writable;
// the rest of [toolset] survives untouched (and unreadable via GET).
func (b *Bridge) SetToolsetSettings(patch map[string]any) error {
	if len(patch) == 0 {
		return fmt.Errorf("需要至少一个设置项")
	}
	normalized := make(map[string]any, len(patch))
	for k, v := range patch {
		check, ok := writableToolsetKeys[k]
		if !ok {
			return fmt.Errorf("不允许的设置项 %q", k)
		}
		nv, err := check(v)
		if err != nil {
			return fmt.Errorf("%s: %w", k, err)
		}
		normalized[k] = nv
	}
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return err
	}
	table, err := readConfigTable(path)
	if err != nil {
		return err
	}
	ask := configSection(configSection(table, "toolset"), "ask_user_question")
	for k, v := range normalized {
		ask[k] = v
	}
	return writeConfigTable(path, table)
}
