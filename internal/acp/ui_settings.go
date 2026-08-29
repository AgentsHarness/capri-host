package acp

import "fmt"

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
