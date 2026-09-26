package acp

import "fmt"

// SetConfigAgentName writes or clears [agent].name in config.toml.
// An empty name removes the key, which is how the TUI clears the default.
// Other keys in [agent] are left alone.
func (b *Bridge) SetConfigAgentName(name string) error {
	if err := validateAgentKey(name); name != "" && err != nil {
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
	agent := configSection(table, "agent")
	if name == "" {
		delete(agent, "name")
	} else {
		agent["name"] = name
	}
	return writeConfigTable(path, table)
}

// SetSubagentToggle writes [subagents.toggle].<name>.
func (b *Bridge) SetSubagentToggle(name string, enabled bool) error {
	if err := validateAgentKey(name); err != nil {
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
	sub := configSection(table, "subagents")
	toggle, ok := sub["toggle"].(map[string]any)
	if !ok {
		toggle = map[string]any{}
		sub["toggle"] = toggle
	}
	toggle[name] = enabled
	return writeConfigTable(path, table)
}

func validateAgentKey(name string) error {
	if name == "" {
		return fmt.Errorf("需要名称")
	}
	if len(name) > 128 {
		return fmt.Errorf("名称过长")
	}
	for _, r := range name {
		if r == '/' || r == '\\' || r == '\n' || r == '\r' || r == 0 {
			return fmt.Errorf("名称含有不能写入配置的字符")
		}
	}
	return nil
}
