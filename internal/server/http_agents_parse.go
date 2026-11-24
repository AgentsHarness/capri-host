package server

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

func scopeRank(scope string) int {
	switch scope {
	case "project":
		return 3
	case "user":
		return 2
	case "bundled":
		return 1
	default:
		return 0
	}
}

func listAgents(grokDir, cwd string) ([]agentRow, string) {
	sections := parseConfigTOML(filepath.Join(grokDir, "config.toml"))
	configured := ""
	if agent, ok := sections["agent"]; ok {
		if name, ok := agent["name"].(string); ok {
			configured = strings.TrimSpace(name)
		}
	}
	toggle := map[string]bool{}
	if sub, ok := sections["subagents.toggle"]; ok {
		for k, v := range sub {
			if b, ok := v.(bool); ok {
				toggle[k] = b
			}
		}
	}
	enabled := func(name string) bool {
		if v, ok := toggle[name]; ok {
			return v
		}
		return true
	}

	rows := make([]agentRow, 0, len(userVisibleBuiltins))
	index := map[string]int{}
	for _, b := range userVisibleBuiltins {
		index[b.Name] = len(rows)
		preview, stored, truncated := clipText(b.PromptBody)
		rows = append(rows, agentRow{
			Name: b.Name, Description: b.Description, Scope: "builtin",
			Enabled: enabled(b.Name), Builtin: true,
			Model: "inherit", PromptMode: b.PromptMode,
			Tools: []string{}, DisallowedTools: []string{}, Skills: []string{},
			PromptPreview: preview, PromptBody: stored, PromptTruncated: truncated,
		})
	}

	var discovered []foundAgent
	for _, dir := range projectAgentDirs(cwd) {
		discovered = append(discovered, readAgentDir(dir, "project")...)
	}
	discovered = append(discovered, readAgentDir(filepath.Join(grokDir, "agents"), "user")...)
	home, _ := os.UserHomeDir()
	if home != "" {
		legacy := filepath.Join(home, ".grok")
		if legacy != grokDir {
			discovered = append(discovered, readAgentDir(filepath.Join(legacy, "agents"), "user")...)
		}
		discovered = append(discovered, readAgentDir(filepath.Join(home, ".claude", "agents"), "user")...)
	}
	discovered = append(discovered, readAgentDir(filepath.Join(grokDir, "bundled", "agents"), "bundled")...)

	for _, d := range discovered {
		if subagentBuiltinNames[d.name] && d.scope != "project" {
			continue
		}
		row := d.row(enabled(d.name))
		if i, ok := index[d.name]; ok {
			if scopeRank(d.scope) > scopeRank(rows[i].Scope) {
				rows[i] = row
			}
			continue
		}
		index[d.name] = len(rows)
		rows = append(rows, row)
	}
	def := configured
	if def == "" {
		def = "grok-build"
	}
	for i := range rows {
		rows[i].IsDefault = rows[i].Name == def
	}
	return rows, configured
}

func projectAgentDirs(cwd string) []string {
	cwd = filepath.Clean(cwd)
	if cwd == "" || cwd == "." || !filepath.IsAbs(cwd) {
		return nil
	}
	var dirs []string
	cur := cwd
	for i := 0; i < 32; i++ {
		dirs = append(dirs, filepath.Join(cur, ".grok", "agents"))
		if _, err := os.Stat(filepath.Join(cur, ".git")); err == nil {
			break
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return dirs
}

func readAgentDir(dir, scope string) []foundAgent {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []foundAgent
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		info := parseAgentFile(path)
		if info.name == "" {
			info.name = strings.TrimSuffix(e.Name(), ".md")
		}
		info.scope = scope
		info.path = path
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

type foundAgent struct {
	name, desc, scope, path              string
	model, promptMode, effort, isolation string
	tools, disallowed, skills            []string
	toolsDeclared                        bool
	promptPreview, promptBody            string
	promptTruncated                      bool
}

func (f foundAgent) row(enabled bool) agentRow {
	mode := f.promptMode
	if mode == "" {
		mode = "extend"
	}
	model := f.model
	if model == "" {
		model = "inherit"
	}
	return agentRow{
		Name: f.name, Description: f.desc, Scope: f.scope, Path: f.path,
		Enabled: enabled, Builtin: false,
		Model: model, PromptMode: mode,
		Tools: nonNil(f.tools), ToolsDeclared: f.toolsDeclared,
		DisallowedTools: nonNil(f.disallowed), Skills: nonNil(f.skills),
		Effort: f.effort, Isolation: f.isolation,
		PromptPreview: f.promptPreview, PromptBody: f.promptBody,
		PromptTruncated: f.promptTruncated,
	}
}

func parseAgentFile(path string) foundAgent {
	raw, err := os.ReadFile(path)
	if err != nil {
		return foundAgent{}
	}
	return parseAgentText(string(raw))
}

func parseAgentText(text string) foundAgent {
	yamlText, body, ok := splitFrontmatter(text)
	if !ok {
		return foundAgent{}
	}
	var front agentFront
	if err := yaml.Unmarshal([]byte(yamlText), &front); err != nil {
		name, desc := looseFrontScalars(yamlText)
		out := foundAgent{name: name, desc: desc, model: "inherit", promptMode: "extend"}
		out.applyBody(body)
		return out
	}
	mode := front.PromptMode
	if mode == "" {
		mode = front.PromptModeSnake
	}
	disallowed := front.DisallowedTools
	if !disallowed.set {
		disallowed = front.DisallowedSnake
	}
	out := foundAgent{
		name:          strings.TrimSpace(front.Name),
		desc:          strings.TrimSpace(front.Description),
		model:         normalizeModel(front.Model),
		promptMode:    normalizePromptMode(mode),
		tools:         front.Tools.list(),
		toolsDeclared: front.Tools.set,
		disallowed:    disallowed.list(),
		skills:        front.Skills.list(),
		effort:        scalarString(front.Effort),
		isolation:     scalarString(front.Isolation),
	}
	out.applyBody(body)
	return out
}

func (a *foundAgent) applyBody(body string) {
	preview, stored, truncated := clipText(body)
	a.promptPreview = preview
	a.promptBody = stored
	a.promptTruncated = truncated
}

type agentFront struct {
	Name            string      `yaml:"name"`
	Description     string      `yaml:"description"`
	Model           string      `yaml:"model"`
	PromptMode      string      `yaml:"promptMode"`
	PromptModeSnake string      `yaml:"prompt_mode"`
	Tools           yamlStrings `yaml:"tools"`
	DisallowedTools yamlStrings `yaml:"disallowedTools"`
	DisallowedSnake yamlStrings `yaml:"disallowed_tools"`
	Skills          yamlStrings `yaml:"skills"`
	Effort          any         `yaml:"effort"`
	Isolation       any         `yaml:"isolation"`
}

// yamlStrings accepts the TUI's string-or-list fields (`tools: read, grep`
// and `tools:\n  - read`). set distinguishes a missing key from an empty list.
type yamlStrings struct {
	set  bool
	vals []string
}

func (y *yamlStrings) UnmarshalYAML(value *yaml.Node) error {
	y.set = true
	switch value.Kind {
	case yaml.ScalarNode:
		y.vals = splitList(value.Value)
	case yaml.SequenceNode:
		var xs []string
		if err := value.Decode(&xs); err != nil {
			return err
		}
		y.vals = splitJoined(xs)
	default:
		y.vals = []string{}
	}
	return nil
}

func (y yamlStrings) list() []string {
	return nonNil(y.vals)
}

func splitFrontmatter(text string) (yamlText, body string, ok bool) {
	text = strings.TrimPrefix(text, "\uFEFF")
	text = strings.TrimLeft(text, "\r\n")
	if !strings.HasPrefix(text, "---") {
		return "", "", false
	}
	rest := text[3:]
	rest = strings.TrimPrefix(rest, "\r")
	if !strings.HasPrefix(rest, "\n") {
		return "", "", false
	}
	rest = rest[1:]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return "", "", false
	}
	yamlText = rest[:end]
	after := rest[end+4:]
	after = strings.TrimPrefix(after, "\r")
	if strings.HasPrefix(after, "\n") {
		after = after[1:]
	}
	return yamlText, strings.TrimSpace(after), true
}

func looseFrontScalars(yamlText string) (name, description string) {
	for _, line := range strings.Split(yamlText, "\n") {
		line = strings.TrimSpace(line)
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		switch strings.TrimSpace(key) {
		case "name":
			name = val
		case "description":
			description = val
		}
	}
	return name, description
}

func normalizeModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" || strings.EqualFold(model, "inherit") {
		return "inherit"
	}
	return model
}

func normalizePromptMode(mode string) string {
	if strings.EqualFold(strings.TrimSpace(mode), "full") {
		return "full"
	}
	return "extend"
}

// splitList splits a comma-separated tool list, keeping commas inside
// parentheses (`Agent(a, b), read_file`).
func splitList(s string) []string {
	var out []string
	var b strings.Builder
	depth := 0
	flush := func() {
		item := strings.TrimSpace(b.String())
		b.Reset()
		if item != "" {
			out = append(out, item)
		}
	}
	for _, r := range s {
		switch r {
		case '(':
			depth++
			b.WriteRune(r)
		case ')':
			if depth > 0 {
				depth--
			}
			b.WriteRune(r)
		case ',':
			if depth == 0 {
				flush()
				continue
			}
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	flush()
	return nonNil(out)
}

func splitJoined(xs []string) []string {
	var out []string
	for _, item := range xs {
		out = append(out, splitList(item)...)
	}
	return nonNil(out)
}

func scalarString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(t)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func nonNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

// clipText returns the 120-rune preview, the stored body, and whether the
// stored body was cut at agentTextCap. An empty input stays empty.
func clipText(body string) (preview, stored string, truncated bool) {
	body = strings.TrimSpace(body)
	if body == "" {
		return "", "", false
	}
	runes := []rune(body)
	if len(runes) > promptPreviewRunes {
		preview = string(runes[:promptPreviewRunes])
	} else {
		preview = body
	}
	stored = body
	if len(stored) > agentTextCap {
		stored = stored[:agentTextCap]
		for len(stored) > 0 && !utf8.ValidString(stored) {
			stored = stored[:len(stored)-1]
		}
		truncated = true
	}
	return preview, stored, truncated
}
