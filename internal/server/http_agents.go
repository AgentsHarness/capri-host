package server

// Agents / personas modal. The TUI reads these files in-process; the
// browser cannot, so the host lists and edits the same directories:
// built-in names, ~/.grok/agents, project .grok/agents, bundled agents,
// and user/project personas. Plugin-provided agents are not scanned.

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"
)

// Text stored on a list row. Longer files stay on disk; the row says it was cut.
const agentTextCap = 256 << 10

// TUI format_agent_detail shows this many characters of the prompt, then "...".
const promptPreviewRunes = 120

// userVisibleBuiltins matches the TUI agents modal (kebab-case names).
// Compiled built-ins keep their tool list inside the agent binary. browser-use
// is the one whose prompt is a short fixed string the TUI also shows.
var userVisibleBuiltins = []builtinAgent{
	{Name: "grok-build", Description: "Default coding agent", PromptMode: "extend"},
	{Name: "general-purpose", Description: "General-purpose subagent", PromptMode: "extend"},
	{Name: "explore", Description: "Read-only codebase exploration", PromptMode: "extend"},
	{Name: "plan", Description: "Planning subagent", PromptMode: "extend"},
	{
		Name: "browser-use", Description: "Browser subagent", PromptMode: "full",
		PromptBody: "You are a web browsing agent. You can navigate, interact with, and " +
			"extract information from web pages. Use the available browsing tools " +
			"to complete the user's request.",
	},
}

type builtinAgent struct {
	Name, Description, PromptMode, PromptBody string
}

var subagentBuiltinNames = map[string]bool{
	"general-purpose": true,
	"explore":         true,
	"plan":            true,
}

// agentRow is what the TUI agents tab shows: the list line, plus the fields
// format_agent_detail prints when the row is expanded. Tools is the authored
// allowlist. ToolsDeclared is false when the file omits `tools`, which means
// the agent inherits the default toolset (the TUI prints that compiled list;
// it is not in these files). Plugin-provided agents are still not scanned.
type agentRow struct {
	Name            string   `json:"name"`
	Description     string   `json:"description"`
	Scope           string   `json:"scope"`
	Path            string   `json:"path,omitempty"`
	Enabled         bool     `json:"enabled"`
	Builtin         bool     `json:"builtin"`
	IsDefault       bool     `json:"isDefault"`
	Model           string   `json:"model"`
	PromptMode      string   `json:"promptMode"`
	Tools           []string `json:"tools"`
	ToolsDeclared   bool     `json:"toolsDeclared"`
	DisallowedTools []string `json:"disallowedTools"`
	Skills          []string `json:"skills"`
	Effort          string   `json:"effort,omitempty"`
	Isolation       string   `json:"isolation,omitempty"`
	PromptPreview   string   `json:"promptPreview,omitempty"`
	PromptBody      string   `json:"promptBody,omitempty"`
	PromptTruncated bool     `json:"promptTruncated,omitempty"`
}

// personaRow is the personas tab plus the persona detail screen: model,
// effort, isolation, instructions file, and the inputs/outputs lists.
type personaRow struct {
	Name                  string      `json:"name"`
	Description           string      `json:"description,omitempty"`
	Instructions          string      `json:"instructions,omitempty"`
	InstructionsFile      string      `json:"instructionsFile,omitempty"`
	Scope                 string      `json:"scope"`
	Path                  string      `json:"path,omitempty"`
	Deletable             bool        `json:"deletable"`
	Model                 string      `json:"model,omitempty"`
	ReasoningEffort       string      `json:"reasoningEffort,omitempty"`
	DefaultIsolation      string      `json:"defaultIsolation,omitempty"`
	Inputs                []personaIO `json:"inputs"`
	Outputs               []personaIO `json:"outputs"`
	HasInputs             bool        `json:"hasInputs"`
	HasOutputs            bool        `json:"hasOutputs"`
	InstructionsTruncated bool        `json:"instructionsTruncated,omitempty"`
}

type personaIO struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description,omitempty"`
}

func (s *Server) handleAgentsList(w http.ResponseWriter, r *http.Request) {
	dir, err := s.grokDir()
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	rows, configured := listAgents(dir, r.URL.Query().Get("cwd"))
	writeJSON(w, 200, map[string]any{"ok": true, "agents": rows, "configuredDefault": configured})
}

func (s *Server) handleAgentsDefault(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if err := s.bridge.SetConfigAgentName(strings.TrimSpace(body.Name)); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleAgentsToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Enabled == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 enabled"})
		return
	}
	if err := s.bridge.SetSubagentToggle(strings.TrimSpace(body.Name), *body.Enabled); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handlePersonasList(w http.ResponseWriter, r *http.Request) {
	dir, err := s.grokDir()
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "personas": listPersonas(dir, r.URL.Query().Get("cwd"))})
}

func (s *Server) handlePersonasCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name         string `json:"name"`
		Description  string `json:"description"`
		Instructions string `json:"instructions"`
		Scope        string `json:"scope"`
		Cwd          string `json:"cwd"`
	}
	if !readBody(w, r, &body) {
		return
	}
	dir, err := s.grokDir()
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	path, err := createPersona(dir, body.Cwd, body.Name, body.Description, body.Instructions, body.Scope)
	if err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": path})
}

func (s *Server) handlePersonasDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path string `json:"path"`
	}
	if !readBody(w, r, &body) {
		return
	}
	dir, err := s.grokDir()
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if err := deletePersona(dir, body.Path); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) registerAgentRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/agents", s.handleAgentsList)
	mux.HandleFunc("POST /api/agents/default", s.handleAgentsDefault)
	mux.HandleFunc("POST /api/agents/toggle", s.handleAgentsToggle)
	mux.HandleFunc("GET /api/personas", s.handlePersonasList)
	mux.HandleFunc("POST /api/personas", s.handlePersonasCreate)
	mux.HandleFunc("POST /api/personas/delete", s.handlePersonasDelete)
}

func (s *Server) grokDir() (string, error) {
	path, err := s.bridge.ConfigTOMLPath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(path), nil
}

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

func listPersonas(grokDir, cwd string) []personaRow {
	var rows []personaRow
	seen := map[string]bool{}
	addDir := func(dir, scope string, deletable bool) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".toml") {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, name := range names {
			stem := strings.TrimSuffix(name, ".toml")
			if seen[stem] {
				continue
			}
			seen[stem] = true
			path := filepath.Join(dir, name)
			row := readPersonaFile(path)
			row.Name = stem
			row.Scope = scope
			row.Path = path
			row.Deletable = deletable
			rows = append(rows, row)
		}
	}
	// Project wins over user over bundled when names collide: add in that order.
	if cwd != "" && filepath.IsAbs(cwd) {
		addDir(filepath.Join(filepath.Clean(cwd), ".grok", "personas"), "project", true)
	}
	addDir(filepath.Join(grokDir, "personas"), "user", true)
	addDir(filepath.Join(grokDir, "bundled", "personas"), "bundled", false)
	if rows == nil {
		rows = []personaRow{}
	}
	return rows
}

func readPersonaFile(path string) personaRow {
	raw, err := os.ReadFile(path)
	if err != nil {
		return emptyPersona()
	}
	var doc struct {
		Description      string         `toml:"description"`
		Instructions     string         `toml:"instructions"`
		InstructionsFile string         `toml:"instructions_file"`
		Model            string         `toml:"model"`
		ReasoningEffort  string         `toml:"reasoning_effort"`
		DefaultIsolation string         `toml:"default_isolation"`
		Inputs           []personaIODoc `toml:"inputs"`
		Outputs          []personaIODoc `toml:"outputs"`
	}
	if err := toml.Unmarshal(raw, &doc); err != nil {
		return emptyPersona()
	}
	desc := strings.TrimSpace(doc.Description)
	instr := doc.Instructions
	if desc == "" {
		desc = firstParagraph(instr)
	}
	_, stored, truncated := clipText(instr)
	row := emptyPersona()
	row.Description = desc
	row.Instructions = stored
	row.InstructionsTruncated = truncated
	row.InstructionsFile = strings.TrimSpace(doc.InstructionsFile)
	row.Model = strings.TrimSpace(doc.Model)
	row.ReasoningEffort = strings.TrimSpace(doc.ReasoningEffort)
	row.DefaultIsolation = strings.TrimSpace(doc.DefaultIsolation)
	row.Inputs = personaIOs(doc.Inputs)
	row.Outputs = personaIOs(doc.Outputs)
	row.HasInputs = len(row.Inputs) > 0
	row.HasOutputs = len(row.Outputs) > 0
	return row
}

type personaIODoc struct {
	Name        string `toml:"name"`
	IOType      string `toml:"io_type"`
	Required    bool   `toml:"required"`
	Description string `toml:"description"`
}

func emptyPersona() personaRow {
	return personaRow{Inputs: []personaIO{}, Outputs: []personaIO{}}
}

func personaIOs(items []personaIODoc) []personaIO {
	out := make([]personaIO, 0, len(items))
	for _, item := range items {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			name = "?"
		}
		kind := strings.TrimSpace(item.IOType)
		if kind == "" {
			kind = "file"
		}
		out = append(out, personaIO{
			Name: name, Type: kind, Required: item.Required,
			Description: strings.TrimSpace(item.Description),
		})
	}
	return out
}

// firstParagraph is the TUI fallback when a persona has instructions but no
// description: the first prose block, skipping a leading heading.
func firstParagraph(s string) string {
	var b strings.Builder
	started := false
	for _, line := range strings.Split(s, "\n") {
		t := strings.TrimSpace(line)
		if !started {
			if t == "" || strings.HasPrefix(t, "#") || strings.HasPrefix(t, "```") {
				continue
			}
			if strings.HasPrefix(t, "- ") || strings.HasPrefix(t, "* ") {
				continue
			}
			started = true
			b.WriteString(t)
			continue
		}
		if t == "" {
			break
		}
		b.WriteByte(' ')
		b.WriteString(t)
	}
	return strings.TrimSpace(b.String())
}

func sanitizePersonaName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errNeedName
	}
	var b strings.Builder
	alnum := false
	for _, r := range name {
		if r == '-' || r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			b.WriteRune(r)
			if r != '-' && r != '_' {
				alnum = true
			}
			continue
		}
		// Letters outside ASCII still count as a name character, stored as '-'.
		if r > 127 {
			b.WriteByte('-')
			alnum = true
			continue
		}
		b.WriteByte('-')
	}
	if !alnum {
		return "", errNeedName
	}
	return b.String(), nil
}

var errNeedName = errString("名称至少要有一个字母或数字")

type errString string

func (e errString) Error() string { return string(e) }

func personaDir(grokDir, cwd, scope string) (string, error) {
	switch scope {
	case "user", "":
		return filepath.Join(grokDir, "personas"), nil
	case "project":
		cwd = filepath.Clean(cwd)
		if !filepath.IsAbs(cwd) {
			return "", errString("项目人格需要当前会话的工作目录")
		}
		return filepath.Join(cwd, ".grok", "personas"), nil
	default:
		return "", errString("scope 只能是 user 或 project")
	}
}

func createPersona(grokDir, cwd, name, description, instructions, scope string) (string, error) {
	sanitized, err := sanitizePersonaName(name)
	if err != nil {
		return "", err
	}
	dir, err := personaDir(grokDir, cwd, scope)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, sanitized+".toml")
	if _, err := os.Stat(path); err == nil {
		return "", errString("人格已存在: " + sanitized)
	}
	doc := struct {
		Description  string `toml:"description,omitempty"`
		Instructions string `toml:"instructions,omitempty"`
	}{Description: strings.TrimSpace(description), Instructions: strings.TrimSpace(instructions)}
	raw, err := toml.Marshal(doc)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func deletePersona(grokDir, path string) error {
	clean, err := filepath.Abs(path)
	if err != nil {
		return errString("路径无效")
	}
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return errString("找不到这个人格文件")
	}
	if strings.Contains(real, string(filepath.Separator)+"bundled"+string(filepath.Separator)) {
		return errString("不能删除内置人格")
	}
	userDir, _ := filepath.EvalSymlinks(filepath.Join(grokDir, "personas"))
	inUser := userDir != "" && (real == userDir || strings.HasPrefix(real, userDir+string(filepath.Separator)))
	inProject := strings.Contains(real, string(filepath.Separator)+".grok"+string(filepath.Separator)+"personas"+string(filepath.Separator))
	if !inUser && !inProject {
		return errString("只能删除用户或项目目录里的人格")
	}
	if filepath.Ext(real) != ".toml" {
		return errString("不是人格文件")
	}
	return os.Remove(real)
}
