package server

import (
	"github.com/BurntSushi/toml"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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
