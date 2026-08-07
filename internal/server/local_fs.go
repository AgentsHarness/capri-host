package server

// Local ~/.grok inventory + config.toml reading. Everything here is
// read-only host-side disk access (never routed through the agent) with
// silent tolerance for missing/unreadable paths. No TOML/YAML dependency:
// both formats are parsed with small hand-rolled readers for the subset
// the host needs.

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// scanExtensions builds the local extension inventory:
// {hooks:[…], plugins:[…], skills:[…]}. Missing directories and
// unreadable files are skipped silently; the result never contains nil
// arrays (JSON null) — always at least an empty list.
func scanExtensions(home string) map[string]any {
	grokDir := filepath.Join(home, ".grok")
	return map[string]any{
		"hooks":   scanHooks(grokDir),
		"plugins": scanPlugins(grokDir),
		"skills":  scanSkills(grokDir),
	}
}

// scanHooks collects hooks from two sources:
//   - [hooks.<name>] sections of config.toml → {name, command?, enabled}
//   - ~/.grok/hooks/<name> directories (SKILL.md-style hook dirs) →
//     {name, source:"user-dir"}
//
// config.toml may have no hooks section and the hooks dir may not exist;
// both cases yield no entries.
func scanHooks(grokDir string) []map[string]any {
	out := make([]map[string]any, 0)
	sections := parseConfigTOML(filepath.Join(grokDir, "config.toml"))
	names := make([]string, 0, len(sections))
	for sec := range sections {
		const prefix = "hooks."
		if strings.HasPrefix(sec, prefix) && !strings.Contains(strings.TrimPrefix(sec, prefix), ".") {
			names = append(names, strings.TrimPrefix(sec, prefix))
		}
	}
	sort.Strings(names)
	for _, name := range names {
		kv := sections["hooks."+name]
		h := map[string]any{"name": name}
		if cmd, ok := kv["command"].(string); ok && cmd != "" {
			h["command"] = cmd
		}
		enabled := true // a configured hook is enabled unless disabled
		if e, ok := kv["enabled"].(bool); ok {
			enabled = e
		}
		h["enabled"] = enabled
		out = append(out, h)
	}
	for _, name := range listSubdirs(filepath.Join(grokDir, "hooks")) {
		out = append(out, map[string]any{"name": name, "source": "user-dir"})
	}
	return out
}

// scanPlugins lists ~/.grok/installed-plugins/* directories →
// {name, enabled:true, source:"installed"}. marketplace-cache is
// internal and intentionally not part of the inventory.
func scanPlugins(grokDir string) []map[string]any {
	out := make([]map[string]any, 0)
	for _, name := range listSubdirs(filepath.Join(grokDir, "installed-plugins")) {
		if name == "marketplace-cache" {
			continue // internal cache, not a user plugin
		}
		out = append(out, map[string]any{"name": name, "enabled": true, "source": "installed"})
	}
	return out
}

// scanSkills lists ~/.grok/skills/* (scope "user") and
// ~/.grok/bundled/skills/* (scope "bundled"). Each skill's name comes
// from the `name` field of its SKILL.md frontmatter when present, else
// the directory name.
func scanSkills(grokDir string) []map[string]any {
	out := make([]map[string]any, 0)
	for _, scope := range []struct{ dir, scope string }{
		{"skills", "user"},
		{"bundled/skills", "bundled"},
	} {
		base := filepath.Join(grokDir, scope.dir)
		for _, dir := range listSubdirs(base) {
			out = append(out, map[string]any{
				"name":  skillName(filepath.Join(base, dir)),
				"scope": scope.scope,
			})
		}
	}
	return out
}

// skillName returns the frontmatter `name` of dir/SKILL.md, falling back
// to the directory's base name when the file is missing or has no usable
// frontmatter name.
func skillName(dir string) string {
	if name, ok := frontmatterName(filepath.Join(dir, "SKILL.md")); ok && name != "" {
		return name
	}
	return filepath.Base(dir)
}

// frontmatterName extracts the `name` field from a `---`-delimited YAML
// frontmatter block at the top of a file. Hand-rolled on purpose: only
// the first `name:` line inside the block is considered; quotes are
// stripped; anything else is ignored.
func frontmatterName(path string) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	lines := strings.Split(string(data), "\n")
	i := 0
	for i < len(lines) && strings.TrimSpace(lines[i]) != "---" {
		i++
	}
	if i >= len(lines) {
		return "", false
	}
	i++
	for i < len(lines) {
		line := strings.TrimSpace(lines[i])
		if line == "---" {
			return "", false // block closed before any name
		}
		if strings.HasPrefix(line, "name:") {
			v := strings.TrimSpace(strings.TrimPrefix(line, "name:"))
			if v == "" {
				return "", false
			}
			v = strings.Trim(v, `"'`)
			return v, true
		}
		i++
	}
	return "", false
}

// listSubdirs returns the sorted base names of the direct subdirectories
// of dir (dot-directories excluded). A missing or unreadable dir yields
// an empty list.
func listSubdirs(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// parseConfigTOML is a hand-rolled reader for the subset of TOML that
// ~/.grok/config.toml uses: [section] / [section.sub] headers followed by
// `key = value` lines. Returns section → key → scalar value. Only scalar
// values (bool / number / string) are kept — arrays, tables and inline
// expressions are dropped. A missing/unreadable file yields an empty map.
func parseConfigTOML(path string) map[string]map[string]any {
	out := map[string]map[string]any{}
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	section := ""
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			if section != "" {
				if _, ok := out[section]; !ok {
					out[section] = map[string]any{}
				}
			}
			continue
		}
		eq := strings.Index(line, "=")
		if eq <= 0 || section == "" {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		if key == "" {
			continue
		}
		if v := parseTOMLValue(strings.TrimSpace(line[eq+1:])); v != nil {
			out[section][key] = v
		}
	}
	return out
}

// parseTOMLValue parses a scalar TOML value defensively: double- or
// single-quoted strings (quotes stripped, minimal escapes), true/false,
// integers and floats, and bare unquoted strings. Anything else
// (arrays, inline tables, dangling quotes) → nil (dropped).
func parseTOMLValue(s string) any {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	// Quoted string: scan to the closing quote (skipping \" escapes);
	// anything after it is an inline comment and ignored.
	if s[0] == '"' || s[0] == '\'' {
		inner, _, ok := scanQuotedString(s, s[0])
		if !ok {
			return nil
		}
		return inner
	}
	// Strip inline comments from bare values.
	if i := strings.Index(s, " #"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	// Arrays / inline tables are dropped (scalar subset only).
	if strings.HasPrefix(s, "[") || strings.HasPrefix(s, "{") {
		return nil
	}
	switch strings.ToLower(s) {
	case "true":
		return true
	case "false":
		return false
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// Bare unquoted string (TOML requires quotes for strings with spaces,
	// but be lenient — the agent's own parser is the authority).
	return s
}

// scanQuotedString reads a quoted value starting at s[0] == q and returns
// the unescaped inner content plus the remainder after the closing quote.
// Double quotes process \" \n \t \\ escapes; single quotes are literal.
func scanQuotedString(s string, q byte) (inner, rest string, ok bool) {
	var b strings.Builder
	i := 1
	for i < len(s) {
		c := s[i]
		if c == q {
			return b.String(), s[i+1:], true
		}
		if c == '\\' && q == '"' && i+1 < len(s) {
			switch n := s[i+1]; n {
			case '"':
				b.WriteByte('"')
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte('\\')
				b.WriteByte(n)
			}
			i += 2
			continue
		}
		b.WriteByte(c)
		i++
	}
	return "", "", false // dangling quote
}
