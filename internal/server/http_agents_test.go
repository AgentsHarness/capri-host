package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentsAndPersonasRoundTrip(t *testing.T) {
	grok := withFakeGrokHome(t)
	if err := os.MkdirAll(filepath.Join(grok, "agents"), 0o755); err != nil {
		t.Fatal(err)
	}
	agent := "---\nname: reviewer\ndescription: reviews diffs\n---\n# body\n"
	if err := os.WriteFile(filepath.Join(grok, "agents", "reviewer.md"), []byte(agent), 0o644); err != nil {
		t.Fatal(err)
	}
	proj := t.TempDir()
	if err := os.MkdirAll(filepath.Join(proj, ".grok", "agents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proj, ".grok", "agents", "local.md"), []byte("---\nname: local\ndescription: project\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := newLocalServer(t)
	rec := getJSON(t, s, "/api/agents?cwd="+proj)
	if rec.Code != http.StatusOK {
		t.Fatalf("list %d %s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	rows, _ := body["agents"].([]any)
	names := map[string]bool{}
	for _, row := range rows {
		m, _ := row.(map[string]any)
		names[m["name"].(string)] = true
		if m["name"] == "grok-build" && m["isDefault"] != true {
			t.Fatalf("grok-build should be the implicit default: %v", m)
		}
	}
	if !names["reviewer"] || !names["local"] || !names["explore"] {
		t.Fatalf("agents = %v", names)
	}
	for _, row := range rows {
		m := row.(map[string]any)
		if m["name"] != "reviewer" {
			continue
		}
		if m["model"] != "inherit" || m["promptMode"] != "extend" || m["toolsDeclared"] != false {
			t.Fatalf("reviewer defaults: %v", m)
		}
		if m["promptBody"] != "# body" {
			t.Fatalf("reviewer prompt: %v", m["promptBody"])
		}
	}

	rec = postJSON(t, s, "/api/agents/default", `{"name":"reviewer"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("default %d %s", rec.Code, rec.Body.String())
	}
	rec = postJSON(t, s, "/api/agents/toggle", `{"name":"explore","enabled":false}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("toggle %d %s", rec.Code, rec.Body.String())
	}
	rec = getJSON(t, s, "/api/agents")
	body = decodeBody(t, rec)
	for _, row := range body["agents"].([]any) {
		m := row.(map[string]any)
		if m["name"] == "reviewer" && m["isDefault"] != true {
			t.Fatalf("reviewer not default: %v", m)
		}
		if m["name"] == "explore" && m["enabled"] != false {
			t.Fatalf("explore still enabled: %v", m)
		}
	}

	rec = postJSON(t, s, "/api/personas", `{"name":"Careful","description":"slow","instructions":"think","scope":"user"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create %d %s", rec.Code, rec.Body.String())
	}
	created := decodeBody(t, rec)["path"].(string)
	rec = getJSON(t, s, "/api/personas")
	found := false
	for _, row := range decodeBody(t, rec)["personas"].([]any) {
		m := row.(map[string]any)
		if m["name"] == "Careful" && m["deletable"] == true {
			found = true
		}
	}
	if !found {
		t.Fatalf("persona missing: %s", rec.Body.String())
	}
	rec = postJSON(t, s, "/api/personas/delete", `{"path":"`+created+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete %d %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(created); !os.IsNotExist(err) {
		t.Fatalf("file still there: %v", err)
	}

	bundled := filepath.Join(grok, "bundled", "personas")
	if err := os.MkdirAll(bundled, 0o755); err != nil {
		t.Fatal(err)
	}
	bundledFile := filepath.Join(bundled, "stock.toml")
	if err := os.WriteFile(bundledFile, []byte("description = \"stock\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec = postJSON(t, s, "/api/personas/delete", `{"path":"`+bundledFile+`"}`)
	if rec.Code == http.StatusOK {
		t.Fatal("bundled persona must not delete")
	}
}

func TestAgentFrontmatterCarriesTUIDetail(t *testing.T) {
	text := "---\n" +
		"name: reviewer\n" +
		"description: reviews diffs\n" +
		"model: grok-4\n" +
		"promptMode: full\n" +
		"tools: read_file, Agent(a, b), grep\n" +
		"disallowedTools:\n  - web_search\n" +
		"skills:\n  - review\n" +
		"effort: high\n" +
		"isolation: worktree\n" +
		"---\n" +
		strings.Repeat("x", 130) + "\n"
	got := parseAgentText(text)
	if got.name != "reviewer" || got.desc != "reviews diffs" {
		t.Fatalf("identity: %+v", got)
	}
	if got.model != "grok-4" || got.promptMode != "full" {
		t.Fatalf("model/mode: %+v", got)
	}
	if !got.toolsDeclared || strings.Join(got.tools, ",") != "read_file,Agent(a, b),grep" {
		t.Fatalf("tools: %+v", got.tools)
	}
	if strings.Join(got.disallowed, ",") != "web_search" || strings.Join(got.skills, ",") != "review" {
		t.Fatalf("deny/skills: %+v", got)
	}
	if got.effort != "high" || got.isolation != "worktree" {
		t.Fatalf("effort/isolation: %+v", got)
	}
	if len([]rune(got.promptPreview)) != promptPreviewRunes || got.promptTruncated {
		t.Fatalf("preview: %q truncated=%v", got.promptPreview, got.promptTruncated)
	}
	if len(got.promptBody) != 130 {
		t.Fatalf("body len %d", len(got.promptBody))
	}
}

func TestPersonaFileCarriesDetail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reviewer.toml")
	raw := "" +
		"model = \"grok-4\"\n" +
		"reasoning_effort = \"high\"\n" +
		"default_isolation = \"worktree\"\n" +
		"instructions_file = \"PROMPT.md\"\n" +
		"instructions = \"\"\"\n# Title\n\nReads the diff carefully.\nSecond sentence.\n\"\"\"\n" +
		"[[inputs]]\nname = \"diff\"\nio_type = \"text\"\nrequired = true\ndescription = \"the diff\"\n" +
		"[[outputs]]\nname = \"notes\"\n"
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	row := readPersonaFile(path)
	if row.Description != "Reads the diff carefully. Second sentence." {
		t.Fatalf("description fallback: %q", row.Description)
	}
	if row.Model != "grok-4" || row.ReasoningEffort != "high" || row.DefaultIsolation != "worktree" {
		t.Fatalf("scalars: %+v", row)
	}
	if row.InstructionsFile != "PROMPT.md" || !row.HasInputs || !row.HasOutputs {
		t.Fatalf("file/flags: %+v", row)
	}
	if len(row.Inputs) != 1 || row.Inputs[0].Name != "diff" || row.Inputs[0].Type != "text" || !row.Inputs[0].Required {
		t.Fatalf("inputs: %+v", row.Inputs)
	}
	if len(row.Outputs) != 1 || row.Outputs[0].Name != "notes" || row.Outputs[0].Type != "file" {
		t.Fatalf("outputs: %+v", row.Outputs)
	}
}
