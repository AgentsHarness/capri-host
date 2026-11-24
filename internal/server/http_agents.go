package server

// Agents / personas modal. The TUI reads these files in-process; the
// browser cannot, so the host lists and edits the same directories:
// built-in names, ~/.grok/agents, project .grok/agents, bundled agents,
// and user/project personas. Plugin-provided agents are not scanned.

import (
	"net/http"
	"path/filepath"
	"strings"
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
