package acp

import "encoding/json"

// Event is pushed to browsers via SSE (and later Hub).
type Event map[string]any

// ContentBlock is an ACP prompt content block (subset we accept from HTTP).
type ContentBlock map[string]any

// SessionConfig for session/new.
type SessionConfig struct {
	Cwd                   string           `json:"cwd"`
	AdditionalDirectories []string         `json:"additionalDirectories"`
	MCPServers            []map[string]any `json:"mcpServers"`
}

// PermissionScope is the bash command scope a client attaches to a
// permission selection (the TUI's BashCommandSelectedTerms analog). The
// bridge serializes it into the ACP response `_meta` as the wire keys
// command_parts / is_glob.
type PermissionScope struct {
	CommandParts []string `json:"commandParts"`
	IsGlob       bool     `json:"isGlob"`
}

// Status is a snapshot for GET /api/status and SSE hello.
type Status struct {
	Ready         bool           `json:"ready"`
	Busy          bool           `json:"busy"`
	Booting       bool           `json:"booting"`
	SessionID     string         `json:"sessionId,omitempty"`
	Cwd           string         `json:"cwd,omitempty"`
	HostID        string         `json:"hostId"`
	HostName      string         `json:"hostName"`
	HomeDir       string         `json:"homeDir,omitempty"`
	AgentInfo     map[string]any `json:"agentInfo,omitempty"`
	Modes         any            `json:"modes,omitempty"`
	ConfigOptions any            `json:"configOptions,omitempty"`
	// SessionModelState from the latest session/new or session/load.
	Models          any          `json:"models,omitempty"`
	BootError       string       `json:"bootError,omitempty"`
	Text            string       `json:"text,omitempty"`
	PendingRequests []PendingReq `json:"pendingRequests,omitempty"`
	Capabilities    ClientCaps   `json:"capabilities"`
	// Live per-session states (dashboard active/idle/awaiting classification).
	Roster []SessionState `json:"roster,omitempty"`
}

// SessionState is the host-side live state of one session in the roster.
// The dashboard classification (TUI: Working/Active · Awaiting input ·
// Idle) is derived from Busy + AwaitingInput, which the host tracks from
// in-flight turns and pending client requests.
type SessionState struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd,omitempty"`
	Title     string `json:"title,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
	Busy      bool   `json:"busy"`
	// AwaitingInput: a permission / x.ai question is pending for this session.
	AwaitingInput bool  `json:"awaitingInput"`
	LastActiveAt  int64 `json:"lastActiveAt,omitempty"`
	CreatedAt     int64 `json:"createdAt,omitempty"`

	// Runtime session model state (from session/new | session/load), used
	// for the ready event caption; not serialized.
	modes      any
	configOpts any
	models     any

	// Latest context usage (session/update _meta.totalTokens / usage_update)
	// and git head (x.ai/git_head_changed) — served via /api/session-info;
	// not serialized.
	usageUsed   int64
	usageSize   int64
	gitBranch   string
	gitWorktree bool
	gitMainRepo string
}

// State returns the dashboard classification: "active" (turn in flight),
// "awaiting" (turn in flight + waiting on user input), else "idle".
func (s *SessionState) State() string {
	if s.Busy {
		if s.AwaitingInput {
			return "awaiting"
		}
		return "active"
	}
	return "idle"
}

// MarshalJSON — expose both the flat fields and the derived state so
// clients (and the /api/sessions enrichment) get `status.state` for free.
func (s SessionState) MarshalJSON() ([]byte, error) {
	type alias SessionState
	return json.Marshal(struct {
		alias
		State string `json:"state"`
	}{alias(s), s.State()})
}

// ClientCaps — intentionally no fs/terminal execution surface.
type ClientCaps struct {
	FS       FSCaps `json:"fs"`
	Terminal bool   `json:"terminal"`
}

type FSCaps struct {
	ReadTextFile  bool `json:"readTextFile"`
	WriteTextFile bool `json:"writeTextFile"`
}

// DefaultClientCaps: pure agent execution — Client does not run fs/terminal.
func DefaultClientCaps() ClientCaps {
	return ClientCaps{
		FS:       FSCaps{ReadTextFile: false, WriteTextFile: false},
		Terminal: false,
	}
}

type PendingReq struct {
	RequestID string         `json:"requestId"`
	Method    string         `json:"method"`
	Params    map[string]any `json:"params,omitempty"`
}

// SessionInfoDetail — POST /api/session-info response: authoritative live
// details of the active session (TUI /session-info analog). The host serves
// this on demand so clients don't have to reconstruct state from cached
// events.
type SessionInfoDetail struct {
	SessionID   string     `json:"sessionId"`
	Title       string     `json:"title,omitempty"`
	Cwd         string     `json:"cwd,omitempty"`
	UpdatedAt   string     `json:"updatedAt,omitempty"`
	State       string     `json:"state,omitempty"` // active / awaiting / idle
	Busy        bool       `json:"busy"`
	Model       *ModelInfo `json:"model,omitempty"`
	ContextUsed int64      `json:"contextUsed,omitempty"`
	ContextSize int64      `json:"contextSize,omitempty"`
	GitBranch   string     `json:"gitBranch,omitempty"`
	GitWorktree bool       `json:"gitIsWorktree,omitempty"`
	GitMainRepo string     `json:"gitMainRepo,omitempty"`
	HostID      string     `json:"hostId"`
	HostName    string     `json:"hostName"`
	HomeDir     string     `json:"homeDir,omitempty"`
}

// ModelInfo — the active session's model (id, display name, reasoning
// effort, context window) from its SessionModelState catalog.
type ModelInfo struct {
	ModelID         string `json:"modelId"`
	Name            string `json:"name,omitempty"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
	ContextWindow   int64  `json:"contextWindow,omitempty"`
}
