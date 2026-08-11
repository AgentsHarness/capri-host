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
	// Meta carries client-supplied session seeds (e.g. the TUI's
	// yoloMode/autoMode permission flags) forwarded verbatim as the
	// session/new params `_meta`. Absent = current behavior exactly
	// (no `_meta` key on the wire).
	Meta map[string]any `json:"meta,omitempty"`
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
	Ready     bool           `json:"ready"`
	Busy      bool           `json:"busy"`
	Booting   bool           `json:"booting"`
	SessionID string         `json:"sessionId,omitempty"`
	Cwd       string         `json:"cwd,omitempty"`
	HostID   string `json:"hostId"`
	HostName string `json:"hostName"`
	// Mode 标明部署模式："local"（未配置 HUB_URL，浏览器直连本进程）或
	// "hub"（配置了 HUB_URL，本进程作为 hub 客户端中继，内嵌前端应跨源
	// 直连 hub）。由 server 层在 handleStatus 填充，bridge 自身不感知。
	Mode string `json:"mode,omitempty"`
	// HubURL 是 hub 模式的 hub 浏览器侧入口（前端跨源直连 /ws/fe 与
	// /api/* 用）。仅 Mode=="hub" 时非空。
	HubURL    string         `json:"hubUrl,omitempty"`
	HomeDir   string         `json:"homeDir,omitempty"`
	AgentInfo map[string]any `json:"agentInfo,omitempty"`
	// AgentCapabilities from the agent's initialize response (what the
	// agent declared, e.g. x.ai extension support).
	AgentCapabilities any `json:"agentCapabilities,omitempty"`
	// AuthMeta: the `_meta` from the authenticate response (AuthMeta:
	// email/auth_mode/team_id/team_name/is_zdr/team_role/
	// coding_data_retention_opt_out/show_resolved_model/gate/
	// subscription_tier), passthrough to clients. Nil when the agent
	// returned no `_meta` (absent key ≠ off).
	AuthMeta      any `json:"authMeta,omitempty"`
	Modes         any `json:"modes,omitempty"`
	ConfigOptions any `json:"configOptions,omitempty"`
	// SessionMeta: the `_meta` from the latest session/new | session/load |
	// session/resume response (agent → host), passthrough to clients. Nil
	// when the agent returned no `_meta`.
	SessionMeta any `json:"sessionMeta,omitempty"`
	// SessionModelState from the latest session/new or session/load.
	Models          any          `json:"models,omitempty"`
	BootError       string       `json:"bootError,omitempty"`
	Text            string       `json:"text,omitempty"`
	PendingRequests []PendingReq `json:"pendingRequests,omitempty"`
	Capabilities    ClientCaps   `json:"capabilities"`
	// Live per-session states (dashboard active/idle/awaiting classification).
	Roster []SessionState `json:"roster,omitempty"`
	// AgentStartedAt (unix ms) stamps the current agent process spawn —
	// clients detect agent restarts by comparing it across hello events.
	AgentStartedAt int64 `json:"agentStartedAt,omitempty"`
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

	// sessionMeta: the `_meta` from the latest session/new | session/load |
	// session/resume response (agent → host), surfaced on the ready event
	// and in Status.SessionMeta; not serialized on roster rows.
	sessionMeta any

	// Latest context usage (session/update _meta.totalTokens / usage_update)
	// and git head (x.ai/git_head_changed) — served via /api/session-info;
	// not serialized.
	usageUsed   int64
	usageSize   int64
	gitBranch   string
	gitWorktree bool
	gitMainRepo string

	// busyCount: session/prompt turns currently in flight for this session.
	// Busy is the boolean projection (busyCount > 0). Concurrent prompts
	// are forwarded — the agent (xai-grok-shell) queues mid-turn turns in
	// its own pending_inputs — so several turns can be in flight at once;
	// only the last resolver flips the session idle. Not serialized.
	busyCount int
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
	// SessionID: owning session (multi-session clients filter pending on
	// switch/resume; absent on very old hosts).
	SessionID string `json:"sessionId,omitempty"`
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
