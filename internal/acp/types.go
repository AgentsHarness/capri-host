package acp

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

// Status is a snapshot for GET /api/status and SSE hello.
type Status struct {
	Ready           bool           `json:"ready"`
	Busy            bool           `json:"busy"`
	Booting         bool           `json:"booting"`
	SessionID       string         `json:"sessionId,omitempty"`
	Cwd             string         `json:"cwd,omitempty"`
	HostID          string         `json:"hostId"`
	HostName        string         `json:"hostName"`
	AgentInfo       map[string]any `json:"agentInfo,omitempty"`
	Modes           any            `json:"modes,omitempty"`
	ConfigOptions   any            `json:"configOptions,omitempty"`
	BootError       string         `json:"bootError,omitempty"`
	Text            string         `json:"text,omitempty"`
	PendingRequests []PendingReq   `json:"pendingRequests,omitempty"`
	Capabilities    ClientCaps     `json:"capabilities"`
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
