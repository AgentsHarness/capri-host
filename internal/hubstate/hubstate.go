// Package hubstate holds the hub link's snapshot type and pairing sentinel,
// shared by internal/hub and internal/server.
//
// It exists as its own leaf package because internal/hub's tests assemble the
// real server chain (server.New(...).Handler()) for the in-process relay, and
// the server's HubController needs State to type-check. With server importing
// this package instead of hub, the test graph stays acyclic; hub re-exports
// both names as aliases, so every existing caller is unaffected.
package hubstate

import "errors"

// ErrBadPairCode is returned by Pair when the code cannot be valid at all, so
// no request is sent to the hub.
var ErrBadPairCode = errors.New("配对码格式不正确")

// State is a point-in-time snapshot of the hub link. Safe from any goroutine
// at any time, including before Run has started.
type State struct {
	// Configured is always true for a live Client (one only exists when
	// HUB_URL is set). It is part of the snapshot so an absent client can
	// be reported with the same shape.
	Configured bool   `json:"configured"`
	HubURL     string `json:"hubUrl,omitempty"`
	HostID     string `json:"hostId,omitempty"`
	HostName   string `json:"hostName,omitempty"`
	// Paired means a hub token is held (from a pairing, HOST_TOKEN, or the
	// persisted state file). It says nothing about reachability.
	Paired bool `json:"paired"`
	// Connected means a session is live right now.
	Connected bool `json:"connected"`
	// Transport is "quic" or "ws" while connected, empty otherwise.
	Transport string `json:"transport,omitempty"`
	// ConnectedSince is RFC3339 and only set while connected.
	ConnectedSince string `json:"connectedSince,omitempty"`
	UptimeSec      int64  `json:"uptimeSec,omitempty"`
	// LastError is the most recent session failure, kept after the session
	// ends so a disconnected host can explain itself.
	LastError string `json:"lastError,omitempty"`
}
