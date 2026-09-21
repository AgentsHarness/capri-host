// Package hubstate holds the vocabulary shared between the host's hub layer
// and the processes that drive it from outside: the link snapshot, the pairing
// sentinel, and the shape of the credential the host persists.
//
// It exists as its own leaf package because internal/hub's tests assemble the
// real server chain (server.New(...).Handler()) for the in-process relay, and
// the server's HubController needs State to type-check. With server importing
// this package instead of hub, the test graph stays acyclic; hub re-exports
// both names as aliases, so every existing caller is unaffected.
package hubstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// ErrBadPairCode is returned by Pair when the code cannot be valid at all, so
// no request is sent to the hub.
var ErrBadPairCode = errors.New("配对码格式不正确")

// StoredToken is the credential the host persists after pairing, on disk as
// ~/.capri-host/hub.json.
//
// Three programs read this file: the host, to reconnect after a restart without
// anyone typing a code; and both GUI supervisors (Capri.app, Capri.exe), to
// tell "this machine is already paired with that hub" from "you need a fresh
// code". The shape therefore lives here rather than being re-declared at each
// reader, where the three would drift apart one silent field at a time.
type StoredToken struct {
	URL    string `json:"url"`
	HostID string `json:"hostId"`
	Token  string `json:"token"`
}

// Usable reports whether this credential can be reused for hubURL: it must
// carry a token and be bound to that exact address.
//
// The address comparison mirrors the host's own check when it starts up (see
// Client.ensureToken) — a token minted for one hub is not a credential for
// another, and reusing it there would produce an authentication failure the
// user cannot act on.
func (t *StoredToken) Usable(hubURL string) bool {
	if t == nil {
		return false
	}
	bound := strings.TrimSpace(t.URL)
	want := strings.TrimSpace(hubURL)
	return bound != "" && want != "" && bound == want && strings.TrimSpace(t.Token) != ""
}

// URLOrEmpty is the address this credential is bound to, empty when there is
// no credential at all. It exists so callers holding a possibly-nil token —
// which is the normal state of a machine that has never paired — can read the
// field without a nil check at every site.
func (t *StoredToken) URLOrEmpty() string {
	if t == nil {
		return ""
	}
	return strings.TrimSpace(t.URL)
}

// ReadToken reads a persisted credential from path.
//
// A missing or unparseable file is nil, not an error: hub.json is a cache of
// something the user can always reproduce by pairing again, so a corrupt one
// means "needs a code", never "cannot start".
func ReadToken(path string) *StoredToken {
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var t StoredToken
	if json.Unmarshal(b, &t) != nil {
		return nil
	}
	return &t
}

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

	// StoredURL is the hub URL the on-disk credential in hub.json is bound to,
	// empty when there is no credential on disk.
	StoredURL string `json:"storedUrl,omitempty"`
	// CanReuse reports whether the on-disk credential is usable for HubURL
	// (or for StoredURL when no hub is currently configured).
	CanReuse bool `json:"canReuse,omitempty"`
}

// NormalizeURL cleans an address a person typed. An empty input stays empty.
// A bare host gets https. Path, query and fragment are dropped because the
// client appends its own paths.
func NormalizeURL(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", nil
	}
	if !strings.Contains(s, "://") {
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("hub 地址无法解析: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("hub 地址只支持 http/https，收到 %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return "", errors.New("hub 地址缺少主机名")
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String(), nil
}
