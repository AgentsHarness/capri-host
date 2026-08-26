package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// This file is the client's control surface: everything a tray menu, an HTTP
// endpoint or a diagnostic needs in order to answer "am I paired, am I
// connected, and how do I re-pair without restarting the process".
//
// Before this existed the only exported symbols were NewClient and Run, and
// Run's caller dropped the reference — so the pairing code could only ever be
// supplied once, through HUB_PAIR_CODE at startup, and nothing could observe
// whether the link was actually up.

// Transport names reported in State.Transport.
const (
	TransportQUIC = "quic"
	TransportWS   = "ws"
)

// pairCodeLen and pairCodeAlphabet mirror the hub's generator (capri-hub's
// internal/hub/hub.go): 6 characters from an alphabet with the look-alikes
// I, L, O, 0 and 1 removed.
//
// Validating locally is not redundant with the hub's own check — the hub rate
// limits POST /api/pair per IP precisely because a 6-character code is only
// 31^6 wide, and spending that budget on a code that cannot possibly be valid
// would make a genuine retry wait behind a typo.
const (
	pairCodeLen      = 6
	pairCodeAlphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
)

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

// State returns a snapshot of the hub link.
func (c *Client) State() State {
	st := State{
		Configured: true,
		HubURL:     c.cfg.URL,
		HostID:     c.cfg.HostID,
		HostName:   c.cfg.HostName,
		Connected:  c.connected.Load(),
	}

	c.stateMu.Lock()
	st.Paired = c.token != "" || c.cfg.Token != ""
	c.stateMu.Unlock()

	// A token on disk counts as paired even before Run's first ensureToken
	// has copied it into memory — otherwise a tray opened during boot
	// reports "not paired" on a host that is merely still connecting.
	if !st.Paired {
		c.stateMu.Lock()
		if s := c.loadStateLocked(); s != nil && s.URL == c.cfg.URL && s.Token != "" {
			st.Paired = true
		}
		c.stateMu.Unlock()
	}

	if st.Connected {
		if tr, ok := c.transport.Load().(string); ok {
			st.Transport = tr
		}
		if ns := c.connectedAt.Load(); ns > 0 {
			since := time.Unix(0, ns)
			st.ConnectedSince = since.Format(time.RFC3339)
			st.UptimeSec = int64(time.Since(since).Seconds())
		}
	}

	c.ctlMu.Lock()
	st.LastError = c.lastErr
	c.ctlMu.Unlock()

	return st
}

// Pair exchanges a pairing code for a hub token, persists it, and makes the
// running client adopt it — no process restart.
//
// The existing token is replaced only after the hub accepts the new code, so a
// mistyped code leaves a working pairing intact. On success any live session is
// torn down and the reconnect loop comes straight back up on the new
// credential.
func (c *Client) Pair(ctx context.Context, code string) error {
	code = NormalizePairCode(code)
	if err := ValidatePairCode(code); err != nil {
		return err
	}

	token, err := c.pair(ctx, code)
	if err != nil {
		return err
	}

	c.stateMu.Lock()
	c.token = token
	c.saveStateLocked(stateFile{URL: c.cfg.URL, HostID: c.cfg.HostID, Token: token})
	path := c.stateFileLocked()
	c.stateMu.Unlock()

	c.setLastErr(nil)
	log.Printf("[hub-client] 配对成功，token 已保存到 %s", path)
	c.requestRepair()
	return nil
}

// NormalizePairCode uppercases and strips the separators a person is likely to
// type or paste ("abc-123", "ABC 123"). It does not attempt to correct
// look-alike characters: the hub's alphabet already excludes I/L/O/0/1, so a
// code containing one is wrong rather than ambiguous, and silently rewriting it
// would turn a clear error into a confusing rejection from the hub.
func NormalizePairCode(s string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(strings.TrimSpace(s)) {
		if r == '-' || r == '_' || r == ' ' || r == '\t' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ValidatePairCode reports whether s could be a hub pairing code. Pass it the
// output of NormalizePairCode.
func ValidatePairCode(s string) error {
	if len(s) != pairCodeLen {
		return fmt.Errorf("%w：应为 %d 位字符，收到 %d 位", ErrBadPairCode, pairCodeLen, len(s))
	}
	for _, r := range s {
		if !strings.ContainsRune(pairCodeAlphabet, r) {
			return fmt.Errorf("%w：不含字符 %q（配对码不使用 I、L、O、0、1）", ErrBadPairCode, r)
		}
	}
	return nil
}

// ── internal plumbing ─────────────────────────────────────────────────

// requestRepair asks Run to rebuild the session on the current token as soon
// as possible. It both cancels the live session (so a connected client does
// not keep using the old credential) and pokes repairCh (so a client waiting
// in a pairing retry or reconnect backoff wakes immediately instead of
// sleeping out its timer).
func (c *Client) requestRepair() {
	c.forceRepair.Store(true)

	select {
	case c.repairCh <- struct{}{}:
	default: // a repair is already pending; one wakeup is enough
	}

	c.ctlMu.Lock()
	cancel := c.sessCancel
	c.ctlMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// setSessCancel publishes the cancel func for the session Run is about to
// start, so requestRepair can interrupt it. Pass nil once the session ends.
func (c *Client) setSessCancel(cancel context.CancelFunc) {
	c.ctlMu.Lock()
	c.sessCancel = cancel
	c.ctlMu.Unlock()
}

// noteConnected records that a transport handshake succeeded. Paired with
// noteDisconnected in runSession, which every transport funnels through.
func (c *Client) noteConnected(transport string) {
	c.transport.Store(transport)
	c.connectedAt.Store(time.Now().UnixNano())
	c.connected.Store(true)
	c.setLastErr(nil)
}

// noteDisconnected records that the live session ended.
func (c *Client) noteDisconnected() {
	c.connected.Store(false)
	c.connectedAt.Store(0)
}

// setLastErr stores (or clears) the last session failure for State.
func (c *Client) setLastErr(err error) {
	c.ctlMu.Lock()
	if err == nil {
		c.lastErr = ""
	} else {
		c.lastErr = err.Error()
	}
	c.ctlMu.Unlock()
}
