package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/AgentsHarness/capri-host/internal/hubstate"
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

// ErrBadPairCode and State are defined in internal/hubstate so the server
// can name them without importing this package — the in-process relay test
// assembles the real server chain here, and server → hub would cycle the
// test graph. Aliases keep the hub API identical for every existing caller.

// ErrBadPairCode is returned by Pair when the code cannot be valid at all, so
// no request is sent to the hub.
var ErrBadPairCode = hubstate.ErrBadPairCode

// State is a point-in-time snapshot of the hub link. Safe from any goroutine
// at any time, including before Run has started.
type State = hubstate.State

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

// Rename updates this host's display name on the hub. It hits the admin
// rename endpoint (FE_TOKEN-gated, not the host pairing token), so a host
// with no AccessToken cannot rename — the caller reports that. The hub
// updates its registry live (no reconnection needed): the next event frame
// this client sends already carries the new name, and browsers refresh via
// the hub's hosts_changed broadcast. c.cfg.HostName is updated on success so
// subsequent frames are consistent.
func (c *Client) Rename(ctx context.Context, newName string) error {
	if c.cfg.URL == "" {
		return errors.New("未配置 hub，无法在 hub 上改名")
	}
	if c.cfg.AccessToken == "" {
		return errors.New("hub 未设置 FE_TOKEN，无法通过本机改名（请在 hub 侧管理界面改名）")
	}
	body, _ := json.Marshal(map[string]any{"hostName": newName})
	req, err := http.NewRequestWithContext(ctx, "POST",
		c.cfg.URL+"/api/hosts/"+url.PathEscape(c.cfg.HostID)+"/rename", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.AccessToken)
	res, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("无法连接 hub: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		var out struct{ Error string `json:"error"` }
		_ = json.NewDecoder(io.LimitReader(res.Body, 4<<10)).Decode(&out)
		if out.Error != "" {
			return fmt.Errorf("hub 拒绝改名: %s", out.Error)
		}
		return fmt.Errorf("hub 拒绝改名 (HTTP %d)", res.StatusCode)
	}
	c.cfg.HostName = newName
	log.Printf("[hub-client] hub 上的显示名已更新为 %q", newName)
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
