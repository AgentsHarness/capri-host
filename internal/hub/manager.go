package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// This file exists so the hub address itself can be chosen at runtime, not
// only at startup.
//
// Client cannot do that on its own: cfg.URL is read without a lock by the
// forwarding loop and both transports, so mutating it in place would be a data
// race. Retargeting therefore means building a NEW client and swapping it,
// which needs an owner — this manager. It is also what lets a host that was
// launched with no hub at all pair from the tray: there is always a manager to
// call, even when there is no client yet.
//
// Manager satisfies the server's HubController interface, so the HTTP API and
// the tray both drive the same object.

// PairPersist is called after a successful pairing against a NEW hub address,
// so the choice survives a restart. Returning an error is logged and does not
// fail the pairing: the link is already up, and refusing to report success
// because a config file could not be written would be the wrong trade.
type PairPersist func(hubURL string) error

// Manager owns the live hub client and can point it at a different hub.
type Manager struct {
	bridge  *acp.Bridge
	persist PairPersist

	mu     sync.Mutex
	cfg    Config
	cur    *Client
	cancel context.CancelFunc

	// retarget wakes the supervision loop when cfg changed. Buffered by one:
	// the loop only needs to know that something changed, not how often.
	retarget chan struct{}
}

// NewManager returns a manager for cfg. cfg.URL may be empty, which is how a
// host with no hub configured still gets a pairing entry point.
func NewManager(cfg Config, persist PairPersist) *Manager {
	return &Manager{
		cfg:      cfg,
		persist:  persist,
		retarget: make(chan struct{}, 1),
	}
}

// Run supervises the hub client until ctx is done, rebuilding it whenever the
// hub address changes. Blocks; call it in a goroutine.
func (m *Manager) Run(ctx context.Context, bridge *acp.Bridge) {
	m.mu.Lock()
	m.bridge = bridge
	m.mu.Unlock()

	for ctx.Err() == nil {
		m.mu.Lock()
		cfg := m.cfg
		m.mu.Unlock()

		if cfg.URL == "" {
			// Local mode. Nothing to dial, but stay alive: PairWith can give
			// us an address at any time, and exiting here would make the tray
			// button silently do nothing.
			select {
			case <-ctx.Done():
				return
			case <-m.retarget:
				continue
			}
		}

		runCtx, cancel := context.WithCancel(ctx)
		cl := NewClient(cfg)

		m.mu.Lock()
		m.cur, m.cancel = cl, cancel
		m.mu.Unlock()

		cl.Run(runCtx, bridge) // blocks until runCtx is cancelled
		cancel()

		m.mu.Lock()
		if m.cur == cl {
			m.cur, m.cancel = nil, nil
		}
		m.mu.Unlock()

		if ctx.Err() != nil {
			return
		}
		// Run returned without the process shutting down, which means we
		// cancelled it to retarget. The timeout is a guard, not a schedule:
		// if Run ever returns for a reason we did not cause, this keeps the
		// loop from spinning on it.
		select {
		case <-ctx.Done():
			return
		case <-m.retarget:
		case <-time.After(time.Second):
		}
	}
}

// State reports the hub link, with a client-free fallback so a host in local
// mode — or one between clients during a retarget — answers with the same
// shape instead of an error.
func (m *Manager) State() State {
	m.mu.Lock()
	cl, cfg := m.cur, m.cfg
	m.mu.Unlock()

	if cl != nil {
		return cl.State()
	}
	st := State{
		Configured: cfg.URL != "",
		HubURL:     cfg.URL,
		HostID:     cfg.HostID,
		HostName:   cfg.HostName,
	}
	// Read the token straight off disk. Without this a tray opened in the
	// instant between two clients reports "未配对" on a host that is paired,
	// and the hub menu entry would blink out of existence.
	if st.Configured {
		if cfg.Token != "" {
			st.Paired = true
		} else if s := readStateFile(stateFilePathFor(cfg)); s != nil && s.URL == cfg.URL && s.Token != "" {
			st.Paired = true
		}
	}
	return st
}

// HubURL is the address currently configured, empty in local mode.
func (m *Manager) HubURL() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.URL
}

// HostName is the display name currently in use (after the boot-time
// identity adoption), shown in the tray tooltip and the info dialog.
func (m *Manager) HostName() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.HostName
}

// Rename updates the host's display name everywhere it appears: the hub
// registry (so browsers see the new name live), the running bridge (so the
// local API and grok frames use it), and config.toml (so it survives a
// restart). An empty or whitespace-only name is refused.
//
// The hub update runs through the live client when one is connected; in
// local mode or before the first pairing it is skipped (there is no hub to
// tell). A hub-side failure is reported but does not roll back the local
// change: the name is the user's choice and the hub rename is best-effort
// (the next event frame carries the new name anyway, and the hub will
// reconcile when it sees it).
func (m *Manager) Rename(ctx context.Context, newName string) error {
	newName = strings.TrimSpace(newName)
	if newName == "" {
		return errors.New("本机代号不能为空")
	}

	m.mu.Lock()
	m.cfg.HostName = newName
	cl := m.cur
	m.mu.Unlock()

	if err := config.Save(config.Settings{HostName: config.String(newName)}); err != nil {
		log.Printf("[hub-manager] 改名写回配置失败（本次运行仍然生效）: %v", err)
	}
	if m.bridge != nil {
		m.bridge.SetHostName(newName)
	}
	if cl != nil {
		if err := cl.Rename(ctx, newName); err != nil {
			log.Printf("[hub-manager] 通知 hub 改名失败（本地已更新）: %v", err)
		}
	}
	log.Printf("[hub-manager] 本机代号已改为 %q", newName)
	return nil
}

// Pair pairs against the hub already configured. It satisfies the server's
// HubController interface, which predates runtime address changes.
func (m *Manager) Pair(ctx context.Context, code string) error {
	return m.PairWith(ctx, "", code)
}

// PairWith pairs against rawURL, switching the host to that hub when it
// differs from the current one. An empty rawURL means "keep the current hub".
//
// The code is validated before anything is sent, and a new hub is contacted
// through a throwaway client, so neither a typo nor an unreachable address
// tears down a link that was working.
func (m *Manager) PairWith(ctx context.Context, rawURL, code string) error {
	code = NormalizePairCode(code)
	if err := ValidatePairCode(code); err != nil {
		return err
	}
	want, err := NormalizeHubURL(rawURL)
	if err != nil {
		return err
	}

	m.mu.Lock()
	cur, cfg, bridge := m.cur, m.cfg, m.bridge
	m.mu.Unlock()

	if want == "" {
		want = cfg.URL
	}
	if want == "" {
		return errors.New("请先填写 hub 地址")
	}

	// Same hub with a live client: let the client re-pair itself. That path
	// swaps the token on the running session instead of rebuilding it, so a
	// connected host never drops.
	if want == cfg.URL && cur != nil {
		return cur.Pair(ctx, code)
	}

	probeCfg := cfg
	probeCfg.URL = want
	token, err := NewClient(probeCfg).pair(ctx, code)
	if err != nil {
		return err
	}

	if err := writeStateFile(stateFilePathFor(probeCfg), stateFile{
		URL: want, HostID: probeCfg.HostID, Token: token,
	}); err != nil {
		return fmt.Errorf("配对成功但无法保存凭证: %w", err)
	}

	m.mu.Lock()
	changed := m.cfg.URL != want
	m.cfg.URL = want
	// Drop any startup-supplied credential. HOST_TOKEN and HUB_PAIR_CODE take
	// priority in ensureToken, so leaving them set would make the next client
	// ignore the token we just earned.
	m.cfg.Token = ""
	m.cfg.PairCode = ""
	cancel := m.cancel
	m.mu.Unlock()

	log.Printf("[hub-manager] 已配对 %s，凭证已保存", want)

	if changed && m.persist != nil {
		if err := m.persist(want); err != nil {
			log.Printf("[hub-manager] 无法把 hub 地址写入配置（本次运行仍然生效）: %v", err)
		}
	}

	// Tear down the old client so the supervision loop rebuilds against the
	// new address, and wake it in case it is idling in local mode.
	if cancel != nil {
		cancel()
	}
	select {
	case m.retarget <- struct{}{}:
	default:
	}

	if bridge == nil {
		// Run has not started yet (pairing during boot). The loop will pick
		// the new address up on its first iteration.
		log.Printf("[hub-manager] hub 客户端尚未启动，将在启动时使用 %s", want)
	}
	return nil
}

// stateFilePathFor resolves where a config's token lives, matching the
// client's own default so both agree on one file.
func stateFilePathFor(cfg Config) string {
	if cfg.StateFile != "" {
		return cfg.StateFile
	}
	return defaultStateFile()
}

// NormalizeHubURL cleans up an address a person typed. An empty input stays
// empty, which callers read as "unchanged".
//
// A bare host gets https, because that is what a hub behind a reverse proxy
// looks like and typing the scheme every time is friction. A hub served as
// plain HTTP therefore has to be entered with http:// — the alternative,
// guessing from the port number, is wrong often enough to be worse than a rule
// that can be stated in one line in the dialog.
func NormalizeHubURL(raw string) (string, error) {
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
	// Everything past the origin is dropped: the client appends its own paths
	// (/api/pair, /api/host/...), so a pasted "…/api/pairing" would otherwise
	// become "…/api/pairing/api/pair" and fail with a confusing 404.
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String(), nil
}
