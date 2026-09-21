package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
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

	var st State
	if cl != nil {
		st = cl.State()
	} else {
		st = State{
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
			} else if hubstate.ReadToken(stateFilePathFor(cfg)).Usable(cfg.URL) {
				st.Paired = true
			}
		}
	}
	token := hubstate.ReadToken(stateFilePathFor(cfg))
	if token != nil {
		st.StoredURL = token.URLOrEmpty()
		checkURL := st.HubURL
		if checkURL == "" {
			checkURL = st.StoredURL
		}
		st.CanReuse = token.Usable(checkURL)
	}
	return st
}

// Disconnect clears the configured hub address and shuts down the running hub
// client, returning the host to local-only mode.
func (m *Manager) Disconnect() error {
	m.pointAt("")
	return nil
}

// HubURL is the address currently configured, empty in local mode.
func (m *Manager) HubURL() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.URL
}

// Rename updates the host's display name everywhere it appears: the hub
// registry (so browsers see the new name live), the running bridge (so the
// local API and grok frames use it), and config.json (so it survives a
// restart). An empty or whitespace-only name is refused.
//
// The hub update runs through the live client when one is connected; in
// local mode or before the first pairing it is skipped (there is no hub to
// tell). A hub-side failure is logged and does not roll back the local
// change, and it is not stored in LastError: that field is the session
// failure the menus show. The name is the user's choice; the next event
// frame carries it anyway.
func (m *Manager) Rename(ctx context.Context, newName string) error {
	newName = strings.TrimSpace(newName)
	if newName == "" {
		return errors.New("本机代号不能为空")
	}

	m.mu.Lock()
	m.cfg.HostName = newName
	cl := m.cur
	m.mu.Unlock()

	if err := config.UpdateFile(func(f *config.File) { f.HostName = newName }); err != nil {
		log.Printf("[hub-manager] 改名写回配置失败（本次运行仍然生效）: %v", err)
	}
	if m.bridge != nil {
		m.bridge.SetHostName(newName)
	}
	if cl != nil {
		// Apply locally first so State() and the next event frame agree with
		// the file even if the hub call fails. The hub result stays out of
		// LastError: that field is the session failure the menus show.
		cl.name.Store(newName)
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
	cur, cfg := m.cur, m.cfg
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

	// The credential is secured; hand off the move-and-wake to the same helper
	// the reuse path uses, so the two cannot drift apart.
	log.Printf("[hub-manager] 已配对 %s，凭证已保存", want)
	m.pointAt(want)
	return nil
}

// Reuse points the host at hubURL using the credential already on disk.
//
// This is the "I have paired with this hub before" path, and it exists because
// pairing is the wrong answer to it: a code has to be fetched from the hub,
// expires in 15 minutes, and buys nothing when hub.json already holds a valid
// token for that exact address. Both GUI supervisors used to send the user
// through pairing anyway.
//
// It refuses when no usable credential exists, because then the caller really
// does have to pair — silently falling back would hide that.
//
// No context: nothing here blocks. It reads one small file and nudges the
// supervision loop, and pairing's own long path (the hub round trip) does not
// happen at all.
func (m *Manager) Reuse(rawURL string) error {
	want, err := NormalizeHubURL(rawURL)
	if err != nil {
		return err
	}

	m.mu.Lock()
	cur, cfg := m.cur, m.cfg
	m.mu.Unlock()

	if want == "" {
		want = cfg.URL
	}
	if want == "" {
		return errors.New("请先填写 hub 地址")
	}

	path := stateFilePathFor(cfg)
	if !hubstate.ReadToken(path).Usable(want) {
		return fmt.Errorf("%s 里没有 %s 的可用凭证，请用配对码配对", path, want)
	}
	// Already pointed there with a live client: it is running on exactly this
	// credential, so there is nothing to change and nothing to wake.
	if want == cfg.URL && cur != nil {
		return nil
	}

	m.pointAt(want)
	log.Printf("[hub-manager] 复用已有凭证连接 %s", want)
	return nil
}

// pointAt moves the manager to want, persists the choice, and wakes the
// supervision loop so it rebuilds against the new address.
//
// The credential is the caller's business — pairing mints one, reuse finds one
// on disk — so this is only the move-and-wake half the two paths share.
func (m *Manager) pointAt(want string) {
	m.mu.Lock()
	changed := m.cfg.URL != want
	m.cfg.URL = want
	// Any startup-supplied credential is now stale: HOST_TOKEN and
	// HUB_PAIR_CODE take priority in ensureToken, so leaving them set would
	// make the next client ignore the credential we just settled on.
	m.cfg.Token = ""
	m.cfg.PairCode = ""
	cancel := m.cancel
	bridge := m.bridge
	m.mu.Unlock()

	if changed && m.persist != nil {
		if err := m.persist(want); err != nil {
			log.Printf("[hub-manager] 无法把 hub 地址写入配置（本次运行仍然生效）: %v", err)
		}
	}

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
// empty, which callers read as "unchanged". See hubstate.NormalizeURL.
func NormalizeHubURL(raw string) (string, error) {
	return hubstate.NormalizeURL(raw)
}
