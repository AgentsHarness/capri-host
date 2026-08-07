package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	promptTimeout   = 30 * time.Minute
	bootTimeout     = 2 * time.Minute
	approvalTimeout = 15 * time.Minute
	protocolVersion = 1
)

// Bridge owns one grok agent stdio process and manages multiple ACP
// sessions inside it (the agent is multi-session, like the Grok Build TUI:
// each session runs its own turn; the host tracks their live states for the
// dashboard active/idle classification).
// It does NOT implement fs/* or terminal/* — agent runs tools itself.
type Bridge struct {
	cfg GrokConfig

	mu       sync.Mutex
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	cancelRd context.CancelFunc

	ready     bool
	booting   bool
	// bootDone is closed when an in-flight boot attempt finishes (success
	// or failure). Concurrent ensureBooted callers wait on it instead of
	// racing the boot and failing with "grok 进程未运行".
	bootDone  chan struct{}
	bootError string
	agentInfo map[string]any
	textBuf   string
	// Roster: every session created in this process (or loaded), keyed by
	// sessionId, with host-side live state (busy / awaiting input).
	sessions        map[string]*SessionState
	activeSessionID string

	// Last focused session survives process death / host restart so we can
	// session/load it instead of session/new (which would open a blank chat).
	// Written whenever createSession / LoadSession makes a session active;
	// also snapshotted into killProcess / waitProcess before the roster is
	// wiped. Persisted under ~/.acp-host/last-session.json.
	lastSessionID  string
	lastSessionCwd string

	nextAgentID atomic.Int64
	pending     sync.Map // id(float64/int) -> chan rpcResult

	nextClientReqID atomic.Int64
	clientReqs      sync.Map // requestId string -> *clientRequest

	subscribersMu sync.Mutex
	subscribers   map[chan Event]struct{}

	hostID   string
	hostName string
	homeDir  string
}

type GrokConfig struct {
	Bin      string
	HostID   string
	HostName string
	// LastSessionFile overrides the default ~/.acp-host/last-session.json
	// so tests can inject a temp path without touching the real home.
	LastSessionFile string
	// GrokHome overrides the grok data dir (~/.grok) used to locate
	// session updates files (task timeline / [bg] badge scans).
	GrokHome string
}

// lastSessionFile is the on-disk form of the most recently active session.
type lastSessionFile struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd,omitempty"`
}

type rpcResult struct {
	result map[string]any
	err    error
}

type clientRequest struct {
	AgentID   any
	SessionID string // originating session (dashboard awaiting-input state)
	Method    string
	Params    map[string]any
	done      chan struct{} // closed when resolved/rejected
	cancel    bool
	// isPermission: session/request_permission replies with the nested
	// {outcome:{outcome:"selected"|"cancelled"}} shape; x.ai/* requests
	// reply with the raw result object the browser supplied.
	isPermission bool
	outcome      map[string]any // session/request_permission result
	result       map[string]any // generic x.ai/* request result
	errMsg       string
}

func NewBridge(cfg GrokConfig) *Bridge {
	homeDir, _ := os.UserHomeDir()
	b := &Bridge{
		cfg:         cfg,
		hostID:      cfg.HostID,
		hostName:    cfg.HostName,
		homeDir:     homeDir,
		sessions:    make(map[string]*SessionState),
		subscribers: make(map[chan Event]struct{}),
		bootDone:    make(chan struct{}),
	}
	b.nextAgentID.Store(1)
	b.nextClientReqID.Store(1)
	if id, cwd := b.loadLastSessionFile(); id != "" {
		b.lastSessionID = id
		b.lastSessionCwd = cwd
	}
	return b
}

// lastSessionPath returns the file used to remember the last active session.
func (b *Bridge) lastSessionPath() string {
	if b.cfg.LastSessionFile != "" {
		return b.cfg.LastSessionFile
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".acp-host", "last-session.json")
}

// loadLastSessionFile reads the persisted last-session pointer (best-effort).
func (b *Bridge) loadLastSessionFile() (id, cwd string) {
	path := b.lastSessionPath()
	if path == "" {
		return "", ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", ""
	}
	var st lastSessionFile
	if json.Unmarshal(raw, &st) != nil || st.SessionID == "" {
		return "", ""
	}
	return st.SessionID, st.Cwd
}

// persistLastSessionLocked writes lastSessionID/Cwd to disk. Caller holds b.mu.
func (b *Bridge) persistLastSessionLocked() {
	if b.lastSessionID == "" {
		return
	}
	path := b.lastSessionPath()
	if path == "" {
		return
	}
	raw, err := json.Marshal(lastSessionFile{
		SessionID: b.lastSessionID,
		Cwd:       b.lastSessionCwd,
	})
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, raw, 0o600)
}

// rememberSessionLocked records sid/cwd as the last active session and
// persists it. Caller holds b.mu. Empty sid is a no-op.
func (b *Bridge) rememberSessionLocked(sid, cwd string) {
	if sid == "" {
		return
	}
	if b.lastSessionID == sid && b.lastSessionCwd == cwd {
		return
	}
	b.lastSessionID = sid
	b.lastSessionCwd = cwd
	b.persistLastSessionLocked()
}

// restoreLastSession reloads the last known session after process death or
// turn failure. It never calls session/new — a failed restore stays failed
// so the UI does not silently open a blank conversation. Callers that need
// a brand-new chat must use NewSession explicitly (UI "new session").
func (b *Bridge) restoreLastSession(ctx context.Context) error {
	b.mu.Lock()
	sid := b.lastSessionID
	cwd := b.lastSessionCwd
	b.mu.Unlock()

	if sid == "" {
		return errors.New("没有可恢复的会话")
	}
	if cwd == "" {
		cwd = mustCwd()
	}
	if _, err := b.LoadSession(ctx, sid, cwd); err != nil {
		log.Printf("[acp-host] restore session %s failed: %v", sid, err)
		return fmt.Errorf("恢复会话失败: %w", err)
	}
	log.Printf("[acp-host] restored session %s (cwd=%s)", sid, cwd)
	return nil
}

func mustCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// Subscribe returns a buffered event channel; call unsubscribe to remove.
func (b *Bridge) Subscribe() (ch chan Event, unsubscribe func()) {
	ch = make(chan Event, 64)
	b.subscribersMu.Lock()
	b.subscribers[ch] = struct{}{}
	b.subscribersMu.Unlock()
	return ch, func() {
		b.subscribersMu.Lock()
		delete(b.subscribers, ch)
		b.subscribersMu.Unlock()
		// drain
		for {
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}

func (b *Bridge) Broadcast(ev Event) {
	if ev == nil {
		return
	}
	b.subscribersMu.Lock()
	defer b.subscribersMu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// slow consumer: drop
		}
	}
}

// ── roster helpers ────────────────────────────────────────────────

// activeSession returns the current active session (nil if none).
// Callers must hold b.mu.
func (b *Bridge) activeSessionLocked() *SessionState {
	if id := b.activeSessionID; id != "" {
		return b.sessions[id]
	}
	return nil
}

// broadcastRosterChange pushes sessions_changed so clients refresh the
// dashboard list whenever the roster or a session's live state changed.
func (b *Bridge) broadcastRosterChange() {
	b.Broadcast(Event{"type": "sessions_changed", "params": map[string]any{}})
}

// setSessionBusy flips a session's in-flight turn state and notifies
// clients on transition (busy ↔ idle = dashboard active ↔ idle).
func (b *Bridge) setSessionBusy(id string, busy bool) {
	b.mu.Lock()
	s := b.sessions[id]
	changed := false
	if s != nil && s.Busy != busy {
		s.Busy = busy
		changed = true
	}
	b.mu.Unlock()
	if changed {
		b.broadcastRosterChange()
	}
}

// setSessionAwaiting flips a session's awaiting-input state (pending
// permission / x.ai question) and notifies clients on transition.
func (b *Bridge) setSessionAwaiting(id string, awaiting bool) {
	b.mu.Lock()
	s := b.sessions[id]
	changed := false
	if s != nil && s.AwaitingInput != awaiting {
		s.AwaitingInput = awaiting
		changed = true
	}
	b.mu.Unlock()
	if changed {
		b.broadcastRosterChange()
	}
}

// sessionIdFrom returns the sessionId carried by an agent notification
// envelope, falling back to the active session (defensive: some wire forms
// omit it while a turn is in flight).
func (b *Bridge) sessionIdFrom(params map[string]any) string {
	if sid, ok := params["sessionId"].(string); ok && sid != "" {
		return sid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if act := b.activeSessionLocked(); act != nil {
		return act.SessionID
	}
	return ""
}

func (b *Bridge) Snapshot() Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	var pending []PendingReq
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		pending = append(pending, PendingReq{
			RequestID: key.(string),
			Method:    cr.Method,
			Params:    cr.Params,
		})
		return true
	})
	roster := make([]SessionState, 0, len(b.sessions))
	busy := false
	for _, s := range b.sessions {
		roster = append(roster, *s)
		if s.Busy {
			busy = true
		}
	}
	// Most recently active first — dashboard ordering.
	sort.Slice(roster, func(i, j int) bool {
		if roster[i].LastActiveAt != roster[j].LastActiveAt {
			return roster[i].LastActiveAt > roster[j].LastActiveAt
		}
		return roster[i].CreatedAt > roster[j].CreatedAt
	})
	act := b.activeSessionLocked()
	var sid, cwd string
	var modes, configOpts, models any
	if act != nil {
		sid = act.SessionID
		cwd = act.Cwd
		modes = act.modes
		configOpts = act.configOpts
		models = act.models
	}
	return Status{
		Ready:           b.ready,
		Busy:            busy,
		Booting:         b.booting,
		SessionID:       sid,
		Cwd:             cwd,
		HostID:          b.hostID,
		HostName:        b.hostName,
		HomeDir:         b.homeDir,
		AgentInfo:       b.agentInfo,
		Modes:           modes,
		ConfigOptions:   configOpts,
		Models:          models,
		BootError:       b.bootError,
		Text:            b.textBuf,
		PendingRequests: pending,
		Capabilities:    DefaultClientCaps(),
		Roster:          roster,
	}
}

// Boot ensures the agent process is up (initialize + authenticate).
// It deliberately does NOT create a session — opening a client must not
// auto-create a new conversation. Sessions are created on demand: the
// first prompt (Prompt), or explicitly via NewSession / session/new.
func (b *Bridge) Boot(ctx context.Context) error {
	return b.ensureBooted(ctx)
}

// ensureBooted starts the grok agent process (if needed) and completes
// initialize + authenticate. Does not create any session. Concurrent
// callers wait for an in-flight boot instead of racing it (a racer would
// hit a nil stdin, misread it as a wedged agent, and kill a healthy
// process).
func (b *Bridge) ensureBooted(ctx context.Context) error {
	b.mu.Lock()
	if b.ready {
		b.mu.Unlock()
		return nil
	}
	if b.booting {
		done := b.bootDone
		b.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		// The in-flight boot finished; re-check its outcome.
		b.mu.Lock()
		ready := b.ready
		errMsg := b.bootError
		b.mu.Unlock()
		if ready {
			return nil
		}
		if errMsg != "" {
			return errors.New(errMsg)
		}
		return errors.New("agent 启动失败")
	}
	b.booting = true
	b.bootError = ""
	b.bootDone = make(chan struct{})
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.booting = false
		close(b.bootDone)
		b.mu.Unlock()
	}()

	if err := b.ensureProcess(); err != nil {
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	caps := DefaultClientCaps()
	initRes, err := b.request(ctx, "initialize", map[string]any{
		"protocolVersion": protocolVersion,
		"clientCapabilities": map[string]any{
			"fs": map[string]any{
				"readTextFile":  caps.FS.ReadTextFile,
				"writeTextFile": caps.FS.WriteTextFile,
			},
			"terminal": caps.Terminal,
			// x.ai extension capabilities (aligned with the Grok Build TUI):
			// bash output streams incrementally without ANSI color (we render
			// it as plain text), git head changes are notified, hunk tracking
			// stays agent-side.
			"meta": map[string]any{
				"x.ai/incrementalBashOutput": true,
				"x.ai/bashOutputNoColor":     true,
				"x.ai/gitHeadChanged":        true,
				"x.ai/hunkTracker":           map[string]any{"mode": "agent_only"},
			},
		},
		"clientInfo": map[string]any{
			"name":    "acp-host",
			"title":   "ACP Host",
			"version": "0.1.0",
		},
	}, bootTimeout)
	if err != nil {
		// A live-but-wedged process can fail initialize repeatedly; kill it so
		// the next Boot spawns a fresh agent instead of reusing it. A client
		// that disconnected mid-boot is NOT a wedged agent — leave the
		// process running so the next boot attempt can retry initialize on it.
		if !errors.Is(err, context.Canceled) {
			b.killProcess()
		}
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	b.mu.Lock()
	b.agentInfo = initRes
	b.mu.Unlock()

	if err := b.authenticate(ctx, initRes); err != nil {
		if !errors.Is(err, context.Canceled) {
			b.killProcess()
		}
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	// No session is auto-created at boot anymore (opening a client must
	// not spawn a new conversation), so announce the bare-ready state —
	// a client that connected while we were booting would otherwise stay
	// on "启动中…" forever. A session-level ready (with sessionId) follows
	// from createSession / LoadSession when one becomes active.
	b.mu.Lock()
	noSession := len(b.sessions) == 0
	agentInfo := b.agentInfo
	hostID := b.hostID
	hostName := b.hostName
	b.mu.Unlock()
	if noSession {
		b.Broadcast(Event{
			"type":      "ready",
			"agentInfo": agentInfo,
			"hostId":    hostID,
			"hostName":  hostName,
		})
	}
	return nil
}

// NewSession creates a brand-new session in the (already booted) agent
// process and makes it the active session. Other sessions keep running —
// this is what powers the parallel dashboard.
func (b *Bridge) NewSession(ctx context.Context, sc SessionConfig) error {
	if err := b.ensureBooted(ctx); err != nil {
		return err
	}
	return b.createSession(ctx, sc)
}

// createSession calls session/new and registers the session in the roster.
func (b *Bridge) createSession(ctx context.Context, sc SessionConfig) error {
	cwd := sc.Cwd
	if cwd == "" {
		cwd = mustCwd()
	}
	addDirs := sc.AdditionalDirectories
	if addDirs == nil {
		addDirs = []string{}
	}
	mcp := sc.MCPServers
	if mcp == nil {
		mcp = []map[string]any{}
	}

	sessRes, err := b.request(ctx, "session/new", map[string]any{
		"cwd":                   cwd,
		"additionalDirectories": addDirs,
		"mcpServers":            mcp,
	}, bootTimeout)
	if err != nil {
		// A client that disconnected mid-request is not an agent failure —
		// keep the process so other sessions / the next attempt survive.
		if !errors.Is(err, context.Canceled) {
			b.killProcess()
		}
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	sid, _ := sessRes["sessionId"].(string)
	if sid == "" {
		b.killProcess()
		err := errors.New("session/new 未返回 sessionId")
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	now := time.Now().UnixMilli()
	b.mu.Lock()
	s := b.sessions[sid]
	if s == nil {
		s = &SessionState{SessionID: sid, CreatedAt: now}
		b.sessions[sid] = s
	}
	s.Cwd = cwd
	s.modes = sessRes["modes"]
	s.configOpts = sessRes["configOptions"]
	s.models = sessRes["models"]
	b.activeSessionID = sid
	b.rememberSessionLocked(sid, cwd)
	b.textBuf = ""
	b.ready = true
	b.bootError = ""
	modes := s.modes
	configOpts := s.configOpts
	models := s.models
	agentInfo := b.agentInfo
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := s.Cwd
	b.mu.Unlock()

	b.Broadcast(Event{
		"type":          "ready",
		"sessionId":     sid,
		"cwd":           sessCwd,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"models":        models,
		"hostId":        hostID,
		"hostName":      hostName,
	})
	b.broadcastRosterChange()
	return nil
}

func (b *Bridge) authenticate(ctx context.Context, init map[string]any) error {
	methodsRaw, _ := init["authMethods"].([]any)
	methods := map[string]bool{}
	for _, m := range methodsRaw {
		mm, _ := m.(map[string]any)
		if id, _ := mm["id"].(string); id != "" {
			methods[id] = true
		}
	}
	var methodID string
	if os.Getenv("XAI_API_KEY") != "" && methods["xai.api_key"] {
		methodID = "xai.api_key"
	} else if methods["cached_token"] {
		methodID = "cached_token"
	}
	if methodID == "" {
		return errors.New("没有可用的认证方式，请先运行 `grok login`，或设置环境变量 XAI_API_KEY")
	}
	_, err := b.request(ctx, "authenticate", map[string]any{
		"methodId": methodID,
		"_meta":    map[string]any{"headless": true},
	}, bootTimeout)
	return err
}

func (b *Bridge) setBootError(msg string) {
	b.mu.Lock()
	b.bootError = msg
	b.ready = false
	b.mu.Unlock()
}

func (b *Bridge) ensureProcess() error {
	b.mu.Lock()
	if b.cmd != nil && b.cmd.Process != nil {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	cmd := exec.Command(b.cfg.Bin, "agent", "stdio")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("无法启动 %s: %w（请确认 grok CLI 已安装，或设置 GROK_BIN）", b.cfg.Bin, err)
	}

	rdCtx, cancelRd := context.WithCancel(context.Background())
	b.mu.Lock()
	b.cmd = cmd
	b.stdin = stdin
	b.cancelRd = cancelRd
	b.mu.Unlock()

	go b.readStdout(rdCtx, stdout)
	go b.readStderr(stderr)
	go b.waitProcess(cmd)

	log.Printf("[acp-host] spawned %s agent stdio pid=%d", b.cfg.Bin, cmd.Process.Pid)
	return nil
}

func (b *Bridge) waitProcess(cmd *exec.Cmd) {
	err := cmd.Wait()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	b.mu.Lock()
	same := b.cmd == cmd
	lastID, lastCwd := b.lastSessionID, b.lastSessionCwd
	if same {
		// Snapshot the active session before wiping the in-memory roster so
		// the next prompt can session/load it instead of opening a blank chat.
		if act := b.activeSessionLocked(); act != nil {
			b.rememberSessionLocked(act.SessionID, act.Cwd)
			lastID, lastCwd = act.SessionID, act.Cwd
		}
		b.cmd = nil
		b.stdin = nil
		b.ready = false
		b.sessions = make(map[string]*SessionState)
		b.activeSessionID = ""
		b.textBuf = ""
		// lastSessionID / lastSessionCwd intentionally survive.
		if b.cancelRd != nil {
			b.cancelRd()
			b.cancelRd = nil
		}
	}
	b.mu.Unlock()
	if !same {
		return
	}
	b.failAllPending(fmt.Errorf("grok 进程已退出 (code=%d)", code))
	_ = lastID
	_ = lastCwd
	log.Printf("[acp-host] grok process exited (code=%d), lastSession=%s", code, lastID)
	b.Broadcast(Event{
		"type": "status",
		"text": "连接HOST异常，请检查后重试",
	})
	b.broadcastRosterChange()
}

func (b *Bridge) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		t := sc.Text()
		if t != "" {
			b.Broadcast(Event{"type": "log", "text": t})
		}
	}
}

func (b *Bridge) readStdout(ctx context.Context, r io.Reader) {
	sc := bufio.NewScanner(r)
	// 64MB max token: x.ai/session/updates can return a very large single-line
	// JSON array for big sessions; a too-small buffer would silently kill the
	// read loop (and wedge the whole bridge).
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !sc.Scan() {
			return
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}
		b.onAgentMessage(msg)
	}
}

func (b *Bridge) onAgentMessage(msg map[string]any) {
	if method, _ := msg["method"].(string); method == "session/update" {
		params, _ := msg["params"].(map[string]any)
		b.handleSessionUpdate(params)
		return
	}

	// x.ai/* extension notifications (no id). Also accept the wrapped
	// leader form {"method":"_x.ai/foo","params":{"method":"x.ai/foo",...}}.
	if method, _ := msg["method"].(string); strings.HasPrefix(method, "x.ai/") || strings.HasPrefix(method, "_x.ai/") {
		params, _ := msg["params"].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}
		realMethod, realParams := unwrapExtMethod(method, params)
		if msg["id"] == nil {
			b.handleXaiNotification(realMethod, realParams)
			return
		}
		// Agent → client extension request: forward for browser interaction.
		b.forwardXaiRequest(msg["id"], realMethod, realParams)
		return
	}

	id := msg["id"]
	if id == nil {
		return
	}

	// Response to our request?
	if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
		c := ch.(chan rpcResult)
		if errObj, has := msg["error"]; has && errObj != nil {
			em := "agent error"
			if m, ok := errObj.(map[string]any); ok {
				if s, ok := m["message"].(string); ok {
					em = s
				}
			}
			c <- rpcResult{err: errors.New(em)}
		} else {
			res, _ := msg["result"].(map[string]any)
			if res == nil {
				res = map[string]any{}
			}
			c <- rpcResult{result: res}
		}
		return
	}

	// Agent → client request
	if method, _ := msg["method"].(string); method != "" {
		params, _ := msg["params"].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}
		b.handleAgentRequest(id, method, params)
	}
}

func idKey(id any) string {
	switch v := id.(type) {
	case float64:
		return fmt.Sprintf("%v", v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprintf("%v", id)
	}
}

func (b *Bridge) handleSessionUpdate(params map[string]any) {
	// Multi-session attribution: the agent's session/update envelope carries
	// {sessionId, update}; every derived event is tagged so clients can
	// bucket by session (dashboard) or filter to their active session.
	sid := b.sessionIdFrom(params)

	// Every standard session/update carries the session-accumulated
	// context token count in `_meta.totalTokens` (the TUI's ⇣ counter
	// source, "Accumulated token count across the session"). Surface it
	// as a usage event so clients can render live context usage — this
	// is the field x.ai extension notifications do NOT carry.
	if meta, ok := params["_meta"].(map[string]any); ok {
		if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
			b.trackUsage(sid, used, 0)
			b.Broadcast(Event{"type": "usage", "used": used, "size": nil, "sessionId": sid})
		}
	}
	update, _ := params["update"].(map[string]any)
	if update == nil {
		return
	}
	kind, _ := update["sessionUpdate"].(string)
	switch kind {
	case "agent_message_chunk":
		if text := contentText(update["content"]); text != "" {
			b.mu.Lock()
			b.textBuf += text
			b.mu.Unlock()
			ev := Event{"type": "chunk", "text": text, "sessionId": sid}
			if mid, ok := update["messageId"]; ok {
				ev["messageId"] = mid
			}
			b.Broadcast(ev)
		}
		// Image blocks ride the same chunk carrier: emit one typed image
		// event per block (text parts still go through the chunk event).
		for _, img := range contentImages(update["content"]) {
			if ev, ok := imageEvent(sid, img); ok {
				b.Broadcast(ev)
			}
		}
	case "user_message_chunk":
		if text := contentText(update["content"]); text != "" {
			b.Broadcast(Event{"type": "user_chunk", "text": text, "sessionId": sid})
		}
		for _, img := range contentImages(update["content"]) {
			if ev, ok := imageEvent(sid, img); ok {
				b.Broadcast(ev)
			}
		}
	case "agent_thought_chunk":
		// content is typically { "type":"text", "text":"..." }; accept a few shapes
		if text := contentText(update["content"]); text != "" {
			b.Broadcast(Event{"type": "thought", "text": text, "sessionId": sid})
		}
	case "tool_call":
		b.Broadcast(Event{"type": "tool_call", "toolCall": update, "sessionId": sid})
	case "tool_call_update":
		b.Broadcast(Event{"type": "tool_call_update", "toolCallUpdate": update, "sessionId": sid})
	case "plan":
		b.Broadcast(Event{"type": "plan", "entries": update["entries"], "sessionId": sid})
	case "usage_update":
		b.trackUsage(sid, toInt64(update["used"]), toInt64(update["size"]))
		b.Broadcast(Event{
			"type":      "usage",
			"used":      update["used"],
			"size":      update["size"],
			"cost":      update["cost"],
			"sessionId": sid,
		})
	case "current_mode_update":
		b.mu.Lock()
		if ms := update["modeState"]; ms != nil {
			if s := b.sessions[sid]; s != nil {
				s.modes = ms
			}
		}
		var modes any
		if s := b.sessions[sid]; s != nil {
			modes = s.modes
		}
		b.mu.Unlock()
		b.Broadcast(Event{"type": "modes_update", "modes": modes, "sessionId": sid})
	case "config_option_update":
		b.mu.Lock()
		if co := update["configOptions"]; co != nil {
			if s := b.sessions[sid]; s != nil {
				s.configOpts = co
			}
		}
		var co any
		if s := b.sessions[sid]; s != nil {
			co = s.configOpts
		}
		b.mu.Unlock()
		b.Broadcast(Event{"type": "config_options_update", "configOptions": co, "sessionId": sid})
	case "available_commands_update":
		b.Broadcast(Event{"type": "commands_update", "commands": update["commands"], "sessionId": sid})
	case "session_info_update":
		b.mu.Lock()
		if s := b.sessions[sid]; s != nil {
			if t, ok := update["title"].(string); ok && t != "" {
				s.Title = t
			}
			if u, ok := update["updatedAt"].(string); ok && u != "" {
				s.UpdatedAt = u
			}
		}
		b.mu.Unlock()
		b.Broadcast(Event{
			"type":      "session_info",
			"title":     update["title"],
			"updatedAt": update["updatedAt"],
			"sessionId": sid,
		})
	default:
		// Some builds deliver x.ai-style lifecycle updates over the standard
		// session/update carrier; route them through the same
		// session_notification event so the frontend handles them identically
		// to the x.ai/session_notification channel.
		switch kind {
		case "subagent_spawned", "subagent_finished",
			"task_backgrounded", "task_completed", "monitor_event",
			"response_started", "reasoning_completed", "response_completed",
			"auto_compact_started", "auto_compact_completed",
			"auto_compact_failed", "auto_compact_cancelled",
			"auto_continue_completed", "image_compressed",
			"session_recap", "session_recap_unavailable":
			b.Broadcast(Event{
				"type":      "session_notification",
				"method":    "session/update",
				"params":    map[string]any{"update": update},
				"sessionId": sid,
			})
		default:
			b.Broadcast(Event{"type": "unknown_update", "update": update, "sessionId": sid})
		}
	}
}

// unwrapExtMethod normalizes the two x.ai wire forms:
//   - direct:  {"method":"x.ai/foo", "params":{...}}          -> x.ai/foo
//   - wrapped: {"method":"_x.ai/foo","params":{"method":"x.ai/foo","params":{...}}} -> x.ai/foo
func unwrapExtMethod(method string, params map[string]any) (string, map[string]any) {
	if !strings.HasPrefix(method, "_x.ai/") {
		return method, params
	}
	if inner, ok := params["method"].(string); ok && strings.HasPrefix(inner, "x.ai/") {
		if innerParams, ok := params["params"].(map[string]any); ok {
			return inner, innerParams
		}
		return inner, map[string]any{}
	}
	return strings.TrimPrefix(method, "_"), params
}

// handleXaiNotification forwards x.ai/* extension notifications to browsers
// as typed SSE events. Unknown methods fall back to a generic
// ext_notification event so nothing is silently dropped.
func (b *Bridge) handleXaiNotification(method string, params map[string]any) {
	// Tag every extension notification with its originating session so the
	// dashboard can bucket multi-session activity.
	sid := b.sessionIdFrom(params)
	withSid := func(ev Event) Event {
		if sid != "" {
			ev["sessionId"] = sid
		}
		return ev
	}
	switch method {
	case "x.ai/session_notification", "x.ai/session/update":
		// Track title/updatedAt into the roster from the session_info
		// carrier updates the agent sends after creating sessions.
		if up, ok := params["update"].(map[string]any); ok {
			if kind, _ := up["sessionUpdate"].(string); kind == "session_info" {
				b.mu.Lock()
				if s := b.sessions[sid]; s != nil {
					if t, ok := up["title"].(string); ok && t != "" {
						s.Title = t
					}
					if u, ok := up["updatedAt"].(string); ok && u != "" {
						s.UpdatedAt = u
					}
				}
				b.mu.Unlock()
			}
		}
		b.Broadcast(withSid(Event{"type": "session_notification", "method": method, "params": params}))
		// grok ships usage inside response_completed / turn_completed
		// notifications instead of the standard usage_update carrier —
		// surface it as a normal usage event so clients can show live
		// token counts while busy. Two distinct numbers ride the same
		// event, both from standard fields:
		//   - used:  context-window usage, `_meta.totalTokens` when the
		//            agent provides it (the TUI ⇣ counter source).
		//            x.ai notifications don't carry it, so on turn end
		//            we simply keep the last known usage (same as the
		//            TUI, which only refreshes from meta.totalTokens).
		//   - usage: the standard TurnCompleted/ResponseCompleted usage
		//            object passed through untouched (totalTokens is the
		//            TURN-ACCUMULATED count across every model call in
		//            the turn — NOT a context-window size). The client
		//            separates the two; no custom field names.
		if up, ok := params["update"].(map[string]any); ok {
			if kind, _ := up["sessionUpdate"].(string); kind == "response_completed" || kind == "turn_completed" {
				ev := Event{"type": "usage"}
				if meta, ok := params["_meta"].(map[string]any); ok {
					if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
						ev["used"] = used
						b.trackUsage(sid, used, 0)
					}
				}
				if u, ok := up["usage"].(map[string]any); ok {
					ev["usage"] = u
				}
				// Only broadcast when there is something to update —
				// an empty usage event (no _meta.totalTokens, no usage
				// object) would otherwise null the client's `used`.
				if len(ev) > 1 {
					b.Broadcast(withSid(ev))
				}
			}
		}
	case "x.ai/task_backgrounded":
		b.Broadcast(withSid(Event{"type": "task_backgrounded", "params": params}))
	case "x.ai/task_completed":
		b.Broadcast(withSid(Event{"type": "task_completed", "params": params}))
	case "x.ai/monitor_event":
		b.Broadcast(withSid(Event{"type": "monitor_event", "params": params}))
	case "x.ai/git_head_changed":
		// Stash the head into the roster so /api/session-info can serve it.
		b.mu.Lock()
		if s := b.sessions[sid]; s != nil {
			if v, ok := params["branch"].(string); ok {
				s.gitBranch = v
			}
			if v, ok := params["isWorktree"].(bool); ok {
				s.gitWorktree = v
			}
			if v, ok := params["mainRepo"].(string); ok {
				s.gitMainRepo = v
			}
		}
		b.mu.Unlock()
		b.Broadcast(withSid(Event{"type": "git_head_changed", "params": params}))
	case "x.ai/yolo_mode_changed":
		b.Broadcast(withSid(Event{"type": "yolo_mode_changed", "params": params}))
	case "x.ai/mcp/server_status":
		b.Broadcast(withSid(Event{"type": "mcp_server_status", "params": params}))
	case "x.ai/mcp/tools_changed", "x.ai/mcp_initialized":
		b.Broadcast(withSid(Event{"type": "mcp_tools_changed", "params": params}))
	case "x.ai/mcp/servers_updated":
		b.Broadcast(withSid(Event{"type": "mcp_servers_updated", "params": params}))
	case "x.ai/sessions/changed":
		b.Broadcast(Event{"type": "sessions_changed", "params": params})
	case "x.ai/models/update":
		// Machine-wide catalog refresh (config.toml changed) — update the
		// active session's catalog while preserving its current selection
		// when still available (TUI update_catalog semantics). The raw
		// payload is a SessionModelState; forwarding it verbatim would
		// clobber the session's model with the machine-wide current.
		b.mu.Lock()
		if act := b.activeSessionLocked(); act != nil {
			b.applyModelsCatalog(act, params)
			models := act.models
			b.mu.Unlock()
			if models != nil {
				b.Broadcast(withSid(Event{"type": "models_update", "params": models}))
			}
		} else {
			b.mu.Unlock()
		}
	case "x.ai/announcements/update":
		b.Broadcast(Event{"type": "announcements_update", "params": params})
	case "x.ai/scheduled_task_fired":
		b.Broadcast(withSid(Event{"type": "scheduled_task_fired", "params": params}))
	case "x.ai/scheduled_task_inject_prompt":
		b.Broadcast(withSid(Event{"type": "scheduled_task_inject_prompt", "params": params}))
	case "x.ai/scheduled_task_created":
		// Wire fields may be snake_case (task_id/prompt/interval/
		// next_fire_at) or camelCase, and taskId may sit at the top level
		// instead of inside a task object — normalize to one typed event.
		task, _ := params["task"].(map[string]any)
		b.Broadcast(withSid(Event{
			"type": "scheduled_task_created",
			"task": map[string]any{
				"taskId":     pick([]map[string]any{task, params}, "taskId", "task_id"),
				"prompt":     pick([]map[string]any{task, params}, "prompt"),
				"interval":   pick([]map[string]any{task, params}, "interval"),
				"nextFireAt": pick([]map[string]any{task, params}, "nextFireAt", "next_fire_at"),
			},
		}))
	case "x.ai/scheduled_task_deleted":
		b.Broadcast(withSid(Event{
			"type":   "scheduled_task_deleted",
			"taskId": pick([]map[string]any{params}, "taskId", "task_id"),
		}))
	case "x.ai/session/prompt_complete":
		b.Broadcast(withSid(Event{"type": "prompt_complete", "params": params}))
	default:
		b.Broadcast(withSid(Event{"type": "ext_notification", "method": method, "params": params}))
	}
}

func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

// toInt64 is the lenient variant of asInt: 0 for missing/unparsable values.
func toInt64(v any) int64 {
	n, _ := asInt(v)
	return n
}

// pick returns the first present value found under any of the given keys,
// checking each map in order (snake_case/camelCase wire compatibility).
// Returns nil when nothing matched.
func pick(maps []map[string]any, keys ...string) any {
	for _, m := range maps {
		for _, k := range keys {
			if v, ok := m[k]; ok {
				return v
			}
		}
	}
	return nil
}

// trackUsage stashes the session's latest context usage (used/total tokens)
// so /api/session-info can serve it on demand.
func (b *Bridge) trackUsage(sid string, used, size int64) {
	if sid == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.sessions[sid]; s != nil {
		if used > 0 {
			s.usageUsed = used
		}
		if size > 0 {
			s.usageSize = size
		}
	}
}

// handleAgentRequest — minimal client: only session/request_permission is interactive.
// fs/* and terminal/* are rejected (capabilities declared false; agent should not call them).
func (b *Bridge) handleAgentRequest(id any, method string, params map[string]any) {
	switch method {
	case "session/request_permission":
		b.forwardPermission(id, method, params)
	case "fs/read_text_file", "fs/write_text_file",
		"terminal/create", "terminal/output", "terminal/wait_for_exit",
		"terminal/kill", "terminal/release":
		b.respondError(id, fmt.Sprintf("客户端未提供 %s（纯 Agent 执行模式）", method), -32601)
	default:
		b.respondError(id, fmt.Sprintf("客户端不支持方法 %s", method), -32601)
	}
}

func (b *Bridge) forwardPermission(id any, method string, params map[string]any) {
	reqID := fmt.Sprintf("acp_cr_%d", b.nextClientReqID.Add(1))
	cr := &clientRequest{
		AgentID:      id,
		SessionID:    b.sessionIdFrom(params),
		Method:       method,
		Params:       params,
		done:         make(chan struct{}),
		isPermission: true,
	}
	b.clientReqs.Store(reqID, cr)
	b.setSessionAwaiting(cr.SessionID, true)
	b.Broadcast(Event{
		"type":      "client_request",
		"requestId": reqID,
		"method":    method,
		"params":    params,
		"sessionId": cr.SessionID,
	})

	go b.waitClientResolution(reqID, cr)
}

// forwardXaiRequest forwards an agent → client extension request
// (x.ai/ask_user_question, x.ai/exit_plan_mode, x.ai/mcp/sdk_call, …) to
// the browser as a generic client_request; the browser's response is passed
// back verbatim as the JSON-RPC result.
func (b *Bridge) forwardXaiRequest(id any, method string, params map[string]any) {
	reqID := fmt.Sprintf("acp_cr_%d", b.nextClientReqID.Add(1))
	cr := &clientRequest{
		AgentID:   id,
		SessionID: b.sessionIdFrom(params),
		Method:    method,
		Params:    params,
		done:      make(chan struct{}),
	}
	b.clientReqs.Store(reqID, cr)
	b.setSessionAwaiting(cr.SessionID, true)
	b.Broadcast(Event{
		"type":      "client_request",
		"requestId": reqID,
		"method":    method,
		"params":    params,
		"sessionId": cr.SessionID,
	})

	go b.waitClientResolution(reqID, cr)
}

// waitClientResolution blocks until the browser resolves the request,
// the approval timeout elapses, or the agent connection dies, then writes
// the JSON-RPC response back to the agent.
func (b *Bridge) waitClientResolution(reqID string, cr *clientRequest) {
	// Whatever happens (resolve / cancel / timeout), the session is no
	// longer waiting on input.
	defer b.setSessionAwaiting(cr.SessionID, false)
	timer := time.NewTimer(approvalTimeout)
	defer timer.Stop()
	select {
	case <-cr.done:
		b.clientReqs.Delete(reqID)
		id := cr.AgentID
		if cr.cancel {
			if cr.isPermission {
				b.respond(id, map[string]any{
					"outcome": map[string]any{"outcome": "cancelled"},
				})
			} else {
				b.respondError(id, "已取消", -32800)
			}
			return
		}
		if cr.errMsg != "" {
			b.respondError(id, cr.errMsg, -32001)
			return
		}
		if cr.isPermission {
			b.respond(id, cr.outcome)
			return
		}
		b.respond(id, cr.result)
	case <-timer.C:
		b.clientReqs.Delete(reqID)
		if cr.isPermission {
			b.respond(cr.AgentID, map[string]any{
				"outcome": map[string]any{"outcome": "cancelled"},
			})
		} else {
			b.respondError(cr.AgentID, "审批超时", -32002)
		}
	}
}

// RespondPermission resolves a pending session/request_permission.
func (b *Bridge) RespondPermission(requestID, optionID string, cancelled bool) error {
	v, ok := b.clientReqs.LoadAndDelete(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	if cancelled {
		cr.cancel = true
	} else {
		cr.outcome = map[string]any{
			"outcome": map[string]any{
				"outcome":  "selected",
				"optionId": optionID,
			},
		}
	}
	close(cr.done)
	return nil
}

// RespondClientRequest resolves a forwarded x.ai/* request with either a
// raw result object (passed through as the JSON-RPC result) or an error
// message.
func (b *Bridge) RespondClientRequest(requestID string, result map[string]any, errMsg string) error {
	v, ok := b.clientReqs.LoadAndDelete(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	if errMsg != "" {
		cr.errMsg = errMsg
	} else {
		cr.result = result
	}
	close(cr.done)
	return nil
}

func (b *Bridge) write(msg map[string]any) error {
	b.mu.Lock()
	stdin := b.stdin
	b.mu.Unlock()
	if stdin == nil {
		return errors.New("grok 进程未运行")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = stdin.Write(data)
	return err
}

func (b *Bridge) respond(id any, result map[string]any) {
	_ = b.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func (b *Bridge) respondError(id any, message string, code int) {
	_ = b.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func (b *Bridge) request(ctx context.Context, method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	id := b.nextAgentID.Add(1)
	key := idKey(id)
	ch := make(chan rpcResult, 1)
	// Agent replies with JSON numbers (float64); idKey normalizes both sides.
	b.pending.Store(key, ch)

	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	if err := b.write(msg); err != nil {
		b.pending.Delete(key)
		return nil, err
	}

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-tctx.Done():
		b.pending.Delete(key)
		if err := ctx.Err(); err != nil {
			// The caller's context went away (e.g. the browser disconnected
			// mid-turn). Surface the cause so callers can tell a dead client
			// apart from a dead agent instead of treating every failure as
			// "agent wedged".
			return nil, fmt.Errorf("%s: %w", method, err)
		}
		return nil, fmt.Errorf("%s 超时", method)
	case res := <-ch:
		return res.result, res.err
	}
}

func (b *Bridge) failAllPending(err error) {
	b.pending.Range(func(key, value any) bool {
		b.pending.Delete(key)
		ch := value.(chan rpcResult)
		select {
		case ch <- rpcResult{err: err}:
		default:
		}
		return true
	})
}

// Prompt sends session/prompt to the given session (default: active) and
// blocks until the turn finishes. When no session exists yet (the host
// never auto-creates one at boot), the first prompt restores the last
// known session if one was remembered; only a machine with no last-session
// pointer starts a brand-new conversation. A failed restore is an error —
// never silently open a blank chat. Busy is per-session: other sessions
// can keep running turns in parallel (the agent process is multi-session).
func (b *Bridge) Prompt(ctx context.Context, sessionID string, blocks []ContentBlock) (stopReason string, err error) {
	b.mu.Lock()
	if sessionID == "" {
		sessionID = b.activeSessionID
	}
	s := b.sessions[sessionID]
	if s == nil && sessionID == "" {
		// No active conversation yet. Prefer restoring the last one
		// (survives agent crash / host restart). Only create a fresh
		// session when nothing was ever remembered. An explicitly
		// targeted unknown sessionId below stays a 404.
		hasLast := b.lastSessionID != ""
		b.mu.Unlock()
		if hasLast {
			if err := b.restoreLastSession(ctx); err != nil {
				return "", err
			}
		} else if err := b.NewSession(ctx, SessionConfig{}); err != nil {
			return "", err
		}
		b.mu.Lock()
		sessionID = b.activeSessionID
		s = b.sessions[sessionID]
	}
	if s == nil {
		b.mu.Unlock()
		return "", &HTTPError{Code: 404, Msg: "会话不存在"}
	}
	if s.Busy {
		b.mu.Unlock()
		return "", &HTTPError{Code: 409, Msg: "上一条消息还在处理中"}
	}
	s.Busy = true
	s.LastActiveAt = time.Now().UnixMilli()
	// Keep last-session pointer fresh even if the user only talks to this
	// session without re-loading it (multi-session focus via prompt).
	b.rememberSessionLocked(sessionID, s.Cwd)
	b.mu.Unlock()
	b.setSessionBusy(sessionID, true)
	b.Broadcast(Event{"type": "busy", "sessionId": sessionID})

	defer func() {
		b.setSessionBusy(sessionID, false)
	}()

	if err := b.ensureBooted(ctx); err != nil {
		return "", err
	}

	// convert blocks
	prompt := make([]any, 0, len(blocks))
	for _, bl := range blocks {
		prompt = append(prompt, map[string]any(bl))
	}

	res, err := b.request(ctx, "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    prompt,
	}, promptTimeout)
	if err != nil {
		b.Broadcast(Event{"type": "error", "message": err.Error(), "sessionId": sessionID})
		// The client (browser) went away mid-turn. The agent process may be
		// perfectly healthy — and other sessions may be running parallel
		// turns in the same process — so never killProcess here: that would
		// abort every session's work, not just this one. Cancel the orphaned
		// turn so the session does not stay busy for the full promptTimeout,
		// and return; the client can simply resend.
		if errors.Is(err, context.Canceled) {
			b.Cancel(sessionID)
			b.Broadcast(Event{"type": "status", "text": "连接已断开，本次回复已取消，请重新发送", "sessionId": sessionID})
			return "", err
		}
		// Self-heal: a turn-end failure usually means the agent process is
		// wedged (it streams output but errors when resolving the turn).
		// Kill it and reload the SAME session — never session/new on this
		// path (a failed restore leaves the user without an active chat
		// rather than a silent blank one). The failed turn is not retried.
		b.mu.Lock()
		if s := b.sessions[sessionID]; s != nil {
			b.rememberSessionLocked(s.SessionID, s.Cwd)
		} else if sessionID != "" {
			b.rememberSessionLocked(sessionID, b.lastSessionCwd)
		}
		b.mu.Unlock()

		b.killProcess()
		rebootCtx, cancel := context.WithTimeout(context.Background(), bootTimeout)
		defer cancel()
		if rebootErr := b.restoreLastSession(rebootCtx); rebootErr != nil {
			log.Printf("[acp-host] auto-recovery failed: %v", rebootErr)
		}
		// Surface a single user-facing line regardless of whether the
		// background restore succeeded — the turn itself still failed and
		// the user needs to check the host / resend.
		b.Broadcast(Event{"type": "status", "text": "连接HOST异常，请检查后重试"})
		return "", err
	}
	sr, _ := res["stopReason"].(string)
	if sr == "" {
		sr = "unknown"
	}
	b.Broadcast(Event{"type": "done", "stopReason": sr, "sessionId": sessionID})
	return sr, nil
}

// Cancel sends session/cancel for the given session (default: active) and
// cancels its pending client requests.
func (b *Bridge) Cancel(sessionID string) {
	b.mu.Lock()
	if sessionID == "" {
		sessionID = b.activeSessionID
	}
	s := b.sessions[sessionID]
	sid := ""
	if s != nil {
		sid = s.SessionID
	}
	b.mu.Unlock()
	if sid != "" {
		_ = b.write(map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/cancel",
			"params":  map[string]any{"sessionId": sid},
		})
	}
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		if sid != "" && cr.SessionID != sid {
			return true // not this session's request
		}
		b.clientReqs.Delete(key)
		cr.cancel = true
		select {
		case <-cr.done:
		default:
			close(cr.done)
		}
		return true
	})
	b.Broadcast(Event{"type": "cancelled", "sessionId": sid})
}

// killProcess kills the grok agent process (if any) and resets the in-memory
// roster, so the next Boot starts from a fresh process. The last-session
// pointer is preserved so restoreLastSession can session/load it.
// Used when a turn or a boot step fails on a possibly wedged agent.
func (b *Bridge) killProcess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if act := b.activeSessionLocked(); act != nil {
		b.rememberSessionLocked(act.SessionID, act.Cwd)
	}
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	b.cmd = nil
	b.stdin = nil
	b.ready = false
	b.sessions = make(map[string]*SessionState)
	b.activeSessionID = ""
	b.textBuf = ""
	// lastSessionID / lastSessionCwd intentionally survive.
	if b.cancelRd != nil {
		b.cancelRd()
		b.cancelRd = nil
	}
}

// SetMode calls session/set_mode on the active session.
func (b *Bridge) SetMode(ctx context.Context, modeID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid string
	if act != nil {
		sid = act.SessionID
	}
	b.mu.Unlock()
	if sid == "" {
		return nil, errors.New("没有活跃会话")
	}
	return b.request(ctx, "session/set_mode", map[string]any{
		"sessionId": sid,
		"modeId":    modeID,
	}, 30*time.Second)
}

// SetModel calls session/set_model (grok's /model switch; the wire method
// is snake_case per the ACP method table). An optional reasoningEffort is
// forwarded in _meta, matching how the TUI applies --effort.
func (b *Bridge) SetModel(ctx context.Context, modelID, reasoningEffort string) error {
	if err := b.Boot(ctx); err != nil {
		return err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid string
	if act != nil {
		sid = act.SessionID
	}
	b.mu.Unlock()
	if sid == "" {
		return errors.New("没有活跃会话")
	}
	params := map[string]any{
		"sessionId": sid,
		"modelId":   modelID,
	}
	if reasoningEffort != "" {
		params["_meta"] = map[string]any{"reasoningEffort": reasoningEffort}
	}
	if _, err := b.request(ctx, "session/set_model", params, 30*time.Second); err != nil {
		return err
	}
	// The agent does not push a model_changed notification for set_model,
	// so the cached SessionModelState (hello / ready / status snapshot)
	// would stay stale and revert the UI caption on the next reconnect.
	// Patch the cache and re-broadcast so every client converges.
	b.mu.Lock()
	var models any
	var name string
	if act := b.sessions[sid]; act != nil {
		b.patchSessionModels(act, modelID, reasoningEffort)
		models = act.models
		name = modelDisplayName(models, modelID)
	}
	b.mu.Unlock()
	if models != nil {
		b.Broadcast(Event{"type": "models_update", "params": models, "sessionId": sid})
	}
	b.Broadcast(Event{
		"type":            "model",
		"modelId":         modelID,
		"modelName":       name,
		"reasoningEffort": reasoningEffort,
		"sessionId":       sid,
	})
	return nil
}

// patchSessionModels updates a session's cached SessionModelState after a
// successful session/set_model: the current model id, the matching catalog
// entry's default effort, and (host extension) the top-level reasoning
// effort the client selected.
func (b *Bridge) patchSessionModels(s *SessionState, modelID, effort string) {
	models, ok := s.models.(map[string]any)
	if !ok {
		return
	}
	models["currentModelId"] = modelID
	if effort != "" {
		models["reasoningEffort"] = effort
	}
	if avail, ok := models["availableModels"].([]any); ok {
		for _, raw := range avail {
			mm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if id, _ := mm["modelId"].(string); id == modelID {
				if meta, ok := mm["_meta"].(map[string]any); ok && effort != "" {
					meta["reasoningEffort"] = effort
				}
			}
		}
	}
}

// modelDisplayName resolves a model's display name from a SessionModelState
// catalog ("" when the id is unknown / no catalog).
func modelDisplayName(models any, modelID string) string {
	m, ok := models.(map[string]any)
	if !ok {
		return ""
	}
	avail, _ := m["availableModels"].([]any)
	for _, raw := range avail {
		mm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := mm["modelId"].(string); id == modelID {
			if n, ok := mm["name"].(string); ok {
				return n
			}
		}
	}
	return ""
}

// applyModelsCatalog refreshes a session's cached catalog from a
// machine-wide x.ai/models/update broadcast, keeping the session's current
// model when it is still offered (falling back to the broadcast's current
// otherwise, like the TUI's update_catalog). Reasoning-effort fields are
// left untouched — the broadcast carries each model's static default, not
// this session's choice.
func (b *Bridge) applyModelsCatalog(s *SessionState, incoming map[string]any) {
	models, ok := s.models.(map[string]any)
	if !ok {
		if len(incoming) == 0 {
			return
		}
		models = map[string]any{}
		s.models = models
	}
	if inAvail, ok := incoming["availableModels"].([]any); ok {
		cur, _ := models["currentModelId"].(string)
		if cur != "" {
			still := false
			for _, raw := range inAvail {
				if mm, ok := raw.(map[string]any); ok {
					if id, _ := mm["modelId"].(string); id == cur {
						still = true
						break
					}
				}
			}
			if !still {
				cur = ""
			}
		}
		if cur == "" {
			cur, _ = incoming["currentModelId"].(string)
		}
		models["availableModels"] = inAvail
		models["currentModelId"] = cur
	} else if c, ok := incoming["currentModelId"].(string); ok {
		// Catalog-less payload: only the current id changed.
		models["currentModelId"] = c
	}
}

// ListSessions calls session/list if supported and enriches every session
// item with the host-side live status (dashboard active/idle/awaiting).
func (b *Bridge) ListSessions(ctx context.Context) ([]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	res, err := b.request(ctx, "session/list", map[string]any{}, 30*time.Second)
	if err != nil {
		return nil, err
	}
	sessions, _ := res["sessions"].([]any)

	b.mu.Lock()
	defer b.mu.Unlock()

	// [bg] badge census: scan each session's persisted updates for task
	// events (best-effort; missing files stay unbadged). Orphan log paths
	// are collected per session and probed in ONE lsof call afterwards.
	census := make(map[censusKey]TaskSummary, len(sessions))
	orphans := make(map[censusKey][]string, len(sessions))
	for _, it := range sessions {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		sid, _ := m["sessionId"].(string)
		cwd, _ := m["cwd"].(string)
		if sid == "" || cwd == "" {
			continue
		}
		sum, ps := sessionTaskCensus(b.grokHome(), cwd, sid)
		census[censusKey{sid, cwd}] = sum
		orphans[censusKey{sid, cwd}] = ps
	}
	open := probeOrphanPaths(orphans)

	for _, it := range sessions {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		sid, _ := m["sessionId"].(string)
		if s := b.sessions[sid]; s != nil {
			m["status"] = map[string]any{
				"state":         s.State(),
				"busy":          s.Busy,
				"awaitingInput": s.AwaitingInput,
				"lastActiveAt":  s.LastActiveAt,
			}
			// Host-side title/updatedAt win over the agent list when the
			// session is live (agent may not persist them yet).
			if s.Title != "" {
				m["title"] = s.Title
			}
			if s.UpdatedAt != "" {
				m["updatedAt"] = s.UpdatedAt
			}
		} else {
			m["status"] = map[string]any{"state": "idle", "busy": false, "awaitingInput": false}
		}
		cwd, _ := m["cwd"].(string)
		if sum, ok := census[censusKey{sid, cwd}]; ok {
			sum = applyProbe(sum, orphans[censusKey{sid, cwd}], open)
			m["hasTasks"] = sum.HasTasks
			m["bgCount"] = sum.BgCount
			m["bgRunning"] = sum.BgRunning
		}
	}
	return sessions, nil
}

// SessionStateOf returns the host-side live state of one session (nil if
// the session is not in the roster). For sessions only known to the agent
// (never created/loaded in this process), the caller falls back to
// session/list-derived data.
func (b *Bridge) SessionStateOf(sessionID string) *SessionState {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.sessions[sessionID]
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

// SessionInfo returns authoritative live details of the active session
// (TUI /session-info analog), served on demand via POST /api/session-info:
// roster fields, tracked context usage / git head, and the model from the
// session's SessionModelState catalog. Nil when no session is active.
func (b *Bridge) SessionInfo() *SessionInfoDetail {
	b.mu.Lock()
	defer b.mu.Unlock()
	act := b.activeSessionLocked()
	if act == nil {
		return nil
	}
	d := &SessionInfoDetail{
		SessionID:   act.SessionID,
		Title:       act.Title,
		Cwd:         act.Cwd,
		UpdatedAt:   act.UpdatedAt,
		State:       act.State(),
		Busy:        act.Busy,
		ContextUsed: act.usageUsed,
		ContextSize: act.usageSize,
		GitBranch:   act.gitBranch,
		GitWorktree: act.gitWorktree,
		GitMainRepo: act.gitMainRepo,
		HostID:      b.hostID,
		HostName:    b.hostName,
		HomeDir:     b.homeDir,
	}
	// Model from SessionModelState: {currentModelId, availableModels:[…]}.
	m, ok := act.models.(map[string]any)
	if !ok {
		return d
	}
	cur, _ := m["currentModelId"].(string)
	if cur == "" {
		cur, _ = m["current"].(string)
	}
	if cur == "" {
		return d
	}
	mi := &ModelInfo{ModelID: cur, Name: modelDisplayName(act.models, cur)}
	if eff, ok := m["reasoningEffort"].(string); ok && eff != "" {
		mi.ReasoningEffort = eff
	}
	if avail, ok := m["availableModels"].([]any); ok {
		for _, raw := range avail {
			mm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if id, _ := mm["modelId"].(string); id != cur {
				continue
			}
			if meta, ok := mm["_meta"].(map[string]any); ok {
				if eff, ok := meta["reasoningEffort"].(string); ok && eff != "" && mi.ReasoningEffort == "" {
					mi.ReasoningEffort = eff
				}
				if cw := toInt64(meta["totalContextTokens"]); cw > 0 {
					mi.ContextWindow = cw
				}
			}
		}
	}
	d.Model = mi
	return d
}

// LoadSession switches the active session to a historical one (session/load),
// so subsequent prompts and live updates belong to that session.
//
// The load response carries the restored session's SessionModelState (the
// model that was active when the session was saved, after availability
// fallbacks). We surface it on the ready event so the UI caption matches
// the restored session instead of the process-global boot model.
//
// If the target session is already live in this host and has a turn in
// flight (Busy), we only re-focus the UI onto it — calling agent
// session/load would conflict with the running turn (previously 409).
func (b *Bridge) LoadSession(ctx context.Context, sessionID, cwd string) (map[string]any, error) {
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil && s.Busy {
		// Focus-only path for an in-flight session.
		if cwd != "" {
			s.Cwd = cwd
		}
		b.activeSessionID = sessionID
		b.rememberSessionLocked(sessionID, s.Cwd)
		b.textBuf = ""
		b.ready = true
		b.bootError = ""
		sessRes := map[string]any{"busy": true}
		if s.models != nil {
			sessRes["models"] = s.models
		}
		if s.modes != nil {
			sessRes["modes"] = s.modes
		}
		if s.configOpts != nil {
			sessRes["configOptions"] = s.configOpts
		}
		agentInfo := b.agentInfo
		modes := s.modes
		configOpts := s.configOpts
		models := s.models
		hostID := b.hostID
		hostName := b.hostName
		sessCwd := s.Cwd
		b.mu.Unlock()

		b.Broadcast(Event{
			"type":          "ready",
			"sessionId":     sessionID,
			"cwd":           sessCwd,
			"agentInfo":     agentInfo,
			"modes":         modes,
			"configOptions": configOpts,
			"models":        models,
			"hostId":        hostID,
			"hostName":      hostName,
		})
		// Re-announce busy so the client can attach the spinner after history load.
		b.Broadcast(Event{"type": "busy", "sessionId": sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sessRes, err := b.request(ctx, "session/load", map[string]any{
		"sessionId":  sessionID,
		"cwd":        cwd,
		"mcpServers": []any{},
	}, bootTimeout)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	act := b.sessions[sessionID]
	if act == nil {
		act = &SessionState{SessionID: sessionID, CreatedAt: time.Now().UnixMilli()}
		b.sessions[sessionID] = act
	}
	act.Cwd = cwd
	b.textBuf = ""
	b.ready = true
	b.bootError = ""
	// Prefer fields from the load response — they reflect the restored session.
	if m, ok := sessRes["models"]; ok && m != nil {
		act.models = m
	}
	if modes, ok := sessRes["modes"]; ok && modes != nil {
		act.modes = modes
	}
	if co, ok := sessRes["configOptions"]; ok && co != nil {
		act.configOpts = co
	}
	b.activeSessionID = sessionID
	b.rememberSessionLocked(sessionID, cwd)
	agentInfo := b.agentInfo
	modes := act.modes
	configOpts := act.configOpts
	models := act.models
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := act.Cwd
	b.mu.Unlock()

	if sessRes == nil {
		sessRes = map[string]any{}
	}
	// Cold load is never mid-turn.
	sessRes["busy"] = false

	b.Broadcast(Event{
		"type":          "ready",
		"sessionId":     sessionID,
		"cwd":           sessCwd,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"models":        models,
		"hostId":        hostID,
		"hostName":      hostName,
	})
	b.broadcastRosterChange()
	return sessRes, nil
}

// ── session history (x.ai/session/updates) ───────────────────────

// UpdatesPage is one page of a session's stored updates (message history).
// Each element of Updates is the full JSONL storage envelope
// {timestamp, method, params}, as returned by the x.ai/session/updates
// extension.
type UpdatesPage struct {
	Updates    []any `json:"updates"`
	TotalCount int   `json:"totalCount"`
	HasMore    bool  `json:"hasMore"`
}

// SessionUpdates fetches a session's stored updates (message history) via
// the ACP extension method x.ai/session/updates. Each element of the result
// is the full storage envelope {timestamp, method, params}.
func (b *Bridge) SessionUpdates(ctx context.Context, sessionID, cwd string, offset *int64, limit *int) (UpdatesPage, error) {
	if err := b.Boot(ctx); err != nil {
		return UpdatesPage{}, err
	}
	params := map[string]any{"sessionId": sessionID, "cwd": cwd}
	if offset != nil {
		params["offset"] = *offset
	}
	if limit != nil {
		params["limit"] = *limit
	}
	// ACP wire convention: extension methods are sent with an underscore
	// prefix ("_" + method). The agent's decoder strips it and routes the
	// remainder to the extension handler; sending "x.ai/session/updates"
	// bare yields -32601 method_not_found.
	res, err := b.request(ctx, "_x.ai/session/updates", params, 2*time.Minute)
	if err != nil {
		return UpdatesPage{}, err
	}
	updates, _ := res["updates"].([]any)
	total, _ := res["totalCount"].(float64)
	hasMore, _ := res["hasMore"].(bool)
	page := UpdatesPage{Updates: updates, TotalCount: int(total), HasMore: hasMore}
	log.Printf("[acp-host] session updates via _x.ai/session/updates ok (total=%d)", page.TotalCount)
	return page, nil
}

// GitInfo returns the git branch/worktree state for a cwd. The branch comes
// from the protocol method `_x.ai/git/info` (GitInfoData.currentBranch);
// worktree state is not part of that payload (the TUI probes it locally
// with git2), so it is probed here with the standard git plumbing commands
// `rev-parse --git-dir` vs `--git-common-dir`.
func (b *Bridge) GitInfo(ctx context.Context, cwd string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	out := map[string]any{"branch": "", "isWorktree": false, "mainRepo": ""}
	// GitInfoRequest is camelCase on the wire: {sessionId?, gitRoot?}.
	// The response is an ExtMethodResult envelope {"result": GitInfoData,
	// "error": …} — the git extensions return to_ext_response, unlike
	// session/updates which uses the bare payload.
	res, err := b.request(ctx, "_x.ai/git/info", map[string]any{"gitRoot": cwd}, 15*time.Second)
	if err == nil {
		payload := res
		if inner, ok := res["result"].(map[string]any); ok {
			payload = inner
		}
		if br, ok := payload["currentBranch"].(string); ok {
			out["branch"] = br
		}
	}
	// Not a git repo (or the agent errored) → empty branch, no worktree.
	isWorktree, mainRepo := probeWorktree(cwd)
	out["isWorktree"] = isWorktree
	out["mainRepo"] = mainRepo
	return out, nil
}

// probeWorktree reports whether cwd lives in a linked worktree and, if so,
// the main repo path (shortened to ~/… under $HOME). Mirrors the TUI's
// get_worktree_info (git-dir != common-dir ⇒ worktree).
func probeWorktree(cwd string) (bool, string) {
	gitDir := runGit(cwd, "rev-parse", "--git-dir")
	commonDir := runGit(cwd, "rev-parse", "--git-common-dir")
	if gitDir == "" || commonDir == "" || gitDir == commonDir {
		return false, ""
	}
	abs := commonDir
	if !filepath.IsAbs(abs) {
		abs = filepath.Join(cwd, commonDir)
	}
	abs = filepath.Clean(abs)
	mainRoot := filepath.Dir(abs)
	if home, err := os.UserHomeDir(); err == nil {
		if rel, err := filepath.Rel(home, mainRoot); err == nil &&
			rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			mainRoot = "~/" + rel
		}
	}
	return true, mainRoot
}

// runGit executes git plumbing in cwd and returns trimmed stdout ("" on
// failure — non-repo, detached plumbing errors, etc.).
func runGit(cwd string, args ...string) string {
	cmd := exec.Command("git", append([]string{"-C", cwd}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// ── x.ai client → agent extension methods ──────────────────────────
//
// Extension requests use the "_" prefix wire convention (same as
// _x.ai/session/updates above); the agent's decoder strips it and routes
// the remainder to the extension handler.

// ForkSession calls x.ai/session/fork: forks the current session into a new
// one (optionally a git worktree). Params follow the TUI's fork payload:
// {sourceSessionId, sourceCwd, newCwd, sessionKind:"fork", newSessionId?}.
func (b *Bridge) ForkSession(ctx context.Context, params map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid, cwd string
	if act != nil {
		sid = act.SessionID
		cwd = act.Cwd
	}
	b.mu.Unlock()
	p := map[string]any{
		"sourceSessionId": sid,
		"sourceCwd":       cwd,
		"newCwd":          cwd,
		"sessionKind":     "fork",
	}
	for k, v := range params {
		p[k] = v
	}
	return b.request(ctx, "_x.ai/session/fork", p, 60*time.Second)
}

// RenameSession calls x.ai/session/rename: {sessionId, title}.
func (b *Bridge) RenameSession(ctx context.Context, title string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid string
	if act != nil {
		sid = act.SessionID
	}
	b.mu.Unlock()
	return b.request(ctx, "_x.ai/session/rename", map[string]any{
		"sessionId": sid,
		"title":     title,
	}, 30*time.Second)
}

// Recap fires x.ai/recap (fire-and-forget "where was I" summary; the recap
// arrives later as a SessionRecap session/update). Returns the ack.
func (b *Bridge) Recap(ctx context.Context, auto bool) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid string
	if act != nil {
		sid = act.SessionID
	}
	b.mu.Unlock()
	return b.request(ctx, "_x.ai/recap", map[string]any{
		"sessionId": sid,
		"auto":      auto,
	}, 30*time.Second)
}

// SubagentCancel calls x.ai/subagent/cancel: {subagentId}.
func (b *Bridge) SubagentCancel(ctx context.Context, subagentID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/subagent/cancel", map[string]any{
		"subagentId": subagentID,
	}, 30*time.Second)
}

// TaskKill calls x.ai/task/kill: {sessionId, taskId}.
func (b *Bridge) TaskKill(ctx context.Context, taskID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid string
	if act != nil {
		sid = act.SessionID
	}
	b.mu.Unlock()
	return b.request(ctx, "_x.ai/task/kill", map[string]any{
		"sessionId": sid,
		"taskId":    taskID,
	}, 30*time.Second)
}

// TaskList calls x.ai/task/list: {sessionId} → {tasks: [TaskSnapshot…]}.
// Each snapshot includes output/command so the FE block viewer can show
// live stdout (TUI OpenBlockViewer for BgTask).
func (b *Bridge) TaskList(ctx context.Context) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	var sid string
	if act != nil {
		sid = act.SessionID
	}
	b.mu.Unlock()
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/task/list", map[string]any{
		"sessionId": sid,
	}, 30*time.Second)
}

// SessionDelete calls x.ai/session/delete: {sessionId} (defaults to the
// active session). Deletion can take a while (worktree cleanup etc.), so
// it gets a generous 60s timeout.
func (b *Bridge) SessionDelete(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	b.mu.Unlock()
	if sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/session/delete", map[string]any{
		"sessionId": sessionID,
	}, 60*time.Second)
}

// CompactConversation calls x.ai/compact_conversation: {sessionId, note?}
// (manual context compaction; the sessionId defaults to the active one).
func (b *Bridge) CompactConversation(ctx context.Context, sessionID, note string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	b.mu.Unlock()
	if sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	params := map[string]any{"sessionId": sessionID}
	if note != "" {
		params["note"] = note
	}
	return b.request(ctx, "_x.ai/compact_conversation", params, 60*time.Second)
}

// RewindPoints calls x.ai/rewind/points: {sessionId} → the agent's list of
// rewindable conversation points. Raw result is returned; the HTTP layer
// normalizes the field names (snake_case/camelCase) for the frontend.
func (b *Bridge) RewindPoints(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	b.mu.Unlock()
	if sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/rewind/points", map[string]any{
		"sessionId": sessionID,
	}, 60*time.Second)
}

// RewindExecute calls x.ai/rewind/execute: {sessionId, targetIndex} —
// rolls the conversation back to the given rewind point.
func (b *Bridge) RewindExecute(ctx context.Context, sessionID string, targetIndex int) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	b.mu.Unlock()
	if sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/rewind/execute", map[string]any{
		"sessionId":   sessionID,
		"targetIndex": targetIndex,
	}, 60*time.Second)
}

// SchedulerDelete calls x.ai/scheduler/delete: {sessionId, taskId} — stops
// a scheduled task (sessionId defaults to the active one).
func (b *Bridge) SchedulerDelete(ctx context.Context, sessionID, taskID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	b.mu.Unlock()
	if sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/scheduler/delete", map[string]any{
		"sessionId": sessionID,
		"taskId":    taskID,
	}, 30*time.Second)
}

// ── admin extension methods (billing / memory / plan / permissions / MCP) ─

// resolveSessionID returns the given sessionId, or the active session's id
// when empty ("" when no session is active).
func (b *Bridge) resolveSessionID(sessionID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		return act.SessionID
	}
	return sessionID
}

// Billing calls x.ai/billing: {sessionId} → account billing/quota info
// (sessionId defaults to the active session).
func (b *Bridge) Billing(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/billing", map[string]any{
		"sessionId": sessionID,
	}, 30*time.Second)
}

// MemoryFlush calls x.ai/memory/flush: {sessionId} — persists the session's
// memory (sessionId defaults to the active session).
func (b *Bridge) MemoryFlush(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/flush", map[string]any{
		"sessionId": sessionID,
	}, 30*time.Second)
}

// MemoryRewrite calls x.ai/memory/rewrite: {sessionId} — rewrites the
// session's memory from the conversation (sessionId defaults to the active
// session).
func (b *Bridge) MemoryRewrite(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/rewrite", map[string]any{
		"sessionId": sessionID,
	}, 30*time.Second)
}

// TogglePlanMode calls x.ai/toggle_plan_mode: {sessionId} — switches the
// session's plan mode on/off. The HTTP layer tries to extract the resulting
// planMode bool from the reply.
func (b *Bridge) TogglePlanMode(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/toggle_plan_mode", map[string]any{
		"sessionId": sessionID,
	}, 30*time.Second)
}

// PermissionsReset calls x.ai/permissions/reset: {sessionId} — clears the
// session's remembered permission decisions (sessionId defaults to the
// active session).
func (b *Bridge) PermissionsReset(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/permissions/reset", map[string]any{
		"sessionId": sessionID,
	}, 30*time.Second)
}

// MCPList calls x.ai/mcp/list → the agent's MCP server registry. MCP
// servers are host-global (not session-scoped), so no sessionId is sent
// and the absence of an active session is not an error — the agent is the
// authority on whether it needs one.
func (b *Bridge) MCPList(ctx context.Context) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/mcp/list", map[string]any{}, 30*time.Second)
}

// MCPToggle calls x.ai/mcp/toggle: {name, enabled} — enables/disables one
// MCP server.
func (b *Bridge) MCPToggle(ctx context.Context, name string, enabled bool) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/mcp/toggle", map[string]any{
		"name":    name,
		"enabled": enabled,
	}, 30*time.Second)
}

// MCPUpsert calls x.ai/mcp/upsert: {server: {name, command, args?, env?}}
// — adds or updates one MCP server. The server object is passed through
// verbatim; the agent is the authority on its schema.
func (b *Bridge) MCPUpsert(ctx context.Context, server map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/mcp/upsert", map[string]any{
		"server": server,
	}, 30*time.Second)
}

// MCPDelete calls x.ai/mcp/delete: {name} — removes one MCP server.
func (b *Bridge) MCPDelete(ctx context.Context, name string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/mcp/delete", map[string]any{
		"name": name,
	}, 30*time.Second)
}

// MCPAuthTrigger calls x.ai/mcp/auth_trigger: {name} — starts the OAuth
// flow for one MCP server.
func (b *Bridge) MCPAuthTrigger(ctx context.Context, name string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/mcp/auth_trigger", map[string]any{
		"name": name,
	}, 30*time.Second)
}

// Shutdown kills grok.
func (b *Bridge) Shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	if b.cancelRd != nil {
		b.cancelRd()
	}
}

// HTTPError carries an HTTP status.
type HTTPError struct {
	Code int
	Msg  string
}

func (e *HTTPError) Error() string { return e.Msg }

// contentText extracts text from an ACP ContentBlock-like value.
func contentText(v any) string {
	switch c := v.(type) {
	case string:
		return c
	case map[string]any:
		if t, ok := c["text"].(string); ok {
			return t
		}
		// nested content
		if inner, ok := c["content"]; ok {
			return contentText(inner)
		}
	case []any:
		var b strings.Builder
		for _, item := range c {
			b.WriteString(contentText(item))
		}
		return b.String()
	}
	return ""
}

// contentImages scans an ACP content value — a plain string, a single
// block, or a (possibly nested) block array — for image blocks shaped
// {type:"image", data, mimeType|mime}. It mirrors contentText's traversal
// so image blocks are found without disturbing the text path.
func contentImages(v any) []map[string]any {
	var out []map[string]any
	var walk func(any)
	walk = func(v any) {
		switch c := v.(type) {
		case map[string]any:
			if t, _ := c["type"].(string); t == "image" {
				out = append(out, c)
				return
			}
			if inner, ok := c["content"]; ok {
				walk(inner)
			}
		case []any:
			for _, item := range c {
				walk(item)
			}
		}
	}
	walk(v)
	return out
}

// imageEvent builds the SSE image event for one image block:
// {type:"image", sessionId, data, mimeType}. data is passed through
// as-is (raw base64 or a data URI); blocks without a string data are
// skipped (ok=false). mimeType falls back to image/png when absent.
func imageEvent(sid string, block map[string]any) (Event, bool) {
	data, ok := block["data"].(string)
	if !ok {
		return nil, false
	}
	mime, _ := block["mimeType"].(string)
	if mime == "" {
		mime, _ = block["mime"].(string)
	}
	if mime == "" {
		mime = "image/png"
	}
	return Event{"type": "image", "sessionId": sid, "data": data, "mimeType": mime}, true
}
