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
	"strconv"
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

	ready   bool
	booting bool
	// bootDone is closed when an in-flight boot attempt finishes (success
	// or failure). Concurrent ensureBooted callers wait on it instead of
	// racing the boot and failing with "grok 进程未运行".
	bootDone  chan struct{}
	bootError string
	agentInfo map[string]any
	// authMeta: the `_meta` from the authenticate response (AuthMeta:
	// email/auth_mode/team_id/team_name/is_zdr/team_role/
	// coding_data_retention_opt_out/show_resolved_model/gate/
	// subscription_tier), surfaced in Status and ready events. Nil when
	// the agent returned no `_meta` (absent key ≠ off).
	authMeta map[string]any
	// initAgentCapabilities is the `agentCapabilities` object from the
	// agent's initialize response, surfaced in Status/Snapshot.
	initAgentCapabilities any
	textBuf               string
	// genRate 是 per-session 的生成输出速率（tok/s）估算器（见
	// genrate.go）：chunk 到达时观察、tool_call / 回合终态 seal、
	// user_message_chunk 复位；节流后广播 gen_rate 事件。
	genRate *genRateTracker
	// agentStartedAt (unix ms) stamps the CURRENT agent process spawn.
	// Clients compare it across hello events to detect an agent restart —
	// the agent's permission mode is in-memory only and resets on restart,
	// so the browser re-seeds its known flags to keep behavior in sync.
	agentStartedAt int64
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

	// ── goal engine (host-side /goal, see goal.go) ────────────────────
	goalMu     sync.Mutex
	goal       *GoalState
	goalStop   chan struct{} // closed to end the goal loop
	goalLoopOn bool

	subscribersMu sync.Mutex
	subscribers   map[chan Event]struct{}

	// eventSeq 是广播事件的全局单调序号（Broadcast 附加到每个事件）。
	// 本地 SSE 与 hub 中继（/ws/fe 推回）携带同一 seq，前端选中本机
	// host 时双路（本地 SSE + hub WS）收到同一事件可按 (hostId, seq)
	// 去重；hub-client 转发时保留该 seq（不再自行分配）。
	eventSeq uint64

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

// RPCError is a JSON-RPC error reply from the agent — the agent process
// is alive and answered (it parsed the request and rejected it), it just
// failed the call itself (e.g. the model API's 400 "Internal Error: …").
// Distinct from transport failures (timeout / write error / boot failure),
// which mean the agent may be wedged: callers keep the process for
// RPCError and only self-heal (kill + restore) on real transport failures.
type RPCError struct {
	Code int
	Msg  string
}

func (e *RPCError) Error() string { return e.Msg }

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
	// meta: ACP response `_meta` for session/request_permission replies —
	// the bash scope (BashCommandSelectedTerms: command_parts/is_glob) or
	// the followup_message the client attached, serialized exactly like the
	// TUI sends them.
	meta map[string]any
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
		genRate:     newGenRateTracker(),
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
	ch = make(chan Event, 512)
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
	// 全局事件序号：所有订阅者（本地 SSE、hub-client 转发）看到同一
	// 事件同一 seq。注意 hub-client 转发时必须保留该 seq（见其
	// seqAndReplay），否则双路去重失效。
	b.eventSeq++
	ev["seq"] = b.eventSeq
	for ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// Slow consumer: drop. Seq still advances for other
			// subscribers, so a lagging FE/SSE client may see a hole
			// (e.g. 1,3). That is expected: FE gap-pulls via
			// GET /api/events?after= and (hostId,seq) dedup makes
			// replays of already-seen seqs harmless. Do not merge
			// chunks just to hide holes — that breaks dual-path seq
			// identity with the hub uplink.
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

// releaseBusy drops one in-flight prompt's claim on a session and, when
// the LAST in-flight turn finished, flips the session idle and notifies
// clients (busy → idle transition only — a turn that resolves while a
// sibling is still running must not report an idle the session never
// had). It only releases when the roster still holds the SAME session
// object the turn started on: killProcess wipes and rebuilds the roster
// (and a failed turn's auto-recovery may restore the session), so a stale
// turn must not clear the busy flag of a newer turn on the restored
// session.
func (b *Bridge) releaseBusy(s *SessionState, sessionID string) {
	b.mu.Lock()
	last := false
	if cur := b.sessions[sessionID]; cur == s && s.busyCount > 0 {
		s.busyCount--
		if s.busyCount == 0 {
			s.Busy = false
			last = true
		}
	}
	b.mu.Unlock()
	if last {
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

// sessionIdExplicit returns the sessionId explicitly carried by
// notification params — camelCase "sessionId" (the standard carrier key)
// or snake_case "session_id" (some x.ai rails) — and "" when absent. NO
// active-session fallback: the official session/update carrier always
// carries the id, so handleSessionUpdate uses this directly — falling back
// to the host's active session there would mis-tag events in multi-client
// setups.
func sessionIdExplicit(params map[string]any) string {
	if sid, ok := params["sessionId"].(string); ok && sid != "" {
		return sid
	}
	if sid, ok := params["session_id"].(string); ok && sid != "" {
		return sid
	}
	return ""
}

// sessionIdFrom returns the sessionId carried by an agent notification
// envelope: the explicit params id wins (sessionIdExplicit); otherwise the
// active session is the defensive fallback. Only rails that genuinely
// never carry an explicit id (machine-wide broadcasts such as
// x.ai/announcements/update / x.ai/models/update / x.ai/leader_reconnected)
// hit the fallback — for those the active-session tag is the host's best
// guess. Session-scoped carriers always carry the id and never fall back.
func (b *Bridge) sessionIdFrom(params map[string]any) string {
	if sid := sessionIdExplicit(params); sid != "" {
		return sid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if act := b.activeSessionLocked(); act != nil {
		return act.SessionID
	}
	return ""
}

// cloneAny deep-copies JSON-like structures (maps/slices) so snapshots
// handed out for lock-free serialization never share storage with live
// bridge state (a concurrent in-place map write during json.Marshal is a
// fatal race — "concurrent map read and map write").
func cloneAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneAny(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneAny(val)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(t))
		for k, val := range t {
			out[k] = val
		}
		return out
	default:
		return v
	}
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
		row := *s
		// Deep-copy the shared map fields: the roster rows are serialized
		// outside b.mu (SSE hello / /api/status / /api/session-state) while
		// readStdout may still mutate them under the lock (model changes,
		// usage updates). Sharing the maps would race the serializer.
		row.modes = cloneAny(s.modes)
		row.configOpts = cloneAny(s.configOpts)
		row.models = cloneAny(s.models)
		row.sessionMeta = cloneAny(s.sessionMeta)
		roster = append(roster, row)
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
	var modes, configOpts, models, sessionMeta any
	if act != nil {
		sid = act.SessionID
		cwd = act.Cwd
		modes = cloneAny(act.modes)
		configOpts = cloneAny(act.configOpts)
		models = cloneAny(act.models)
		sessionMeta = cloneAny(act.sessionMeta)
	}
	// AuthMeta/SessionMeta 是 any 字段：有类型的 nil map 装进 any 后
	// omitempty 会失效（输出 null），所以 nil 时不装。
	var authMeta any
	if b.authMeta != nil {
		authMeta = b.authMeta
	}
	return Status{
		Ready:             b.ready,
		Busy:              busy,
		Booting:           b.booting,
		SessionID:         sid,
		Cwd:               cwd,
		HostID:            b.hostID,
		HostName:          b.hostName,
		HomeDir:           b.homeDir,
		AgentInfo:         b.agentInfo,
		AgentCapabilities: b.initAgentCapabilities,
		AuthMeta:          authMeta,
		Modes:             modes,
		ConfigOptions:     configOpts,
		SessionMeta:       sessionMeta,
		Models:            models,
		BootError:         b.bootError,
		Text:              b.textBuf,
		PendingRequests:   pending,
		Capabilities:      DefaultClientCaps(),
		Roster:            roster,
		AgentStartedAt:    b.agentStartedAt,
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
		// The waiter's own ctx may be cancelled first (e.g. the client
		// disconnected), but that is NOT a boot failure: the boot is
		// bounded by bootTimeout and may still succeed. Reporting ctx.Err()
		// here would be a false failure — a caller could treat it as a
		// wedged agent and killProcess a healthy process. Keep waiting for
		// the in-flight boot (bootDone always closes, even on boot error)
		// and return its real outcome.
		<-done
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
			// stays agent-side. codeNavigation / folderTrust / fs_notify are
			// env-opt-in (absent = off, same as the TUI).
			"meta": initCapabilitiesMeta(),
		},
		"clientInfo": map[string]any{
			"name":    "acp-host",
			"title":   "ACP Host",
			"version": "0.1.0",
		},
		// `_meta` mirrors the Grok Build TUI's build_initialize_meta
		// (xai-grok-pager/src/acp/mod.rs): clientType defaults to the
		// pager's PAGER_CLIENT_TYPE and clientVersion to its build-time
		// version; the remaining seeds are env-driven and omitted when the
		// env var is absent (matching the TUI's default omit).
		"_meta": initMetaSeeds(),
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
	b.initAgentCapabilities = initRes["agentCapabilities"]
	b.mu.Unlock()

	// protocolVersion is informational for the host: a mismatch (the agent
	// speaking a different protocol) is only logged, never fatal — the wire
	// surface we use is small and stable. (JSON numbers decode as float64.)
	if pv, ok := initRes["protocolVersion"]; ok {
		if n, isNum := asInt(pv); !isNum || n != protocolVersion {
			log.Printf("[acp-host] agent protocolVersion = %v, host expects %d (continuing)", pv, protocolVersion)
		}
	}

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
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	b.mu.Unlock()
	if noSession {
		ev := Event{
			"type":      "ready",
			"agentInfo": agentInfo,
			"hostId":    hostID,
			"hostName":  hostName,
		}
		// authenticate 响应 `_meta`（AuthMeta）与 agentInfo 并列透传，
		// 仅非空才带（absent key ≠ off）。
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
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

// initClientType / initClientVersion mirror the Grok Build TUI's initialize
// `_meta` (xai-grok-pager/src/client_identity.rs): PAGER_CLIENT_TYPE is the
// literal "grok-pager"; PAGER_CLIENT_VERSION is the build-time crate version,
// which the host approximates with its own release constant.
const (
	initClientType    = "grok-pager"
	initClientVersion = "0.1.0"
)

// initMetaSeeds builds the initialize request `_meta` (the TUI's
// build_initialize_meta analog): clientType/clientVersion always present,
// the remaining seeds only when their env var is set (absent env = key
// omitted, matching the TUI's default). Verified against the agent's reads:
// clientIdentifier (acp_agent.rs:134), clientSource (mod.rs:2212),
// systemPromptOverride (mod.rs:1119), rules (mod.rs:1099), mcpApps
// (acp_agent.rs:167), bufferingSettings (acp_agent.rs:176), startupHints
// (agent_ops.rs:4108).
func initMetaSeeds() map[string]any {
	meta := map[string]any{
		"clientType":    initClientType,
		"clientVersion": initClientVersion,
	}
	addStringEnv(meta, "clientIdentifier", "ACP_INIT_CLIENT_IDENTIFIER")
	addStringEnv(meta, "clientSource", "ACP_INIT_CLIENT_SOURCE")
	addStringEnv(meta, "systemPromptOverride", "ACP_INIT_SYSTEM_PROMPT_OVERRIDE")
	addStringEnv(meta, "rules", "ACP_INIT_RULES")
	if boolEnv("ACP_INIT_MCP_APPS") {
		meta["mcpApps"] = true
	}
	if v := jsonEnv("ACP_INIT_BUFFERING_SETTINGS"); v != nil {
		// BufferingSettings is camelCase on the wire (update_chunk_merge.rs).
		meta["bufferingSettings"] = v
	}
	if v := jsonEnv("ACP_INIT_STARTUP_HINTS"); v != nil {
		// StartupHints is camelCase (session/acp_types.rs).
		meta["startupHints"] = v
	}
	return meta
}

// initCapabilitiesMeta builds the clientCapabilities.meta object: the four
// always-on TUI capabilities plus env-opt-in codeNavigation / folderTrust /
// fs_notify (absent = off, matching the TUI where those keys are absent).
func initCapabilitiesMeta() map[string]any {
	meta := map[string]any{
		"x.ai/incrementalBashOutput": true,
		"x.ai/bashOutputNoColor":     true,
		"x.ai/gitHeadChanged":        true,
		"x.ai/hunkTracker":           map[string]any{"mode": "agent_only"},
	}
	if boolEnv("ACP_CAP_CODE_NAVIGATION") {
		// Agent reads meta["x.ai/codeNavigation"]["enabled"] (code_nav.rs:9).
		meta["x.ai/codeNavigation"] = map[string]any{"enabled": true}
	}
	if boolEnv("ACP_CAP_FOLDER_TRUST_INTERACTIVE") {
		// Agent reads meta["x.ai/folderTrust"]["interactive"]
		// (folder_trust_prompt.rs:75).
		meta["x.ai/folderTrust"] = map[string]any{"interactive": true}
	}
	if boolEnv("ACP_CAP_FS_NOTIFY") {
		// Agent reads meta["x.ai/fs_notify"] as a bool (agent_ops.rs:4044).
		meta["x.ai/fs_notify"] = true
	}
	return meta
}

// addStringEnv adds key → env value to m when the env var is non-empty.
func addStringEnv(m map[string]any, key, env string) {
	if v := os.Getenv(env); v != "" {
		m[key] = v
	}
}

// boolEnv reports whether an env var is set to a truthy value
// (1/true/yes/on; case-insensitive). Unset or anything else = false.
func boolEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// jsonEnv parses an env var as a JSON object; nil when unset or invalid.
func jsonEnv(name string) map[string]any {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}
	var v map[string]any
	if json.Unmarshal([]byte(raw), &v) != nil {
		return nil
	}
	return v
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
	params := map[string]any{
		"cwd":                   cwd,
		"additionalDirectories": addDirs,
		"mcpServers":            mcp,
	}
	// Client-supplied session seeds (permission mode flags etc.) ride the
	// params `_meta`, exactly like the TUI's SessionFlags.to_meta() —
	// absent key ≠ off, so only send when the client provided seeds.
	if len(sc.Meta) > 0 {
		params["_meta"] = sc.Meta
	}

	sessRes, err := b.request(ctx, "session/new", params, bootTimeout)
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
	// session/new 响应 `_meta`（agent 下发）原样存下，ready 事件 / Status
	// 透传；缺省为 nil（absent key ≠ off）。
	s.sessionMeta = sessRes["_meta"]
	b.activeSessionID = sid
	b.rememberSessionLocked(sid, cwd)
	b.textBuf = ""
	b.ready = true
	b.bootError = ""
	modes := s.modes
	configOpts := s.configOpts
	models := s.models
	sessionMeta := s.sessionMeta
	agentInfo := b.agentInfo
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := s.Cwd
	b.mu.Unlock()

	ev := Event{
		"type":          "ready",
		"sessionId":     sid,
		"cwd":           sessCwd,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"models":        models,
		"hostId":        hostID,
		"hostName":      hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
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
	// The agent's AuthRequestMeta (mvp_agent/mod.rs:1203) reads
	// headless/reauth/use_oauth/force_interactive/request_seq from the
	// authenticate `_meta`. headless stays true; the rest are env-driven
	// and omitted when absent (absent key = current behavior exactly).
	meta := map[string]any{"headless": true}
	if boolEnv("ACP_AUTH_REAUTH") {
		meta["reauth"] = true
	}
	if boolEnv("ACP_AUTH_USE_OAUTH") {
		meta["use_oauth"] = true
	}
	if boolEnv("ACP_AUTH_FORCE_INTERACTIVE") {
		meta["force_interactive"] = true
	}
	if v, ok := intEnv("ACP_AUTH_REQUEST_SEQ"); ok {
		meta["request_seq"] = v
	}
	res, err := b.request(ctx, "authenticate", map[string]any{
		"methodId": methodID,
		"_meta":    meta,
	}, bootTimeout)
	if err != nil {
		return err
	}
	// AuthMeta: 响应 `_meta`（email/auth_mode/team_id/team_name/is_zdr/
	// team_role/coding_data_retention_opt_out/show_resolved_model/gate/
	// subscription_tier）原样存下，Status / ready 事件透传；缺省为 nil
	// （absent key ≠ off）。显式归 nil：类型断言的失败值是有类型的 nil
	// map，直接存入 any 字段会让 omitempty 失效。
	b.mu.Lock()
	b.authMeta = nil
	if m, ok := res["_meta"].(map[string]any); ok {
		b.authMeta = m
	}
	b.mu.Unlock()
	return nil
}

// intEnv parses an env var as an integer (ok=false when unset/invalid).
func intEnv(name string) (int64, bool) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
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
	b.agentStartedAt = time.Now().UnixMilli()
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
			// Never die silently: a scan error (e.g. a line exceeding the
			// 64MB buffer) would otherwise leave the bridge parsing nothing
			// while the agent keeps running — every session appears frozen.
			if err := sc.Err(); err != nil && err != io.EOF {
				log.Printf("[acp-host] stdout 扫描错误: %v — 触发 agent 重启自愈", err)
				go b.killProcess()
			}
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
			code := 0
			if m, ok := errObj.(map[string]any); ok {
				if s, ok := m["message"].(string); ok {
					em = s
				}
				if c, ok := m["code"].(float64); ok {
					code = int(c)
				}
			}
			// Typed wrap: the agent REPLIED with an error, so the process is
			// healthy — callers distinguish this from transport failures
			// (timeout etc.) and skip the kill+restore self-heal.
			c <- rpcResult{err: &RPCError{Code: code, Msg: em}}
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
	// 官方载体恒带 sessionId：直接用显式 id，绝不回落活动会话（多客户端
	// 下回落会误标到宿主的活动会话）。
	sid := sessionIdExplicit(params)

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
	// Kind 分发与 x.ai 通道共享（dispatchSessionUpdateKind）。modeled
	// kind 只发 typed 事件（FE 已适配）；仅当 dispatch 返回 false
	// （未建模 kind）时 generic session_notification 作为前向兼容载体
	// 发出，形状保持现状。
	if !b.dispatchSessionUpdateKind(sid, params, func(ev Event) Event {
		ev["sessionId"] = sid
		return ev
	}) {
		b.Broadcast(Event{
			"type":      "session_notification",
			"method":    "session/update",
			"params":    map[string]any{"update": update},
			"sessionId": sid,
		})
	}

	// grok ships usage inside turn_completed / response_completed
	// updates instead of the standard usage_update carrier — surface it
	// as a normal usage event, exactly like handleXaiNotification does:
	//   - used:  context-window usage, `_meta.totalTokens` (the TUI ⇣
	//            counter source).
	//   - usage: the standard TurnCompleted/ResponseCompleted usage
	//            object passed through untouched.
	// typed 事件（dispatch 已发）是 FE 回合封口语义；usage 提取保留在此
	// （官方载体带 size:nil；x.ai 载体不带 —— 保持原状，不统一）。
	if kind, _ := update["sessionUpdate"].(string); kind == "turn_completed" || kind == "response_completed" {
		ev := Event{"type": "usage"}
		if meta, ok := params["_meta"].(map[string]any); ok {
			if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
				ev["used"] = used
				b.trackUsage(sid, used, 0)
			}
		}
		if u, ok := update["usage"].(map[string]any); ok {
			ev["usage"] = u
		}
		// Only broadcast when there is something to update —
		// an empty usage event (no _meta.totalTokens, no usage
		// object) would otherwise null the client's `used`.
		if len(ev) > 1 {
			ev["size"] = nil
			ev["sessionId"] = sid
			b.Broadcast(ev)
		}
	}
}

// dispatchSessionUpdateKind routes one sessionUpdate `update` to its typed
// events. Returns whether the kind is modeled (handled): true → the caller
// must NOT emit the generic session_notification (FE 消费 typed 事件);
// false → the kind is unmodeled and the caller emits the generic
// session_notification as the forward-compat carrier (compaction_checkpoint
// / rewind_marker / unknown 及未来任何新 kind). Shared by BOTH sessionUpdate
// carriers — the official session/update envelope (handleSessionUpdate) and
// the x.ai channel (x.ai/session_notification / x.ai/session/update,
// handleXaiNotification) — so every kind behaves identically regardless of
// which rail it rides.
//
//   - tag: sessionId 归属约定（官方载体恒打 sessionId；x.ai 载体空则省略，
//     withSid 约定）。
//
// Typed kind 事件（{type: <kind>, update, sessionId, meta?}）在
// params._meta 非空时携带 `meta`（官方与 x.ai 载体统一，两个载体都可能有
// _meta——eventId 等）；仅非空才带（absent key ≠ off）。
func (b *Bridge) dispatchSessionUpdateKind(sid string, params map[string]any, tag func(Event) Event) bool {
	update, _ := params["update"].(map[string]any)
	if update == nil {
		return false
	}
	kind, _ := update["sessionUpdate"].(string)
	// meta 保全：params._meta 非空时随 typed kind 事件携带。
	var kindMeta any
	if m, ok := params["_meta"].(map[string]any); ok && len(m) > 0 {
		kindMeta = m
	}
	switch kind {
	case "agent_message_chunk":
		if text := contentText(update["content"]); text != "" {
			b.mu.Lock()
			b.textBuf += text
			b.mu.Unlock()
			ev := Event{"type": "chunk", "text": text}
			if mid, ok := update["messageId"]; ok {
				ev["messageId"] = mid
			}
			// Mirror user_message_chunk: forward the chunk + content-block
			// meta (wire shape: update._meta = ContentChunk.meta →
			// hideFromScrollback; update.content._meta = TextContent.meta →
			// displayText / displayAsCron) so live rendering classifies
			// system-injected prompts exactly like the replay path does.
			if meta, ok := update["_meta"].(map[string]any); ok {
				if v, ok := meta["hideFromScrollback"]; ok {
					ev["hideFromScrollback"] = v
				}
			}
			if content, ok := update["content"].(map[string]any); ok {
				if meta, ok := content["_meta"].(map[string]any); ok {
					for _, k := range []string{"displayText", "displayAsCron"} {
						if v, ok := meta[k]; ok {
							ev[k] = v
						}
					}
				}
			}
			// fullUpdate carries the ENTIRE original update map so no
			// field of the agent_message_chunk payload is ever dropped.
			ev["fullUpdate"] = update
			b.Broadcast(tag(ev))
			// 生成输出速率（按流式字符估算 + usage 自校准，见 genrate.go）：
			// 与 chunk 同轨广播，节流后每个 session 每 ≥250ms 至多一条
			// active。时间源优先 _meta.agentTimestampMs（agentClock=true，
			// 取消 800ms 空闲封顶等防攒包启发式）。
			now, agentClock := metaAgentTs(params)
			if rate, ok := b.genRate.observe(sid, text, now, agentClock); ok {
				b.Broadcast(tag(Event{"type": "gen_rate", "rate": rate, "active": true}))
			}
		}
		// Image blocks ride the same chunk carrier: emit one typed image
		// event per block (text parts still go through the chunk event).
		for _, img := range contentImages(update["content"]) {
			if ev, ok := imageEvent(sid, img); ok {
				b.Broadcast(ev)
			}
		}
		return true
	case "user_message_chunk":
		// 用户输入打断生成段：静默复位速率估算器（不发任何事件，
		// 客户端显示值在下一段 live 更新前保留）。
		b.genRate.reset(sid)
		if text := contentText(update["content"]); text != "" {
			ev := Event{"type": "user_chunk", "text": text}
			// Forward the chunk + content-block meta (wire shape:
			// update._meta = ContentChunk.meta → hideFromScrollback;
			// update.content._meta = TextContent.meta → displayText /
			// displayAsCron) so live rendering classifies system-injected
			// prompts exactly like the replay path does.
			if meta, ok := update["_meta"].(map[string]any); ok {
				if v, ok := meta["hideFromScrollback"]; ok {
					ev["hideFromScrollback"] = v
				}
			}
			if content, ok := update["content"].(map[string]any); ok {
				if meta, ok := content["_meta"].(map[string]any); ok {
					for _, k := range []string{"displayText", "displayAsCron"} {
						if v, ok := meta[k]; ok {
							ev[k] = v
						}
					}
				}
			}
			// fullUpdate carries the ENTIRE original update map so no
			// field of the user_message_chunk payload is ever dropped.
			ev["fullUpdate"] = update
			b.Broadcast(tag(ev))
		}
		for _, img := range contentImages(update["content"]) {
			if ev, ok := imageEvent(sid, img); ok {
				b.Broadcast(ev)
			}
		}
		return true
	case "agent_thought_chunk":
		// content is typically { "type":"text", "text":"..." }; accept a few shapes
		if text := contentText(update["content"]); text != "" {
			ev := Event{"type": "thought", "text": text}
			// Forward the chunk meta verbatim (hideFromScrollback etc.)
			// so live rendering treats it like the replay path does.
			if meta, ok := update["_meta"].(map[string]any); ok {
				ev["meta"] = meta
			}
			b.Broadcast(tag(ev))
			// 思考文本同样更新生成速率（见 genrate.go）。
			now, agentClock := metaAgentTs(params)
			if rate, ok := b.genRate.observe(sid, text, now, agentClock); ok {
				b.Broadcast(tag(Event{"type": "gen_rate", "rate": rate, "active": true}))
			}
		}
		return true
	case "tool_call":
		// 工具执行打断生成段：seal 冻结速率（active:false 不受节流
		// 限制；本段从未发布过速率则静默）。
		if rate, ok := b.genRate.seal(sid); ok {
			b.Broadcast(tag(Event{"type": "gen_rate", "rate": rate, "active": false}))
		}
		b.Broadcast(tag(Event{"type": "tool_call", "toolCall": update}))
		return true
	case "tool_call_update":
		b.Broadcast(tag(Event{"type": "tool_call_update", "toolCallUpdate": update}))
		return true
	case "plan":
		b.Broadcast(tag(Event{"type": "plan", "entries": update["entries"]}))
		return true
	case "usage_update":
		b.trackUsage(sid, toInt64(update["used"]), toInt64(update["size"]))
		b.Broadcast(tag(Event{
			"type": "usage",
			"used": update["used"],
			"size": update["size"],
			"cost": update["cost"],
		}))
		return true
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
		b.Broadcast(tag(Event{"type": "modes_update", "modes": modes}))
		return true
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
		b.Broadcast(tag(Event{"type": "config_options_update", "configOptions": co}))
		return true
	case "available_commands_update":
		b.Broadcast(tag(Event{"type": "commands_update", "commands": update["commands"]}))
		return true
	case "session_info_update", "session_info":
		// session_info_update 是官方 ACP SessionUpdate 的 kind（serde tag
		// "sessionUpdate"，agent-client-protocol-schema client.rs:106）；
		// 旧版 x.ai 载体曾用裸 "session_info" 形式——两者等价合并。
		// title/updatedAt roster 跟踪两个载体都要有。
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
		b.Broadcast(tag(Event{
			"type":      "session_info",
			"title":     update["title"],
			"updatedAt": update["updatedAt"],
		}))
		return true
	case "scheduled_task_created":
		// 复用 x.ai 通道的归一化 helper（task/rawTask/rawParams/meta 保全，
		// 见 broadcastScheduledTaskCreated）。
		b.broadcastScheduledTaskCreated(sid, params, tag)
		return true
	case "scheduled_task_deleted":
		b.broadcastScheduledTaskDeleted(sid, params, tag)
		return true
	case "task_backgrounded", "task_completed", "monitor_event":
		// 形状与其它 kind typed 事件统一：{type, update, sessionId}
		// （update = 原始 update 对象）。x.ai standalone 通知通道
		// （x.ai/task_backgrounded / x.ai/task_completed /
		// x.ai/monitor_event 方法）保持 {type, params, sessionId} 不动
		// —— 不同载体，形状跟随 wire（见 handleXaiNotification）。
		ev := Event{"type": kind, "update": update}
		if kindMeta != nil {
			ev["meta"] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	case "model_auto_switched", "model_changed":
		// roster models 缓存更新 + models_update / model 广播（typed 语义
		// 事件，见 handleModelSwitchKind）；kind modeled → 无 generic，
		// FE 改用 typed 消费。
		b.handleModelSwitchKind(sid, update, tag)
		return true
	case "turn_completed", "response_completed":
		// 回合终态：seal 生成段并冻结速率（同 tool_call）；update 携带
		// 真实 usage 时改发精确值（真实 token / 累计生成时长）并自校准
		// 估算系数（见 genrate.go sealWithUsage）。
		var rate float64
		var ok bool
		if real, has := usageRealTokens(update); has {
			rate, ok = b.genRate.sealWithUsage(sid, real)
		} else {
			rate, ok = b.genRate.seal(sid)
		}
		if ok {
			b.Broadcast(tag(Event{"type": "gen_rate", "rate": rate, "active": false}))
		}
		// typed 事件是 FE 回合封口语义（update 原样含 stop_reason /
		// prompt_id / usage 等字段）；usage 提取保留在两个载体
		// （handleSessionUpdate / handleXaiNotification）。
		ev := Event{"type": kind, "update": update}
		if kindMeta != nil {
			ev["meta"] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	case "compaction_checkpoint", "rewind_marker", "unknown":
		// 仅持久化、不发 typed UI（grokbuild-acp-protocol.md §13：
		// compaction checkpoint / rewind marker 不上 live 通道）；unknown
		// 前向兼容忽略。未建模 → 由调用方发 generic（保持不变）。
		return false
	case "diff_review", "retry_state", "auto_compact_started",
		"auto_compact_completed", "auto_compact_failed", "auto_compact_cancelled",
		"auto_continue_completed", "memory_flush_started", "memory_flush_completed",
		"memory_dream_completed", "memory_session_saved", "memory_files",
		"feedback_request", "relay_sync_status", "auto_recovery_started",
		"auto_recovery_exhausted", "hook_annotation", "hook_execution",
		"hooks_changed", "plugins_changed", "plugin_updates_installed",
		"session_summary_generated", "session_recap", "session_recap_unavailable",
		"last_turn_summary", "subagent_spawned", "subagent_progress",
		"subagent_finished",
		"scheduled_task_fired", "tool_call_delta_chunk", "image_compressed",
		"image_dropped", "workflow_updated", "goal_updated",
		"pending_interaction", "interaction_resolved", "response_started",
		"reasoning_completed":
		// 单发 typed 事件：{type: <kind>, update: <原始 update>, sessionId,
		// meta?}（sessionId 空则省略，withSid 约定；params._meta 非空时带
		// meta），字段原样透传不归一化。载荷形状以 grok 源码
		// extensions/notification.rs 的 SessionUpdate 枚举（tag =
		// "sessionUpdate"，snake_case）为准。modeled → 无 generic。
		ev := Event{"type": kind, "update": update}
		if kindMeta != nil {
			ev["meta"] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	default:
		// 未建模的 kind（未来任何新 kind）：generic session_notification
		// 由调用方发出（前向兼容，不新增 typed 事件）。
		return false
	}
}

// broadcastScheduledTaskCreated normalizes the scheduled_task_created wire
// shapes — the SessionUpdate::ScheduledTaskCreated carrier（snake_case
// task_id/prompt/human_schedule/next_fire_at，extensions/notification.rs）
// or the standalone x.ai notification（camelCase task 对象 / 顶层
// taskId）— into ONE typed event. The normalized `task` keeps the FE
// contract (taskId/prompt/interval/nextFireAt); `rawTask`/`rawParams`/
// `meta` preserve every remaining original field (humanSchedule, status,
// enabled, createdAt, lastFiredAt, timezone, _meta.eventId /
// x.ai/schedulerGeneration / x.ai/schedulerRevision, …) so nothing is
// dropped. Shared by the x.ai method channel (x.ai/scheduled_task_created)
// and the sessionUpdate kind dispatch.
func (b *Bridge) broadcastScheduledTaskCreated(sid string, params map[string]any, tag func(Event) Event) {
	task, _ := params["task"].(map[string]any)
	update, _ := params["update"].(map[string]any)
	ev := Event{
		"type": "scheduled_task_created",
		"task": map[string]any{
			"taskId":     pick([]map[string]any{task, update, params}, "taskId", "task_id"),
			"prompt":     pick([]map[string]any{task, update, params}, "prompt"),
			"interval":   pick([]map[string]any{task, update, params}, "interval", "humanSchedule", "human_schedule"),
			"nextFireAt": pick([]map[string]any{task, update, params}, "nextFireAt", "next_fire_at"),
		},
	}
	// Full original payloads ride along — nothing is dropped anymore.
	ev["rawParams"] = params
	if len(update) > 0 {
		ev["rawTask"] = update
	}
	if meta, ok := params["_meta"].(map[string]any); ok {
		ev["meta"] = meta
	}
	b.Broadcast(tag(ev))
}

// broadcastScheduledTaskDeleted normalizes the scheduled_task_deleted wire
// shapes (SessionUpdate::ScheduledTaskDeleted {task_id} or the standalone
// x.ai notification) into ONE typed event. rawParams preserves the full
// original params (e.g. session_id + _meta.eventId / scheduler
// generation-revision stamps). Shared by the x.ai method channel and the
// sessionUpdate kind dispatch.
func (b *Bridge) broadcastScheduledTaskDeleted(sid string, params map[string]any, tag func(Event) Event) {
	task, _ := params["task"].(map[string]any)
	update, _ := params["update"].(map[string]any)
	ev := Event{
		"type":   "scheduled_task_deleted",
		"taskId": pick([]map[string]any{task, update, params}, "taskId", "task_id"),
	}
	// rawParams preserves the full original params (e.g. session_id +
	// _meta.eventId / scheduler generation-revision stamps).
	ev["rawParams"] = params
	if meta, ok := params["_meta"].(map[string]any); ok {
		ev["meta"] = meta
	}
	b.Broadcast(tag(ev))
}

// handleModelSwitchKind applies model_auto_switched / model_changed
// (extensions/notification.rs SessionUpdate::ModelAutoSwitched /
// ModelChanged; 字段名以 acp-fe chat.ts:3242-3260 与 grok 源码为准):
//   - model_changed:       model_id/modelId + reasoning_effort/reasoningEffort
//   - model_auto_switched: previous_model_id/new_model_id/reason（无 effort）
//
// 更新 roster 会话的 models 缓存（currentModelId 置为新模型 id；有
// reasoningEffort 时同步，patchSessionModels 语义，bridge.go 2099-2187
// 附近），然后广播 `models_update`（形状与现有一致：{type:"models_update",
// params: <session 的 models>, sessionId}）与 `model`（形状与 SetModel
// 一致：{type:"model", modelId, modelName, reasoningEffort, sessionId}；
// modelName 用 modelDisplayName 解析，空字符串省略）。这两个广播即该
// kind 的 typed 语义事件（modeled → 无 generic session_notification）。
func (b *Bridge) handleModelSwitchKind(sid string, update map[string]any, tag func(Event) Event) {
	// 新模型 id：model_changed 走 model_id/modelId，model_auto_switched
	// 走 new_model_id/newModelId。
	id := ""
	for _, k := range []string{"model_id", "modelId", "new_model_id", "newModelId"} {
		if v, ok := update[k].(string); ok && v != "" {
			id = v
			break
		}
	}
	effort := ""
	for _, k := range []string{"reasoning_effort", "reasoningEffort"} {
		if v, ok := update[k].(string); ok && v != "" {
			effort = v
			break
		}
	}
	if id == "" {
		// 字段缺失/为空：无可应用的状态（kind modeled，无 generic）。
		return
	}
	b.mu.Lock()
	var models any
	if s := b.sessions[sid]; s != nil {
		b.patchSessionModels(s, id, effort)
		models = s.models
	}
	b.mu.Unlock()
	if models != nil {
		b.Broadcast(tag(Event{"type": "models_update", "params": models}))
	}
	ev := Event{"type": "model", "modelId": id}
	if name := modelDisplayName(models, id); name != "" {
		ev["modelName"] = name
	}
	if effort != "" {
		ev["reasoningEffort"] = effort
	}
	b.Broadcast(tag(ev))
}

// unwrapExtMethod normalizes the two x.ai wire forms:
//   - direct:  {"method":"x.ai/foo", "params":{...}}          -> x.ai/foo
//   - wrapped: {"method":"_x.ai/foo","params":{"method":"x.ai/foo","params":{...}}} -> x.ai/foo
//
// For the wrapped leader form, an explicit sessionId carried on the OUTER
// envelope params (leader routing meta) is preserved into the inner params
// when the inner ones lack both id keys — so sessionIdFrom prefers the
// explicit id instead of falling back to the host's active session.
func unwrapExtMethod(method string, params map[string]any) (string, map[string]any) {
	if !strings.HasPrefix(method, "_x.ai/") {
		return method, params
	}
	if inner, ok := params["method"].(string); ok && strings.HasPrefix(inner, "x.ai/") {
		if innerParams, ok := params["params"].(map[string]any); ok {
			_, hasCamel := innerParams["sessionId"]
			_, hasSnake := innerParams["session_id"]
			if !hasCamel && !hasSnake {
				if sid, ok := params["sessionId"].(string); ok && sid != "" {
					innerParams["sessionId"] = sid
				} else if sid, ok := params["session_id"].(string); ok && sid != "" {
					innerParams["session_id"] = sid
				}
			}
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
		// Kind 分发（含 session_info 的 title/updatedAt roster 跟踪）在
		// dispatchSessionUpdateKind —— 与官方 session/update 载体一致。
		// modeled kind 只发 typed；仅当 dispatch 返回 false（未建模）时
		// generic 照发，携带完整 params 使 `_meta`（eventId 等）保全
		// 方式保持现状。
		if !b.dispatchSessionUpdateKind(sid, params, withSid) {
			b.Broadcast(withSid(Event{"type": "session_notification", "method": method, "params": params}))
		}
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
		// typed 事件（dispatch 已发）是 FE 回合封口语义；usage 提取保留在
		// 此（x.ai 载体不带 size:nil，官方载体带 —— 保持原状，不统一）。
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
				ev := Event{"type": "models_update", "params": models}
				// raw carries the ORIGINAL notification params so no field
				// of the machine-wide broadcast is lost to the merge.
				ev["raw"] = params
				b.Broadcast(withSid(ev))
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
		// 归一化逻辑抽到 broadcastScheduledTaskCreated（与 sessionUpdate
		// kind 分发共享）：task/rawTask/rawParams/meta 保全，见该 helper。
		b.broadcastScheduledTaskCreated(sid, params, withSid)
	case "x.ai/scheduled_task_deleted":
		b.broadcastScheduledTaskDeleted(sid, params, withSid)
	case "x.ai/session/prompt_complete":
		b.Broadcast(withSid(Event{"type": "prompt_complete", "params": params}))
	case "x.ai/session/updates/chunk":
		// 分块拉取会话更新（extensions/session_updates.rs:290-330
		// send_streamed_chunks）：{sessionId, index, updates: [原始
		// updates.jsonl 行], done}，可选 routing meta。原样透传。
		b.Broadcast(withSid(Event{"type": "session_updates_chunk", "params": params}))
	case "x.ai/queue/changed":
		// prompt 队列变更（session/prompt_queue.rs QUEUE_CHANGED_METHOD）。
		b.Broadcast(withSid(Event{"type": "queue_changed", "params": params}))
	case "x.ai/config_changed":
		// 配置变更（MCP 初始化取消 / agent/app.rs leader 广播）。
		b.Broadcast(withSid(Event{"type": "config_changed", "params": params}))
	case "x.ai/settings/update":
		// 设置热更新推送（agent/mvp_agent/mod.rs:2042）。
		b.Broadcast(withSid(Event{"type": "settings_update", "params": params}))
	case "x.ai/fs_notify":
		// 文件系统变更（session/fs_watch.rs:342）：{sessionId,
		// event:{kind, paths}}。
		b.Broadcast(withSid(Event{"type": "fs_notify", "params": params}))
	case "x.ai/fs/index":
		// 全量文件索引（session/fs_watch.rs:428）。
		b.Broadcast(withSid(Event{"type": "fs_index", "params": params}))
	case "x.ai/fs/index/delta":
		// 增量文件索引（session/fs_watch.rs:368）。
		b.Broadcast(withSid(Event{"type": "fs_index_delta", "params": params}))
	case "x.ai/search/fuzzy/status":
		// 模糊搜索进度（extensions/search.rs:158）。
		b.Broadcast(withSid(Event{"type": "search_fuzzy_status", "params": params}))
	case "x.ai/search/content/status":
		// 内容搜索进度（extensions/search.rs:222）。
		b.Broadcast(withSid(Event{"type": "search_content_status", "params": params}))
	case "x.ai/git/worktree/status":
		// worktree 创建进度（extensions/worktree.rs:37）。
		b.Broadcast(withSid(Event{"type": "git_worktree_status", "params": params}))
	case "x.ai/mcp/init_progress":
		// MCP 初始化进度（extensions/mcp.rs INIT_PROGRESS）。
		b.Broadcast(withSid(Event{"type": "mcp_init_progress", "params": params}))
	case "x.ai/terminal/pty/notification":
		// PTY 输出通知（terminal/pty_session.rs:23 NOTIFICATION_METHOD）。
		b.Broadcast(withSid(Event{"type": "pty_notification", "params": params}))
	case "x.ai/session/interjection":
		// 回合中插话（session/acp_session_impl/interjection.rs:171）：
		// {sessionId, text, interjectionId?}。
		b.Broadcast(withSid(Event{"type": "session_interjection", "params": params}))
	case "x.ai/follow_ups":
		// 回合结束后的建议 chips（TUI app/acp_handler/follow_ups.rs 渲染；
		// params 含 response_id / follow_ups 列表，原样透传）。
		b.Broadcast(withSid(Event{"type": "follow_ups", "params": params}))
	case "x.ai/leader/version_mismatch":
		// leader 与 client 版本不一致横幅（TUI xai-grok-pager/src/acp/
		// version_mismatch.rs 渲染；wire params 为 {clientVersion,
		// leaderVersion, message}——message 被 TUI 忽略，原样透传）。
		b.Broadcast(withSid(Event{"type": "leader_version_mismatch", "params": params}))
	case "x.ai/leader_reconnected":
		// leader 重连信号（xai-grok-pager-bin/src/main.rs:1354 等，
		// params 可为空）。
		b.Broadcast(withSid(Event{"type": "leader_reconnected", "params": params}))
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
	// Peek first: the user's answer may have landed at the same instant
	// the approval timer fired. A plain select would pick randomly and
	// could reply "cancelled" to a just-confirmed approval; the answer
	// must always win.
	select {
	case <-cr.done:
		b.resolveClientRequest(reqID, cr)
		return
	default:
	}
	select {
	case <-cr.done:
		b.resolveClientRequest(reqID, cr)
	case <-timer.C:
		// Re-check once more: an answer arriving while the timer fired
		// must not be lost to the timeout path.
		select {
		case <-cr.done:
			b.resolveClientRequest(reqID, cr)
			return
		default:
		}
		b.clientReqs.Delete(reqID)
		if cr.isPermission {
			b.respond(cr.AgentID, permissionResult(map[string]any{"outcome": "cancelled"}, cr.meta))
		} else {
			b.respondError(cr.AgentID, "审批超时", -32002)
		}
	}
}

// resolveClientRequest writes the JSON-RPC response for a client request
// the browser resolved before the approval timeout (cancel / error /
// permission outcome / raw result).
func (b *Bridge) resolveClientRequest(reqID string, cr *clientRequest) {
	b.clientReqs.Delete(reqID)
	id := cr.AgentID
	if cr.cancel {
		if cr.isPermission {
			b.respond(id, permissionResult(map[string]any{"outcome": "cancelled"}, cr.meta))
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
		b.respond(id, permissionResult(cr.outcome, cr.meta))
		return
	}
	b.respond(id, cr.result)
}

// permissionResult wraps an ACP permission outcome with the optional
// response `_meta` — the ACP wire shape is result = {outcome, _meta?}
// (RequestPermissionResponse; `_meta` reserved for client/agent extension
// metadata). The meta object carries exactly the keys the TUI puts there:
// BashCommandSelectedTerms {command_parts, is_glob} or {followup_message}.
func permissionResult(outcome map[string]any, meta map[string]any) map[string]any {
	if len(meta) == 0 {
		return map[string]any{"outcome": outcome}
	}
	return map[string]any{
		"outcome": outcome,
		"_meta":   meta,
	}
}

// RespondPermission resolves a pending session/request_permission without
// response meta (legacy signature — no scope / followup to attach).
func (b *Bridge) RespondPermission(requestID, optionID string, cancelled bool) error {
	return b.RespondPermissionWithMeta(requestID, optionID, cancelled, nil, "")
}

// RespondPermissionWithMeta resolves a pending session/request_permission,
// forwarding the client's bash scope and/or followup message to the agent
// via the ACP response `_meta` — the same wire shape the TUI sends:
//
//   - scope (Selected replies): {command_parts: [...], is_glob: bool}
//     (BashCommandSelectedTerms, word-scope or authored glob).
//   - followupMessage on a cancelled reply: resolved with the request's
//     RejectOnce option + {followup_message: text} — the TUI
//     dispatch_permission_followup semantics, because the agent only reads
//     followup text off the RejectOnce branch. Without a RejectOnce option
//     it falls back to a plain Cancelled outcome (TUI does the same).
//
// Both fields are optional; with none set the wire response is byte-identical
// to the legacy RespondPermission.
func (b *Bridge) RespondPermissionWithMeta(requestID, optionID string, cancelled bool, scope *PermissionScope, followupMessage string) error {
	v, ok := b.clientReqs.LoadAndDelete(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	if cancelled {
		if msg := strings.TrimSpace(followupMessage); msg != "" {
			if rejectOnce := rejectOnceOptionID(cr.Params); rejectOnce != "" {
				cr.outcome = map[string]any{
					"outcome":  "selected",
					"optionId": rejectOnce,
				}
				cr.meta = map[string]any{"followup_message": msg}
				close(cr.done)
				return nil
			}
		}
		cr.cancel = true
		close(cr.done)
		return nil
	}
	cr.outcome = map[string]any{
		"outcome":  "selected",
		"optionId": optionID,
	}
	if scope != nil && len(scope.CommandParts) > 0 {
		cr.meta = map[string]any{
			"command_parts": scope.CommandParts,
			"is_glob":       scope.IsGlob,
		}
	}
	close(cr.done)
	return nil
}

// rejectOnceOptionID returns the optionId of the request's RejectOnce
// option ("" when absent). The agent serializes PermissionOptionKind as
// snake_case ("reject_once"); the camelCase form is tolerated defensively.
func rejectOnceOptionID(params map[string]any) string {
	raw, _ := params["options"].([]any)
	for _, item := range raw {
		opt, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := opt["kind"].(string)
		if kind != "reject_once" && kind != "rejectOnce" {
			continue
		}
		id, _ := opt["optionId"].(string)
		return id
	}
	return ""
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

// PromptOpts carries the optional official session/prompt fields: the
// unstable `messageId` (UUID) and an extension `_meta` map. Empty fields
// are omitted from the wire (absent key ≠ off, matching the TUI).
type PromptOpts struct {
	MessageID string         // wire: messageId (UUID; 非空才发)
	Meta      map[string]any // wire: _meta (非空才发)
}

// Prompt sends session/prompt to the given session (default: active) and
// blocks until the turn finishes. When no session exists yet (the host
// never auto-creates one at boot), the first prompt restores the last
// known session if one was remembered; only a machine with no last-session
// pointer starts a brand-new conversation. A failed restore is an error —
// never silently open a blank chat. Busy is per-session: other sessions
// can keep running turns in parallel (the agent process is multi-session),
// and a busy session is NOT rejected — the agent accepts mid-turn
// session/prompt and queues it in its own authoritative pending_inputs, so
// the host forwards and the turns run in submission order.
func (b *Bridge) Prompt(ctx context.Context, sessionID string, blocks []ContentBlock) (stopReason string, err error) {
	sr, _, err := b.PromptWithOpts(ctx, sessionID, blocks, PromptOpts{})
	return sr, err
}

// PromptWithOpts is Prompt with the official optional fields (messageId /
// _meta) forwarded on the session/prompt params when set; empty opts keep
// the wire byte-identical to Prompt. Returns the turn's stopReason plus
// the response `_meta` (the agent's prompt-result meta, nil when absent)
// so the HTTP layer can pass it through to the browser.
func (b *Bridge) PromptWithOpts(ctx context.Context, sessionID string, blocks []ContentBlock, opts PromptOpts) (stopReason string, meta map[string]any, err error) {
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
				return "", nil, err
			}
		} else if err := b.NewSession(ctx, SessionConfig{}); err != nil {
			return "", nil, err
		}
		b.mu.Lock()
		sessionID = b.activeSessionID
		s = b.sessions[sessionID]
	}
	if s == nil {
		b.mu.Unlock()
		return "", nil, &HTTPError{Code: 404, Msg: "会话不存在"}
	}
	// A busy session no longer 409s: the agent accepts mid-turn
	// session/prompt and queues it in its own pending_inputs (popped when
	// the current turn ends), so we just forward. Busy stays the "any turn
	// in flight" projection via busyCount — an overlapping prompt must not
	// let the first resolver flip the session idle while the second is
	// still running.
	first := s.busyCount == 0
	s.busyCount++
	s.Busy = true
	s.LastActiveAt = time.Now().UnixMilli()
	// Keep last-session pointer fresh even if the user only talks to this
	// session without re-loading it (multi-session focus via prompt).
	b.rememberSessionLocked(sessionID, s.Cwd)
	b.mu.Unlock()
	if first {
		// 0→1 transition: announce the busy turn exactly once. A second
		// in-flight prompt must not re-announce — the session never left
		// busy between the two.
		b.Broadcast(Event{"type": "busy", "sessionId": sessionID})
	}

	defer func() {
		b.releaseBusy(s, sessionID)
	}()

	if err := b.ensureBooted(ctx); err != nil {
		return "", nil, err
	}

	// convert blocks
	prompt := make([]any, 0, len(blocks))
	for _, bl := range blocks {
		prompt = append(prompt, map[string]any(bl))
	}

	params := map[string]any{
		"sessionId": sessionID,
		"prompt":    prompt,
	}
	if opts.MessageID != "" {
		params["messageId"] = opts.MessageID
	}
	if len(opts.Meta) > 0 {
		params["_meta"] = opts.Meta
	}
	res, err := b.request(ctx, "session/prompt", params, promptTimeout)
	if err != nil {
		// source 标记（前端据此渲染）：agent 报错（RPCError — 进程活着、
		// 拒绝了回合）vs 传输失败（超时/写失败 — agent 可能不可达）。
		// 老版本客户端忽略该字段，仅按带 sessionId 的回合错误处理。
		source := "transport"
		var rpcErr *RPCError
		if errors.As(err, &rpcErr) {
			source = "agent"
		}
		b.Broadcast(Event{"type": "error", "message": err.Error(), "sessionId": sessionID, "source": source})
		// The client (browser) went away mid-turn. The agent process may be
		// perfectly healthy — and other sessions may be running parallel
		// turns in the same process — so never killProcess here: that would
		// abort every session's work, not just this one. Cancel the orphaned
		// turn so the session does not stay busy for the full promptTimeout,
		// and return; the client can simply resend.
		if errors.Is(err, context.Canceled) {
			b.Cancel(sessionID)
			b.Broadcast(Event{"type": "status", "text": "连接已断开，本次回复已取消，请重新发送", "sessionId": sessionID})
			return "", nil, err
		}
		// A plain JSON-RPC error is the AGENT rejecting the turn (e.g. the
		// model API's 400 "Internal Error: …") — the process answered and is
		// healthy; the error event above already surfaced the failure, no
		// self-heal needed. Only transport-level failures (timeout / write
		// error / boot failure — anything but RPCError) suggest a wedged
		// process: kill it and reload the SAME session — never session/new
		// on this path (a failed restore leaves the user without an active
		// chat rather than a silent blank one). The failed turn is not
		// retried.
		if !errors.As(err, &rpcErr) {
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
		}
		return "", nil, err
	}
	sr, _ := res["stopReason"].(string)
	if sr == "" {
		sr = "unknown"
	}
	// 响应 `_meta`（agent 下发的 prompt-result meta）原样透传：done 事件
	// 带 `meta`、返回值给 HTTP 层；仅非空才带（absent key ≠ off）。
	meta, _ = res["_meta"].(map[string]any)
	ev := Event{"type": "done", "stopReason": sr, "sessionId": sessionID}
	if len(meta) > 0 {
		ev["meta"] = meta
	}
	b.Broadcast(ev)
	return sr, meta, nil
}

// Cancel sends session/cancel for the given session (default: active) and
// cancels its pending client requests.
func (b *Bridge) Cancel(sessionID string) {
	b.CancelWithMeta(sessionID, nil)
}

// CancelWithMeta is Cancel with an optional `_meta` forwarded on the
// session/cancel params. The agent reads cancelTrigger ("esc"|"ctrl_c"),
// cancelSubagents (default true), and rewindIfNoOutput / rewindIfPristine
// from that meta (mvp_agent/acp_agent.rs:2079-2108); an empty meta keeps
// the wire byte-identical to Cancel.
func (b *Bridge) CancelWithMeta(sessionID string, meta map[string]any) {
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
		params := map[string]any{"sessionId": sid}
		if len(meta) > 0 {
			params["_meta"] = meta
		}
		_ = b.write(map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/cancel",
			"params":  params,
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
	b.mu.Unlock()

	// Fail in-flight RPCs (prompts on OTHER sessions) so they return an
	// error immediately instead of hanging until the 30-minute prompt
	// timeout. waitProcess only covers natural exits; this is a forced
	// kill (wedged agent), so fail them here, outside the lock.
	b.failAllPending(fmt.Errorf("grok 进程已重启"))
	b.broadcastRosterChange()
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

// ListSessionsOpts carries the optional official session/list params the
// host forwards on the wire: cwd (scope), cursor (pagination), and Meta
// (→ `_meta`, the ACP list request's meta — read by the agent at
// handlers/session.rs:310 as `args.meta`). Zero values are omitted, so the
// default wire shape stays `{}` exactly.
type ListSessionsOpts struct {
	Cwd    string
	Cursor string
	Meta   map[string]any
}

// ListSessions calls session/list if supported and enriches every session
// item with the host-side live status (dashboard active/idle/awaiting).
// Optional opts forward the official request fields (cwd/cursor/`_meta`);
// the local enrichment is untouched. Returns the sessions plus the
// response's pagination cursor (nextCursor / next_cursor, "" = no more
// pages) and `_meta` (nil when absent) for passthrough to the browser.
func (b *Bridge) ListSessions(ctx context.Context, opts ...ListSessionsOpts) (sessions []any, nextCursor string, meta map[string]any, err error) {
	if err := b.Boot(ctx); err != nil {
		return nil, "", nil, err
	}
	params := map[string]any{}
	if len(opts) > 0 {
		if opts[0].Cwd != "" {
			params["cwd"] = opts[0].Cwd
		}
		if opts[0].Cursor != "" {
			params["cursor"] = opts[0].Cursor
		}
		if len(opts[0].Meta) > 0 {
			params["_meta"] = opts[0].Meta
		}
	}
	res, err := b.request(ctx, "session/list", params, 30*time.Second)
	if err != nil {
		return nil, "", nil, err
	}
	sessions, _ = res["sessions"].([]any)
	// 分页游标：agent 可能回 camelCase `nextCursor` 或 snake_case
	// `next_cursor`，两个都兼容；空串 = 没有更多页。
	nextCursor, _ = res["nextCursor"].(string)
	if nextCursor == "" {
		nextCursor, _ = res["next_cursor"].(string)
	}
	// 响应 `_meta` 原样透传（仅非空才发，absent key ≠ off）。
	meta, _ = res["_meta"].(map[string]any)

	// Snapshot the live per-session state under the lock (cheap), then run
	// the census file reads and the lsof probe OUTSIDE it: sessionTaskCensus
	// reads each session's updates.jsonl and probeOrphanPaths spawns lsof
	// (bounded at 5s) — holding b.mu across that would stall every other
	// session's event handling (state writes take b.mu) for the whole probe.
	type liveState struct {
		live          bool // session is in the live roster (b.sessions)
		state         string
		busy          bool
		awaitingInput bool
		lastActiveAt  int64
		title         string
		updatedAt     string
	}
	states := make(map[string]*liveState, len(sessions))
	b.mu.Lock()
	for _, it := range sessions {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		sid, _ := m["sessionId"].(string)
		st := &liveState{state: "idle"}
		if s := b.sessions[sid]; s != nil {
			st.live = true
			st.state = s.State()
			st.busy = s.Busy
			st.awaitingInput = s.AwaitingInput
			st.lastActiveAt = s.LastActiveAt
			// Host-side title/updatedAt win over the agent list when the
			// session is live (agent may not persist them yet).
			st.title = s.Title
			st.updatedAt = s.UpdatedAt
		}
		states[sid] = st
	}
	b.mu.Unlock()

	// [bg] badge census: scan each session's persisted updates for task
	// events (best-effort; missing files stay unbadged). Orphan log paths
	// are collected per session and probed in ONE lsof call afterwards.
	// Lock-free: only touches the freshly-fetched session items and
	// immutable config (grokHome).
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
		if st := states[sid]; st != nil && st.live {
			m["status"] = map[string]any{
				"state":         st.state,
				"busy":          st.busy,
				"awaitingInput": st.awaitingInput,
				"lastActiveAt":  st.lastActiveAt,
			}
			if st.title != "" {
				m["title"] = st.title
			}
			if st.updatedAt != "" {
				m["updatedAt"] = st.updatedAt
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
	return sessions, nextCursor, meta, nil
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
	// Same deep-copy contract as Snapshot: the caller serializes this
	// outside b.mu while readStdout may mutate the shared maps under it.
	cp.modes = cloneAny(s.modes)
	cp.configOpts = cloneAny(s.configOpts)
	cp.models = cloneAny(s.models)
	cp.sessionMeta = cloneAny(s.sessionMeta)
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
//
// An optional meta map (client-supplied permission-mode seeds, e.g.
// yoloMode/autoMode) is forwarded as the session/load params `_meta` —
// the TUI's SessionFlags.to_meta() analog. Omitted = current behavior
// exactly (no `_meta` key on the wire).
func (b *Bridge) LoadSession(ctx context.Context, sessionID, cwd string, meta ...map[string]any) (map[string]any, error) {
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
		sessionMeta := s.sessionMeta
		authMeta := b.authMeta
		hostID := b.hostID
		hostName := b.hostName
		sessCwd := s.Cwd
		b.mu.Unlock()

		ev := Event{
			"type":          "ready",
			"sessionId":     sessionID,
			"cwd":           sessCwd,
			"agentInfo":     agentInfo,
			"modes":         modes,
			"configOptions": configOpts,
			"models":        models,
			"hostId":        hostID,
			"hostName":      hostName,
		}
		// 已存 roster 的 sessionMeta / authenticate 的 authMeta 原样透传，
		// 仅非空才带。
		if sessionMeta != nil {
			ev["sessionMeta"] = sessionMeta
		}
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
		// Re-announce busy so the client can attach the spinner after history load.
		b.Broadcast(Event{"type": "busy", "sessionId": sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		"sessionId":  sessionID,
		"cwd":        cwd,
		"mcpServers": []any{},
	}
	if len(meta) > 0 && len(meta[0]) > 0 {
		params["_meta"] = meta[0]
	}
	sessRes, err := b.request(ctx, "session/load", params, bootTimeout)
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
	// session/load 响应 `_meta` 原样存下（缺省保留 roster 旧值，与
	// models/modes 的处理一致），ready 事件 / Status 透传。
	if m, ok := sessRes["_meta"]; ok && m != nil {
		act.sessionMeta = m
	}
	b.activeSessionID = sessionID
	b.rememberSessionLocked(sessionID, cwd)
	agentInfo := b.agentInfo
	modes := act.modes
	configOpts := act.configOpts
	models := act.models
	sessionMeta := act.sessionMeta
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := act.Cwd
	b.mu.Unlock()

	if sessRes == nil {
		sessRes = map[string]any{}
	}
	// Cold load is never mid-turn.
	sessRes["busy"] = false

	ev := Event{
		"type":          "ready",
		"sessionId":     sessionID,
		"cwd":           sessCwd,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"models":        models,
		"hostId":        hostID,
		"hostName":      hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
	b.broadcastRosterChange()
	return sessRes, nil
}

// ResumeSession switches the active session to a paused one (session/resume),
// mirroring LoadSession: the resume response's models/modes/configOptions
// win over the roster cache, the ready event is broadcast in the same shape,
// and a busy in-flight target takes the same focus-only path (re-focus +
// re-announce busy, no agent call — session/resume would conflict with the
// running turn).
//
// An optional meta map (client-supplied seeds, e.g. permission-mode flags)
// is forwarded as the session/resume params `_meta` — the LoadSession
// analog. Omitted = current behavior exactly (no `_meta` key on the wire;
// additionalDirectories stays []).
func (b *Bridge) ResumeSession(ctx context.Context, sessionID, cwd string, meta ...map[string]any) (map[string]any, error) {
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil && s.Busy {
		// Focus-only path for an in-flight session (mirror LoadSession).
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
		sessionMeta := s.sessionMeta
		authMeta := b.authMeta
		hostID := b.hostID
		hostName := b.hostName
		sessCwd := s.Cwd
		b.mu.Unlock()

		ev := Event{
			"type":          "ready",
			"sessionId":     sessionID,
			"cwd":           sessCwd,
			"agentInfo":     agentInfo,
			"modes":         modes,
			"configOptions": configOpts,
			"models":        models,
			"hostId":        hostID,
			"hostName":      hostName,
		}
		if sessionMeta != nil {
			ev["sessionMeta"] = sessionMeta
		}
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
		// Re-announce busy so the client can attach the spinner after history load.
		b.Broadcast(Event{"type": "busy", "sessionId": sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		"sessionId":             sessionID,
		"cwd":                   cwd,
		"mcpServers":            []any{},
		"additionalDirectories": []any{},
	}
	if len(meta) > 0 && len(meta[0]) > 0 {
		params["_meta"] = meta[0]
	}
	sessRes, err := b.request(ctx, "session/resume", params, bootTimeout)
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
	// Prefer fields from the resume response — they reflect the session.
	if m, ok := sessRes["models"]; ok && m != nil {
		act.models = m
	}
	if modes, ok := sessRes["modes"]; ok && modes != nil {
		act.modes = modes
	}
	if co, ok := sessRes["configOptions"]; ok && co != nil {
		act.configOpts = co
	}
	// session/resume 响应 `_meta` 原样存下（缺省保留 roster 旧值），
	// ready 事件 / Status 透传。
	if m, ok := sessRes["_meta"]; ok && m != nil {
		act.sessionMeta = m
	}
	b.activeSessionID = sessionID
	b.rememberSessionLocked(sessionID, cwd)
	agentInfo := b.agentInfo
	modes := act.modes
	configOpts := act.configOpts
	models := act.models
	sessionMeta := act.sessionMeta
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := act.Cwd
	b.mu.Unlock()

	if sessRes == nil {
		sessRes = map[string]any{}
	}
	// A resumed session is never mid-turn.
	sessRes["busy"] = false

	ev := Event{
		"type":          "ready",
		"sessionId":     sessionID,
		"cwd":           sessCwd,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"models":        models,
		"hostId":        hostID,
		"hostName":      hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
	b.broadcastRosterChange()
	return sessRes, nil
}

// CloseSession calls session/close (sessionId defaults to the active one)
// and removes the session from the roster on success: the active-session
// pointer is cleared when it pointed at the closed session, and a matching
// in-memory last-session pointer is cleared too (the disk file is left
// untouched — it is only a hint, and restoring a closed session fails with
// a 404 anyway).
func (b *Bridge) CloseSession(ctx context.Context, sessionID string) (map[string]any, error) {
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
	sessRes, err := b.request(ctx, "session/close", map[string]any{
		"sessionId": sessionID,
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	delete(b.sessions, sessionID)
	if b.activeSessionID == sessionID {
		b.activeSessionID = ""
	}
	if b.lastSessionID == sessionID {
		b.lastSessionID = ""
		b.lastSessionCwd = ""
	}
	b.mu.Unlock()
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

// SessionUpdatesOpts carries the optional x.ai/session/updates request
// fields (extensions/session_updates.rs Request, camelCase): offset/limit
// keep the existing pagination, stream delivers the updates as chunked
// notifications (agent replies {totalCount, chunkCount, …}), chunkSize
// sets the updates per chunk (default 64), turnIndex tails by user-message
// turn count. Zero values are omitted — the wire shape for the existing
// callers is unchanged.
type SessionUpdatesOpts struct {
	Offset    *int64
	Limit     *int
	Stream    bool
	ChunkSize *int
	TurnIndex *int
}

// SessionUpdates fetches a session's stored updates (message history) via
// the ACP extension method x.ai/session/updates. Each element of the result
// is the full storage envelope {timestamp, method, params}.
func (b *Bridge) SessionUpdates(ctx context.Context, sessionID, cwd string, opts ...SessionUpdatesOpts) (UpdatesPage, error) {
	if err := b.Boot(ctx); err != nil {
		return UpdatesPage{}, err
	}
	params := map[string]any{"sessionId": sessionID, "cwd": cwd}
	if len(opts) > 0 {
		if opts[0].Offset != nil {
			params["offset"] = *opts[0].Offset
		}
		if opts[0].Limit != nil {
			params["limit"] = *opts[0].Limit
		}
		if opts[0].Stream {
			params["stream"] = true
		}
		if opts[0].ChunkSize != nil {
			params["chunkSize"] = *opts[0].ChunkSize
		}
		if opts[0].TurnIndex != nil {
			params["turnIndex"] = *opts[0].TurnIndex
		}
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

// XaiCall sends a client→agent x.ai extension request with the official
// "_" wire prefix ("_x.ai/<method>") and returns the RAW result map
// (ExtMethodResult envelopes are NOT unwrapped here — callers may unwrap
// with UnwrapExtResult).
// Session defaulting rule: if params contains "sessionId" or "session_id"
// whose value is "" (empty string), it is replaced with the active
// session's id; when no session is active this returns HTTPError 404.
// Keys absent from params are left absent. 60s timeout.
func (b *Bridge) XaiCall(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if params == nil {
		params = map[string]any{}
	}
	for _, k := range []string{"sessionId", "session_id"} {
		if v, ok := params[k].(string); ok && v == "" {
			sid := b.resolveSessionID("")
			if sid == "" {
				return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
			}
			params[k] = sid
		}
	}
	return b.request(ctx, "_"+method, params, 60*time.Second)
}

// XaiNotify sends a client→agent x.ai extension NOTIFICATION (fire-and-forget,
// no JSON-RPC id) with the official "_" wire prefix. The agent's decoder
// strips "_" and routes the remainder to its ext_notification handler —
// the transport for methods that have NO ext_method arm and no response
// (x.ai/queue/*, x.ai/toggle_plan_mode, x.ai/permissions/reset, …; TUI
// parity: xai-grok-pager app/effects/mod.rs sends these as ExtNotification).
// Session defaulting matches XaiCall: an empty "sessionId"/"session_id" in
// params is replaced with the active session's id; when no session is
// active this returns HTTPError 404. Note: the agent's ext_notification
// handlers read only "sessionId" on this rail — "session_id" is a dead key
// (kept in the loop purely for XaiCall parity). The host resolves the id
// itself — a notification carries no response, so XaiCall's request-side
// auto-fill cannot apply. Returns {ok:true} once the line is written; the
// agent's authoritative state comes back via its re-broadcasts (e.g. queue
// → x.ai/queue/changed).
func (b *Bridge) XaiNotify(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if params == nil {
		params = map[string]any{}
	}
	for _, k := range []string{"sessionId", "session_id"} {
		if v, ok := params[k].(string); ok && v == "" {
			sid := b.resolveSessionID("")
			if sid == "" {
				return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
			}
			params[k] = sid
		}
	}
	if err := b.write(map[string]any{
		"jsonrpc": "2.0",
		"method":  "_" + method,
		"params":  params,
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

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
// Recap calls x.ai/recap: {sessionId, auto} — triggers a session recap.
// The sessionId defaults to the active session (404 when none is active).
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
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
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
	sessRes, err := b.request(ctx, "_x.ai/session/delete", map[string]any{
		"sessionId": sessionID,
	}, 60*time.Second)
	if err != nil {
		return nil, err
	}
	// Deleting the ACTIVE session drops the frontend into the EMPTY state
	// (no auto-new). Clear the roster entry, the active-session pointer and
	// any last-session hint pointing at the deleted session, so the next
	// prompt without a sessionId creates a fresh session instead of trying
	// to restore the deleted one (same cleanup as CloseSession).
	b.mu.Lock()
	delete(b.sessions, sessionID)
	if b.activeSessionID == sessionID {
		b.activeSessionID = ""
	}
	if b.lastSessionID == sessionID {
		b.lastSessionID = ""
		b.lastSessionCwd = ""
	}
	b.mu.Unlock()
	b.broadcastRosterChange()
	return sessRes, nil
}

// CompactConversation calls x.ai/compact_conversation: {sessionId, userContext?}
// (manual context compaction; the sessionId defaults to the active one). The
// agent's request struct only accepts `userContext` for the compaction note —
// a `note` key would be silently ignored by serde, so the host translates.
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
		params["userContext"] = note
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

// RewindExecute calls x.ai/rewind/execute: {sessionId, targetPromptIndex,
// force, mode} — rolls the conversation back to the given rewind point. The
// agent's handler only accepts `target_prompt_index`/`targetPromptIndex` (a
// raw `targetIndex` key would be rejected as invalid params), so the host
// translates. `force: true` mirrors the TUI's /rewind (rewind_execute_params):
// without it the agent typically declines the rollback (success:false,
// nothing truncated). `mode` is passed through: "conversation_only" (the
// default, TUI behavior) rolls back the conversation only; "all" also
// reverts the point's file snapshots.
func (b *Bridge) RewindExecute(ctx context.Context, sessionID string, targetIndex int, mode string) (map[string]any, error) {
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
	if mode == "" {
		mode = "conversation_only"
	}
	return b.request(ctx, "_x.ai/rewind/execute", map[string]any{
		"sessionId":         sessionID,
		"targetPromptIndex": targetIndex,
		"force":             true,
		"mode":              mode,
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

// MemoryFlush calls x.ai/memory/flush: {session_id} — persists the session's
// memory (sessionId defaults to the active session). The agent's request
// struct is plain snake_case (`session_id`), so the host must NOT send the
// camelCase `sessionId` key it accepts elsewhere.
func (b *Bridge) MemoryFlush(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/flush", map[string]any{
		"session_id": sessionID,
	}, 30*time.Second)
}

// MemoryRewrite calls x.ai/memory/rewrite: {sessionId, rawText, contextSummary}
// — rewrites the session's memory from the given text. The agent's request
// struct is camelCase with all three fields required; the host must forward
// rawText/contextSummary or the call fails with invalid params.
func (b *Bridge) MemoryRewrite(ctx context.Context, sessionID, rawText, contextSummary string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/rewrite", map[string]any{
		"sessionId":      sessionID,
		"rawText":        rawText,
		"contextSummary": contextSummary,
	}, 30*time.Second)
}

// TogglePlanMode sends x.ai/toggle_plan_mode: {sessionId} — switches the
// session's plan mode on/off. The agent only handles this method as a
// fire-and-forget NOTIFICATION (no request branch; a request-style call
// would get -32601 method_not_found), so the host writes it without a
// JSON-RPC id and returns a bare ok — the frontend applies its local
// desired state. SessionId defaults to the active session.
func (b *Bridge) TogglePlanMode(ctx context.Context, sessionID string) (map[string]any, error) {
	return b.XaiNotify(ctx, "x.ai/toggle_plan_mode", map[string]any{"sessionId": sessionID})
}

// PermissionsReset sends x.ai/permissions/reset: {sessionId} — clears the
// remembered permission decisions. Like toggle_plan_mode the agent only
// handles it as a notification (it resets ALL resident sessions, ignoring
// the sessionId), so the host writes it without a JSON-RPC id.
func (b *Bridge) PermissionsReset(ctx context.Context, sessionID string) (map[string]any, error) {
	return b.XaiNotify(ctx, "x.ai/permissions/reset", map[string]any{"sessionId": sessionID})
}

// SetPermissionMode sends x.ai/yolo_mode_changed as a fire-and-forget
// notification — the agent's permission-mode switch channel (ask / auto /
// always-approve). session/set_mode only understands the session-mode ids
// (plan / default / ask), so permission modes MUST go through this
// notification instead (TUI parity: the pager persists + fires the same
// payload). The agent applies it to every resident session of this client,
// no sessionId needed. 'normal' maps to the agent's 'ask' canonical; the
// unknown-id fallback is 'ask' too, so a stale frontend never dead-ends.
func (b *Bridge) SetPermissionMode(ctx context.Context, mode string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{"yolo_mode": false, "auto_mode": false}
	switch mode {
	case "auto":
		params["auto_mode"] = true
		params["permission_mode"] = "auto"
	case "always-approve", "always_approve", "yolo":
		params["yolo_mode"] = true
		params["permission_mode"] = "always-approve"
	default: // normal / ask / anything unknown → 普通（ask）模式
		params["permission_mode"] = "ask"
	}
	if err := b.write(map[string]any{
		"jsonrpc": "2.0",
		"method":  "_x.ai/yolo_mode_changed",
		"params":  params,
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// MCPList calls x.ai/mcp/list: {sessionId?} → the agent's MCP server
// registry. The active session's id is injected when one exists so the
// agent can attach per-session state (enabled/status) to each entry; the
// absence of an active session is not an error (the agent returns the
// bare catalog).
func (b *Bridge) MCPList(ctx context.Context) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{}
	if sid := b.resolveSessionID(""); sid != "" {
		params["sessionId"] = sid
	}
	return b.request(ctx, "_x.ai/mcp/list", params, 30*time.Second)
}

// MCPToggle calls x.ai/mcp/toggle: {session_id, server_name, enabled} —
// enables/disables one MCP server. The agent's request struct is plain
// snake_case with `session_id`/`server_name` required, so the host resolves
// the active session and translates the frontend's `{name, enabled}` body.
func (b *Bridge) MCPToggle(ctx context.Context, name string, enabled bool) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sid := b.resolveSessionID("")
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/mcp/toggle", map[string]any{
		"session_id":  sid,
		"server_name": name,
		"enabled":     enabled,
	}, 30*time.Second)
}

// MCPUpsert calls x.ai/mcp/upsert: {session_id, server_name, ...config} —
// adds or updates one MCP server. The agent expects the config flattened at
// the top level (command/args/env/cwd/url/…), NOT wrapped in a `server`
// object; `name` becomes `server_name`. Config keys are passed through
// verbatim.
func (b *Bridge) MCPUpsert(ctx context.Context, server map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sid := b.resolveSessionID("")
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	serverName, _ := server["name"].(string)
	params := make(map[string]any, len(server)+1)
	params["session_id"] = sid
	params["server_name"] = serverName
	for k, v := range server {
		if k == "name" {
			continue
		}
		params[k] = v
	}
	return b.request(ctx, "_x.ai/mcp/upsert", params, 30*time.Second)
}

// MCPDelete calls x.ai/mcp/delete: {session_id, server_name} — removes one
// MCP server. Same snake_case contract as MCPToggle.
func (b *Bridge) MCPDelete(ctx context.Context, name string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sid := b.resolveSessionID("")
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/mcp/delete", map[string]any{
		"session_id":  sid,
		"server_name": name,
	}, 30*time.Second)
}

// MCPAuthTrigger calls x.ai/mcp/auth_trigger: {session_id, server_name} —
// starts the OAuth flow for one MCP server. Same snake_case contract.
func (b *Bridge) MCPAuthTrigger(ctx context.Context, name string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sid := b.resolveSessionID("")
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/mcp/auth_trigger", map[string]any{
		"session_id":  sid,
		"server_name": name,
	}, 30*time.Second)
}

// Shutdown kills grok and stops the goal loop.
func (b *Bridge) Shutdown() {
	b.mu.Lock()
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	if b.cancelRd != nil {
		b.cancelRd()
	}
	b.mu.Unlock()

	// Stop the goal loop: a continuation turn can be blocked on a
	// 30-minute prompt or waiting for the session to go idle, and must
	// not outlive shutdown. Same mechanism as GoalClear — close the stop
	// channel so the loop exits at its next checkpoint. stopGoalLoopLocked
	// guards against double-close (goalLoopOn / goalStop nil checks);
	// the process kill above fails the in-flight RPC, so the turn unwinds
	// promptly instead of holding the loop open.
	b.goalMu.Lock()
	b.stopGoalLoopLocked()
	b.goalMu.Unlock()
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
