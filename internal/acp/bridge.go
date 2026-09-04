package acp

import (
	"bufio"
	"bytes"
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

	"github.com/AgentsHarness/capri-host/internal/procattr"
)

const (
	promptTimeout   = 30 * time.Minute
	bootTimeout     = 2 * time.Minute
	approvalTimeout = 15 * time.Minute
	protocolVersion = 1
	// turnStaleAfter bounds how long an agent-observed turn may stay open
	// with no update at all for that session. Only the observed leg of Busy
	// is subject to it: a session/prompt this host sent stays busy for the
	// whole promptTimeout regardless. Silent-but-alive gaps longer than this
	// (a single 40-minute tool call with no updates) are indistinguishable
	// from a turn whose terminal update was lost, so the observed turn is
	// dropped and the session re-opens on the next update it emits.
	turnStaleAfter = 30 * time.Minute
	// turnLivenessWindow is how recently an update must have arrived for the
	// host to treat "the agent is still working" as proven. The prompt
	// budget (promptTimeout) expiring on a session inside this window says
	// nothing about the turn, so the failure is kept out of the event stream
	// (see reportPromptFailure); outside it, a wedged agent must still
	// surface as an error.
	turnLivenessWindow = 5 * time.Minute
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
	// bootOK marks a boot attempt that COMPLETED successfully (process up,
	// initialize + authenticate done). It is distinct from ready: a
	// successful boot no longer creates a session, so ready only flips once
	// createSession / LoadSession / session/resume succeeds — between those
	// two moments a waiter must not mistake "booted, no session yet" for a
	// boot failure. Cleared on boot start, boot failure and process death.
	bootOK    bool
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
	// genRate 是 per-session 的生成输出速率（字符/s）估算器（见
	// genrate.go）：chunk 到达时观察、tool_call / 回合终态收尾、
	// user_message_chunk 复位；节流后广播 gen_rate 事件。
	genRate *genRateTracker
	// usageLastUsed 是 per-session 上次广播的 _meta.totalTokens（流水期
	// usage 事件的值去重：同一值反复到达只广播一次，见 handleSessionUpdate）。
	// 仅流水期顶部广播使用；turn-end 提取的事件不受影响。b.mu 保护。
	usageLastUsed map[string]int64
	// liveTools 是 per-session 的实时工具事件合成 ID 注入器（与历史主路径
	// 同一套 synth:call:<ts>:<k>；首次非重放事件用历史视图做种子）。b.mu 保护。
	liveTools map[string]*liveToolResolver
	// replayDropped 累计被 Broadcast 就地拦下的 session/load 重放事件数
	// （见 Broadcast 注释）；LoadSession 收尾时打一行统计后清零。
	replayDropped atomic.Uint64
	// agentStartedAt (unix ms) stamps the CURRENT agent process spawn.
	// Clients compare it across hello events to detect an agent restart —
	// the agent's permission mode is in-memory only and resets on restart,
	// so the browser re-seeds its known flags to keep behavior in sync.
	agentStartedAt int64
	// permMode is the host's process-global view of the agent's canonical
	// permission mode (ask / auto / always-approve). Written on every
	// client-initiated toggle (SetPermissionMode) and every agent echo of
	// yolo_mode_changed; reset to "ask" on agent (re)spawn. Surfaced on
	// hello so a connecting client restores its badge from the agent's
	// real state instead of stale browser storage.
	permMode string
	// Roster: every session created in this process (or loaded), keyed by
	// sessionId, with host-side live state (busy / awaiting input).
	sessions        map[string]*SessionState
	activeSessionID string

	// turns is the agent-observed turn state per session, keyed by
	// sessionId: the update stream says "a turn is in flight for this
	// session" until turn_completed / response_completed says otherwise.
	// It is the second leg of SessionState.Busy — busyCount alone only
	// counts session/prompt calls this host process made and stops
	// counting when that RPC dies (the 30-minute promptTimeout among
	// them), which left sessions that were still working reported as idle.
	// It also covers sessions the agent lists but this process never
	// created/loaded, and turns started by another client. b.mu protects it.
	turns map[string]*observedTurn

	// queueSnapshots caches the most recent x.ai/queue/changed params per
	// session (sessionId → params). The agent's prompt queue is in-memory
	// only — never persisted, never replayed on session/load — so a client
	// that missed broadcasts while disconnected would keep a stale mirror
	// forever. Every forwarded queue_changed is stashed here and served on
	// demand via /api/queue/status for the FE to re-align after load.
	queueSnapshots map[string]map[string]any

	// Last focused session survives process death / host restart so we can
	// session/load it instead of session/new (which would open a blank chat).
	// Written whenever createSession / LoadSession makes a session active;
	// also snapshotted into resetRoster before the roster is wiped.
	// Persisted under ~/.capri-host/last-session.json.
	lastSessionID  string
	lastSessionCwd string

	nextAgentID atomic.Int64
	pending     sync.Map // id(float64/int) -> chan rpcResult

	nextClientReqID atomic.Int64
	clientReqs      sync.Map // requestId string -> *clientRequest

	// ── goal engine（host-side /goal，独立组件见 goal.go）──────────────
	goal goalEngine

	// ── 会话历史归一化视图缓存（session_history.go）───────────────────
	// 按 (size, mtime) 失效；只存行级元数据，不缓存信封内容。
	hist historyCache

	// bus 是事件发布/订阅内核（event_bus.go）：订阅者集合 + 全局 seq。
	bus eventBus

	hostID   string
	hostName string
	homeDir  string
}

type GrokConfig struct {
	Bin      string
	HostID   string
	HostName string
	// LastSessionFile overrides the default ~/.capri-host/last-session.json
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
	// raw carries a JSON-RPC result that is NOT a JSON object (e.g. the
	// bare array workspace_list_recent returns). requestRaw() returns it
	// verbatim; request() keeps the old behavior of coercing it to {}.
	raw any
	err error
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

// SetHostName updates the display name shown in snapshots and the messages
// sent to grok. It is called by the tray's rename action so the new name
// takes effect immediately, not only after a restart. The write happens
// under b.mu (which Snapshot already holds when reading hostName); the
// unguarded reads in the stdio loop are benign on amd64 and, worst case,
// show a stale name for one frame until the next read picks up the new one.
func (b *Bridge) SetHostName(name string) {
	b.mu.Lock()
	b.hostName = name
	b.mu.Unlock()
}

func NewBridge(cfg GrokConfig) *Bridge {
	homeDir, _ := os.UserHomeDir()
	b := &Bridge{
		cfg:           cfg,
		hostID:        cfg.HostID,
		hostName:      cfg.HostName,
		homeDir:       homeDir,
		sessions:      make(map[string]*SessionState),
		turns:         make(map[string]*observedTurn),
		bootDone:      make(chan struct{}),
		genRate:       newGenRateTracker(),
		usageLastUsed: make(map[string]int64),
		liveTools:     make(map[string]*liveToolResolver),
	}
	b.bus.init()
	b.nextAgentID.Store(1)
	b.nextClientReqID.Store(1)
	b.goal.host = b
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
	return filepath.Join(home, ".capri-host", "last-session.json")
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
		log.Printf("[capri-host] restore session %s failed: %v", sid, err)
		return fmt.Errorf("恢复会话失败: %w", err)
	}
	log.Printf("[capri-host] restored session %s (cwd=%s)", sid, cwd)
	return nil
}

func mustCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// normCwd 去掉 cwd 的尾部斜杠(根目录 "/" 除外),保证同一工作区的
// "/a/b" 与 "/a/b/" 在 grok 侧落进同一个会话目录,不再分裂成两个
// 编码目录(如 …/capri-host 与 …/capri-host%2F)。
func normCwd(cwd string) string {
	if cwd == "" {
		return cwd
	}
	if t := strings.TrimRight(cwd, "/"); t != "" {
		return t
	}
	return "/"
}

// Subscribe returns a buffered event channel; call unsubscribe to remove.
func (b *Bridge) Subscribe() (ch chan Event, unsubscribe func()) {
	return b.bus.subscribe()
}

// Broadcast publishes one event on the host's event bus.
//
// session/load 的整段重放（params._meta.isReplay，agent 在 replay.rs 给两条
// 载体的每条通知都盖上）在这里就地拦掉：重放内容与 FE 经 HTTP
// /api/session-updates 拉的是同一份，SSE/WS 再灌一遍只是几十 MB 白流量。
// 拦在 publish 之前是必须的——全局 seq 由 eventBus.publish 分配
// （event_bus.go），任何更晚的丢弃点（hub 上行、序列化前）都会在序列里留下
// 永久空洞，FE 的 EventSequencer 会把后续所有 live 事件压在乱序缓冲里等一个
// 谁都补不出来的前驱（liveSequencing.ts PENDING_SEQ_CAP）。
// session_load_started / finished 是 host 自造的边界事件，不带该标记，照旧
// 广播（FE 的多 tab 门控靠它们开关）。
func (b *Bridge) Broadcast(ev Event) {
	if replay, _ := ev[kReplayInternal].(bool); replay {
		b.replayDropped.Add(1)
		return
	}
	b.bus.publish(ev)
}

// ── roster helpers ────────────────────────────────────────────────

// openTurnLocked arms the agent-observed turn for sid at now (unix ms).
// Callers hold b.mu.
func (b *Bridge) openTurnLocked(sid string, now int64) {
	if sid == "" {
		return
	}
	if b.turns == nil {
		b.turns = make(map[string]*observedTurn)
	}
	w := b.turns[sid]
	if w == nil {
		w = &observedTurn{}
		b.turns[sid] = w
	}
	w.open = true
	w.seenAt = now
}

// closeTurnLocked disarms the agent-observed turn for sid, keeping the
// last-activity stamp for the session-list report. Callers hold b.mu.
func (b *Bridge) closeTurnLocked(sid string) {
	if w := b.turns[sid]; w != nil {
		w.open = false
	}
}

// syncBusyLocked re-projects a roster session's Busy from the two legs the
// host can observe — its own in-flight session/prompt count and the
// agent-observed turn — and reports whether the flag flipped (the caller
// announces the change). Callers hold b.mu.
func (b *Bridge) syncBusyLocked(s *SessionState) bool {
	w := b.turns[s.SessionID]
	busy := s.busyCount > 0 || (w != nil && w.open)
	if s.Busy == busy {
		return false
	}
	s.Busy = busy
	return true
}

// noteTurnActivity records that the agent is working on a turn for sid: it
// opens the observed turn, refreshes LastActiveAt and re-projects Busy.
//
// Called from the event stream on every turn-scoped update, so it must stay
// lock-cheap and broadcast only on the idle→busy / busy→idle flip (a flip
// happens at most once per turn; the update itself is never re-broadcast).
// No `busy` event is synthesized here: that event means "this frontend's
// turn started" (it arms the Waiting-for-response window), and a turn the
// host did not drive is already rendered from its own chunk/tool updates.
// Clients learn the new badge from sessions_changed + the next list fetch.
func (b *Bridge) noteTurnActivity(sid string) {
	if sid == "" {
		return
	}
	now := time.Now().UnixMilli()
	b.mu.Lock()
	b.openTurnLocked(sid, now)
	flipped := false
	if s := b.sessions[sid]; s != nil {
		if now > s.LastActiveAt {
			s.LastActiveAt = now
		}
		flipped = b.syncBusyLocked(s)
	}
	b.mu.Unlock()
	if flipped {
		b.broadcastRosterChange()
	}
}

// noteTurnEnd clears the agent-observed turn for sid (the agent reported
// turn_completed / response_completed, or a turn the host drove came to a
// definitive end) and re-projects Busy. See noteTurnActivity for the
// broadcast discipline.
func (b *Bridge) noteTurnEnd(sid string) {
	if sid == "" {
		return
	}
	b.mu.Lock()
	b.closeTurnLocked(sid)
	flipped := false
	if s := b.sessions[sid]; s != nil {
		flipped = b.syncBusyLocked(s)
	}
	b.mu.Unlock()
	if flipped {
		b.broadcastRosterChange()
	}
}

// settleTurnsLocked drops observed turns that have gone silent for longer
// than turnStaleAfter without ever reporting a terminal update, so a lost
// update cannot pin a finished session to "active" forever (turns the host
// drives with a prompt in flight are never touched by the observed leg's
// expiry — busyCount keeps them busy on its own). Awaiting-input turns are
// exempt: the agent is legitimately silent while it waits for the user.
// Stale closed entries are pruned to keep the map bounded. Reports whether
// any session's Busy flipped; the caller broadcasts once. Callers hold b.mu.
func (b *Bridge) settleTurnsLocked(now int64) bool {
	changed := false
	for sid, w := range b.turns {
		age := now - w.seenAt
		if !w.open {
			if age > turnStaleAfter.Milliseconds() {
				delete(b.turns, sid)
			}
			continue
		}
		if age <= turnStaleAfter.Milliseconds() {
			continue
		}
		s := b.sessions[sid]
		if s != nil && s.AwaitingInput {
			continue
		}
		w.open = false
		if s != nil && b.syncBusyLocked(s) {
			changed = true
		}
	}
	return changed
}

// turnEvidenceKinds are the sessionUpdate kinds that prove the agent is
// executing a turn: they arm the observed leg of Busy, which
// turn_completed / response_completed then clears.
//
// Deliberately a whitelist rather than "everything except the terminal
// kinds": a false "active" is sticky (only a terminal update or the
// turnStaleAfter expiry clears it) while a false "idle" self-heals on the
// next update, so only kinds that cannot arrive outside a running turn are
// admitted. Excluded on purpose: session metadata (title / modes / config /
// commands / model switches — those fire while a session sits idle),
// usage_update (it also rides along at turn end), the background-task and
// monitor rails (a backgrounded shell can finish long after its turn did),
// and subagent_finished (a detached subagent can report after its parent
// turn ended).
var turnEvidenceKinds = map[string]bool{
	"agent_message_chunk":     true,
	"agent_thought_chunk":     true,
	"user_message_chunk":      true,
	"tool_call":               true,
	"tool_call_update":        true,
	"tool_call_delta_chunk":   true,
	"plan":                    true,
	"plan_update":             true,
	"diff_review":             true,
	"retry_state":             true,
	"response_started":        true,
	"reasoning_completed":     true,
	"pending_interaction":     true,
	"interaction_resolved":    true,
	"subagent_spawned":        true,
	"subagent_progress":       true,
	"workflow_updated":        true,
	"goal_updated":            true,
	"scheduled_task_fired":    true,
	"auto_compact_started":    true,
	"auto_compact_completed":  true,
	"auto_compact_failed":     true,
	"auto_compact_cancelled":  true,
	"auto_continue_completed": true,
	"memory_flush_started":    true,
	"memory_flush_completed":  true,
}

// observeUpdateKind maintains the observed turn from one sessionUpdate kind:
// execution evidence opens it, a terminal kind closes it. Unknown/unmodelled
// kinds are ignored — a future kind does not get to decide session state
// until it is classified here.
func (b *Bridge) observeUpdateKind(sid, kind string) {
	switch kind {
	case "turn_completed", "response_completed":
		b.noteTurnEnd(sid)
	default:
		if turnEvidenceKinds[kind] {
			b.noteTurnActivity(sid)
		}
	}
}

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
	b.Broadcast(Event{kType: "sessions_changed", kParams: map[string]any{}})
}

// releaseBusy drops one in-flight prompt's claim on a session and, when that
// leaves the session with nothing in flight, flips it idle and notifies
// clients (busy → idle transition only — a turn that resolves while a
// sibling is still running must not report an idle the session never
// had). It only releases when the roster still holds the SAME session
// object the turn started on: resetRoster wipes and rebuilds the roster
// (and a user-requested restart may restore the session), so a stale
// turn must not clear the busy flag of a newer turn on the restored
// session.
//
// turnEnded says whether the agent actually finished the turn: a prompt that
// was answered (with a result or with an agent-side JSON-RPC error — the
// agent answers session/prompt at turn end) did, as did one the host itself
// aborted (client disconnect → session/cancel) or never delivered. A plain
// transport failure did not — on the 30-minute promptTimeout of a long turn
// the agent is usually still working, so the observed turn stays armed and
// Busy keeps following the update stream instead of falsely reporting idle
// for the rest of that turn.
func (b *Bridge) releaseBusy(s *SessionState, sessionID string, turnEnded bool) {
	b.mu.Lock()
	changed := false
	if cur := b.sessions[sessionID]; cur == s && s.busyCount > 0 {
		s.busyCount--
		if turnEnded {
			b.closeTurnLocked(sessionID)
		}
		changed = b.syncBusyLocked(s)
	}
	b.mu.Unlock()
	if changed {
		b.broadcastRosterChange()
	}
}

// setSessionAwaiting flips a session's awaiting-input state (pending
// permission / x.ai question) and notifies clients on transition. A pending
// interaction is itself proof that a turn is in flight — and State() only
// reports "awaiting" while the session is busy — so flipping to true also
// arms the observed turn and refreshes LastActiveAt, which a turn the host
// no longer tracks with a prompt would otherwise lose.
func (b *Bridge) setSessionAwaiting(id string, awaiting bool) {
	now := time.Now().UnixMilli()
	b.mu.Lock()
	s := b.sessions[id]
	changed := false
	if s != nil && s.AwaitingInput != awaiting {
		s.AwaitingInput = awaiting
		changed = true
	}
	if awaiting {
		b.openTurnLocked(id, now)
		if s != nil {
			if now > s.LastActiveAt {
				s.LastActiveAt = now
			}
			if b.syncBusyLocked(s) {
				changed = true
			}
		}
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
	if sid, ok := params[kSessionID].(string); ok && sid != "" {
		return sid
	}
	if sid, ok := params[kSessionIDS].(string); ok && sid != "" {
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
	// Read-time settle: hello / /api-status roster rows and the enriched
	// session/list must agree about which observed turns already went stale.
	// No sessions_changed here — the push rides on the ListSessions settle,
	// and every client that reads this snapshot gets the fresh value anyway.
	b.settleTurnsLocked(time.Now().UnixMilli())
	var pending []PendingReq
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		pending = append(pending, PendingReq{
			RequestID: key.(string),
			Method:    cr.Method,
			Params:    cr.Params,
			SessionID: cr.SessionID,
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
		PendingRequests:   pending,
		Capabilities:      DefaultClientCaps(),
		Roster:            roster,
		AgentStartedAt:    b.agentStartedAt,
		// b.mu is already held (Snapshot locks at entry) — read the field
		// directly, never via PermissionMode() (it would self-deadlock).
		PermissionMode: b.permMode,
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
	// bootOK: the process is up and initialize + authenticate already
	// succeeded (just no session active yet) — nothing left to boot.
	if b.ready || b.bootOK {
		b.mu.Unlock()
		return nil
	}
	if b.booting {
		// The waiter's own ctx may be cancelled first (e.g. the client
		// disconnected), but that is NOT a boot failure: the boot is
		// bounded by bootTimeout and may still succeed. Reporting ctx.Err()
		// here would be a false failure. Keep waiting for
		// the in-flight boot (bootDone always closes, even on boot error)
		// and return its real outcome.
		//
		// A finished boot has three outcomes, and a waiter must not
		// misread any of them:
		//   - ready: a session became active — done.
		//   - bootError: the boot failed — report its real error.
		//   - bootOK: the boot succeeded but no session is active yet
		//     (ensureBooted never creates a session; ready only flips once
		//     createSession / LoadSession / session/resume succeeds). This
		//     is NOT a boot failure — return nil so the caller proceeds to
		//     establish its own session on the booted process.
		// A waiter that wakes from a bootDone a NEWER boot has already
		// replaced (booting=true with a different channel) must chain onto
		// that newer boot instead of inventing a failure: its outcome
		// fields are reset (bootOK=false, bootError=""), so judging them
		// would report "agent 启动失败" against a healthy process. In
		// production a finished boot flips booting=false and closes its
		// bootDone in one critical section (the boot role's defer), so
		// booting=true with the SAME channel means the channel was closed
		// without a live boot behind it — treat it as a finished boot and
		// judge its outcome.
		done := b.bootDone
		b.mu.Unlock()
		<-done
		b.mu.Lock()
		ready := b.ready
		errMsg := b.bootError
		ok := b.bootOK
		booting := b.booting
		sameDone := b.bootDone == done
		b.mu.Unlock()
		if booting && !sameDone {
			// The boot we waited on ended and a newer one is in flight —
			// chain onto it (each recursion waits on a strictly newer
			// bootDone, so this cannot loop on the same boot).
			return b.ensureBooted(ctx)
		}
		if ready || ok {
			return nil
		}
		if errMsg != "" {
			return errors.New(errMsg)
		}
		return errors.New("agent 启动失败")
	}
	b.booting = true
	b.bootError = ""
	b.bootOK = false
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
		b.Broadcast(Event{kType: kError, "message": err.Error()})
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
			kMetaOut: initCapabilitiesMeta(),
		},
		"clientInfo": map[string]any{
			"name":    "capri-host",
			kTitle:    "ACP Host",
			"version": Version,
		},
		// `_meta` mirrors the Grok Build TUI's build_initialize_meta
		// (xai-grok-pager/src/acp/mod.rs): clientType defaults to the
		// pager's PAGER_CLIENT_TYPE and clientVersion to its build-time
		// version; the remaining seeds are env-driven and omitted when the
		// env var is absent (matching the TUI's default omit).
		kMeta: initMetaSeeds(),
	}, bootTimeout)
	if err != nil {
		// 假设 agent 可靠：初始化失败直接报错，不杀进程、不自愈。
		// 进程要不要重启由用户通过重启接口决定。
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
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
			log.Printf("[capri-host] agent protocolVersion = %v, host expects %d (continuing)", pv, protocolVersion)
		}
	}

	if err := b.authenticate(ctx, initRes); err != nil {
		// 同 initialize：只报错，不杀进程。
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
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
	b.bootOK = true
	b.mu.Unlock()
	if noSession {
		ev := Event{
			kType:       "ready",
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

// Version is stamped at build time via
// go build -ldflags "-X github.com/AgentsHarness/capri-host/internal/acp.Version=<tag>".
// Plain `go build` / `go run` falls back to "dev". It feeds both the
// initialize `clientInfo.version` and the `_meta` clientVersion, so the
// released binary's version always equals its git tag.
var Version = "dev"

// initClientType mirrors the Grok Build TUI's initialize `_meta`
// (xai-grok-pager/src/client_identity.rs): PAGER_CLIENT_TYPE is the literal
// "grok-pager"; clientVersion rides acp.Version (build-time injected).
const initClientType = "grok-pager"

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
		"clientVersion": Version,
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
	cwd = normCwd(cwd)
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
		params[kMeta] = sc.Meta
	}

	sessRes, err := b.request(ctx, "session/new", params, bootTimeout)
	if err != nil {
		// 假设 agent 可靠：创建失败直接报错，不杀进程、不自愈。
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
		return err
	}

	sid, _ := sessRes[kSessionID].(string)
	if sid == "" {
		// 协议级异常：agent 应答了但没给 sessionId —— 报错返回，不杀
		// 进程；是否需要重启由用户决定。
		err := errors.New("session/new 未返回 sessionId")
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
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
	s.configOpts = sessRes[kConfigOptions]
	s.models = sessRes["models"]
	// session/new 响应 `_meta`（agent 下发）原样存下，ready 事件 / Status
	// 透传；缺省为 nil（absent key ≠ off）。
	s.sessionMeta = sessRes[kMeta]
	b.activeSessionID = sid
	b.rememberSessionLocked(sid, cwd)
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
		kType:          "ready",
		kSessionID:     sid,
		"cwd":          sessCwd,
		"agentInfo":    agentInfo,
		"modes":        modes,
		kConfigOptions: configOpts,
		"models":       models,
		"hostId":       hostID,
		"hostName":     hostName,
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
	var methodIDs []string
	for _, m := range methodsRaw {
		mm, _ := m.(map[string]any)
		if id, _ := mm[kID].(string); id != "" {
			methodIDs = append(methodIDs, id)
		}
	}
	// 透传 agent 的认证信息：客户端不做能力判断（不检查 XAI_API_KEY
	// 等环境变量），完全采用 agent 广告的方法——agent 只在有可用凭证
	// 时才广告对应方法（env key / BYOK / 登录态），认证端还会按
	// `[auth] preferred_method` 兜底校验，客户端自行推导 api_key vs
	// session 曾导致 OIDC refresh 回归。
	// 优先采用 agent 声明的 defaultAuthMethodId（agent 是权威），
	// 未声明时取广告列表的第一个方法。
	// 注意：initialize 响应的认证字段在 `_meta`（带下划线，与请求侧
	// 一致）；读 `meta` 会永远落空而回退到第一个方法（xai.api_key），
	// 导致 session 认证（OIDC/cached_token）失效、模型请求 401。
	var methodID string
	if meta, ok := init[kMeta].(map[string]any); ok {
		if id, _ := meta["defaultAuthMethodId"].(string); id != "" {
			for _, m := range methodIDs {
				if m == id {
					methodID = id
					break
				}
			}
		}
	}
	if methodID == "" && len(methodIDs) > 0 {
		methodID = methodIDs[0]
	}
	if methodID == "" {
		return errors.New("没有可用的认证方式：agent 未广告任何认证方法（请先运行 `grok login`，或设置 XAI_API_KEY / per-model api_key）")
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
		kMeta:      meta,
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
	if m, ok := res[kMeta].(map[string]any); ok {
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
	b.bootOK = false
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
	// The agent talks JSON-RPC over the pipes attached below, so it never
	// needs a console window. Suppressing it is what keeps a double-clicked
	// GUI host from popping a terminal (see internal/procattr).
	procattr.HideConsole(cmd)
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
	// Fresh agent process: its in-memory permission mode is the default
	// ask (the agent never persists it), so the host's mirror resets too.
	b.permMode = "ask"
	b.mu.Unlock()

	go b.readStdout(rdCtx, stdout)
	go b.readStderr(stderr)
	go b.waitProcess(cmd)

	log.Printf("[capri-host] spawned %s agent stdio pid=%d", b.cfg.Bin, cmd.Process.Pid)
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
	b.mu.Unlock()
	if !same {
		// 进程已被外部清理取代（host 不再主动 killProcess，此分支仅
		// 防御）：补一条实际死亡时间戳，时间线才完整。
		if cmd.Process != nil {
			log.Printf("[capri-host] agent process %d reaped (superseded, code=%d)", cmd.Process.Pid, code)
		}
		return
	}
	lastID, _ := b.resetRoster("process-exit")
	log.Printf("[capri-host] grok process exited (code=%d), lastSession=%s", code, lastID)
	b.Broadcast(Event{
		kType:    "status",
		kText:    "连接HOST异常，请检查后重试",
		"action": "restart-agent", // 进程已死：唯一恢复动作是用户重启
	})
}

func (b *Bridge) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		t := sc.Text()
		if t != "" {
			b.Broadcast(Event{kType: "log", kText: t})
		}
	}
}

func (b *Bridge) readStdout(ctx context.Context, r io.Reader) {
	// 不用 bufio.Scanner：其单行上限（曾为 64MB）在
	// x.ai/session/updates 返回超大单行 JSON 时会把整条 stdout 通道
	// 打成永久损坏（Scanner 超长后不可恢复，所有在飞 RPC 全部失败、
	// 前端要求重启 agent）。改用 Reader 手动按行切分，只保留一个
	// 512MB 的防御上限防内存失控；JSON-RPC 行本来就整体驻留内存。
	br := bufio.NewReaderSize(r, 64*1024)
	const maxLineBytes = 512 << 20 // 512MB 单行防御上限
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line, err := br.ReadBytes('\n')
		if len(line) > maxLineBytes {
			log.Printf("[capri-host] stdout 单行超过 %dMB 上限（%d 字节），agent 输出通道已损坏", maxLineBytes>>20, len(line))
			b.failAllPending(fmt.Errorf("agent 输出通道已损坏: 单行 %d 字节超上限", len(line)))
			b.Broadcast(Event{
				kType:    "status",
				kText:    "agent 输出通道异常，请重启 agent",
				"action": "restart-agent",
			})
			return
		}
		if err != nil {
			// EOF 且该行有内容：处理完最后一行再退出（Scanner 对无
			// 尾随换行的最后一行同样会返回）。
			if len(line) > 0 && err == io.EOF {
				b.handleStdoutLine(line)
			}
			if err != io.EOF {
				log.Printf("[capri-host] stdout 扫描错误: %v — agent 输出通道已损坏", err)
				b.failAllPending(fmt.Errorf("agent 输出通道已损坏: %v", err))
				b.Broadcast(Event{
					kType:    "status",
					kText:    "agent 输出通道异常，请重启 agent",
					"action": "restart-agent",
				})
			}
			return
		}
		b.handleStdoutLine(line)
	}
}

func (b *Bridge) handleStdoutLine(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		return
	}
	b.onAgentMessage(msg)
}

func (b *Bridge) onAgentMessage(msg map[string]any) {
	if method, _ := msg[kMethod].(string); method == "session/update" {
		params, _ := msg[kParams].(map[string]any)
		b.handleSessionUpdate(params)
		return
	}

	// x.ai/* extension notifications (no id). Also accept the wrapped
	// leader form {"method":"_x.ai/foo","params":{"method":"x.ai/foo",...}}.
	if method, _ := msg[kMethod].(string); strings.HasPrefix(method, "x.ai/") || strings.HasPrefix(method, "_x.ai/") {
		params, _ := msg[kParams].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}
		realMethod, realParams := unwrapExtMethod(method, params)
		if msg[kID] == nil {
			b.handleXaiNotification(realMethod, realParams)
			return
		}
		// Agent → client extension request: forward for browser interaction.
		b.forwardXaiRequest(msg[kID], realMethod, realParams)
		return
	}

	id := msg[kID]
	if id == nil {
		return
	}

	// Response to our request?
	if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
		c := ch.(chan rpcResult)
		if errObj, has := msg[kError]; has && errObj != nil {
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
		} else if m, ok := msg[kResult].(map[string]any); ok {
			c <- rpcResult{result: m}
		} else {
			// Non-object result (a bare array, e.g. workspace_list_recent's
			// payload): keep it verbatim instead of coercing to {}, which
			// swallowed the data. requestRaw() surfaces it as-is.
			c <- rpcResult{raw: msg[kResult]}
		}
		return
	}

	// Agent → client request
	if method, _ := msg[kMethod].(string); method != "" {
		params, _ := msg[kParams].(map[string]any)
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
	// agent 在 session/load 的整段重放里给每条通知打
	// params._meta.isReplay（replay.rs）。派生事件一律盖上 host 内部标记
	// （kReplayInternal），由 Broadcast 在分配 seq 之前整条拦掉（契约
	// lite-replay [F]）——重放内容 FE 走 HTTP /api/session-updates 已拿到。
	replay := paramsIsReplay(params)
	tag := func(ev Event) Event {
		ev[kSessionID] = sid
		if replay {
			ev[kReplayInternal] = true
		}
		return ev
	}

	update, _ := params[kUpdate].(map[string]any)
	// Every standard session/update carries the session-accumulated
	// context token count in `_meta.totalTokens` (the TUI's ⇣ counter
	// source, "Accumulated token count across the session"). Surface it
	// as a usage event so clients can render live context usage — this
	// is the field x.ai extension notifications do NOT carry.
	// turn_completed / response_completed 跳过此顶部广播：回合终态的
	// usage 提取（下方）会广播同 `used` 的终态事件，此处再发就是
	// 严格重复。其余 kind 值去重：实测流水期 _meta.totalTokens 在一段
	// 输出内恒定（同一 used 连续 70+ 条），只广播值变化——上下文计数
	// 器无需逐条刷新。
	if k, _ := update[kSessionUpdate].(string); k != "turn_completed" && k != "response_completed" {
		if meta, ok := params[kMeta].(map[string]any); ok {
			if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
				b.mu.Lock()
				last := b.usageLastUsed[sid]
				b.usageLastUsed[sid] = used
				b.mu.Unlock()
				if used != last {
					b.trackUsage(sid, used, 0)
					b.Broadcast(tag(Event{kType: "usage", kUsed: used, kSize: nil}))
				}
			}
		}
	}
	if update == nil {
		return
	}
	// Kind 分发与 x.ai 通道共享（dispatchSessionUpdateKind）。modeled
	// kind 只发 typed 事件（FE 已适配）；仅当 dispatch 返回 false
	// （未建模 kind）时 generic session_notification 作为前向兼容载体
	// 发出，形状保持现状。
	if !b.dispatchSessionUpdateKind(sid, params, tag) {
		b.Broadcast(tag(Event{
			kType:   "session_notification",
			kMethod: "session/update",
			kParams: map[string]any{kUpdate: update},
		}))
	}

	// grok ships usage inside turn_completed / response_completed
	// updates instead of the standard usage_update carrier — surface the
	// context-window `used` (_meta.totalTokens, the TUI ⇣ counter source)
	// as a normal usage event, exactly like handleXaiNotification does.
	// 实测 FE 只 merge used/size（tools.ts 从未读 usage 对象），回合
	// 账本对象是死字段——不再透传。typed 事件（dispatch 已发）是 FE
	// 回合封口语义；usage 提取保留在此（官方载体带 size:nil；x.ai 载体
	// 不带 —— 保持原状，不统一）。
	if kind, _ := update[kSessionUpdate].(string); kind == "turn_completed" || kind == "response_completed" {
		ev := Event{kType: "usage"}
		if meta, ok := params[kMeta].(map[string]any); ok {
			if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
				ev[kUsed] = used
				b.trackUsage(sid, used, 0)
			}
		}
		// Only broadcast when there is something to update — an empty
		// usage event (no _meta.totalTokens) would otherwise null the
		// client's `used`.
		if len(ev) > 1 {
			ev[kSize] = nil
			b.Broadcast(tag(ev))
		}
	}
}

// paramsIsReplay 报告这条 session/update 的 params._meta 是否带 agent 的
// 重放标记（isReplay，replay.rs 为 session/load 重放的每条通知盖上）。
// 只有真布尔值算重放——agent 写的是 serde bool，字符串/数字一律不认。
func paramsIsReplay(params map[string]any) bool {
	meta, ok := params[kMeta].(map[string]any)
	if !ok {
		return false
	}
	v, _ := meta["isReplay"].(bool)
	return v
}

// attachStreamMeta copies the shell-stamped NotificationMeta (params._meta)
// onto a typed stream event:
//   - turnStartMs → ev["turnStartMs"]: authoritative turn start — without
//     it the FE measures adopted (queue-drained) turns from the adoption
//     moment and renders fake "Worked for 0.0s" markers.
//   - agentTimestampMs → ev["agentTimestampMs"]: authoritative send time
//     (user_chunk echoes fix the optimistic user row's ts with it).
//   - eventId → ev["eventId"]: agent 事件 id（"{sessionId}-{N}"）提升为
//     事件顶层（透传，不当 live↔快照主键；N 会撞车）。params._meta
//     无该键时不带（absent key ≠ off）。
//   - agentTimestampMs - streamStartMs → ev["elapsedMs"]: real thought
//     duration (live thought blocks otherwise seal against the local
//     first-seen timer, understating turns adopted mid-flight).
//
// turn_completed already carries params._meta as `meta`; only the
// high-frequency stream events need this.
func attachStreamMeta(ev Event, params map[string]any) Event {
	meta, ok := params[kMeta].(map[string]any)
	if !ok || len(meta) == 0 {
		return ev
	}
	if v, ok := meta["turnStartMs"]; ok {
		ev["turnStartMs"] = v
	}
	if v, ok := meta["agentTimestampMs"]; ok {
		ev["agentTimestampMs"] = v
	}
	if v, ok := meta["eventId"]; ok {
		ev["eventId"] = v
	}
	if v, ok := meta["streamStartMs"]; ok {
		ev["streamStartMs"] = v
	}
	if agent, ok1 := asInt(meta["agentTimestampMs"]); ok1 {
		if start, ok2 := asInt(meta["streamStartMs"]); ok2 && agent >= start {
			ev["elapsedMs"] = agent - start
		}
	}
	return ev
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
	update, _ := params[kUpdate].(map[string]any)
	if update == nil {
		return false
	}
	kind, _ := update[kSessionUpdate].(string)
	// 回合观察（Busy 的观察腿）：两个载体（官方 session/update 与
	// x.ai/session_notification）都经过这里，是唯一收口点。
	b.observeUpdateKind(sid, kind)
	// meta 保全：params._meta 非空时随 typed kind 事件携带。
	var kindMeta any
	if m, ok := params[kMeta].(map[string]any); ok && len(m) > 0 {
		kindMeta = m
	}
	// image 事件不经 tag 闭包（imageEvent 自带 sessionId），重放标记只能在
	// 这里单独补上。整个 dispatch 的广播点都过 tag，所以在这一处包一层即
	// 覆盖两条载体（官方 session/update 与 x.ai/session/update）——漏掉任一
	// 条都会让 session/load 的重放只有一半不上总线，进而把同一批历史里的
	// 图片与文字事件劈成两半。
	replay := paramsIsReplay(params)
	if replay {
		inner := tag
		tag = func(ev Event) Event {
			ev[kReplayInternal] = true
			return inner(ev)
		}
	}
	switch kind {
	case "agent_message_chunk":
		if text := contentText(update[kContent]); text != "" {
			ev := attachStreamMeta(Event{kType: "chunk", kText: text}, params)
			if mid, ok := update["messageId"]; ok {
				ev["messageId"] = mid
			}
			// Mirror user_message_chunk: forward the chunk + content-block
			// meta (wire shape: update._meta = ContentChunk.meta →
			// hideFromScrollback; update.content._meta = TextContent.meta →
			// displayText / displayAsCron) so live rendering classifies
			// system-injected prompts exactly like the replay path does.
			if meta, ok := update[kMeta].(map[string]any); ok {
				if v, ok := meta["hideFromScrollback"]; ok {
					ev["hideFromScrollback"] = v
				}
			}
			if content, ok := update[kContent].(map[string]any); ok {
				if meta, ok := content[kMeta].(map[string]any); ok {
					for _, k := range []string{"displayText", "displayAsCron"} {
						if v, ok := meta[k]; ok {
							ev[k] = v
						}
					}
				}
			}
			// 不携带 fullUpdate（整份 update）：typed 字段即 wire 契约，
			// 两条出口（SSE / hub）本就剥离该键，且 FE 无消费者。
			b.Broadcast(tag(ev))
			// 生成输出速率（按流式字符估算，见 genrate.go）：与 chunk 同轨
			// 广播，节流后每个 session 每 ≥250ms 至多一条 active。时间源
			// 优先 _meta.agentTimestampMs + streamStartMs（agent 累计口径：
			// 速率 = 流起点之后的全部字符 / 墙钟，含首包）。
			now, streamStartMs, agentClock := metaAgentTs(params)
			if rate, ok := b.genRate.observe(sid, text, now, agentClock, streamStartMs); ok {
				b.Broadcast(tag(Event{kType: "gen_rate", kRate: rate, kActive: true}))
			}
		}
		// Image blocks ride the same chunk carrier: emit one typed image
		// event per block (text parts still go through the chunk event).
		for _, img := range contentImages(update[kContent]) {
			b.broadcastImage(sid, img, replay)
		}
		return true
	case "user_message_chunk":
		// 用户输入打断生成段：静默复位速率估算器（不发任何事件，
		// 客户端显示值在下一段 live 更新前保留）。
		b.genRate.reset(sid)
		if text := contentText(update[kContent]); text != "" {
			ev := attachStreamMeta(Event{kType: "user_chunk", kText: text}, params)
			// Forward the chunk + content-block meta (wire shape:
			// update._meta = ContentChunk.meta → hideFromScrollback;
			// update.content._meta = TextContent.meta → displayText /
			// displayAsCron) so live rendering classifies system-injected
			// prompts exactly like the replay path does.
			if meta, ok := update[kMeta].(map[string]any); ok {
				if v, ok := meta["hideFromScrollback"]; ok {
					ev["hideFromScrollback"] = v
				}
			}
			if content, ok := update[kContent].(map[string]any); ok {
				if meta, ok := content[kMeta].(map[string]any); ok {
					for _, k := range []string{"displayText", "displayAsCron"} {
						if v, ok := meta[k]; ok {
							ev[k] = v
						}
					}
				}
			}
			// 同 agent_message_chunk：不携带 fullUpdate（typed 字段即 wire 契约）。
			b.Broadcast(tag(ev))
		}
		for _, img := range contentImages(update[kContent]) {
			b.broadcastImage(sid, img, replay)
		}
		return true
	case "agent_thought_chunk":
		// content is typically { "type":"text", "text":"..." }; accept a few shapes
		if text := contentText(update[kContent]); text != "" {
			ev := attachStreamMeta(Event{kType: "thought", kText: text}, params)
			// Forward the chunk meta verbatim (hideFromScrollback etc.)
			// so live rendering treats it like the replay path does.
			if meta, ok := update[kMeta].(map[string]any); ok {
				ev[kMetaOut] = meta
			}
			b.Broadcast(tag(ev))
			// 思考文本同样更新生成速率（见 genrate.go）。
			now, streamStartMs, agentClock := metaAgentTs(params)
			if rate, ok := b.genRate.observe(sid, text, now, agentClock, streamStartMs); ok {
				b.Broadcast(tag(Event{kType: "gen_rate", kRate: rate, kActive: true}))
			}
		}
		return true
	case "tool_call":
		// 工具执行打断生成段：复位段状态（新段从首包重新计时）并广播
		// active:false（不带 rate）——前端收到后清除速率显示（只在输
		// 出过程中显示）。
		b.genRate.reset(sid)
		b.liveToolResolve(sid, "tool_call", update, params, replay)
		b.Broadcast(tag(Event{kType: "gen_rate", kActive: false}))
		b.Broadcast(tag(Event{kType: "tool_call", kToolCall: update}))
		return true
	case "tool_call_update":
		b.liveToolResolve(sid, "tool_call_update", update, params, replay)
		b.Broadcast(tag(Event{kType: "tool_call_update", kToolCallUpdate: update}))
		return true
	case "plan":
		b.Broadcast(tag(Event{kType: "plan", kEntries: update[kEntries]}))
		return true
	case "usage_update":
		b.trackUsage(sid, toInt64(update[kUsed]), toInt64(update[kSize]))
		b.Broadcast(tag(Event{
			kType: "usage",
			kUsed: update[kUsed],
			kSize: update[kSize],
			kCost: update[kCost],
		}))
		return true
	case "current_mode_update":
		b.mu.Lock()
		if ms := update[kModeState]; ms != nil {
			if s := b.sessions[sid]; s != nil {
				s.modes = ms
			}
		}
		var modes any
		if s := b.sessions[sid]; s != nil {
			modes = s.modes
		}
		b.mu.Unlock()
		b.Broadcast(tag(Event{kType: "modes_update", "modes": modes}))
		return true
	case "config_option_update":
		b.mu.Lock()
		if co := update[kConfigOptions]; co != nil {
			if s := b.sessions[sid]; s != nil {
				s.configOpts = co
			}
		}
		var co any
		if s := b.sessions[sid]; s != nil {
			co = s.configOpts
		}
		b.mu.Unlock()
		b.Broadcast(tag(Event{kType: "config_options_update", kConfigOptions: co}))
		return true
	case "available_commands_update":
		b.Broadcast(tag(Event{kType: "commands_update", kCommands: update[kCommands]}))
		return true
	case "session_info_update", "session_info":
		// session_info_update 是官方 ACP SessionUpdate 的 kind（serde tag
		// "sessionUpdate"，agent-client-protocol-schema client.rs:106）；
		// 旧版 x.ai 载体曾用裸 "session_info" 形式——两者等价合并。
		// title/updatedAt roster 跟踪两个载体都要有。
		b.mu.Lock()
		if s := b.sessions[sid]; s != nil {
			if t, ok := update[kTitle].(string); ok && t != "" {
				s.Title = t
			}
			if u, ok := update[kUpdatedAt].(string); ok && u != "" {
				s.UpdatedAt = u
			}
		}
		b.mu.Unlock()
		b.Broadcast(tag(Event{
			kType:      "session_info",
			kTitle:     update[kTitle],
			kUpdatedAt: update[kUpdatedAt],
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
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
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
		// 回合终态：复位段状态并广播 active:false（不带 rate）——前端
		// 清除速率显示（只在输出过程中显示，无回合末冻结值）。
		b.genRate.reset(sid)
		b.liveToolResolve(sid, "turn_completed", nil, params, replay)
		b.Broadcast(tag(Event{kType: "gen_rate", kActive: false}))
		// response_completed 不广播 typed 事件：agent 实测从不发该
		// kind（updates.jsonl 3383/3383 回合终态均为 turn_completed），
		// FE 对 typed response_completed 无消费（turnEnd.ts 无 case，
		// events.ts 重写回 generic 后 notifApps 显式忽略）。保留副作用
		// （gen_rate 复位 / 调用方 usage 提取），省掉整份 update 白传。
		if kind == "response_completed" {
			return true
		}
		// typed 事件是 FE 回合封口语义（update 原样含 stop_reason /
		// prompt_id / usage 等字段）；usage 提取保留在两个载体
		// （handleSessionUpdate / handleXaiNotification）。
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
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
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
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
	update, _ := params[kUpdate].(map[string]any)
	ev := Event{
		kType: "scheduled_task_created",
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
	if meta, ok := params[kMeta].(map[string]any); ok {
		ev[kMetaOut] = meta
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
	update, _ := params[kUpdate].(map[string]any)
	ev := Event{
		kType:    "scheduled_task_deleted",
		"taskId": pick([]map[string]any{task, update, params}, "taskId", "task_id"),
	}
	// reason 归一化（1.0.4 起 SessionUpdate::ScheduledTaskDeleted 带
	// reason：completed/expired/deleted/shutdown）：依次从 update →
	// params → task 提取，无来源时缺省 unknown（旧宿主/旧版本数据）。
	if reason, _ := pick([]map[string]any{update, params, task}, "reason").(string); reason != "" {
		ev["reason"] = reason
	} else {
		ev["reason"] = "unknown"
	}
	// rawParams preserves the full original params (e.g. session_id +
	// _meta.eventId / scheduler generation-revision stamps).
	ev["rawParams"] = params
	if meta, ok := params[kMeta].(map[string]any); ok {
		ev[kMetaOut] = meta
	}
	b.Broadcast(tag(ev))
}

func (b *Bridge) liveToolLocked(sid string) *liveToolResolver {
	if b.liveTools == nil {
		b.liveTools = make(map[string]*liveToolResolver)
	}
	r := b.liveTools[sid]
	if r == nil {
		r = &liveToolResolver{}
		b.liveTools[sid] = r
	}
	return r
}

// liveToolResolve 给实时工具事件打上与历史主路径同一套合成 ID。
// session/load 重放（isReplay）整段跳过：HTTP 历史已经按 synth:call:<ts>:<k>
// 注入过，重放再跑会把 live 计数器/open 集冲掉，后续真 live 对不上。
func (b *Bridge) liveToolResolve(sid, su string, update, params map[string]any, replay bool) {
	if replay {
		return
	}
	ts, eventID := paramsSynthMeta(params)

	b.mu.Lock()
	r := b.liveToolLocked(sid)
	if r.seeded {
		r.handleLive(su, update, ts, eventID)
		b.mu.Unlock()
		return
	}
	cwd := ""
	if s := b.sessions[sid]; s != nil {
		cwd = s.Cwd
	}
	b.mu.Unlock()

	var view *normalizedHistory
	if cwd != "" && sid != "" {
		if path := sessionUpdatesFile(b.grokHome(), cwd, sid); path != "" {
			view, _ = b.normalizedSessionHistory(path)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	r = b.liveToolLocked(sid)
	if !r.seeded {
		r.seedFrom(view)
	}
	r.handleLive(su, update, ts, eventID)
}

// handleModelSwitchKind applies model_auto_switched / model_changed
// (extensions/notification.rs SessionUpdate::ModelAutoSwitched /
// ModelChanged; 字段名以 capri-fe chat.ts:3242-3260 与 grok 源码为准):
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
		b.Broadcast(tag(Event{kType: "models_update", kParams: models}))
	}
	ev := Event{kType: "model", "modelId": id}
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
	if inner, ok := params[kMethod].(string); ok && strings.HasPrefix(inner, "x.ai/") {
		if innerParams, ok := params[kParams].(map[string]any); ok {
			_, hasCamel := innerParams[kSessionID]
			_, hasSnake := innerParams[kSessionIDS]
			if !hasCamel && !hasSnake {
				if sid, ok := params[kSessionID].(string); ok && sid != "" {
					innerParams[kSessionID] = sid
				} else if sid, ok := params[kSessionIDS].(string); ok && sid != "" {
					innerParams[kSessionIDS] = sid
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
	// dashboard can bucket multi-session activity. session/load 的重放走
	// dispatchSessionUpdateKind 的 tag 包装拦（两条载体共用）；这里的 generic
	// 兜底广播不过 dispatch，必须自己盖章，否则未建模 kind 的重放仍会上总线。
	sid := b.sessionIdFrom(params)
	replay := paramsIsReplay(params)
	withSid := func(ev Event) Event {
		if sid != "" {
			ev[kSessionID] = sid
		}
		if replay {
			ev[kReplayInternal] = true
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
			b.Broadcast(withSid(Event{kType: "session_notification", kMethod: method, kParams: params}))
		}
		// grok ships usage inside response_completed / turn_completed
		// notifications instead of the standard usage_update carrier —
		// surface the context-window `used` (_meta.totalTokens, the TUI
		// ⇣ counter source) as a normal usage event. x.ai notifications
		// usually don't carry it (keep the last known usage on turn end,
		// same as the TUI). 实测 FE 只 merge used/size，usage 账本对象
		// 是死字段——不再透传。typed 事件（dispatch 已发）是 FE 回合
		// 封口语义；usage 提取保留在此（x.ai 载体不带 size:nil，官方
		// 载体带 —— 保持原状，不统一）。
		if up, ok := params[kUpdate].(map[string]any); ok {
			if kind, _ := up[kSessionUpdate].(string); kind == "response_completed" || kind == "turn_completed" {
				ev := Event{kType: "usage"}
				if meta, ok := params[kMeta].(map[string]any); ok {
					if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
						ev[kUsed] = used
						b.trackUsage(sid, used, 0)
					}
				}
				// Only broadcast when there is something to update —
				// an empty usage event (no _meta.totalTokens) would
				// otherwise null the client's `used`.
				if len(ev) > 1 {
					b.Broadcast(withSid(ev))
				}
			}
		}
	case "x.ai/task_backgrounded":
		b.Broadcast(withSid(Event{kType: "task_backgrounded", kParams: params}))
	case "x.ai/task_completed":
		b.Broadcast(withSid(Event{kType: "task_completed", kParams: params}))
	case "x.ai/monitor_event":
		b.Broadcast(withSid(Event{kType: "monitor_event", kParams: params}))
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
		b.Broadcast(withSid(Event{kType: "git_head_changed", kParams: params}))
	case "x.ai/yolo_mode_changed":
		// Permission-mode change: CLIENT-SCOPED on the agent side (applies
		// to every resident session of the sending client), so the broadcast
		// deliberately carries NO sessionId — withSid's active-session
		// fallback would mis-tag it in multi-session setups and mislead
		// frontends into treating a global toggle as session-scoped.
		// Also mirror the agent's (echoed) mode into the host record so
		// later hello snapshots carry the agent's real state.
		b.recordPermissionMode(permissionModeFromParams(params))
		b.Broadcast(Event{kType: "yolo_mode_changed", kParams: params})
	case "x.ai/mcp/server_status":
		b.Broadcast(withSid(Event{kType: "mcp_server_status", kParams: params}))
	case "x.ai/mcp/tools_changed", "x.ai/mcp_initialized":
		b.Broadcast(withSid(Event{kType: "mcp_tools_changed", kParams: params}))
	case "x.ai/mcp/servers_updated":
		b.Broadcast(withSid(Event{kType: "mcp_servers_updated", kParams: params}))
	case "x.ai/sessions/changed":
		b.Broadcast(Event{kType: "sessions_changed", kParams: params})
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
				ev := Event{kType: "models_update", kParams: models}
				// raw carries the ORIGINAL notification params so no field
				// of the machine-wide broadcast is lost to the merge.
				ev["raw"] = params
				b.Broadcast(withSid(ev))
			}
		} else {
			b.mu.Unlock()
		}
	case "x.ai/announcements/update":
		b.Broadcast(Event{kType: "announcements_update", kParams: params})
	case "x.ai/scheduled_task_fired":
		b.Broadcast(withSid(Event{kType: "scheduled_task_fired", kParams: params}))
	case "x.ai/scheduled_task_inject_prompt":
		b.Broadcast(withSid(Event{kType: "scheduled_task_inject_prompt", kParams: params}))
	case "x.ai/scheduled_task_created":
		// 归一化逻辑抽到 broadcastScheduledTaskCreated（与 sessionUpdate
		// kind 分发共享）：task/rawTask/rawParams/meta 保全，见该 helper。
		b.broadcastScheduledTaskCreated(sid, params, withSid)
	case "x.ai/scheduled_task_deleted":
		b.broadcastScheduledTaskDeleted(sid, params, withSid)
	case "x.ai/session/prompt_complete":
		b.Broadcast(withSid(Event{kType: "prompt_complete", kParams: params}))
	case "x.ai/session/updates/chunk":
		// 分块拉取会话更新（extensions/session_updates.rs:290-330
		// send_streamed_chunks）：{sessionId, index, updates: [原始
		// updates.jsonl 行], done}，可选 routing meta。原样透传。
		b.Broadcast(withSid(Event{kType: "session_updates_chunk", kParams: params}))
	case "x.ai/queue/changed":
		// prompt 队列变更（session/prompt_queue.rs QUEUE_CHANGED_METHOD）。
		// 队列状态不落盘、load 不回放 —— 缓存最近一次快照，供
		// /api/queue/status 在 FE load 会话后主动拉取对齐镜像
		// （断线期间错过的广播靠它补上）。cloneAny 隔离副本，快照
		// 与后续 broadcast 序列化互不共享存储。
		if sid != "" {
			b.mu.Lock()
			if b.queueSnapshots == nil {
				b.queueSnapshots = map[string]map[string]any{}
			}
			if snap, ok := cloneAny(params).(map[string]any); ok {
				b.queueSnapshots[sid] = snap
			}
			b.mu.Unlock()
		}
		b.Broadcast(withSid(Event{kType: "queue_changed", kParams: params}))
	case "x.ai/config_changed":
		// 配置变更（MCP 初始化取消 / agent/app.rs leader 广播）。
		b.Broadcast(withSid(Event{kType: "config_changed", kParams: params}))
	case "x.ai/settings/update":
		// 设置热更新推送（agent/mvp_agent/mod.rs:2042）。
		b.Broadcast(withSid(Event{kType: "settings_update", kParams: params}))
	case "x.ai/fs_notify":
		// 文件系统变更（session/fs_watch.rs:342）：{sessionId,
		// event:{kind, paths}}。
		b.Broadcast(withSid(Event{kType: "fs_notify", kParams: params}))
	case "x.ai/fs/index":
		// 全量文件索引（session/fs_watch.rs:428）。
		b.Broadcast(withSid(Event{kType: "fs_index", kParams: params}))
	case "x.ai/fs/index/delta":
		// 增量文件索引（session/fs_watch.rs:368）。
		b.Broadcast(withSid(Event{kType: "fs_index_delta", kParams: params}))
	case "x.ai/search/fuzzy/status":
		// 模糊搜索进度（extensions/search.rs:158）。
		b.Broadcast(withSid(Event{kType: "search_fuzzy_status", kParams: params}))
	case "x.ai/search/content/status":
		// 内容搜索进度（extensions/search.rs:222）。
		b.Broadcast(withSid(Event{kType: "search_content_status", kParams: params}))
	case "x.ai/git/worktree/status":
		// worktree 创建进度（extensions/worktree.rs:37）。
		b.Broadcast(withSid(Event{kType: "git_worktree_status", kParams: params}))
	case "x.ai/mcp/init_progress":
		// MCP 初始化进度（extensions/mcp.rs INIT_PROGRESS）。
		b.Broadcast(withSid(Event{kType: "mcp_init_progress", kParams: params}))
	case "x.ai/terminal/pty/notification":
		// PTY 输出通知（terminal/pty_session.rs:23 NOTIFICATION_METHOD）。
		b.Broadcast(withSid(Event{kType: "pty_notification", kParams: params}))
	case "x.ai/session/interjection":
		// 回合中插话（session/acp_session_impl/interjection.rs:171）：
		// {sessionId, text, interjectionId?}。
		b.Broadcast(withSid(Event{kType: "session_interjection", kParams: params}))
	case "x.ai/follow_ups":
		// 回合结束后的建议 chips（TUI app/acp_handler/follow_ups.rs 渲染；
		// params 含 response_id / follow_ups 列表，原样透传）。
		b.Broadcast(withSid(Event{kType: "follow_ups", kParams: params}))
	case "x.ai/leader/version_mismatch":
		// leader 与 client 版本不一致横幅（TUI xai-grok-pager/src/acp/
		// version_mismatch.rs 渲染；wire params 为 {clientVersion,
		// leaderVersion, message}——message 被 TUI 忽略，原样透传）。
		b.Broadcast(withSid(Event{kType: "leader_version_mismatch", kParams: params}))
	case "x.ai/leader_reconnected":
		// leader 重连信号（xai-grok-pager-bin/src/main.rs:1354 等，
		// params 可为空）。
		b.Broadcast(withSid(Event{kType: "leader_reconnected", kParams: params}))
	default:
		b.Broadcast(withSid(Event{kType: "ext_notification", kMethod: method, kParams: params}))
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
		kType:       "client_request",
		"requestId": reqID,
		kMethod:     method,
		kParams:     params,
		kSessionID:  cr.SessionID,
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
		kType:       "client_request",
		"requestId": reqID,
		kMethod:     method,
		kParams:     params,
		kSessionID:  cr.SessionID,
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
	// Multi-tab: every connected client received client_request. Once this
	// request is settled (any path), broadcast so other tabs drop the card
	// — without this the answering tab clears locally but siblings keep a
	// zombie permission / question UI until reload.
	defer b.broadcastClientRequestResolved(reqID, cr.SessionID)
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

// broadcastClientRequestResolved notifies every SSE subscriber that a
// forwarded client request (permission / x.ai question / …) is no longer
// pending. Clients remove matching pending / xaiRequests rows by requestId.
func (b *Bridge) broadcastClientRequestResolved(reqID, sessionID string) {
	ev := Event{
		kType:       "client_request_resolved",
		"requestId": reqID,
	}
	if sessionID != "" {
		ev[kSessionID] = sessionID
	}
	b.Broadcast(ev)
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
		kMeta:     meta,
	}
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
// Both fields are optional; with none set the response carries no `_meta`.
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
		kJSONRPC: "2.0",
		kID:      id,
		kResult:  result,
	})
}

func (b *Bridge) respondError(id any, message string, code int) {
	_ = b.write(map[string]any{
		kJSONRPC: "2.0",
		kID:      id,
		kError: map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

// request 是标准 JSON-RPC 请求：固定 timeout，不因 agent 活动而顺延。
// （曾有过按会话活跃度顺延 prompt 截止时刻的设计，2026-08-14 取消：
// 默认 agent 可靠，超时到点即报错，不做续命式的自愈。）
// 返回的 result 按对象处理：非对象 result（如裸数组）保持旧行为被
// 规整为 {}。需要原样拿到任意 JSON 值的调用方用 requestRaw。
func (b *Bridge) request(ctx context.Context, method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	res, err := b.requestRaw(ctx, method, params, timeout)
	if err != nil {
		return nil, err
	}
	if m, ok := res.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{}, nil
}

// requestRaw 与 request 相同，但 result 原样返回（任意 JSON 值：对象、
// 数组、标量、nil），不做对象规整。x.ai 扩展方法里唯一返回裸数组的
// workspace_list_recent 必须走这里，否则数组会被 request 吞成 {}。
func (b *Bridge) requestRaw(ctx context.Context, method string, params map[string]any, timeout time.Duration) (any, error) {
	id := b.nextAgentID.Add(1)
	key := idKey(id)
	ch := make(chan rpcResult, 1)
	// Agent replies with JSON numbers (float64); idKey normalizes both sides.
	b.pending.Store(key, ch)

	msg := map[string]any{
		kJSONRPC: "2.0",
		kID:      id,
		kMethod:  method,
		kParams:  params,
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
		if res.raw != nil {
			return res.raw, res.err
		}
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

// PromptWithOpts sends session/prompt to the given session (default:
// active) and blocks until the turn finishes. When no session exists yet
// (the host never auto-creates one at boot), the first prompt restores the
// last known session if one was remembered; only a machine with no
// last-session pointer starts a brand-new conversation. A failed restore is
// an error — never silently open a blank chat. Busy is per-session: other
// sessions can keep running turns in parallel (the agent process is
// multi-session), and a busy session is NOT rejected — the agent accepts
// mid-turn session/prompt and queues it in its own authoritative
// pending_inputs, so the host forwards and the turns run in submission
// order. The optional fields (messageId / _meta) are forwarded on the
// session/prompt params when set; empty opts omit them from the wire
// entirely. Returns the turn's stopReason plus the response `_meta` (the
// agent's prompt-result meta, nil when absent) so the HTTP layer can pass
// it through to the browser.
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
				// 受理即返回后 prompt 的 HTTP 响应不再携带回合级错误：
				// 恢复失败必须经 live 通道送达。会话尚不存在 → 无
				// sessionId，前端按 host 级错误渲染（横幅 + conn error）。
				b.broadcastPromptError("", err)
				return "", nil, err
			}
		} else if err := b.NewSession(ctx, SessionConfig{}); err != nil {
			b.broadcastPromptError("", err)
			return "", nil, err
		}
		b.mu.Lock()
		sessionID = b.activeSessionID
		s = b.sessions[sessionID]
	}
	if s == nil {
		b.mu.Unlock()
		// 显式未知会话通常被 handlePrompt 的同步检查拦成 HTTP 404；
		// 走到这里说明是竞态（受理后会话被删）或 bridge 直连调用——
		// 补一条 live 事件，前端按带 sessionId 的回合级错误渲染。
		b.broadcastPromptError(sessionID, &HTTPError{Code: 404, Msg: "会话不存在"})
		return "", nil, &HTTPError{Code: 404, Msg: "会话不存在"}
	}
	// A busy session no longer 409s: the agent accepts mid-turn
	// session/prompt and queues it in its own pending_inputs (popped when
	// the current turn ends), so we just forward. Busy stays the "any turn
	// in flight" projection via busyCount — an overlapping prompt must not
	// let the first resolver flip the session idle while the second is
	// still running.
	//
	// A prompt this host sends starts an agent turn, so the observed leg is
	// armed too: both legs then agree, and the release at promptTimeout
	// cannot strand a still-working session in "idle" (the update stream
	// keeps the turn open until turn_completed).
	first := s.busyCount == 0
	s.busyCount++
	s.LastActiveAt = time.Now().UnixMilli()
	b.openTurnLocked(sessionID, s.LastActiveAt)
	b.syncBusyLocked(s)
	// Keep last-session pointer fresh even if the user only talks to this
	// session without re-loading it (multi-session focus via prompt).
	b.rememberSessionLocked(sessionID, s.Cwd)
	b.mu.Unlock()
	if first {
		// 0→1 transition: announce the busy turn exactly once. A second
		// in-flight prompt must not re-announce — the session never left
		// busy between the two.
		b.Broadcast(Event{kType: "busy", kSessionID: sessionID})
	}

	// turnEnded records whether the agent actually finished this turn. It
	// stays false on transport failures (timeout / write error / process
	// death), where the agent may still be working.
	turnEnded := false
	defer func() {
		b.releaseBusy(s, sessionID, turnEnded)
	}()

	if err := b.ensureBooted(ctx); err != nil {
		// The prompt never reached the agent — this turn does not exist, so
		// release both legs instead of leaving a phantom running session.
		turnEnded = true
		return "", nil, err
	}

	// convert blocks
	prompt := make([]any, 0, len(blocks))
	for _, bl := range blocks {
		prompt = append(prompt, map[string]any(bl))
	}

	params := map[string]any{
		kSessionID: sessionID,
		"prompt":   prompt,
	}
	if opts.MessageID != "" {
		params["messageId"] = opts.MessageID
	}
	if len(opts.Meta) > 0 {
		params[kMeta] = opts.Meta
	}
	res, err := b.request(ctx, "session/prompt", params, promptTimeout)
	if err != nil {
		// 错误要不要上事件流、怎么留痕，由 reportPromptFailure 判定：回合
		// 还在正常输出时，prompt 预算耗尽不是回合失败。
		b.reportPromptFailure(sessionID, err)
		// The client (browser) went away mid-turn. The agent process may be
		// perfectly healthy — and other sessions may be running parallel
		// turns in the same process — so nothing is killed or cancelled
		// process-wide: cancel the orphaned turn so the session does not
		// stay busy for the full promptTimeout, and return; the client can
		// simply resend.
		if errors.Is(err, context.Canceled) {
			b.Cancel(sessionID)
			b.Broadcast(Event{kType: "status", kText: "连接已断开，本次回复已取消，请重新发送", kSessionID: sessionID})
			// The host just ordered this turn cancelled, so it stops
			// claiming the session is working. If the agent keeps streaming
			// anyway, its next update re-arms the observed turn — closing
			// here cannot hide a live session for longer than one update.
			turnEnded = true
			return "", nil, err
		}
		// A plain JSON-RPC error is the AGENT rejecting the turn (e.g. the
		// model API's 400 "Internal Error: …") — the process answered and is
		// healthy; the error event above already surfaced the failure.
		// Transport-level failures (timeout / write error) are logged and
		// surfaced by reportPromptFailure — the host never kills or restarts
		// the agent on its own (assume the agent is reliable; the process
		// lifecycle belongs to the user via the restart endpoint). The
		// failed turn is not retried.
		var rpcErr *RPCError
		if errors.As(err, &rpcErr) {
			// The agent answered: this turn is over, both legs release.
			turnEnded = true
		}
		// Otherwise the RPC died on the way, not on the turn: the agent may
		// still be working it, so the observed turn stays armed.
		return "", nil, err
	}
	turnEnded = true
	sr, _ := res["stopReason"].(string)
	if sr == "" {
		sr = "unknown"
	}
	// 响应 `_meta`（agent 下发的 prompt-result meta）原样透传：done 事件
	// 带 `meta`、返回值给 HTTP 层；仅非空才带（absent key ≠ off）。
	meta, _ = res[kMeta].(map[string]any)
	ev := Event{kType: "done", "stopReason": sr, kSessionID: sessionID}
	if len(meta) > 0 {
		ev[kMetaOut] = meta
	}
	b.Broadcast(ev)
	return sr, meta, nil
}

// turnStillStreaming reports whether the agent is provably working on a turn
// for this session right now: an observed turn whose last update landed
// within turnLivenessWindow. A merely-open-but-silent turn does not qualify —
// that is the shape a wedged agent leaves behind.
func (b *Bridge) turnStillStreaming(sessionID string) bool {
	now := time.Now().UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()
	w := b.turns[sessionID]
	return w != nil && w.open && now-w.seenAt <= turnLivenessWindow.Milliseconds()
}

// reportPromptFailure decides how a failed session/prompt reaches clients and
// what it leaves in the log. Returns whether the error rode the live channel.
//
// An agent JSON-RPC error means the turn itself failed (the process answered)
// and a canceled caller context means the host just cancelled the turn — both
// always surface, as before. A transport failure is different: the common one
// is a turn that outlived the 30-minute promptTimeout while the agent kept
// streaming it. Reporting that as a turn error made every client viewing the
// session seal the live stream, drop an error row with a "restart agent"
// button into the scrollback (restarting would kill the healthy turn it was
// warning about) and reset its turn timer — mid-turn. While the update stream
// proves work is ongoing, the failure is a host-side budget event: it stays in
// the log. Clients still see the truth through the session's busy state, which
// the observed turn keeps until turn_completed.
func (b *Bridge) reportPromptFailure(sessionID string, err error) bool {
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) || errors.Is(err, context.Canceled) {
		b.broadcastPromptError(sessionID, err)
		return true
	}
	// 留痕底层错误：区分超时 / 写失败 / 进程退出，事后才能还原事故。
	log.Printf("[capri-host] prompt transport failure (session=%s): %v", sessionID, err)
	if b.turnStillStreaming(sessionID) {
		log.Printf("[capri-host] prompt error not surfaced (session=%s): agent turn still streaming, session stays active", sessionID)
		return false
	}
	b.broadcastPromptError(sessionID, err)
	return true
}

// broadcastPromptError surfaces a prompt-turn failure on the live channel.
// POST /api/prompt accepts immediately and no longer carries turn-level
// error bodies, so every turn failure the host reports must ride the
// SSE/WS event stream — see reportPromptFailure for which failures the host
// does NOT report (a prompt-budget timeout on a turn that is still streaming).
// source 标记（前端据此渲染）：agent 报错（RPCError — 进程活着、拒绝了
// 回合）vs 传输失败（超时/写失败 — agent 可能不可达）；老版本客户端忽略
// 该字段，仅按带 sessionId 的回合错误处理。sessionId 为空（如恢复/新建
// 会话失败，会话尚不存在）时省略该键，前端按 host 级错误渲染。
func (b *Bridge) broadcastPromptError(sessionID string, err error) {
	source := "transport"
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		source = "agent"
	}
	ev := Event{kType: kError, "message": err.Error(), "source": source}
	if sessionID != "" {
		ev[kSessionID] = sessionID
	}
	b.Broadcast(ev)
}

// HasSession reports whether the named session is in the roster — a pure
// in-memory check with no agent interaction. handlePrompt uses it to 404
// an explicitly-targeted unknown session synchronously, before accepting
// the prompt.
func (b *Bridge) HasSession(sessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.sessions[sessionID]
	return ok
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
		params := map[string]any{kSessionID: sid}
		if len(meta) > 0 {
			params[kMeta] = meta
		}
		_ = b.write(map[string]any{
			kJSONRPC: "2.0",
			kMethod:  "session/cancel",
			kParams:  params,
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
	b.Broadcast(Event{kType: "cancelled", kSessionID: sid})
}

// resetRoster 清理 agent 进程死亡后的 in-memory 状态：清空 roster 与
// 队列快照缓存、保留 lastSession 指针、让所有在飞 RPC 立即失败返回。
// 只做清理，绝不杀进程 —— 2026-08-14 起 host 假设 agent 可靠，进程
// 生命周期完全交给外部，host 只在进程退出后把状态收拾干净并报错。
// 返回清理前快照的 lastSession（active 优先），供调用方打日志。
func (b *Bridge) resetRoster(reason string) (string, string) {
	b.mu.Lock()
	if act := b.activeSessionLocked(); act != nil {
		b.rememberSessionLocked(act.SessionID, act.Cwd)
	}
	lastID, lastCwd := b.lastSessionID, b.lastSessionCwd
	b.cmd = nil
	b.stdin = nil
	b.ready = false
	b.bootOK = false
	b.sessions = make(map[string]*SessionState)
	// Agent 进程已死：观察到的回合全部作废（没有进程还会发来
	// turn_completed 收口）。
	b.turns = make(map[string]*observedTurn)
	b.activeSessionID = ""
	// Agent 进程已死：队列是 agent 内存态，重启即清空 —— 快照缓存
	// 一并作废，避免 /api/queue/status 回放过期队列。
	b.queueSnapshots = nil
	b.liveTools = make(map[string]*liveToolResolver)
	// lastSessionID / lastSessionCwd intentionally survive.
	if b.cancelRd != nil {
		b.cancelRd()
		b.cancelRd = nil
	}
	b.mu.Unlock()
	log.Printf("[capri-host] agent state reset (reason=%s)", reason)
	b.failAllPending(fmt.Errorf("grok 进程已退出或不可用"))
	b.broadcastRosterChange()
	return lastID, lastCwd
}

// RestartAgent 由用户显式调用（host 从不自动杀 agent —— 假设 agent
// 可靠，有问题只报错；但保留手动重启通道）：杀掉当前 agent 进程（如
// 有）、清理状态、重新 Boot 并恢复上次会话。任一环节失败都返回错误，
// 由调用方报给用户。
func (b *Bridge) RestartAgent(ctx context.Context) error {
	// 先摘掉引用再杀，waitProcess 醒来时 b.cmd 已置 nil，走防御分支
	// 只补一条 reap 日志，不会误发"连接HOST异常"广播。
	b.mu.Lock()
	cmd := b.cmd
	b.mu.Unlock()
	// lastID 是 resetRoster 清理前快照（active 优先），锁内取值，
	// 无竞态 —— 不要直接读 b.lastSessionID（会被并发 prompt 写）。
	lastID, _ := b.resetRoster("user-restart")
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	if err := b.Boot(ctx); err != nil {
		return fmt.Errorf("重启失败: %w", err)
	}
	// 用快照判断是否需要恢复；restoreLastSession 内部再按锁内最新值
	// 执行（重启间隙若有并发 prompt 更新了 lastSession，恢复它即可）。
	if lastID != "" {
		if err := b.restoreLastSession(ctx); err != nil {
			return fmt.Errorf("重启后恢复会话失败: %w", err)
		}
	}
	log.Printf("[capri-host] agent restarted (user request)")
	return nil
}

// SetMode calls session/set_mode on the given session (empty sessionId
// resolves to the active one).
func (b *Bridge) SetMode(ctx context.Context, sessionID, modeID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, errors.New("没有活跃会话")
	}
	return b.request(ctx, "session/set_mode", map[string]any{
		kSessionID: sessionID,
		"modeId":   modeID,
	}, 30*time.Second)
}

// SetModel calls session/set_model (grok's /model switch; the wire method
// is snake_case per the ACP method table). An optional reasoningEffort is
// forwarded in _meta, matching how the TUI applies --effort. Empty
// sessionId resolves to the active session; the cache patch targets the
// resolved session.
func (b *Bridge) SetModel(ctx context.Context, sessionID, modelID, reasoningEffort string) error {
	if err := b.Boot(ctx); err != nil {
		return err
	}
	// 无 sessionId 直接拒绝，绝不回退到 active 会话：FE 空状态（未锚定）
	// 下发的切换请求落到别的会话上就失去了会话隔离（同 handleSetDefaultModel
	// 的双动作语义）。
	if sessionID == "" {
		return &HTTPError{Code: 400, Msg: "需要 sessionId"}
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return errors.New("没有活跃会话")
	}
	params := map[string]any{
		kSessionID: sessionID,
		"modelId":  modelID,
	}
	if reasoningEffort != "" {
		params[kMeta] = map[string]any{"reasoningEffort": reasoningEffort}
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
	if act := b.sessions[sessionID]; act != nil {
		b.patchSessionModels(act, modelID, reasoningEffort)
		models = act.models
		name = modelDisplayName(models, modelID)
	}
	b.mu.Unlock()
	if models != nil {
		b.Broadcast(Event{kType: "models_update", kParams: models, kSessionID: sessionID})
	}
	b.Broadcast(Event{
		kType:             "model",
		"modelId":         modelID,
		"modelName":       name,
		"reasoningEffort": reasoningEffort,
		kSessionID:        sessionID,
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
				if meta, ok := mm[kMeta].(map[string]any); ok && effort != "" {
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
			params[kMeta] = opts[0].Meta
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
	meta, _ = res[kMeta].(map[string]any)

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
		// turnOpen / turnSeenAt: the agent-observed turn, which is also what
		// stands in for sessions this host process never created/loaded
		// (they have no roster entry, hence no prompt counter).
		turnOpen   bool
		turnSeenAt int64
		turnKnown  bool
	}
	states := make(map[string]*liveState, len(sessions))
	b.mu.Lock()
	settled := b.settleTurnsLocked(time.Now().UnixMilli())
	for _, it := range sessions {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		sid, _ := m[kSessionID].(string)
		st := &liveState{state: "idle"}
		if w := b.turns[sid]; w != nil {
			st.turnOpen = w.open
			st.turnSeenAt = w.seenAt
			st.turnKnown = true
		}
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
	if settled {
		// Observed turns expired: the badge moved, tell clients to refetch.
		b.broadcastRosterChange()
	}

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
		sid, _ := m[kSessionID].(string)
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
		sid, _ := m[kSessionID].(string)
		st := states[sid]
		switch {
		case st != nil && st.live:
			m["status"] = map[string]any{
				"state":         st.state,
				"busy":          st.busy,
				"awaitingInput": st.awaitingInput,
				"lastActiveAt":  st.lastActiveAt,
			}
			if st.title != "" {
				m[kTitle] = st.title
			}
			if st.updatedAt != "" {
				m[kUpdatedAt] = st.updatedAt
			}
		case st != nil && st.turnOpen:
			// The session is not in this host's roster (another client
			// started its turn, or the agent lists a session this process
			// never created/loaded), so there is no prompt counter to read —
			// but the agent's own update stream says it is working. Report
			// what was observed rather than assert idle; awaiting-input is
			// roster state, so it stays false here.
			m["status"] = map[string]any{
				"state":         "active",
				"busy":          true,
				"awaitingInput": false,
				"lastActiveAt":  st.turnSeenAt,
			}
		default:
			status := map[string]any{"state": "idle", "busy": false, "awaitingInput": false}
			if st != nil && st.turnKnown && st.turnSeenAt > 0 {
				status["lastActiveAt"] = st.turnSeenAt
			}
			m["status"] = status
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

// SessionInfo returns authoritative live details of the given session
// (empty sessionId resolves to the active one; TUI /session-info analog),
// served on demand via POST /api/session-info: roster fields, tracked
// context usage / git head, and the model from the session's
// SessionModelState catalog. Nil when the session is unknown or none is
// active.
func (b *Bridge) SessionInfo(sessionID string) *SessionInfoDetail {
	b.mu.Lock()
	defer b.mu.Unlock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	act = b.sessions[sessionID]
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
			if meta, ok := mm[kMeta].(map[string]any); ok {
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
			sessRes[kConfigOptions] = s.configOpts
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
			kType:          "ready",
			kSessionID:     sessionID,
			"cwd":          sessCwd,
			"agentInfo":    agentInfo,
			"modes":        modes,
			kConfigOptions: configOpts,
			"models":       models,
			"hostId":       hostID,
			"hostName":     hostName,
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
		b.Broadcast(Event{kType: "busy", kSessionID: sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	// Capture the session's last-known reasoning effort BEFORE the agent
	// call. Agent session/load remaps persisted model ids to current
	// catalog keys (e.g. deepseek-v4-flash → deepseek-v4-flash-go) and
	// answers WITHOUT a reasoningEffort — the FE would then fall back to
	// the mapped model's default effort (e.g. low), silently discarding
	// the user's choice (e.g. max). The remap broadcast can also race in
	// while the request is in flight, so read the cache up-front rather
	// than after the response.
	prevEffort := ""
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil {
		if m, ok := s.models.(map[string]any); ok {
			if e, ok := m["reasoningEffort"].(string); ok {
				prevEffort = e
			}
		}
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		kSessionID:   sessionID,
		"cwd":        cwd,
		"mcpServers": []any{},
	}
	if len(meta) > 0 && len(meta[0]) > 0 {
		params[kMeta] = meta[0]
	}
	// Multi-tab: agent session/load REPLAYS the full conversation as
	// session/update over the shared SSE bus. The tab that called
	// /api/session-load drops those via historyLoading + HTTP history;
	// every OTHER tab that is viewing the same session would otherwise
	// APPEND the replay onto its existing scrollback (doubled timeline).
	// Announce before the agent call so peers arm the drop gate first.
	b.Broadcast(Event{
		kType:      "session_load_started",
		kSessionID: sessionID,
		"cwd":      cwd,
	})
	sessRes, err := b.request(ctx, "session/load", params, bootTimeout)
	if err != nil {
		// Peers that armed on started must unstick (reload or clear gate).
		b.Broadcast(Event{
			kType:      "session_load_finished",
			kSessionID: sessionID,
			"cwd":      cwd,
			"ok":       false,
		})
		return nil, err
	}

	b.mu.Lock()
	act := b.sessions[sessionID]
	if act == nil {
		act = &SessionState{SessionID: sessionID, CreatedAt: time.Now().UnixMilli()}
		b.sessions[sessionID] = act
	}
	act.Cwd = cwd
	b.ready = true
	b.bootError = ""
	// Prefer fields from the load response — they reflect the restored session.
	if m, ok := sessRes["models"]; ok && m != nil {
		act.models = m
		// 保留用户原选的 reasoningEffort：agent 在 load 时把持久化的
		// 模型 id 映射到当前 catalog 键（如 deepseek-v4-flash →
		// deepseek-v4-flash-go），响应 models 缺省 effort 时用缓存值
		// 补上，避免前端回落到新模型的默认档（如 low）静默覆盖用户
		// 原选的 max。同一 map 同时进 act.models / sessRes / ready 事件。
		if prevEffort != "" {
			if mm, ok := m.(map[string]any); ok {
				if cur, _ := mm["reasoningEffort"].(string); cur == "" {
					mm["reasoningEffort"] = prevEffort
				}
			}
		}
	}
	if modes, ok := sessRes["modes"]; ok && modes != nil {
		act.modes = modes
	}
	if co, ok := sessRes[kConfigOptions]; ok && co != nil {
		act.configOpts = co
	}
	// session/load 响应 `_meta` 原样存下（缺省保留 roster 旧值，与
	// models/modes 的处理一致），ready 事件 / Status 透传。
	if m, ok := sessRes[kMeta]; ok && m != nil {
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
		kType:          "ready",
		kSessionID:     sessionID,
		"cwd":          sessCwd,
		"agentInfo":    agentInfo,
		"modes":        modes,
		kConfigOptions: configOpts,
		"models":       models,
		"hostId":       hostID,
		"hostName":     hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
	// Replay stream is complete once session/load returns — peers can
	// rebuild from HTTP history now (initiator already does via its own
	// loadHistory; this flag is for multi-tab viewers of the same sid).
	if n := b.replayDropped.Swap(0); n > 0 {
		log.Printf("[capri-host] session/load %s：拦下 %d 条重放事件（历史走 HTTP，不上总线）", sessionID[:min(8, len(sessionID))], n)
	}
	b.Broadcast(Event{
		kType:      "session_load_finished",
		kSessionID: sessionID,
		"cwd":      sessCwd,
		"ok":       true,
	})
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
			sessRes[kConfigOptions] = s.configOpts
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
			kType:          "ready",
			kSessionID:     sessionID,
			"cwd":          sessCwd,
			"agentInfo":    agentInfo,
			"modes":        modes,
			kConfigOptions: configOpts,
			"models":       models,
			"hostId":       hostID,
			"hostName":     hostName,
		}
		if sessionMeta != nil {
			ev["sessionMeta"] = sessionMeta
		}
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
		// Re-announce busy so the client can attach the spinner after history load.
		b.Broadcast(Event{kType: "busy", kSessionID: sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	// Same effort-preservation rule as LoadSession: capture the cache
	// up-front (the remap broadcast can race in during the request), then
	// fill a missing reasoningEffort in the resume response models with
	// the session's last-known value.
	prevEffort := ""
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil {
		if m, ok := s.models.(map[string]any); ok {
			if e, ok := m["reasoningEffort"].(string); ok {
				prevEffort = e
			}
		}
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		kSessionID:              sessionID,
		"cwd":                   cwd,
		"mcpServers":            []any{},
		"additionalDirectories": []any{},
	}
	if len(meta) > 0 && len(meta[0]) > 0 {
		params[kMeta] = meta[0]
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
	b.ready = true
	b.bootError = ""
	// Prefer fields from the resume response — they reflect the session.
	if m, ok := sessRes["models"]; ok && m != nil {
		act.models = m
		// 同 LoadSession：响应 models 缺 reasoningEffort 时用会话已知
		// 档位补上（agent 模型 id 映射场景），避免静默重置为用户未
		// 选过的默认档。
		if prevEffort != "" {
			if mm, ok := m.(map[string]any); ok {
				if cur, _ := mm["reasoningEffort"].(string); cur == "" {
					mm["reasoningEffort"] = prevEffort
				}
			}
		}
	}
	if modes, ok := sessRes["modes"]; ok && modes != nil {
		act.modes = modes
	}
	if co, ok := sessRes[kConfigOptions]; ok && co != nil {
		act.configOpts = co
	}
	// session/resume 响应 `_meta` 原样存下（缺省保留 roster 旧值），
	// ready 事件 / Status 透传。
	if m, ok := sessRes[kMeta]; ok && m != nil {
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
		kType:          "ready",
		kSessionID:     sessionID,
		"cwd":          sessCwd,
		"agentInfo":    agentInfo,
		"modes":        modes,
		kConfigOptions: configOpts,
		"models":       models,
		"hostId":       hostID,
		"hostName":     hostName,
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
		kSessionID: sessionID,
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	// 与 SessionDelete 同一套 per-session 清理：名册、队列快照、用量
	// 去重缓存、生成速率分桶与 last-session 指针。
	b.forgetSessionLocked(sessionID)
	b.mu.Unlock()
	b.broadcastRosterChange()
	return sessRes, nil
}

// forgetSessionLocked drops every per-session index entry for sid: the
// roster row, the observed-turn entry, queue snapshot, usage dedup cache,
// live tool resolver and gen-rate bucket, plus the active-session /
// last-session pointers when they pointed at it. The caller holds b.mu.
func (b *Bridge) forgetSessionLocked(sessionID string) {
	delete(b.sessions, sessionID)
	if b.turns != nil {
		delete(b.turns, sessionID)
	}
	if b.queueSnapshots != nil {
		delete(b.queueSnapshots, sessionID)
	}
	if b.usageLastUsed != nil {
		delete(b.usageLastUsed, sessionID)
	}
	if b.liveTools != nil {
		delete(b.liveTools, sessionID)
	}
	b.genRate.discard(sessionID)
	if b.activeSessionID == sessionID {
		b.activeSessionID = ""
	}
	if b.lastSessionID == sessionID {
		b.lastSessionID = ""
		b.lastSessionCwd = ""
	}
}

// ── session history (x.ai/session/updates) ───────────────────────

// UpdatesPage is one page of a session's stored updates (message history).
// Each element of Updates is the full JSONL storage envelope
// {timestamp, method, params}, as returned by the x.ai/session/updates
// extension.
//
// PromptStarts is the agent-side index of every user-message turn start
// (line offsets in updates.jsonl). Present on turnIndex responses; the
// FE uses it for per-turn history paging (load one previous user turn).
// Empty/nil when the agent omitted it (offset/limit pages, older agents).
type UpdatesPage struct {
	Updates      []any `json:"updates"`
	TotalCount   int   `json:"totalCount"`
	HasMore      bool  `json:"hasMore"`
	PromptStarts []int `json:"promptStarts,omitempty"`
	// PromptPreviews 与 PromptStarts 平行：每个存活轮次的首行预览（轮次
	// 目录用）。只在本地归一化路径可用（透传路径无该键 → FE 回退为「已
	// 加载轮才有预览」）。见 session_history.go turnIndexesOf。
	PromptPreviews []string `json:"promptPreviews,omitempty"`
	// Btw：/btw 侧问回放记录（btw_history.jsonl；只在本地归一化路径可用，
	// agent 透传路径不带）。不占 msgSeq 空间，分页游标不受影响。
	Btw []SessionBtw `json:"btw,omitempty"`
	// Projected / OmittedBytes 是 lite 投影的能力回显（契约 [B]，见
	// lite.go）：只在投影真正生效时非空，由 handler 决定回不回相应键。
	// json:"-"：本页从不整体序列化（handler 现拼响应），不给任何出口
	// 多出键的机会。
	Projected    string `json:"-"`
	OmittedBytes int64  `json:"-"`
}

// SessionBtw 是一条 /btw 侧问的回放记录，由 host 从 agent 落盘的
// btw_history.jsonl（xai-grok-shell persistence::BtwEntry，camelCase）读出。
// AfterMsgSeq 是时间线插入锚点：askedAt 之前最近一条信封的 msgSeq；-1 =
// 早于全部信封（置顶）。FE 按「msgSeq ≤ 锚点的最后一条之后」拼接。
type SessionBtw struct {
	BtwSessionId string `json:"btwSessionId"`
	AskedAt      int64  `json:"askedAt"` // epoch ms
	Question     string `json:"question"`
	Answer       string `json:"answer,omitempty"`
	Err          string `json:"error,omitempty"`
	Success      bool   `json:"success"`
	Model        string `json:"model,omitempty"`
	AfterMsgSeq  int    `json:"afterMsgSeq"`
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
	// Detail 是 host 侧响应投影档位（见 lite.go）："lite" 是首屏时间线
	// （合成工具信封、thought 占位、丢掉正文），"meta" 连信封都不回，
	// "full"/缺省/未知值 = 今天的逐字节原样行为。它不上 agent 的 wire
	// （agent 不认识该字段），只在 Bridge.SessionUpdates 出口生效。
	Detail string
}

// SessionUpdates fetches a session's stored updates (message history). Each
// element of the result is the full storage envelope {timestamp, method,
// params}.
//
// 本地归一化优先（msgSeq 契约，见 session_history.go）：会话文件可读且
// 至少一条信封带 params._meta.agentTimestampMs 时，从归一化视图分页
// 服务——排序键 (agentTimestampMs, 文件行号)，不读 eventId；turnIndex/
// offset/limit 在 msgSeq 空间解释，每条 update 顶层带 msgSeq，promptStarts
// 由 host 按 UserRunTurnTracker 重算，totalCount = 归一化条数。回退路径
// （文件不可读 / 没有任何带时间戳的信封）走既有 agent
// `_x.ai/session/updates` 透传（响应无 msgSeq、promptStarts 原样），
// 两条路径互斥、按会话内容自动选择。stream=true 路径不改（agent 以
// chunked 通知推流，本地分页无法替代）。
//
// opts.Detail 的投影（lite.go，契约 [C]）在两条路径的出口统一施加，
// 回退路径不得漏裁；stream=true 不参与投影。
func (b *Bridge) SessionUpdates(ctx context.Context, sessionID, cwd string, opts ...SessionUpdatesOpts) (UpdatesPage, error) {
	o := SessionUpdatesOpts{}
	if len(opts) > 0 {
		o = opts[0]
	}
	if !o.Stream {
		page, err := b.localUpdatesPage(sessionID, cwd, o)
		if err == nil {
			applyUpdatesDetail(&page, o)
			log.Printf("[capri-host] session updates served locally (msgSeq total=%d promptStarts=%d)", page.TotalCount, len(page.PromptStarts))
			return page, nil
		}
		if !errors.Is(err, errLocalHistoryUnavailable) {
			return UpdatesPage{}, err
		}
	}
	if err := b.Boot(ctx); err != nil {
		return UpdatesPage{}, err
	}
	params := map[string]any{kSessionID: sessionID, "cwd": cwd}
	{
		if o.Offset != nil {
			params["offset"] = *o.Offset
		}
		if o.Limit != nil {
			params["limit"] = *o.Limit
		}
		if o.Stream {
			params["stream"] = true
		}
		if o.ChunkSize != nil {
			params["chunkSize"] = *o.ChunkSize
		}
		if o.TurnIndex != nil {
			params["turnIndex"] = *o.TurnIndex
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
	// promptStarts: agent returns []number (JSON → []any of float64).
	// Required for FE turn-based history paging; dropping it forced the
	// FE onto offset pages that can cut mid-turn between two user messages.
	if raw, ok := res["promptStarts"].([]any); ok && len(raw) > 0 {
		ps := make([]int, 0, len(raw))
		for _, v := range raw {
			switch n := v.(type) {
			case float64:
				ps = append(ps, int(n))
			case int:
				ps = append(ps, n)
			case int64:
				ps = append(ps, int(n))
			}
		}
		if len(ps) > 0 {
			page.PromptStarts = ps
		}
	}
	normalizeSyntheticToolCallsInSlice(page.Updates)
	log.Printf("[capri-host] session updates via _x.ai/session/updates ok (total=%d promptStarts=%d)", page.TotalCount, len(page.PromptStarts))
	// 透传出口同样投影（[C]「两条路径共用同一投影函数」）。stream=true 的
	// 信封不走这个响应（以 session_updates_chunk 推流），没有可裁的页，
	// 因此不回显 projected —— FE 按 host 不支持 lite 处理。
	if !o.Stream {
		applyUpdatesDetail(&page, o)
	}
	return page, nil
}

// GitInfo returns the git branch/worktree state for a cwd. The branch comes
// from the protocol method `_x.ai/git/info` (GitInfoData.currentBranch);
// worktree state is not part of that payload (the TUI probes it locally
// with git2), so it is taken from the agent's own three-way probe — the
// `x.ai/git_head_changed` stash (linked `git worktree`, grok standalone
// clone marker, worktree DB record — exactly the TUI's get_worktree_info) —
// when the session has one for this cwd. Otherwise it falls back to the
// local probe, which mirrors the same detection minus the worktree DB:
// the `.git/grok-worktree-source` back-pointer for standalone clones, then
// `rev-parse --git-dir` vs `--git-common-dir` for linked worktrees.
func (b *Bridge) GitInfo(ctx context.Context, sessionID, cwd string) (map[string]any, error) {
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
		if inner, ok := res[kResult].(map[string]any); ok {
			payload = inner
		}
		if br, ok := payload["currentBranch"].(string); ok {
			out["branch"] = br
		}
	}
	// Worktree state: agent's three-way stash first (only valid while the
	// session's cwd matches), local probe as the fallback — a fresh session
	// may not have emitted git_head_changed yet.
	isWorktree, mainRepo := b.stashedWorktree(sessionID, cwd)
	if !isWorktree && mainRepo == "" {
		isWorktree, mainRepo = probeWorktree(cwd)
	}
	out["isWorktree"] = isWorktree
	out["mainRepo"] = mainRepo
	return out, nil
}

// stashedWorktree returns the worktree state the AGENT reported for a
// session via `x.ai/git_head_changed` (the agent's get_worktree_info is the
// same three-way probe the TUI uses: linked git worktree, grok standalone
// clone marker, worktree DB record). The stash is written for the session's
// current location only — a session that moved (cd) keeps the stash of its
// previous cwd, so the query cwd must match the session cwd.
func (b *Bridge) stashedWorktree(sessionID, cwd string) (bool, string) {
	if sessionID == "" || cwd == "" {
		return false, ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.sessions[sessionID]
	if s == nil || s.Cwd == "" || s.Cwd != normCwd(cwd) {
		return false, ""
	}
	return s.gitWorktree, s.gitMainRepo
}

// probeWorktree reports whether cwd lives in a grok worktree and, if so,
// the main repo path (shortened to ~/… under $HOME). Mirrors the TUI's
// get_worktree_info minus the worktree metadata DB (a Go-side read of the
// Rust DB format is deferred): the standalone-clone marker is checked first
// (priority, matching the TUI), then the linked-worktree check
// (git-dir != common-dir ⇒ worktree).
func probeWorktree(cwd string) (bool, string) {
	if main, ok := grokMarkerMainRepo(cwd); ok {
		return true, shortenHome(main)
	}
	// rev-parse 的这两个输出可能一个是绝对路径、一个是相对 cwd 的（在主仓库的
	// 子目录里查询时 git 就是这么给的），直接比字符串会把「主仓库的子目录」误判
	// 成 linked worktree——先各自规范化成绝对路径再比。
	gitDir := absFromCwd(cwd, runGit(cwd, "rev-parse", "--git-dir"))
	commonDir := absFromCwd(cwd, runGit(cwd, "rev-parse", "--git-common-dir"))
	if gitDir == "" || commonDir == "" || gitDir == commonDir {
		return false, ""
	}
	return true, shortenHome(filepath.Dir(commonDir))
}

// absFromCwd normalizes a path printed by git (which may be relative to the
// query cwd) into an absolute, symlink-resolved path so two of them can be
// compared. macOS resolves /var → /private/var, so without EvalSymlinks the
// absolute form and the cwd-relative form of the same `.git` still differ.
func absFromCwd(cwd, p string) string {
	if p == "" {
		return ""
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(cwd, p)
	}
	p = filepath.Clean(p)
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return p
}

// grokMarkerMainRepo walks up from cwd looking for a `.git` directory that
// carries the standalone-clone back-pointer `grok-worktree-source` (written
// by the fast-worktree copy; the content is the source repo root). The scan
// stops at the first existing `.git` — a nested independent repo must not
// inherit its parent's marker (matches the TUI's ancestor walk).
func grokMarkerMainRepo(cwd string) (string, bool) {
	dir := filepath.Clean(cwd)
	for {
		if contents, err := os.ReadFile(filepath.Join(dir, ".git", "grok-worktree-source")); err == nil {
			if trimmed := strings.TrimSpace(string(contents)); trimmed != "" {
				return trimmed, true
			}
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

// shortenHome tilde-collapses a path under $HOME to `~/…`, with a
// component-prefix guard (`/Users/benin` must not match `/Users/benin2`).
// Paths outside $HOME are returned verbatim.
func shortenHome(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	rel, err := filepath.Rel(home, path)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "~/" + rel
	}
	return path
}

// runGit executes git plumbing in cwd and returns trimmed stdout ("" on
// failure — non-repo, detached plumbing errors, etc.).
func runGit(cwd string, args ...string) string {
	cmd := exec.Command("git", append([]string{"-C", cwd}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	// Output is captured, so no window is needed — and this runs often enough
	// that an unsuppressed console would flash repeatedly under the GUI build.
	procattr.HideConsole(cmd)
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
// The result is the JSON-RPC result VERBATIM (any JSON value): most x.ai
// methods return an object, but workspace_list_recent returns a bare
// array — coercing it to a map would swallow the data into {}.
func (b *Bridge) XaiCall(ctx context.Context, method string, params map[string]any) (any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if params == nil {
		params = map[string]any{}
	}
	for _, k := range []string{kSessionID, kSessionIDS} {
		if v, ok := params[k].(string); ok && v == "" {
			sid := b.resolveSessionID("")
			if sid == "" {
				return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
			}
			params[k] = sid
		}
	}
	return b.requestRaw(ctx, "_"+method, params, 60*time.Second)
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
	for _, k := range []string{kSessionID, kSessionIDS} {
		if v, ok := params[k].(string); ok && v == "" {
			sid := b.resolveSessionID("")
			if sid == "" {
				return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
			}
			params[k] = sid
		}
	}
	if err := b.write(map[string]any{
		kJSONRPC: "2.0",
		kMethod:  "_" + method,
		kParams:  params,
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// QueueStatus returns the cached most-recent x.ai/queue/changed snapshot
// for a session (nil when the host has not seen any queue broadcast for it
// since this process started, e.g. after an agent restart). The agent's
// prompt queue is in-memory only — never persisted, never replayed on
// session/load — so this cache is the only way for a client to re-align its
// queue mirror after loading a session. The returned map is a deep clone:
// callers may serialize it without racing the live broadcast handler.
func (b *Bridge) QueueStatus(sessionID string) map[string]any {
	if sessionID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queueSnapshots == nil {
		return nil
	}
	snap, ok := b.queueSnapshots[sessionID]
	if !ok {
		return nil
	}
	if cloned, ok := cloneAny(snap).(map[string]any); ok {
		return cloned
	}
	return nil
}

// ForkSession calls x.ai/session/fork: forks the given session into a new
// one (optionally a git worktree). Empty sessionId resolves to the active
// session; sourceSessionId / sourceCwd / newCwd come from the resolved
// session's live state (unknown id → empty fields, the agent 404s).
// Params follow the TUI's fork payload:
// {sourceSessionId, sourceCwd, newCwd, sessionKind:"fork", newSessionId?}.
func (b *Bridge) ForkSession(ctx context.Context, sessionID string, params map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sid := b.resolveSessionID(sessionID)
	b.mu.Lock()
	var act *SessionState
	if sid != "" {
		act = b.sessions[sid]
	}
	b.mu.Unlock()
	p := map[string]any{
		"sourceSessionId": "",
		"sourceCwd":       "",
		"newCwd":          "",
		"sessionKind":     "fork",
	}
	if act != nil {
		p["sourceSessionId"] = act.SessionID
		p["sourceCwd"] = act.Cwd
		p["newCwd"] = act.Cwd
	}
	for k, v := range params {
		p[k] = v
	}
	return b.request(ctx, "_x.ai/session/fork", p, 60*time.Second)
}

// RenameSession calls x.ai/session/rename: {sessionId, title} (empty
// sessionId resolves to the active one; unknown id → agent 404).
func (b *Bridge) RenameSession(ctx context.Context, sessionID, title string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/session/rename", map[string]any{
		kSessionID: b.resolveSessionID(sessionID),
		kTitle:     title,
	}, 30*time.Second)
}

// Recap fires x.ai/recap (fire-and-forget "where was I" summary; the recap
// arrives later as a SessionRecap session/update). Returns the ack.
// Recap calls x.ai/recap: {sessionId, auto} — triggers a session recap.
// Empty sessionId resolves to the active session (404 when none is active).
func (b *Bridge) Recap(ctx context.Context, sessionID string, auto bool) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/recap", map[string]any{
		kSessionID: sessionID,
		"auto":     auto,
	}, 30*time.Second)
}

// SubagentCancel calls x.ai/subagent/cancel: {sessionId, subagentId}
// (empty sessionId resolves to the active one; unknown id → agent 404).
func (b *Bridge) SubagentCancel(ctx context.Context, sessionID, subagentID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/subagent/cancel", map[string]any{
		kSessionID:   b.resolveSessionID(sessionID),
		"subagentId": subagentID,
	}, 30*time.Second)
}

// TaskKill calls x.ai/task/kill: {sessionId, taskId} (empty sessionId
// resolves to the active one; unknown id → agent 404).
func (b *Bridge) TaskKill(ctx context.Context, sessionID, taskID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/task/kill", map[string]any{
		kSessionID: b.resolveSessionID(sessionID),
		"taskId":   taskID,
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
		kSessionID: sid,
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
		kSessionID: sessionID,
	}, 60*time.Second)
	if err != nil {
		return nil, err
	}
	// Deleting the ACTIVE session drops the frontend into the EMPTY state
	// (no auto-new). forgetSessionLocked clears the roster entry, the
	// active-session pointer and any last-session hint pointing at the
	// deleted session, so the next prompt without a sessionId creates a
	// fresh session instead of trying to restore the deleted one (same
	// cleanup as CloseSession).
	b.mu.Lock()
	b.forgetSessionLocked(sessionID)
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
	params := map[string]any{kSessionID: sessionID}
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
		kSessionID: sessionID,
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
		kSessionID:          sessionID,
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
		kSessionID: sessionID,
		"taskId":   taskID,
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
		kSessionID: sessionID,
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
		kSessionIDS: sessionID,
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
		kSessionID:       sessionID,
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
	return b.XaiNotify(ctx, "x.ai/toggle_plan_mode", map[string]any{kSessionID: sessionID})
}

// PermissionsReset sends x.ai/permissions/reset: {sessionId} — clears the
// remembered permission decisions. Like toggle_plan_mode the agent only
// handles it as a notification, and it resets ALL resident sessions
// (ignoring the sessionId — the agent's handler iterates every session,
// with no client/session scoping), so the host writes it without a
// JSON-RPC id. Frontends should treat it as process-global.
func (b *Bridge) PermissionsReset(ctx context.Context, sessionID string) (map[string]any, error) {
	return b.XaiNotify(ctx, "x.ai/permissions/reset", map[string]any{kSessionID: sessionID})
}

// SetPermissionMode sends x.ai/yolo_mode_changed as a fire-and-forget
// notification — the agent's permission-mode switch channel (ask / auto /
// always-approve). session/set_mode only understands the session-mode ids
// (plan / default / ask), so permission modes MUST go through this
// notification instead (TUI parity: the pager persists + fires the same
// payload).
//
// CLIENT-SCOPED, NOT per-session: the agent applies the notification to
// EVERY resident session of the sending client (no sessionId is sent;
// the agent's ext_notification handler matches on clientIdentifier when
// present, else all sessions). Frontends must treat a permission-mode
// change as process-global — every conversation's displayed mode flips
// together. 'normal' maps to the agent's 'ask' canonical; the
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
	// Record the canonical mode BEFORE the write: the agent never echoes
	// this notification back (it is fire-and-forget), so this record is
	// what hello carries to clients that connect later.
	b.recordPermissionMode(permissionModeFromParams(params))
	if err := b.write(map[string]any{
		kJSONRPC: "2.0",
		kMethod:  "_x.ai/yolo_mode_changed",
		kParams:  params,
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// permissionModeFromParams resolves the canonical permission mode
// (ask / auto / always-approve) out of a yolo_mode_changed params map —
// the same shape the FE and the agent echo use.
func permissionModeFromParams(params map[string]any) string {
	if p, ok := params["permission_mode"].(string); ok {
		switch p {
		case "always-approve", "always_approve", "yolo":
			return "always-approve"
		case "auto":
			return "auto"
		case "ask", "default", "normal", "plan":
			return "ask"
		}
	}
	if y, ok := params["yolo_mode"].(bool); ok && y {
		return "always-approve"
	}
	if a, ok := params["auto_mode"].(bool); ok && a {
		return "auto"
	}
	return "ask"
}

// recordPermissionMode stores the canonical permission mode (b.mu held).
// This is the host's process-global view of the agent's permission mode:
// it is written on every client-initiated toggle AND on every agent echo
// (agent-internal changes, e.g. /yolo, the enable-always-approve option),
// and reset to "ask" whenever the agent process is (re)spawned — the
// agent's permission state lives in its process memory only.
func (b *Bridge) recordPermissionMode(mode string) {
	b.mu.Lock()
	b.permMode = mode
	b.mu.Unlock()
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
		params[kSessionID] = sid
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
		kSessionIDS:   sid,
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
	params[kSessionIDS] = sid
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
		kSessionIDS:   sid,
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
		kSessionIDS:   sid,
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
	// channel so the loop exits at its next checkpoint. stopLoopLocked
	// guards against double-close (loopOn / stop nil checks);
	// the process kill above fails the in-flight RPC, so the turn unwinds
	// promptly instead of holding the loop open.
	b.goal.mu.Lock()
	b.goal.stopLoopLocked()
	b.goal.mu.Unlock()
}

// sessionBusy reports whether the session currently has turns in flight.
// The goal engine polls this while waiting to fire its next continuation
// turn (see goalEngine.goalWaitIdle).
func (b *Bridge) sessionBusy(sessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.sessions[sessionID]; s != nil {
		return s.Busy
	}
	return false
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
		if t, ok := c[kText].(string); ok {
			return t
		}
		// nested content
		if inner, ok := c[kContent]; ok {
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
			if t, _ := c[kType].(string); t == "image" {
				out = append(out, c)
				return
			}
			if inner, ok := c[kContent]; ok {
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
	return Event{kType: "image", kSessionID: sid, "data": data, "mimeType": mime}, true
}

// broadcastImage 广播 image 事件。imageEvent 自带 sessionId、不走调用方的
// tag 闭包，重放标记（kReplayInternal）只能在这里补。
func (b *Bridge) broadcastImage(sid string, block map[string]any, replay bool) {
	ev, ok := imageEvent(sid, block)
	if !ok {
		return
	}
	if replay {
		ev[kReplayInternal] = true
	}
	b.Broadcast(ev)
}
