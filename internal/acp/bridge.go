package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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

// askUserQuestionTimeoutDefault is the host-side wait budget for an
// x.ai/ask_user_question when [toolset.ask_user_question] carries no
// timeout_secs: the agent's own RESPONSE_TIMEOUT (30 min), which is also
// the countdown the FE draws for the question card.
const askUserQuestionTimeoutDefault = 30 * time.Minute

// methodAskUserQuestion is the agent → client extension request that opens
// the question card. It is the one client request whose budget is
// user-configurable rather than a host constant (see clientRequestBudget).
const methodAskUserQuestion = "x.ai/ask_user_question"

var (
	// promptStallTimeout 是发出 prompt 后整个会话持续无任何 update 活动的熔断上限。
	// 正常情况下 agent 在进入模型推理前（1~2ms）就会发出 user_message_chunk 或 queue 广播。
	// 若超过该时间仍 0 活动且整个 session 持续静默，判定为底层进入推理前死锁，提前熔断，
	// 避免前端无意义等待 30 分钟。
	promptStallTimeout = 90 * time.Second
)

// ErrPromptStalled 在发出 prompt 且整个会话持续无任何活动达到 promptStallTimeout 时抛出。
var ErrPromptStalled = errors.New("session/prompt 启动超时: agent 进程无响应（可能发生死锁）")

// Bridge owns one grok agent stdio process and manages multiple ACP
// sessions inside it (the agent is multi-session, like the Grok Build TUI:
// each session runs its own turn; the host tracks their live states for the
// dashboard active/idle classification).
// It does NOT implement fs/* or terminal/* — agent runs tools itself.
type Bridge struct {
	cfg GrokConfig

	// Lock order: agentMessageMu → clientReqMu → mu.
	agentMessageMu sync.Mutex // serializes old stdout delivery with process reset
	clientReqMu    sync.Mutex // serializes client request completion and retirement
	mu             sync.Mutex
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	cancelRd       context.CancelFunc

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
	// ledger 是用量落盘台账（~/.capri-host/usage-ledger.jsonl）。它把回合
	// 用量在源文件被删之前抄一份，使历史用量不随 agent 的会话清理
	// 消失；见 usage_ledger.go。非 nil（关闭时内部判空跳过）。
	ledger *usageLedger
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
	// session" until turn_completed says otherwise.
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

	// promptedIDs remembers recently accepted promptIds (prompt opts
	// Meta["promptId"], minted by the FE per turn) so a client retrying a
	// prompt it could not confirm — the uplink died between send and
	// accept, and the FE fell back to "not delivered" — does not start a
	// second turn. Bounded FIFO; b.mu protects it. The FE reuses the same
	// id when it retries, which is what makes the retry idempotent.
	promptedIDs  map[string]struct{}
	promptedFIFO []string

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

	// ── 列表派生数据缓存（session_tasks.go / bridge_ext_stats.go）──────
	// [bg] 徽章的 census 与单会话统计都要扫整份 updates.jsonl，而
	// sessions_changed 在回合边界会连着触发。按 (size, mtime) 命中即可
	// 跳过重扫——徽章与统计都是派生数据，源文件没变就没有新答案。
	census taskCensusCache
	stats  sessionStatsCache

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
	// UsageLedgerFile overrides the default ~/.capri-host/usage-ledger.jsonl
	// so tests can inject a temp path without touching the real home.
	UsageLedgerFile string
	// UsageLedgerOn turns the usage ledger on. It is explicit opt-in on
	// purpose: the ledger scans the real ~/.grok and writes to the user's
	// ~/.capri-host, so a Bridge built without a GrokHome override (tests,
	// embedders) must not do that by accident. cmd/capri-host sets it.
	UsageLedgerOn bool
	// ResidentCap bounds how many sessions stay loaded in the grok
	// process. Zero = DefaultResidentCap (4). Negative = disable the
	// idle-unload supervisor (tests, or RESIDENT_CAP=0).
	ResidentCap int
	// SpawnEnv 叠加到 agent 子进程的环境变量（KEY=VALUE，后者覆盖先者）。
	// 用于「Agent 代理」开关：关掉时传空的 HTTPS_PROXY 等，覆盖 host 从
	// 自身环境继承来的代理变量。
	SpawnEnv []string
}

// lastSessionFile is the on-disk form of the most recently active session.
type lastSessionFile struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd,omitempty"`
}

type clientRequest struct {
	stdin     io.WriteCloser // bound to the originating process, never the replacement
	retired   bool           // protected by clientReqMu
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
	errCode      int
	// receivedAt: unix ms stamped when the forwarder took the request off
	// the agent pipe — the single timing origin every client counts the
	// ask-question / approval budget from (broadcast + snapshot).
	receivedAt int64
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
	b.ledger = newUsageLedger(b.usageLedgerPath())
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
		log.Printf("[Capri-host] restore session %s failed: %v", sid, err)
		return fmt.Errorf("恢复会话失败: %w", err)
	}
	log.Printf("[Capri-host] restored session %s (cwd=%s)", sid, cwd)
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

// LastSeq is the highest event seq the bus has assigned so far. The hub client
// uses it as a new session epoch's baseline (hello.resume.up): the hub's up
// watermark starts there and the first live event is LastSeq+1.
func (b *Bridge) LastSeq() uint64 {
	return b.bus.lastSeq()
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

// titleIsManualFrom reads x.ai/titleIsManual off params._meta or
// update._meta (live session_info used to drop it, so a later auto-title
// overwrote a manual rename).
func titleIsManualFrom(params, update map[string]any) (bool, bool) {
	for _, m := range []map[string]any{params, update} {
		if m == nil {
			continue
		}
		meta, _ := m[kMeta].(map[string]any)
		if meta == nil {
			continue
		}
		if v, ok := meta["x.ai/titleIsManual"].(bool); ok {
			return v, true
		}
		if v, ok := meta["titleIsManual"].(bool); ok {
			return v, true
		}
	}
	return false, false
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
	log.Printf("[Capri-host] agent restarted (user request)")
	return nil
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

	// Flush the usage ledger so the process's last turns are persisted even
	// if the periodic sync has not fired yet.
	if err := b.syncUsageLedger(); err != nil {
		log.Printf("[Capri-host] 退出前用量台账落盘失败: %v", err)
	}

	// Stop every session's goal loop: a continuation turn can be blocked
	// on a 30-minute prompt or waiting for the session to go idle, and
	// must not outlive shutdown. Same mechanism as GoalClear — close the
	// stop channels so the loops exit at their next checkpoints.
	// stopAllLoopsLocked guards against double-close (loopOn / stop nil
	// checks); the process kill above fails the in-flight RPC, so the
	// turn unwinds promptly instead of holding a loop open.
	b.goal.mu.Lock()
	b.goal.stopAllLoopsLocked()
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
