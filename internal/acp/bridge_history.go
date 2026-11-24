package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AgentsHarness/capri-host/internal/procattr"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (b *Bridge) onAgentMessage(msg map[string]any) {
	if method, _ := msg[kMethod].(string); method == "session/update" && msg[kID] == nil {
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

	// A method identifies an incoming request or notification, never a response.
	if rawMethod, hasMethod := msg[kMethod]; hasMethod {
		method, _ := rawMethod.(string)
		if method != "" {
			params, _ := msg[kParams].(map[string]any)
			if params == nil {
				params = map[string]any{}
			}
			b.handleAgentRequest(id, method, params)
		}
		return
	}
	_, hasResult := msg[kResult]
	errValue, hasError := msg[kError]
	if hasResult == hasError {
		return
	}
	if hasError {
		if errObj, ok := errValue.(map[string]any); !ok || errObj == nil {
			return
		}
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
				if d, ok := m["data"]; ok && d != nil {
					switch dataVal := d.(type) {
					case string:
						if dataVal != "" && dataVal != em {
							em = fmt.Sprintf("%s: %s", em, dataVal)
						}
					default:
						if bs, err := json.Marshal(d); err == nil && len(bs) > 0 {
							em = fmt.Sprintf("%s: %s", em, string(bs))
						}
					}
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
	// turn_completed / response_completed 的 usage 由下方统一提取，
	// 此处跳过以避免重复广播。其余 kind 值去重：实测流水期
	// _meta.totalTokens 在一段输出内恒定（同一 used 连续 70+ 条），
	// 只广播值变化——上下文计数器无需逐条刷新。
	//
	// available_commands_update 例外：agent 不把它落盘（updates.jsonl 里
	// 永远查不到这条），但同样盖着 _meta.totalTokens——它在每轮模型响应
	// 后退一次（AdvertiseTrigger::UsageMeta）、切模型 / skills 重载时也发。
	// 拿它喂上下文缓存会让 /api/session-info 报出一个日志与 TUI 都对不上、
	// 且比真实用量更旧的数（实测 session 0500f：缓存 114884 / 日志 118007）。
	// 广播照发（FE 的命令表要它），只跳过缓存与 usage 事件。
	if k, _ := update[kSessionUpdate].(string); k != "turn_completed" && k != "response_completed" && k != "available_commands_update" {
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

// currentModeIDOf reads the ACP CurrentModeUpdate id (camelCase on the
// wire; snake_case accepted for older x.ai carriers).
func currentModeIDOf(update map[string]any) string {
	if id, ok := update[kCurrentModeId].(string); ok && id != "" {
		return id
	}
	if id, ok := update["current_mode_id"].(string); ok && id != "" {
		return id
	}
	return ""
}

// applyCurrentModeUpdateLocked patches the session's SessionModeState from
// a current_mode_update. Caller holds b.mu. Broadcasts a clone so later
// in-place writes cannot race SSE/hub serialization.
func (b *Bridge) applyCurrentModeUpdateLocked(sid string, update map[string]any) any {
	s := b.sessions[sid]
	if ms := update[kModeState]; ms != nil {
		if s != nil {
			s.modes = ms
		}
		return cloneAny(ms)
	}
	id := currentModeIDOf(update)
	if id == "" {
		if s != nil {
			return cloneAny(s.modes)
		}
		return nil
	}
	if s != nil {
		mm, ok := s.modes.(map[string]any)
		if !ok || mm == nil {
			mm = map[string]any{}
			s.modes = mm
		}
		mm[kCurrentModeId] = id
		return cloneAny(s.modes)
	}
	return map[string]any{kCurrentModeId: id}
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
			log.Printf("[Capri-host] session updates served locally (msgSeq total=%d promptStarts=%d)", page.TotalCount, len(page.PromptStarts))
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
	log.Printf("[Capri-host] session updates via _x.ai/session/updates ok (total=%d promptStarts=%d)", page.TotalCount, len(page.PromptStarts))
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
// component-prefix guard (`/Users/dev` must not match `/Users/dev2`).
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
