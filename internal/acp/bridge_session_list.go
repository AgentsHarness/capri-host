package acp

import (
	"context"
	"time"
)

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
		sum, ps := b.sessionTaskCensusCached(b.grokHome(), cwd, sid)
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
