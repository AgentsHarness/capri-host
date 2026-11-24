package acp

import (
	"context"
	"errors"
	"time"
)

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
	sid := b.resolveSessionID(sessionID)
	res, err := b.request(ctx, "_x.ai/session/rename", map[string]any{
		kSessionID: sid,
		kTitle:     title,
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}
	b.Broadcast(Event{
		kType:           "session_info",
		kSessionID:      sid,
		"title":         title,
		"titleIsManual": true,
	})
	b.broadcastRosterChange()
	return res, nil
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

// SubagentMessage calls x.ai/subagent/message: {sessionId, agentAddress, queue, content}
// to steer or queue text to an active child subagent.
func (b *Bridge) SubagentMessage(ctx context.Context, sessionID, agentAddress string, queue bool, content []any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	sid := b.resolveSessionID(sessionID)
	if sid == "" {
		return nil, errors.New("没有活跃会话")
	}
	params := map[string]any{
		kSessionID:     sid,
		"agentAddress": agentAddress,
		"queue":        queue,
		"content":      content,
	}
	return b.request(ctx, "_x.ai/subagent/message", params, 30*time.Second)
}

// TaskKill calls x.ai/task/kill: {sessionId, taskId, source?} (empty sessionId
// resolves to the active one; unknown id → agent 404).
//
// source 是 agent 侧 TaskKillSource 的 wire 名（clientUi / teardown），它决定
// 任务收尾时 agent 要不要为这条终止唤醒模型；空 = 不上 wire，由 agent 用它的
// 缺省值。宿主不猜语义，原样转发。
func (b *Bridge) TaskKill(ctx context.Context, sessionID, taskID, source string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		kSessionID: b.resolveSessionID(sessionID),
		"taskId":   taskID,
	}
	if source != "" {
		params["source"] = source
	}
	return b.request(ctx, "_x.ai/task/kill", params, 30*time.Second)
}

// TaskList calls x.ai/task/list: {sessionId} → {tasks: [TaskSnapshot…]}.
// Each snapshot includes output/command so the FE block viewer can show
// live stdout (TUI OpenBlockViewer for BgTask).
//
// Without an explicit id it answers for the ACTIVE session (and 404s when
// there is none). Callers that refresh a specific session — e.g. the top
// task strip for a session being resumed — must pass that id: the agent
// registry is per session, and asking for the active one would paint
// another session's tasks onto this view.
func (b *Bridge) TaskList(ctx context.Context, sessionID ...string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if len(sessionID) > 0 && sessionID[0] != "" {
		return b.request(ctx, "_x.ai/task/list", map[string]any{kSessionID: sessionID[0]},
			30*time.Second)
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
	res, err := b.request(ctx, "_x.ai/compact_conversation", params, 60*time.Second)
	if err != nil {
		return nil, err
	}
	b.broadcastRosterChange()
	return res, nil
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
	res, err := b.request(ctx, "_x.ai/rewind/execute", map[string]any{
		kSessionID:          sessionID,
		"targetPromptIndex": targetIndex,
		"force":             true,
		"mode":              mode,
	}, 60*time.Second)
	if err != nil {
		return nil, err
	}
	b.Broadcast(Event{
		kType:               "session_rewound",
		kSessionID:          sessionID,
		"targetPromptIndex": targetIndex,
	})
	b.broadcastRosterChange()
	return res, nil
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
