package acp

import (
	"context"
)

// XaiCall / UnwrapExtResult live in bridge.go / bridge_ext.go (align/core
// and align/ext1 contracts) — do not redefine them here.

// xaiCallUnwrapped is the common shape of every typed wrapper: XaiCall
// followed by UnwrapExtResult, with the error short-circuited.
func (b *Bridge) xaiCallUnwrapped(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	res, err := b.XaiCall(ctx, method, params)
	if err != nil {
		return nil, err
	}
	return UnwrapExtResult(res), nil
}

// xaiNotify writes a fire-and-forget `_x.ai/<method>` notification (no
// JSON-RPC id, so the agent's ext_notification path handles it — a
// request-style call would get -32601) and returns a bare ok. The session
// id is resolved from the active session the same way XaiCall does it
// ("" → active; no active session → HTTPError 404), because the queue
// handlers route on sessionId and silently drop the command without it.
func (b *Bridge) xaiNotify(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sid := b.resolveSessionID(""); sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	} else {
		params["sessionId"] = sid
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

// ── session meta / lifecycle ─────────────────────────────────────────

// SessionInfoExt calls x.ai/session/info — live session metadata
// (agent_name/model/turns/context …). Wire keys: {sessionId?} — the
// sessionId is OPTIONAL on the agent side (it falls back to the first
// resident session), so it is omitted entirely. Returns the unwrapped
// result. Named SessionInfoExt because Bridge.SessionInfo() (host-side
// /api/session-info) already owns the SessionInfo name.
func (b *Bridge) SessionInfoExt(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session/info", map[string]any{})
}

// SessionUsage calls x.ai/session/usage — the session's token/cost ledger.
// Wire keys: {sessionId} REQUIRED; the wrapper passes "" so XaiCall fills
// the active session id.
func (b *Bridge) SessionUsage(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session/usage", map[string]any{"sessionId": ""})
}

// SessionState calls x.ai/session/state — the session's metadata columns
// (plan/planMode/signals/goal/announcement/summary). Wire keys:
// {sessionId, cwd} — BOTH required; sessionId is passed as "" so XaiCall
// fills the active session id, cwd must be supplied by the caller.
func (b *Bridge) SessionState(ctx context.Context, cwd string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session/state", map[string]any{
		"sessionId": "",
		"cwd":       cwd,
	})
}

// SessionImport calls x.ai/session/import — recreate a session on this
// host from mirrored columns (state) and transcript (updates). Wire keys:
// {sessionId, cwd, state?, updates?}; sessionId is passed as "" (filled by
// XaiCall), cwd required; state/updates are omitted when nil/empty.
func (b *Bridge) SessionImport(ctx context.Context, cwd string, state map[string]any, updates []any) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "cwd": cwd}
	if state != nil {
		params["state"] = state
	}
	if updates != nil {
		params["updates"] = updates
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/import", params)
}

// SessionSearch calls x.ai/session/search — full-text search over past
// sessions (FTS5). Wire keys: {query, cwd?, limit?, offset?,
// includeContent?} — no sessionId at all. query is required; cwd is
// omitted when empty (scopes to a workspace); limit/offset are omitted
// when 0 (agent defaults: limit 20, offset 0); includeContent is omitted
// when nil.
func (b *Bridge) SessionSearch(ctx context.Context, query, cwd string, limit, offset int, includeContent *bool) (map[string]any, error) {
	params := map[string]any{"query": query}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if limit != 0 {
		params["limit"] = limit
	}
	if offset != 0 {
		params["offset"] = offset
	}
	if includeContent != nil {
		params["includeContent"] = *includeContent
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/search", params)
}

// SessionRepair calls x.ai/session/repair — out-of-band recovery for
// sessions bricked by corrupted tool-pairing history. Wire keys:
// {sessionId, dryRun?}; sessionId is passed as "" (filled by XaiCall);
// dryRun is omitted unless true (false is the agent default).
func (b *Bridge) SessionRepair(ctx context.Context, dryRun bool) (map[string]any, error) {
	params := map[string]any{"sessionId": ""}
	if dryRun {
		params["dryRun"] = true
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/repair", params)
}

// SessionClose calls x.ai/session/close — the x.ai extension variant of
// session close (distinct from the official ACP session/close): it tears
// down the resident session. Wire keys: {sessionId} REQUIRED; "" is
// passed so XaiCall fills the active session id.
func (b *Bridge) SessionClose(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session/close", map[string]any{"sessionId": ""})
}

// SessionUpdateMcpServers calls x.ai/session/update_mcp_servers —
// mid-session MCP server swap. Wire keys: {sessionId, mcpServers} — both
// required; sessionId is passed as "" (filled by XaiCall), mcpServers is
// the ACP McpServer array (camelCase: url/command/args/headers/env…).
func (b *Bridge) SessionUpdateMcpServers(ctx context.Context, servers []map[string]any) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session/update_mcp_servers", map[string]any{
		"sessionId":  "",
		"mcpServers": servers,
	})
}

// SessionAddLocalWorkspace calls x.ai/session/add_local_workspace —
// add-only mid-session local workspace attach (chat-kind sessions; the
// agent may reject the method when built without the local-workspace
// feature). Wire keys: {sessionId, meta?}; sessionId is passed as ""
// (filled by XaiCall), meta is omitted when nil.
func (b *Bridge) SessionAddLocalWorkspace(ctx context.Context, meta map[string]any) (map[string]any, error) {
	params := map[string]any{"sessionId": ""}
	if meta != nil {
		params["meta"] = meta
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/add_local_workspace", params)
}

// SessionRehydrate calls x.ai/session/rehydrate — recreate a worktree
// session after host restart, preserving the original session identity.
// Wire keys: {sessionId, sourceCwd, repoRoot, worktreePath?}; sessionId is
// passed as "" (filled by XaiCall), sourceCwd/repoRoot required,
// worktreePath omitted when empty.
func (b *Bridge) SessionRehydrate(ctx context.Context, sourceCwd, repoRoot, worktreePath string) (map[string]any, error) {
	params := map[string]any{
		"sessionId": "",
		"sourceCwd": sourceCwd,
		"repoRoot":  repoRoot,
	}
	if worktreePath != "" {
		params["worktreePath"] = worktreePath
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/rehydrate", params)
}

// SessionResolveLocalForWorktreeResume calls
// x.ai/session/resolve_local_for_worktree_resume — repo-wide resolution of
// a session for worktree resume. Wire keys: {sessionId, cwd} — both
// required; sessionId is passed as "" (filled by XaiCall).
func (b *Bridge) SessionResolveLocalForWorktreeResume(ctx context.Context, cwd string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session/resolve_local_for_worktree_resume", map[string]any{
		"sessionId": "",
		"cwd":       cwd,
	})
}

// SessionList calls x.ai/session/list — the merged local+remote session
// list. Wire keys: {cwd?, query?, limit?, cursor?, allowRelax?, _meta?} —
// all optional; cwd/query/cursor are omitted when empty, limit is omitted
// when 0 (agent default), allowRelax/_meta are never sent.
func (b *Bridge) SessionList(ctx context.Context, cwd, query, cursor string, limit int) (map[string]any, error) {
	params := map[string]any{}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if query != "" {
		params["query"] = query
	}
	if cursor != "" {
		params["cursor"] = cursor
	}
	if limit != 0 {
		params["limit"] = limit
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/list", params)
}

// SessionsList calls x.ai/sessions/list — the FleetView roster (resident +
// recently-touched dormant sessions). Takes NO params; no sessionId is
// sent.
func (b *Bridge) SessionsList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/sessions/list", map[string]any{})
}

// SessionSummariesSessionList calls x.ai/session_summaries/session_list —
// summaries of the sessions in one workspace directory. Wire keys:
// {workspace_directory} (snake_case — the agent struct has no rename
// attribute); the value is required.
func (b *Bridge) SessionSummariesSessionList(ctx context.Context, workspaceDir string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session_summaries/session_list", map[string]any{
		"workspace_directory": workspaceDir,
	})
}

// SessionSummariesWorkspaceList calls
// x.ai/session_summaries/workspace_list — all sessions grouped by working
// directory. Takes NO params.
func (b *Bridge) SessionSummariesWorkspaceList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session_summaries/workspace_list", map[string]any{})
}

// SessionSummariesWorkspaceListRecent calls
// x.ai/session_summaries/workspace_list_recent — the most recently touched
// sessions. Wire keys: {limit} — required (clamped to 10000 by the agent).
func (b *Bridge) SessionSummariesWorkspaceListRecent(ctx context.Context, limit int) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/session_summaries/workspace_list_recent", map[string]any{
		"limit": limit,
	})
}

// WorkspacesList calls x.ai/workspaces/list — cloud workspace pages. Wire
// keys: {pageSize?, pageToken?, query?, kind?} — all optional; pageSize is
// omitted when 0 (agent default 50), the strings are omitted when empty.
func (b *Bridge) WorkspacesList(ctx context.Context, pageSize int, pageToken, query, kind string) (map[string]any, error) {
	params := map[string]any{}
	if pageSize != 0 {
		params["pageSize"] = pageSize
	}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	if query != "" {
		params["query"] = query
	}
	if kind != "" {
		params["kind"] = kind
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/workspaces/list", params)
}

// PromptHistory calls x.ai/prompt_history — recent prompt strings for a
// cwd, optionally scoped to one session. Wire keys: {cwd, session_id?,
// filter_session_id?} — SNAKE_CASE (the agent struct has no rename
// attribute); cwd required, session_id/filter_session_id omitted when
// empty.
func (b *Bridge) PromptHistory(ctx context.Context, cwd, sessionID, filterSessionID string) (map[string]any, error) {
	params := map[string]any{"cwd": cwd}
	if sessionID != "" {
		params["session_id"] = sessionID
	}
	if filterSessionID != "" {
		params["filter_session_id"] = filterSessionID
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/prompt_history", params)
}

// ── interjection / feedback / commands ───────────────────────────────

// Btw calls x.ai/btw — dispatch a side question to the session without
// interrupting the current turn. Wire keys: {sessionId, question} — both
// required; sessionId is passed as "" (filled by XaiCall).
func (b *Bridge) Btw(ctx context.Context, question string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/btw", map[string]any{
		"sessionId": "",
		"question":  question,
	})
}

// Feedback calls x.ai/feedback — persist a user rating/comment. The agent
// contract is ClientFeedbackInput, SNAKE_CASE wire keys: {session_id,
// client_type, rating_type?, rating_value?, feedback_text?,
// feedback_categories?, context_type?, turn_number?, request_id?,
// client_version?, metadata?, terminal_info?}. session_id defaults to ""
// (filled by XaiCall) unless the caller's input overrides it; input is
// passed through verbatim.
func (b *Bridge) Feedback(ctx context.Context, input map[string]any) (map[string]any, error) {
	params := map[string]any{"session_id": ""}
	for k, v := range input {
		params[k] = v
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/feedback", params)
}

// FeedbackDismiss calls x.ai/feedback/dismiss — dismiss a solicited
// feedback request. Wire keys: {session_id, request_id} (snake_case,
// both required); session_id is passed as "" (filled by XaiCall).
func (b *Bridge) FeedbackDismiss(ctx context.Context, requestID string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/feedback/dismiss", map[string]any{
		"session_id": "",
		"request_id": requestID,
	})
}

// Interject calls x.ai/interject — inject a prompt into the session's
// running turn. Wire keys: {sessionId, text, interjectionId?, content?} —
// sessionId required ("" → filled by XaiCall), text required,
// interjectionId omitted when empty (content is never sent; use
// QueueInterject-style notifications for structured blocks).
func (b *Bridge) Interject(ctx context.Context, text, interjectionID string) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "text": text}
	if interjectionID != "" {
		params["interjectionId"] = interjectionID
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/interject", params)
}

// CommandsList calls x.ai/commands/list — the slash-command catalog. Wire
// keys: {sessionId?, cwd?, kind?} — all optional, so nothing is sent.
func (b *Bridge) CommandsList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/commands/list", map[string]any{})
}

// ── subagents ────────────────────────────────────────────────────────

// SubagentListRunning calls x.ai/subagent/list_running — live snapshots of
// the session's running subagents. Wire keys: {sessionId} REQUIRED; "" is
// passed so XaiCall fills the active session id.
func (b *Bridge) SubagentListRunning(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/subagent/list_running", map[string]any{"sessionId": ""})
}

// SubagentGet calls x.ai/subagent/get — snapshot of one subagent. Wire
// keys: {subagentId, block?, timeoutMs?} — no sessionId at all;
// subagentId required, block omitted when nil, timeoutMs omitted when 0.
func (b *Bridge) SubagentGet(ctx context.Context, subagentID string, block *bool, timeoutMs int) (map[string]any, error) {
	params := map[string]any{"subagentId": subagentID}
	if block != nil {
		params["block"] = *block
	}
	if timeoutMs != 0 {
		params["timeoutMs"] = timeoutMs
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/subagent/get", params)
}

// ── prompt queue (fire-and-forget notifications) ─────────────────────
//
// The x.ai/queue/* methods have NO request branch in the agent — they are
// handled only as ext_notifications, so every wrapper writes without a
// JSON-RPC id (a request-style call would get -32601). The agent routes on
// sessionId and silently drops the command without it, so each wrapper
// fills sessionId from the active session and fails fast with 404 when
// there is none (mirroring TogglePlanMode/PermissionsReset).

// QueueRemove sends x.ai/queue/remove: {id, sessionId, expectedVersion?}
// (expectedVersion omitted — agent treats it as 0). Removes one queued
// prompt on an exact version match.
func (b *Bridge) QueueRemove(ctx context.Context, id string) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/remove", map[string]any{"id": id})
}

// QueueReorder sends x.ai/queue/reorder: {orderedIds, sessionId} — the
// full ordered list of queued prompt ids.
func (b *Bridge) QueueReorder(ctx context.Context, orderedIDs []string) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/reorder", map[string]any{"orderedIds": orderedIDs})
}

// QueueClear sends x.ai/queue/clear: {sessionId} — clears the requesting
// client's queued prompts.
func (b *Bridge) QueueClear(ctx context.Context) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/clear", map[string]any{})
}

// QueueEdit sends x.ai/queue/edit: {id, newText, sessionId} — in-place
// text edit of a queued prompt.
func (b *Bridge) QueueEdit(ctx context.Context, id, newText string) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/edit", map[string]any{"id": id, "newText": newText})
}

// QueueHoldEdit sends x.ai/queue/hold_edit: {id, sessionId} — pins a
// queued prompt open for editing (combine-edit mode).
func (b *Bridge) QueueHoldEdit(ctx context.Context, id string) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/hold_edit", map[string]any{"id": id})
}

// QueueReleaseEdit sends x.ai/queue/release_edit: {id, sessionId} —
// releases a held queued prompt.
func (b *Bridge) QueueReleaseEdit(ctx context.Context, id string) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/release_edit", map[string]any{"id": id})
}

// QueueInterject sends x.ai/queue/interject: {id, sessionId,
// expectedVersion?, newText?} — promotes a queued prompt into the running
// turn; expectedVersion/newText are omitted (agent defaults: version 0,
// keep stored text).
func (b *Bridge) QueueInterject(ctx context.Context, id string) (map[string]any, error) {
	return b.xaiNotify(ctx, "x.ai/queue/interject", map[string]any{"id": id})
}

// ── sharing ──────────────────────────────────────────────────────────

// ShareSession calls x.ai/share_session — create a shareable URL for the
// session. Wire keys: {session_id} — SNAKE_CASE (the agent struct has no
// rename attribute) and required; "" is passed so XaiCall fills the active
// session id. Returns {share_url}.
func (b *Bridge) ShareSession(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/share_session", map[string]any{"session_id": ""})
}
