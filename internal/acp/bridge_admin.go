package acp

import (
	"context"
	"time"
)

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
// No host deadline: the run is an LLM summary whose length the agent owns, and
// the pager's own /flush waits for the same reply without a deadline. A 30s
// cap returned "memory flush 超时" while the flush was still running.
func (b *Bridge) MemoryFlush(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/flush", map[string]any{
		kSessionIDS: sessionID,
	}, 0)
}

// MemoryRewrite calls x.ai/memory/rewrite: {sessionId, rawText, contextSummary}
// — rewrites the session's memory from the given text. The agent's request
// struct is camelCase with all three fields required; the host must forward
// rawText/contextSummary or the call fails with invalid params.
// No host deadline, same as flush/dream: the rewrite is an LLM call and the
// pager runs it through the same no-deadline memory command helper.
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
	}, 0)
}

// MemoryList calls x.ai/memory/list: {sessionId} → the /memory modal's
// listing ({files, enabled, disabled_reason?, capture_enabled, dream_enabled}).
// The agent's request struct is camelCase here — unlike flush/dream — so the
// host must send `sessionId`, not `session_id`.
func (b *Bridge) MemoryList(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/list", map[string]any{
		kSessionID: sessionID,
	}, 30*time.Second)
}

// MemoryToggle calls x.ai/memory/toggle: {sessionId, enabled} — flips the
// session's memory without running a prompt turn. `enabled` is mandatory in
// the agent's request struct (a missing key is an invalid-params error, not
// a default), so it is a plain bool here and the HTTP layer rejects a body
// that omits it.
func (b *Bridge) MemoryToggle(ctx context.Context, sessionID string, enabled bool) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/toggle", map[string]any{
		kSessionID: sessionID,
		"enabled":  enabled,
	}, 30*time.Second)
}

// MemoryDream calls x.ai/memory/dream: {session_id} — runs memory
// consolidation and returns {disposition, observation_count,
// topics_affected}. The agent reuses the flush request struct, so this one is
// snake_case (`session_id`) while list/toggle above are camelCase.
// No host deadline, same as MemoryFlush: consolidation is model work and the
// pager waits for it without a cap.
func (b *Bridge) MemoryDream(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/dream", map[string]any{
		kSessionIDS: sessionID,
	}, 0)
}

// MemoryForget calls x.ai/memory/forget: {sessionId, path, expectedContentHash}
// — deletes one note listed by MemoryList. The hash is the client's evidence of
// what it previewed (BLAKE3 hex): the store refuses to delete bytes that no
// longer match, so the caller must hash exactly the text it showed the user
// (the pager does the same in views/memory_modal.rs). camelCase, unlike
// flush/dream.
func (b *Bridge) MemoryForget(ctx context.Context, sessionID, path, expectedContentHash string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	return b.request(ctx, "_x.ai/memory/forget", map[string]any{
		kSessionID:            sessionID,
		"path":                path,
		"expectedContentHash": expectedContentHash,
	}, 30*time.Second)
}

// TogglePlanMode sends x.ai/toggle_plan_mode: {sessionId} — switches the
// session's plan mode on/off. The agent only handles this method as a
// fire-and-forget NOTIFICATION (no request branch; a request-style call
// would get -32601 method_not_found), so the host writes it without a
// JSON-RPC id and returns a bare ok — the frontend applies its local
// desired state. SessionId defaults to the active session.
func (b *Bridge) TogglePlanMode(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	res, err := b.XaiNotify(ctx, "x.ai/toggle_plan_mode", map[string]any{kSessionID: sessionID})
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	newMode := "plan"
	if act := b.sessions[sessionID]; act != nil {
		mm, ok := act.modes.(map[string]any)
		if !ok || mm == nil {
			mm = map[string]any{}
			act.modes = mm
		}
		if cur, _ := mm["currentModeId"].(string); cur == "plan" {
			newMode = "default"
		}
		mm["currentModeId"] = newMode
	}
	b.mu.Unlock()
	b.Broadcast(Event{
		kType:      "modes_update",
		"modes":    map[string]any{"currentModeId": newMode},
		kSessionID: sessionID,
	})
	return res, nil
}

// PermissionsReset sends x.ai/permissions/reset: {sessionId} — clears the
// remembered permission decisions. Like toggle_plan_mode the agent only
// handles it as a notification, and it resets ALL resident sessions
// (ignoring the sessionId — the agent's handler iterates every session,
// with no client/session scoping), so the host writes it without a
// JSON-RPC id. Frontends should treat it as process-global.
func (b *Bridge) PermissionsReset(ctx context.Context, sessionID string) (map[string]any, error) {
	res, err := b.XaiNotify(ctx, "x.ai/permissions/reset", map[string]any{kSessionID: sessionID})
	if err != nil {
		return nil, err
	}
	b.Broadcast(Event{kType: "permissions_reset"})
	return res, nil
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
	// Broadcast to all connected FE clients so every tab/device converges
	// immediately (the agent notification is fire-and-forget and does not echo).
	b.Broadcast(Event{kType: "yolo_mode_changed", kParams: params})
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
