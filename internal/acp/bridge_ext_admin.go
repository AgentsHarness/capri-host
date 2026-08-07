package acp

import (
	"context"
)

// ── MCP family ───────────────────────────────────────────────────────
//
// The x.ai/mcp/* methods are a MIXED wire convention (verified in
// extensions/mcp.rs): mcp/list, mcp/call, mcp/read_resource and mcp/setup
// are camelCase; mcp/auth_status, mcp/auth_trigger, mcp/toggle,
// mcp/toggle_tool, mcp/upsert and mcp/delete are snake_case. mcp/list,
// mcp/toggle, mcp/upsert, mcp/delete and mcp/auth_trigger already exist as
// Bridge.MCPList/MCPToggle/MCPUpsert/MCPDelete/MCPAuthTrigger — only the
// unwrapped methods are defined here.

// McpCall calls x.ai/mcp/call — invoke an MCP tool directly, outside the
// LLM loop. Wire keys (camelCase): {sessionId?, server, serverUrl?, tool,
// arguments}; sessionId is OPTIONAL (agent-pool fallback) so it is
// omitted, server/tool required, serverUrl omitted when empty, arguments
// always sent ({} when nil).
func (b *Bridge) McpCall(ctx context.Context, server, tool string, arguments map[string]any, serverURL string) (map[string]any, error) {
	params := map[string]any{"server": server, "tool": tool, "arguments": arguments}
	if arguments == nil {
		params["arguments"] = map[string]any{}
	}
	if serverURL != "" {
		params["serverUrl"] = serverURL
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/mcp/call", params)
}

// McpReadResource calls x.ai/mcp/read_resource — read one MCP resource.
// Wire keys (camelCase): {sessionId?, server, uri}; sessionId is OPTIONAL
// so it is omitted, server/uri required.
func (b *Bridge) McpReadResource(ctx context.Context, server, uri string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/mcp/read_resource", map[string]any{
		"server": server,
		"uri":    uri,
	})
}

// McpAuthStatus calls x.ai/mcp/auth_status — per-server auth state. Wire
// keys (snake_case): {session_id} REQUIRED; "" is passed so XaiCall fills
// the active session id. Returns {servers: [{server_name, status}…]}.
func (b *Bridge) McpAuthStatus(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/mcp/auth_status", map[string]any{"session_id": ""})
}

// McpSetup calls x.ai/mcp/setup — submit setup-form values for a server.
// Wire keys (camelCase): {sessionId, serverName, values} — all required;
// sessionId is passed as "" so XaiCall fills the active session id, values
// is the field-id → value map.
func (b *Bridge) McpSetup(ctx context.Context, serverName string, values map[string]string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/mcp/setup", map[string]any{
		"sessionId":  "",
		"serverName": serverName,
		"values":     values,
	})
}

// McpToggleTool calls x.ai/mcp/toggle_tool — enable/disable one tool of an
// MCP server. Wire keys (snake_case): {session_id, server_name, tool_name,
// enabled} — all required; session_id is passed as "" so XaiCall fills the
// active session id.
func (b *Bridge) McpToggleTool(ctx context.Context, serverName, toolName string, enabled bool) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/mcp/toggle_tool", map[string]any{
		"session_id":  "",
		"server_name": serverName,
		"tool_name":   toolName,
		"enabled":     enabled,
	})
}

// ── skills / workflows ───────────────────────────────────────────────

// SkillsList calls x.ai/skills/list — all discovered skills. Wire keys
// (camelCase): {cwd} REQUIRED (working directory for skill discovery).
func (b *Bridge) SkillsList(ctx context.Context, cwd string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/skills/list", map[string]any{"cwd": cwd})
}

// SkillsAdd calls x.ai/skills/add — add a skill path. Wire keys
// (camelCase): {path, cwd?}; path required, cwd omitted when empty.
func (b *Bridge) SkillsAdd(ctx context.Context, path, cwd string) (map[string]any, error) {
	params := map[string]any{"path": path}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/skills/add", params)
}

// SkillsRemove calls x.ai/skills/remove — remove a skill path. Wire keys
// (camelCase): {path, cwd?}; path required, cwd omitted when empty.
func (b *Bridge) SkillsRemove(ctx context.Context, path, cwd string) (map[string]any, error) {
	params := map[string]any{"path": path}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/skills/remove", params)
}

// SkillsReset calls x.ai/skills/reset — reset the custom skills config.
// Wire keys (camelCase): {cwd?} — omitted when empty (agent defaults to
// ".").
func (b *Bridge) SkillsReset(ctx context.Context, cwd string) (map[string]any, error) {
	params := map[string]any{}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/skills/reset", params)
}

// SkillsConfig calls x.ai/skills/config — the current skills config. Wire
// keys (camelCase): {cwd?} — omitted when empty (agent defaults to ".").
func (b *Bridge) SkillsConfig(ctx context.Context, cwd string) (map[string]any, error) {
	params := map[string]any{}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/skills/config", params)
}

// SkillsToggle calls x.ai/skills/toggle — enable/disable a skill by name.
// Wire keys (camelCase): {name, enabled, cwd?}; name/enabled required,
// cwd omitted when empty.
func (b *Bridge) SkillsToggle(ctx context.Context, name string, enabled bool, cwd string) (map[string]any, error) {
	params := map[string]any{"name": name, "enabled": enabled}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/skills/toggle", params)
}

// WorkflowsList calls x.ai/workflows/list — the session's workflow
// catalog. Wire keys (camelCase): {sessionId} REQUIRED; "" is passed so
// XaiCall fills the active session id.
func (b *Bridge) WorkflowsList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/workflows/list", map[string]any{"sessionId": ""})
}

// ── plugins / hooks / marketplace ────────────────────────────────────

// PluginsList calls x.ai/plugins/list — the session's plugin registry.
// Wire keys (camelCase): {sessionId} REQUIRED; "" is passed so XaiCall
// fills the active session id.
func (b *Bridge) PluginsList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/plugins/list", map[string]any{"sessionId": ""})
}

// PluginsAction calls x.ai/plugins/action — a plugin management action.
// Wire keys (camelCase): {sessionId, action}; sessionId is passed as ""
// (filled by XaiCall), action is the tagged enum
// {"type": "reload"|"install"|"uninstall"|"update"|"add"|"remove"|
// "enable"|"disable", …} with SNAKE_CASE variant fields (e.g.
// {"type":"install","source":…}, {"type":"uninstall","plugin_id":…,
// "confirmed":…}).
func (b *Bridge) PluginsAction(ctx context.Context, action map[string]any) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/plugins/action", map[string]any{
		"sessionId": "",
		"action":    action,
	})
}

// PluginsNotifyUpdates calls x.ai/plugins/notify-updates — broadcast a
// plugin-updates-installed notification to the session. Wire keys
// (camelCase): {sessionId, updates} — updates is an array of
// (name, old_version, new_version) tuples; sessionId is passed as "" so
// XaiCall fills the active session id.
func (b *Bridge) PluginsNotifyUpdates(ctx context.Context, updates []any) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/plugins/notify-updates", map[string]any{
		"sessionId": "",
		"updates":   updates,
	})
}

// HooksList calls x.ai/hooks/list — the session's hook catalog. Wire keys
// (camelCase): {sessionId} REQUIRED; "" is passed so XaiCall fills the
// active session id.
func (b *Bridge) HooksList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hooks/list", map[string]any{"sessionId": ""})
}

// HooksAction calls x.ai/hooks/action — a hook management action. Wire
// keys (camelCase): {sessionId, action}; sessionId is passed as ""
// (filled by XaiCall), action is the tagged enum
// {"type": "reload"|"trust"|"untrust"|"add"|"remove"|"enable"|"disable",
// …} with SNAKE_CASE variant fields (e.g.
// {"type":"enable","hook_name":…}).
func (b *Bridge) HooksAction(ctx context.Context, action map[string]any) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hooks/action", map[string]any{
		"sessionId": "",
		"action":    action,
	})
}

// MarketplaceList calls x.ai/marketplace/list — the marketplace plugin
// catalog. Takes NO params.
func (b *Bridge) MarketplaceList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/marketplace/list", map[string]any{})
}

// MarketplaceAction calls x.ai/marketplace/action — a marketplace
// management action. Wire keys (camelCase): {sessionId, action}; sessionId
// is passed as "" (filled by XaiCall), action is the tagged enum
// {"type": "refresh"|"install"|"update"|"uninstall"|"add_source"|
// "remove_source", …} with SNAKE_CASE variant fields (e.g.
// {"type":"install","source_url_or_path":…,"plugin_relative_path":…}).
func (b *Bridge) MarketplaceAction(ctx context.Context, action map[string]any) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/marketplace/action", map[string]any{
		"sessionId": "",
		"action":    action,
	})
}

// ── bundle ───────────────────────────────────────────────────────────

// BundleSync calls x.ai/bundle/sync — fetch and extract the deployment
// bundle. Wire keys (camelCase): {force?} — omitted unless true.
func (b *Bridge) BundleSync(ctx context.Context, force bool) (map[string]any, error) {
	params := map[string]any{}
	if force {
		params["force"] = true
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/bundle/sync", params)
}

// BundleStatus calls x.ai/bundle/status — cached bundle status. Takes NO
// params.
func (b *Bridge) BundleStatus(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/bundle/status", map[string]any{})
}

// BundleEntryGet calls x.ai/bundle/entry/get — one bundle entry's content.
// Wire keys (camelCase): {kind, name} — both required.
func (b *Bridge) BundleEntryGet(ctx context.Context, kind, name string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/bundle/entry/get", map[string]any{
		"kind": kind,
		"name": name,
	})
}

// ── suggest ──────────────────────────────────────────────────────────

// Suggest calls x.ai/suggest — Tab-completion suggestions. Wire keys
// (camelCase): {text, cursor, cwd, limit, generation, includeAi?,
// aiModel?, sessionId?}; text/cursor/cwd/limit/generation required,
// includeAi omitted when nil, aiModel omitted when empty, sessionId is
// OPTIONAL (agent resolves the session itself) so it is omitted.
func (b *Bridge) Suggest(ctx context.Context, text string, cursor, limit int, generation uint64, cwd string, includeAI *bool, aiModel string) (map[string]any, error) {
	params := map[string]any{
		"text":       text,
		"cursor":     cursor,
		"cwd":        cwd,
		"limit":      limit,
		"generation": generation,
	}
	if includeAI != nil {
		params["includeAi"] = *includeAI
	}
	if aiModel != "" {
		params["aiModel"] = aiModel
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/suggest", params)
}

// SuggestPrompt calls x.ai/suggestPrompt — next-prompt prediction. Wire
// keys (camelCase): {generation, sessionId?, model?}; generation required,
// sessionId OPTIONAL (omitted), model omitted when empty.
func (b *Bridge) SuggestPrompt(ctx context.Context, generation uint64, model string) (map[string]any, error) {
	params := map[string]any{"generation": generation}
	if model != "" {
		params["model"] = model
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/suggestPrompt", params)
}

// ── pr ───────────────────────────────────────────────────────────────

// PrStatus calls x.ai/pr/status — PR status for a branch. Wire keys
// (camelCase): {cwd, branch} — both required.
func (b *Bridge) PrStatus(ctx context.Context, cwd, branch string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/pr/status", map[string]any{
		"cwd":    cwd,
		"branch": branch,
	})
}

// ── auth ─────────────────────────────────────────────────────────────

// AuthGetUrl calls x.ai/auth/get_url — the interactive-login URL (device
// or loopback mode). Takes NO params.
func (b *Bridge) AuthGetUrl(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/get_url", map[string]any{})
}

// AuthSubmitCode calls x.ai/auth/submit_code — submit the device code for
// an in-flight login. Wire keys: {code} — required.
func (b *Bridge) AuthSubmitCode(ctx context.Context, code string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/submit_code", map[string]any{"code": code})
}

// AuthCancel calls x.ai/auth/cancel — stop an in-flight interactive login.
// Wire keys: {request_seq?} — the optional sequence is omitted (cancels
// any attempt).
func (b *Bridge) AuthCancel(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/cancel", map[string]any{})
}

// AuthLogout calls x.ai/auth/logout — sign out. Wire keys: {scope?} — the
// optional scope is omitted (full logout).
func (b *Bridge) AuthLogout(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/logout", map[string]any{})
}

// AuthInfo calls x.ai/auth/info — current auth state. Takes NO params.
func (b *Bridge) AuthInfo(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/info", map[string]any{})
}

// AuthCheckSubscription calls x.ai/auth/check_subscription — subscription
// tier status. Takes NO params.
func (b *Bridge) AuthCheckSubscription(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/check_subscription", map[string]any{})
}

// AuthGetBearerToken calls x.ai/auth/getBearerToken — a valid bearer token
// for the current auth. Takes NO params. Returns {token}.
func (b *Bridge) AuthGetBearerToken(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auth/getBearerToken", map[string]any{})
}

// GetApiKey calls x.ai/getApiKey — the XAI_API_KEY env value. Takes NO
// params. Returns {key}.
func (b *Bridge) GetApiKey(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/getApiKey", map[string]any{})
}

// SetApiKey calls x.ai/setApiKey — persist (or clear, when empty) the API
// key. Wire keys: {key} — required.
func (b *Bridge) SetApiKey(ctx context.Context, key string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/setApiKey", map[string]any{"key": key})
}

// ── misc ─────────────────────────────────────────────────────────────

// AutoTopupRule calls x.ai/auto-topup-rule — the auto top-up billing rule.
// Takes NO params.
func (b *Bridge) AutoTopupRule(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/auto-topup-rule", map[string]any{})
}

// PrivacySetCodingDataRetention calls
// x.ai/privacy/setCodingDataRetention — opt out/in of coding-data
// retention. Wire keys (camelCase): {codingDataRetentionOptOut} —
// required.
func (b *Bridge) PrivacySetCodingDataRetention(ctx context.Context, optOut bool) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/privacy/setCodingDataRetention", map[string]any{
		"codingDataRetentionOptOut": optOut,
	})
}

// ReviewComment calls x.ai/review/comment — record an inline code-review
// comment on a prompt turn. Wire keys (camelCase): {sessionId,
// promptIndex, comment, citation}; sessionId is passed as "" (filled by
// XaiCall), citation is {path, startLine, endLine, text, side?}.
func (b *Bridge) ReviewComment(ctx context.Context, promptIndex int, comment string, citation map[string]any) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/review/comment", map[string]any{
		"sessionId":   "",
		"promptIndex": promptIndex,
		"comment":     comment,
		"citation":    citation,
	})
}

// ReviewCommentDelete calls x.ai/review/comment/delete — tombstone a
// review comment. Wire keys (camelCase): {sessionId, commentId}; sessionId
// is passed as "" so XaiCall fills the active session id.
func (b *Bridge) ReviewCommentDelete(ctx context.Context, commentID string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/review/comment/delete", map[string]any{
		"sessionId": "",
		"commentId": commentID,
	})
}

// RolloutSurvey calls x.ai/rollout/survey — submit a rollout survey
// response. Wire keys (camelCase): {sessionId, preferences, feedback} —
// all required; sessionId is passed as "" so XaiCall fills the active
// session id.
func (b *Bridge) RolloutSurvey(ctx context.Context, preferences []string, feedback string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/rollout/survey", map[string]any{
		"sessionId":   "",
		"preferences": preferences,
		"feedback":    feedback,
	})
}

// ── hunk tracker ─────────────────────────────────────────────────────
//
// All x.ai/hunk-tracker/* methods are camelCase with an OPTIONAL
// sessionId — omitted per convention (the agent falls back to the active
// session). get-files/get-all-file-contents/get-summary take no other
// params.

// HunkGetHunks calls x.ai/hunk-tracker/get-hunks — hunks for the session,
// optionally filtered by path/source. Wire keys (camelCase): {sessionId?,
// path?, source?}; path/source omitted when empty.
func (b *Bridge) HunkGetHunks(ctx context.Context, path, source string) (map[string]any, error) {
	params := map[string]any{}
	if path != "" {
		params["path"] = path
	}
	if source != "" {
		params["source"] = source
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/get-hunks", params)
}

// HunkGetFiles calls x.ai/hunk-tracker/get-files — hunk files + staged
// paths. Wire keys: {sessionId?} — omitted.
func (b *Bridge) HunkGetFiles(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/get-files", map[string]any{})
}

// HunkGetAllFileContents calls x.ai/hunk-tracker/get-all-file-contents —
// baseline/current content of every hunked file. Wire keys: {sessionId?} —
// omitted.
func (b *Bridge) HunkGetAllFileContents(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/get-all-file-contents", map[string]any{})
}

// HunkGetSummary calls x.ai/hunk-tracker/get-summary — hunk summary
// counts. Wire keys: {sessionId?} — omitted.
func (b *Bridge) HunkGetSummary(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/get-summary", map[string]any{})
}

// HunkAction calls x.ai/hunk-tracker/hunk-action — accept/reject one hunk.
// Wire keys (camelCase): {sessionId?, hunkId, action} — hunkId and action
// ("accept"|"reject") required.
func (b *Bridge) HunkAction(ctx context.Context, hunkID, action string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/hunk-action", map[string]any{
		"hunkId": hunkID,
		"action": action,
	})
}

// HunkFileAction calls x.ai/hunk-tracker/file-action — accept/reject all
// hunks of one file. Wire keys (camelCase): {sessionId?, path, action} —
// path and action ("accept"|"reject") required.
func (b *Bridge) HunkFileAction(ctx context.Context, path, action string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/file-action", map[string]any{
		"path":   path,
		"action": action,
	})
}

// HunkTurnAction calls x.ai/hunk-tracker/turn-action — accept/reject all
// hunks of one turn. Wire keys (camelCase): {sessionId?, promptIndex,
// action} — promptIndex and action ("accept"|"reject") required.
func (b *Bridge) HunkTurnAction(ctx context.Context, promptIndex int, action string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/turn-action", map[string]any{
		"promptIndex": promptIndex,
		"action":      action,
	})
}

// HunkAllAction calls x.ai/hunk-tracker/all-action — accept/reject every
// hunk. Wire keys (camelCase): {sessionId?, action} — action
// ("accept"|"reject") required.
func (b *Bridge) HunkAllAction(ctx context.Context, action string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/hunk-tracker/all-action", map[string]any{
		"action": action,
	})
}

// ── cloud (sandbox) ──────────────────────────────────────────────────

// CloudEnvList calls x.ai/cloud/env/list — the sandbox environment list.
// Takes NO params. Returns {environments}.
func (b *Bridge) CloudEnvList(ctx context.Context) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/cloud/env/list", map[string]any{})
}

// CloudEnvCreate calls x.ai/cloud/env/create — create a sandbox
// environment. Wire keys (snake_case, read via raw params in the agent):
// {name?, description?, repository?, default_branch?, container_image?,
// setup_script?} — all optional, omitted when empty.
func (b *Bridge) CloudEnvCreate(ctx context.Context, name, description, repository, defaultBranch, containerImage, setupScript string) (map[string]any, error) {
	params := map[string]any{}
	if name != "" {
		params["name"] = name
	}
	if description != "" {
		params["description"] = description
	}
	if repository != "" {
		params["repository"] = repository
	}
	if defaultBranch != "" {
		params["default_branch"] = defaultBranch
	}
	if containerImage != "" {
		params["container_image"] = containerImage
	}
	if setupScript != "" {
		params["setup_script"] = setupScript
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/cloud/env/create", params)
}

// CloudEnvUpdate calls x.ai/cloud/env/update — update a sandbox
// environment. Wire keys (snake_case): {environment_id, name?,
// description?, repository?, default_branch?, container_image?,
// setup_script?} — environment_id required, the rest omitted when empty.
func (b *Bridge) CloudEnvUpdate(ctx context.Context, environmentID, name, description, repository, defaultBranch, containerImage, setupScript string) (map[string]any, error) {
	params := map[string]any{"environment_id": environmentID}
	if name != "" {
		params["name"] = name
	}
	if description != "" {
		params["description"] = description
	}
	if repository != "" {
		params["repository"] = repository
	}
	if defaultBranch != "" {
		params["default_branch"] = defaultBranch
	}
	if containerImage != "" {
		params["container_image"] = containerImage
	}
	if setupScript != "" {
		params["setup_script"] = setupScript
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/cloud/env/update", params)
}

// CloudEnvDelete calls x.ai/cloud/env/delete — delete a sandbox
// environment. Wire keys (snake_case): {environment_id} — required.
func (b *Bridge) CloudEnvDelete(ctx context.Context, environmentID string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/cloud/env/delete", map[string]any{
		"environment_id": environmentID,
	})
}

// CloudTerminate calls x.ai/cloud/terminate — terminate a sandbox session.
// Wire keys (snake_case): {sandbox_id} — required.
func (b *Bridge) CloudTerminate(ctx context.Context, sandboxID string) (map[string]any, error) {
	return b.xaiCallUnwrapped(ctx, "x.ai/cloud/terminate", map[string]any{
		"sandbox_id": sandboxID,
	})
}
