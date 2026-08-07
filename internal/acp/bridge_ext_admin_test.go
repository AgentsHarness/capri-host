package acp

import (
	"context"
	"reflect"
	"testing"
)

// Admin x.ai wrappers, including the MCP mixed-convention verification:
// mcp/setup is camelCase (sessionId/serverName/values), mcp/toggle_tool is
// snake_case (session_id/server_name/tool_name/enabled) — both with the
// REQUIRED session id passed as "" so XaiCall fills "s1" from
// readyBridge. Responses are unwrapped from the ExtMethodResult envelope.
func TestExtAdminMethodsWirePayloads(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			// MCP camelCase family: sessionId/serverName/values.
			name: "mcp/setup camelCase sessionId/serverName/values",
			call: func(b *Bridge) (map[string]any, error) {
				return b.McpSetup(ctx, "github", map[string]string{"token": "t"})
			},
			method: "_x.ai/mcp/setup",
			params: map[string]any{"sessionId": "s1", "serverName": "github", "values": map[string]any{"token": "t"}},
		},
		{
			// MCP snake_case family: session_id/server_name/tool_name/enabled.
			name:   "mcp/toggle_tool snake_case session_id/server_name/tool_name/enabled",
			call:   func(b *Bridge) (map[string]any, error) { return b.McpToggleTool(ctx, "fs", "read", false) },
			method: "_x.ai/mcp/toggle_tool",
			params: map[string]any{"session_id": "s1", "server_name": "fs", "tool_name": "read", "enabled": false},
		},
		{
			name:   "mcp/auth_status snake_case session_id filled",
			call:   func(b *Bridge) (map[string]any, error) { return b.McpAuthStatus(ctx) },
			method: "_x.ai/mcp/auth_status",
			params: map[string]any{"session_id": "s1"},
		},
		{
			name: "mcp/call omits optional sessionId",
			call: func(b *Bridge) (map[string]any, error) {
				return b.McpCall(ctx, "fs", "list", map[string]any{"path": "/"}, "")
			},
			method: "_x.ai/mcp/call",
			params: map[string]any{"server": "fs", "tool": "list", "arguments": map[string]any{"path": "/"}},
		},
		{
			name:   "mcp/read_resource omits optional sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.McpReadResource(ctx, "fs", "file:///a") },
			method: "_x.ai/mcp/read_resource",
			params: map[string]any{"server": "fs", "uri": "file:///a"},
		},
		{
			name:   "skills/list sends cwd",
			call:   func(b *Bridge) (map[string]any, error) { return b.SkillsList(ctx, "/ws") },
			method: "_x.ai/skills/list",
			params: map[string]any{"cwd": "/ws"},
		},
		{
			name:   "auth/logout sends no params",
			call:   func(b *Bridge) (map[string]any, error) { return b.AuthLogout(ctx) },
			method: "_x.ai/auth/logout",
			params: map[string]any{},
		},
		{
			name:   "hunk-tracker/get-hunks omits optional sessionId/path/source",
			call:   func(b *Bridge) (map[string]any, error) { return b.HunkGetHunks(ctx, "", "") },
			method: "_x.ai/hunk-tracker/get-hunks",
			params: map[string]any{},
		},
		{
			name:   "hunk-tracker/get-hunks with path+source",
			call:   func(b *Bridge) (map[string]any, error) { return b.HunkGetHunks(ctx, "src/a.go", "agent") },
			method: "_x.ai/hunk-tracker/get-hunks",
			params: map[string]any{"path": "src/a.go", "source": "agent"},
		},
		{
			name:   "suggest sends required fields, no sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.Suggest(ctx, "git st", 3, 10, 7, "/ws", nil, "") },
			method: "_x.ai/suggest",
			params: map[string]any{"text": "git st", "cursor": float64(3), "cwd": "/ws", "limit": float64(10), "generation": float64(7)},
		},
		{
			name:   "pr/status sends cwd+branch",
			call:   func(b *Bridge) (map[string]any, error) { return b.PrStatus(ctx, "/ws", "feat/x") },
			method: "_x.ai/pr/status",
			params: map[string]any{"cwd": "/ws", "branch": "feat/x"},
		},
		{
			name:   "workflows/list fills required sessionId",
			call:   func(b *Bridge) (map[string]any, error) { return b.WorkflowsList(ctx) },
			method: "_x.ai/workflows/list",
			params: map[string]any{"sessionId": "s1"},
		},
		{
			name:   "getApiKey sends no params",
			call:   func(b *Bridge) (map[string]any, error) { return b.GetApiKey(ctx) },
			method: "_x.ai/getApiKey",
			params: map[string]any{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, w := readyBridge()
			done := make(chan callResult, 1)
			go func() {
				res, err := c.call(b)
				done <- callResult{res, err}
			}()
			// Envelope form: UnwrapExtResult must surface the inner payload.
			resolveNext(t, b, w, map[string]any{"result": map[string]any{"ok": true}})
			cr := <-done
			if cr.err != nil {
				t.Fatalf("call error: %v", cr.err)
			}
			if cr.res["ok"] != true {
				t.Errorf("result = %v, want unwrapped ok:true", cr.res)
			}
			msg := w.last()
			if msg == nil {
				t.Fatal("no request captured")
			}
			if got := msg["method"]; got != c.method {
				t.Errorf("wire method = %v, want %s (bare names get -32601)", got, c.method)
			}
			gotParams, _ := msg["params"].(map[string]any)
			if !reflect.DeepEqual(gotParams, c.params) {
				t.Errorf("params = %v, want %v", gotParams, c.params)
			}
		})
	}
}

// Optional-field omission rules for the remaining admin families:
// skills cwd?, hunk actions, cloud snake_case keys, auth submit_code.
func TestExtAdminOptionalFieldOmission(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		call   func(b *Bridge) (map[string]any, error)
		method string
		params map[string]any
	}{
		{
			name:   "skills/toggle omits empty cwd",
			call:   func(b *Bridge) (map[string]any, error) { return b.SkillsToggle(ctx, "frontend", true, "") },
			method: "_x.ai/skills/toggle",
			params: map[string]any{"name": "frontend", "enabled": true},
		},
		{
			name: "plugins/action tagged snake_case action passthrough",
			call: func(b *Bridge) (map[string]any, error) {
				return b.PluginsAction(ctx, map[string]any{"type": "install", "source": "https://x"})
			},
			method: "_x.ai/plugins/action",
			params: map[string]any{"sessionId": "s1", "action": map[string]any{"type": "install", "source": "https://x"}},
		},
		{
			name: "hooks/action tagged action passthrough",
			call: func(b *Bridge) (map[string]any, error) {
				return b.HooksAction(ctx, map[string]any{"type": "enable", "hook_name": "pre"})
			},
			method: "_x.ai/hooks/action",
			params: map[string]any{"sessionId": "s1", "action": map[string]any{"type": "enable", "hook_name": "pre"}},
		},
		{
			name:   "marketplace/list sends no params",
			call:   func(b *Bridge) (map[string]any, error) { return b.MarketplaceList(ctx) },
			method: "_x.ai/marketplace/list",
			params: map[string]any{},
		},
		{
			name:   "bundle/entry/get kind+name",
			call:   func(b *Bridge) (map[string]any, error) { return b.BundleEntryGet(ctx, "role", "backend") },
			method: "_x.ai/bundle/entry/get",
			params: map[string]any{"kind": "role", "name": "backend"},
		},
		{
			name:   "bundle/sync omits force when false",
			call:   func(b *Bridge) (map[string]any, error) { return b.BundleSync(ctx, false) },
			method: "_x.ai/bundle/sync",
			params: map[string]any{},
		},
		{
			name:   "suggestPrompt sends generation only",
			call:   func(b *Bridge) (map[string]any, error) { return b.SuggestPrompt(ctx, 3, "") },
			method: "_x.ai/suggestPrompt",
			params: map[string]any{"generation": float64(3)},
		},
		{
			name:   "auth/submit_code",
			call:   func(b *Bridge) (map[string]any, error) { return b.AuthSubmitCode(ctx, "1234") },
			method: "_x.ai/auth/submit_code",
			params: map[string]any{"code": "1234"},
		},
		{
			name:   "hunk-action sends hunkId+action",
			call:   func(b *Bridge) (map[string]any, error) { return b.HunkAction(ctx, "h-9", "accept") },
			method: "_x.ai/hunk-tracker/hunk-action",
			params: map[string]any{"hunkId": "h-9", "action": "accept"},
		},
		{
			name:   "cloud/env/delete snake_case environment_id",
			call:   func(b *Bridge) (map[string]any, error) { return b.CloudEnvDelete(ctx, "env-1") },
			method: "_x.ai/cloud/env/delete",
			params: map[string]any{"environment_id": "env-1"},
		},
		{
			name:   "cloud/terminate snake_case sandbox_id",
			call:   func(b *Bridge) (map[string]any, error) { return b.CloudTerminate(ctx, "sb-1") },
			method: "_x.ai/cloud/terminate",
			params: map[string]any{"sandbox_id": "sb-1"},
		},
		{
			name:   "setApiKey key passthrough",
			call:   func(b *Bridge) (map[string]any, error) { return b.SetApiKey(ctx, "xai-123") },
			method: "_x.ai/setApiKey",
			params: map[string]any{"key": "xai-123"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, w := readyBridge()
			done := make(chan callResult, 1)
			go func() {
				res, err := c.call(b)
				done <- callResult{res, err}
			}()
			// Bare response form: no envelope, UnwrapExtResult passes through.
			resolveNext(t, b, w, map[string]any{"ok": true})
			cr := <-done
			if cr.err != nil {
				t.Fatalf("call error: %v", cr.err)
			}
			if cr.res["ok"] != true {
				t.Errorf("result = %v, want ok:true", cr.res)
			}
			msg := w.last()
			if msg == nil {
				t.Fatal("no request captured")
			}
			if got := msg["method"]; got != c.method {
				t.Errorf("wire method = %v, want %s", got, c.method)
			}
			gotParams, _ := msg["params"].(map[string]any)
			if !reflect.DeepEqual(gotParams, c.params) {
				t.Errorf("params = %v, want %v", gotParams, c.params)
			}
		})
	}
}

// XaiCall fills a REQUIRED snake_case session_id with the active session
// just like the camelCase one — verified through share-adjacent admin
// methods that carry snake_case keys.
func TestXaiCallFillsSnakeCaseSessionID(t *testing.T) {
	b, w := readyBridge()
	done := make(chan callResult, 1)
	go func() {
		res, err := b.McpAuthStatus(context.Background())
		done <- callResult{res, err}
	}()
	resolveNext(t, b, w, map[string]any{"result": map[string]any{"servers": []any{}}})
	cr := <-done
	if cr.err != nil {
		t.Fatal(cr.err)
	}
	msg := w.last()
	gotParams, _ := msg["params"].(map[string]any)
	if gotParams["session_id"] != "s1" {
		t.Errorf("session_id = %v, want s1 (active session)", gotParams["session_id"])
	}
}
