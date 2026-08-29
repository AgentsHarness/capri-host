package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// http_ext_test.go — x.ai 扩展直通测试公共层 + 跨域用例：
// 自由透传（/api/xai-call）、wire 键逐键断言（TestExtWireKeys，经 fake agent
// 录制 host→agent 请求核对）、跨域校验（400）与 agent 失败降级（200
// {ok:false}）语义。域内冒烟/校验用例见各 http_ext_*_test.go。

// http_ext_test.go — 冒烟测试 x.ai 扩展直通端点（http_ext.go）。
// fake agent 对未知方法默认回 {}，因此 200 冒烟验证路由 + XaiCall 往返 +
// 应答形状 {ok:true, result}；400 用例验证必填字段校验。

// wantOK asserts a 200 {ok:true} response.
func wantOK(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if ok, _ := m["ok"].(bool); !ok {
		t.Fatalf("body = %s, want ok:true", rec.Body.String())
	}
	return m
}

func TestXaiCallPassthrough(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// 无 params：默认空 map，成功返回 {ok:true, result:{}}。
	rec := postJSON(t, s, "/api/xai-call", `{"method":"x.ai/noop"}`)
	m := wantOK(t, rec)
	if _, has := m["result"]; !has {
		t.Fatalf("body = %s, want result key", rec.Body.String())
	}

	// params 带空 sessionId：XaiCall 填活动会话。
	createActiveSession(t, s)
	rec = postJSON(t, s, "/api/xai-call", `{"method":"x.ai/session/state","params":{"sessionId":""}}`)
	wantOK(t, rec)
}

func TestXaiCallRejectsMissingMethod(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	rec := postJSON(t, s, "/api/xai-call", `{"params":{}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// 用户主动重启 agent（/api/agent-restart）：杀掉旧进程、重新 boot、
// 恢复上次会话，之后 host 一切正常。host 从不自动重启 —— 这是唯一
// 的重启通道。
func TestAgentRestartEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	sid := createActiveSession(t, s)

	rec := postJSON(t, s, "/api/agent-restart", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent-restart status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}

	// 新进程已 boot、会话已恢复：会话级端点照常工作。
	rec2 := postJSON(t, s, "/api/session-updates", `{"sessionId":"`+sid+`","cwd":"/ws"}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("session-updates after restart status = %d, want 200 (body=%s)", rec2.Code, rec2.Body.String())
	}
}

// ── 必填校验（400）──────────────────────────────────────────────────

func TestExtValidation(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	cases := []struct {
		path string
		body string
	}{
		{"/api/session/state", `{}`},                                                    // 缺 cwd
		{"/api/session-import", `{}`},                                                   // 缺 cwd
		{"/api/session-update-mcp-servers", `{}`},                                       // 缺 mcpServers
		{"/api/session-summaries/session-list", `{}`},                                   // 缺 workspaceDirectory
		{"/api/session-summaries/workspace-list-recent", `{}`},                          // 缺 limit
		{"/api/cloud/terminate", `{}`},                                                  // 缺 sandboxId
		{"/api/cloud/env/update", `{}`},                                                 // 缺 environmentId
		{"/api/cloud/env/delete", `{}`},                                                 // 缺 environmentId
		{"/api/git/files", `{}`},                                                        // 缺 paths
		{"/api/git/stage-content", `{}`},                                                // 缺 path
		{"/api/git/worktree/create", `{}`},                                              // 缺 sourcePath
		{"/api/git/worktree/remove", `{}`},                                              // 缺 worktreePath/idOrPath
		{"/api/git/worktree/apply", `{}`},                                               // 缺 worktreePath
		{"/api/git/worktree/create-from-worktree", `{}`},                                // 缺 sourceWorktreePath/newSessionId
		{"/api/git/worktree/create-from-worktree-sync", `{"sourceWorktreePath":"/wt"}`}, // 缺 newSessionId
		{"/api/git/worktree/resume-session", `{}`},                                      // 缺 sourceCwd
		{"/api/git/worktree/show", `{}`},                                                // 缺 idOrPath
		{"/api/hunk-tracker/hunk-action", `{"action":"accept"}`},                        // 缺 hunkId
		{"/api/hunk-tracker/file-action", `{"path":"a.go"}`},                            // 缺 action
		{"/api/hunk-tracker/turn-action", `{"action":"accept"}`},                        // 缺 promptIndex
		{"/api/hunk-tracker/all-action", `{}`},                                          // 缺 action
		{"/api/plugins/notify-updates", `{}`},                                           // 缺 updates
		{"/api/subagent/get", `{}`},                                                     // 缺 subagentId
		{"/api/terminal/create", `{}`},                                                  // 缺 command
		{"/api/terminal/kill", `{}`},                                                    // 缺 terminalId
		{"/api/terminal/output", `{}`},                                                  // 缺 terminalId
		{"/api/terminal/wait-for-exit", `{}`},                                           // 缺 terminalId
		{"/api/terminal/release", `{}`},                                                 // 缺 terminalId
		{"/api/terminal/background", `{}`},                                              // 缺 terminalId
		{"/api/terminal/pty/load", `{}`},                                                // 缺 terminalId
		{"/api/terminal/pty/resize", `{"terminalId":"t-1"}`},                            // 缺 rows/cols
		{"/api/terminal/pty/input", `{"terminalId":"t-1"}`},                             // 缺 data
		{"/api/fs/write-file", `{}`},                                                    // 缺 path
		{"/api/fs/delete-file", `{}`},                                                   // 缺 path
		{"/api/search/fuzzy/change", `{}`},                                              // 缺 searchId/query
		{"/api/search/fuzzy/close", `{}`},                                               // 缺 searchId
		{"/api/bundle/entry-get", `{}`},                                                 // 缺 kind/name
		{"/api/code/goto-definition", `{"path":"a.go"}`},                                // 缺 row/column
		{"/api/code/goto-references", `{"path":"a.go","row":1}`},                        // 缺 column
		{"/api/code/find-definitions", `{}`},                                            // 缺 symbol
		{"/api/code/find-references", `{}`},                                             // 缺 symbol
		{"/api/review/comment", `{}`},                                                   // 缺 promptIndex/citation
		{"/api/review/comment-delete", `{}`},                                            // 缺 commentId
		{"/api/privacy/set-coding-data-retention", `{}`},                                // 缺 codingDataRetentionOptOut
		{"/api/rollout/survey", `{}`},                                                   // 缺 preferences
	}
	for _, c := range cases {
		rec := postJSON(t, s, c.path, c.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (body=%s)", c.path, rec.Code, rec.Body.String())
		}
	}
}

// ── agent 侧失败降级：200 {ok:false, error} ─────────────────────────

func TestExtErrorDegradation(t *testing.T) {
	t.Setenv(ACPHostFakeAgentErrorMethod, "_x.ai/git/worktree/create")
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/git/worktree/create", `{"sourcePath":"/ws"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if ok, _ := m["ok"].(bool); ok {
		t.Fatalf("body = %s, want ok:false on agent error", rec.Body.String())
	}
	if m["error"] == nil {
		t.Fatalf("body = %s, want error message", rec.Body.String())
	}
}

// ── wire 键断言（录制 host→agent 请求逐键核对）──────────────────────

// recordedParams posts the body and returns the recorded request's params
// for the given wire method (fail on missing).
func recordedParams(t *testing.T, s *Server, recordPath, path, body, method string) map[string]any {
	t.Helper()
	rec := postJSON(t, s, path, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status = %d, body=%s", path, rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), method)
	params, _ := req["params"].(map[string]any)
	return params
}

func TestExtWireKeys(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	// 会话。
	params := recordedParams(t, s, recordPath, "/api/session/state", `{"cwd":"/ws"}`, "_x.ai/session/state")
	want := map[string]any{"sessionId": "sess-new", "cwd": "/ws"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/state params = %v, want %v", params, want)
	}

	// session/fork：targetPromptIndex 透传（agent 按轮截断，0-based 含端点）；
	// 缺省时不出现该键 = 完整副本。
	params = recordedParams(t, s, recordPath, "/api/session-fork", `{"sessionId":"sess-new","targetPromptIndex":3}`, "_x.ai/session/fork")
	want = map[string]any{
		"sourceSessionId": "sess-new", "sourceCwd": "/ws", "newCwd": "/ws",
		"sessionKind": "fork", "targetPromptIndex": float64(3),
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/fork params = %v, want %v", params, want)
	}

	// 不带 targetPromptIndex：完整副本（该键不出现）。同一方法录制了两条
	// 而 findRequest 取第一条，这里手工取最后一条。
	recFork := postJSON(t, s, "/api/session-fork", `{"sessionId":"sess-new"}`)
	if recFork.Code != http.StatusOK {
		t.Fatalf("session-fork status = %d, body=%s", recFork.Code, recFork.Body.String())
	}
	var lastFork map[string]any
	for _, m := range readRecordedRequests(t, recordPath) {
		if m["method"] == "_x.ai/session/fork" {
			lastFork = m
		}
	}
	if lastFork == nil {
		t.Fatalf("no recorded _x.ai/session/fork request")
	}
	params, _ = lastFork["params"].(map[string]any)
	want = map[string]any{
		"sourceSessionId": "sess-new", "sourceCwd": "/ws", "newCwd": "/ws",
		"sessionKind": "fork",
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/fork (full copy) params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-import", `{"cwd":"/ws","state":{"summary":{"title":"t"}},"updates":[{"a":1}]}`, "_x.ai/session/import")
	want = map[string]any{
		"sessionId": "sess-new", "cwd": "/ws",
		"state":   map[string]any{"summary": map[string]any{"title": "t"}},
		"updates": []any{map[string]any{"a": float64(1)}},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/import params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-repair", `{"dryRun":true}`, "_x.ai/session/repair")
	want = map[string]any{"sessionId": "sess-new", "dryRun": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/repair params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-update-mcp-servers", `{"mcpServers":[{"url":"stdio://fs"}]}`, "_x.ai/session/update_mcp_servers")
	want = map[string]any{"sessionId": "sess-new", "mcpServers": []any{map[string]any{"url": "stdio://fs"}}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/update_mcp_servers params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-add-local-workspace", `{"meta":{"kind":"chat"}}`, "_x.ai/session/add_local_workspace")
	want = map[string]any{"sessionId": "sess-new", "meta": map[string]any{"kind": "chat"}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/add_local_workspace params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-resolve-worktree-resume", `{"cwd":"/ws"}`, "_x.ai/session/resolve_local_for_worktree_resume")
	want = map[string]any{"sessionId": "sess-new", "cwd": "/ws"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/resolve_local_for_worktree_resume params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-rehydrate", `{"sourceCwd":"/ws","repoRoot":"/repo","worktreePath":"/wt"}`, "_x.ai/session/rehydrate")
	want = map[string]any{"sessionId": "sess-new", "sourceCwd": "/ws", "repoRoot": "/repo", "worktreePath": "/wt"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/rehydrate params = %v, want %v", params, want)
	}

	// load_history：camelCase beforeId；空则省略（第一页无游标），不传 sessionId。
	params = recordedParams(t, s, recordPath, "/api/session-load-history", `{"beforeId":"c-9"}`, "_x.ai/session/load_history")
	want = map[string]any{"beforeId": "c-9"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/load_history params = %v, want %v", params, want)
	}
	// 第二条（无游标）：findRequest 只取第一条，这里取最后一条录制。
	params = recordedParams(t, s, recordPath, "/api/session-load-history", `{}`, "_x.ai/session/load_history")
	var last map[string]any
	for _, m := range readRecordedRequests(t, recordPath) {
		if m["method"] == "_x.ai/session/load_history" {
			last = m
		}
	}
	if last == nil {
		t.Fatal("no recorded _x.ai/session/load_history request")
	}
	params, _ = last["params"].(map[string]any)
	want = map[string]any{}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/load_history no-cursor params = %v, want %v", params, want)
	}

	// 会话摘要：SNAKE_CASE。
	params = recordedParams(t, s, recordPath, "/api/session-summaries/session-list", `{"workspaceDirectory":"/ws"}`, "_x.ai/session_summaries/session_list")
	want = map[string]any{"workspace_directory": "/ws"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session_summaries/session_list params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/session-summaries/workspace-list-recent", `{"limit":50}`, "_x.ai/session_summaries/workspace_list_recent")
	want = map[string]any{"limit": float64(50)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session_summaries/workspace_list_recent params = %v, want %v", params, want)
	}

	// 云端：SNAKE_CASE。
	params = recordedParams(t, s, recordPath, "/api/cloud/terminate", `{"sandboxId":"sb-1"}`, "_x.ai/cloud/terminate")
	want = map[string]any{"sandbox_id": "sb-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("cloud/terminate params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/cloud/env/create", `{"name":"dev","defaultBranch":"main"}`, "_x.ai/cloud/env/create")
	want = map[string]any{"name": "dev", "default_branch": "main"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("cloud/env/create params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/cloud/env/update", `{"environmentId":"env-1","setupScript":"s"}`, "_x.ai/cloud/env/update")
	want = map[string]any{"environment_id": "env-1", "setup_script": "s"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("cloud/env/update params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/cloud/env/delete", `{"environmentId":"env-1"}`, "_x.ai/cloud/env/delete")
	want = map[string]any{"environment_id": "env-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("cloud/env/delete params = %v, want %v", params, want)
	}

	// 认证。
	params = recordedParams(t, s, recordPath, "/api/api-key-set", `{"key":"xai-1"}`, "_x.ai/setApiKey")
	want = map[string]any{"key": "xai-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("setApiKey params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/auth/cancel", `{"requestSeq":7}`, "_x.ai/auth/cancel")
	want = map[string]any{"request_seq": float64(7)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("auth/cancel params = %v, want %v", params, want)
	}

	// 隐私 / 灰度。
	params = recordedParams(t, s, recordPath, "/api/privacy/set-coding-data-retention", `{"codingDataRetentionOptOut":true}`, "_x.ai/privacy/setCodingDataRetention")
	want = map[string]any{"codingDataRetentionOptOut": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("privacy params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/rollout/survey", `{"preferences":["fast"],"feedback":"g"}`, "_x.ai/rollout/survey")
	want = map[string]any{"sessionId": "sess-new", "preferences": []any{"fast"}, "feedback": "g"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("rollout/survey params = %v, want %v", params, want)
	}

	// Git。
	params = recordedParams(t, s, recordPath, "/api/git/files", `{"cwd":"/ws","paths":["a.go"],"version":"HEAD"}`, "_x.ai/git/files")
	want = map[string]any{"gitRoot": "/ws", "paths": []any{"a.go"}, "version": "HEAD"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("git/files params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/stage-content", `{"cwd":"/ws","path":"a.go","content":"x"}`, "_x.ai/git/stage/content")
	want = map[string]any{"gitRoot": "/ws", "path": "a.go", "content": "x"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("git/stage/content params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/checkout-session-head", `{"cwd":"/ws","stashIfDirty":true}`, "_x.ai/git/checkout_session_head")
	want = map[string]any{"sessionId": "sess-new", "gitRoot": "/ws", "stashIfDirty": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("git/checkout_session_head params = %v, want %v", params, want)
	}

	// Worktree。
	params = recordedParams(t, s, recordPath, "/api/git/worktree/create", `{"sourcePath":"/ws","copyMode":"dirty","ignoredSkipPatterns":["*.log"]}`, "_x.ai/git/worktree/create")
	want = map[string]any{"sessionId": "sess-new", "sourcePath": "/ws", "copyMode": "dirty", "ignoredSkipPatterns": []any{"*.log"}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/create params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/remove", `{"idOrPath":"wt-1","force":true}`, "_x.ai/git/worktree/remove")
	want = map[string]any{"idOrPath": "wt-1", "force": true, "dryRun": false}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/remove params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/apply", `{"worktreePath":"/wt","mode":"merge"}`, "_x.ai/git/worktree/apply")
	want = map[string]any{"sessionId": "sess-new", "worktreePath": "/wt", "mode": "merge"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/apply params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/create-from-worktree", `{"sourceWorktreePath":"/wt","newSessionId":"s-2","label":"fork"}`, "_x.ai/git/worktree/create_from_worktree")
	want = map[string]any{"sourceWorktreePath": "/wt", "newSessionId": "s-2", "label": "fork"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/create_from_worktree params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/create-from-worktree-sync", `{"sourceWorktreePath":"/wt","newSessionId":"s-2"}`, "_x.ai/git/worktree/create_from_worktree_sync")
	want = map[string]any{"sourceWorktreePath": "/wt", "newSessionId": "s-2"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/create_from_worktree_sync params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/resume-session", `{"sourceCwd":"/ws","restoreCode":true,"gitRef":"main"}`, "_x.ai/git/worktree/resume_session")
	want = map[string]any{"sessionId": "sess-new", "sourceCwd": "/ws", "restoreCode": true, "gitRef": "main"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/resume_session params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/list", `{"repo":"/repo","type":["linked"],"includeAll":true}`, "_x.ai/git/worktree/list")
	want = map[string]any{"repo": "/repo", "type": []any{"linked"}, "includeAll": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/list params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/show", `{"idOrPath":"wt-1"}`, "_x.ai/git/worktree/show")
	want = map[string]any{"idOrPath": "wt-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/show params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/git/worktree/gc", `{"dryRun":true,"maxAge":"7d","force":true}`, "_x.ai/git/worktree/gc")
	want = map[string]any{"dryRun": true, "maxAge": "7d", "force": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("worktree/gc params = %v, want %v", params, want)
	}

	// Hunk tracker。
	params = recordedParams(t, s, recordPath, "/api/hunk-tracker/hunk-action", `{"hunkId":"h-1","action":"accept"}`, "_x.ai/hunk-tracker/hunk-action")
	want = map[string]any{"hunkId": "h-1", "action": "accept"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("hunk-tracker/hunk-action params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/hunk-tracker/turn-action", `{"promptIndex":2,"action":"accept"}`, "_x.ai/hunk-tracker/turn-action")
	want = map[string]any{"promptIndex": float64(2), "action": "accept"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("hunk-tracker/turn-action params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/hunk-tracker/all-action", `{"action":"reject"}`, "_x.ai/hunk-tracker/all-action")
	want = map[string]any{"action": "reject"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("hunk-tracker/all-action params = %v, want %v", params, want)
	}

	// 技能 / 插件 / 子代理。
	params = recordedParams(t, s, recordPath, "/api/skills/reset", `{"cwd":"/ws"}`, "_x.ai/skills/reset")
	want = map[string]any{"cwd": "/ws"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("skills/reset params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/skills/config", `{}`, "_x.ai/skills/config")
	want = map[string]any{}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("skills/config params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/plugins/notify-updates", `{"updates":[["p","1","2"]]}`, "_x.ai/plugins/notify-updates")
	want = map[string]any{"sessionId": "sess-new", "updates": []any{[]any{"p", "1", "2"}}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("plugins/notify-updates params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/subagent/get", `{"subagentId":"sub-1","block":true,"timeoutMs":5000}`, "_x.ai/subagent/get")
	want = map[string]any{"subagentId": "sub-1", "block": true, "timeoutMs": float64(5000)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("subagent/get params = %v, want %v", params, want)
	}

	// 终端。
	params = recordedParams(t, s, recordPath, "/api/terminal/create", `{"command":"ls","args":["-la"],"env":{"K":"v"},"cwd":"/ws","outputByteLimit":1000}`, "_x.ai/terminal/create")
	want = map[string]any{
		"sessionId": "sess-new", "command": "ls", "args": []any{"-la"},
		"env": []any{map[string]any{"name": "K", "value": "v"}},
		"cwd": "/ws", "outputByteLimit": float64(1000),
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("terminal/create params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/terminal/output", `{"terminalId":"t-1"}`, "_x.ai/terminal/output")
	want = map[string]any{"sessionId": "sess-new", "terminalId": "t-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("terminal/output params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/terminal/kill", `{"terminalId":"t-1"}`, "_x.ai/terminal/kill")
	want = map[string]any{"terminalId": "t-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("terminal/kill params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/terminal/pty/create", `{"shell":"/bin/zsh","rows":24,"cols":80,"meta":{"x":1}}`, "_x.ai/terminal/pty/create")
	want = map[string]any{"shell": "/bin/zsh", "rows": float64(24), "cols": float64(80), "_meta": map[string]any{"x": float64(1)}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("terminal/pty/create params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/terminal/pty/load", `{"terminalId":"t-1","meta":{"r":1}}`, "_x.ai/terminal/pty/load")
	want = map[string]any{"terminalId": "t-1", "_meta": map[string]any{"r": float64(1)}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("terminal/pty/load params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/terminal/pty/resize", `{"terminalId":"t-1","rows":30,"cols":100}`, "_x.ai/terminal/pty/resize")
	want = map[string]any{"terminalId": "t-1", "rows": float64(30), "cols": float64(100)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("terminal/pty/resize params = %v, want %v", params, want)
	}

	// FS / 搜索 / bundle。
	params = recordedParams(t, s, recordPath, "/api/fs/write-file", `{"path":"/ws/a.txt","content":"hi","createDirs":true}`, "_x.ai/fs/write_file")
	want = map[string]any{"path": "/ws/a.txt", "content": "hi", "createDirs": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("fs/write_file params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/search/fuzzy/open", `{"cwd":"/ws","root":"src","hidden":true,"meta":{"routing":1}}`, "_x.ai/search/fuzzy/open")
	want = map[string]any{"cwd": "/ws", "root": "src", "hidden": true, "_meta": map[string]any{"routing": float64(1)}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("search/fuzzy/open params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/search/fuzzy/change", `{"searchId":"sr-1","query":"foo","dirsOnly":true,"limit":10}`, "_x.ai/search/fuzzy/change")
	want = map[string]any{"searchId": "sr-1", "query": "foo", "dirsOnly": true, "limit": float64(10)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("search/fuzzy/change params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/bundle/sync", `{"force":true}`, "_x.ai/bundle/sync")
	want = map[string]any{"force": true}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("bundle/sync params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/bundle/entry-get", `{"kind":"persona","name":"default"}`, "_x.ai/bundle/entry/get")
	want = map[string]any{"kind": "persona", "name": "default"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("bundle/entry/get params = %v, want %v", params, want)
	}

	// 代码导航（sessionId 填活动会话）。
	params = recordedParams(t, s, recordPath, "/api/code/goto-definition", `{"cwd":"/ws","path":"a.go","row":10,"column":3}`, "_x.ai/code/goto-definition")
	want = map[string]any{"sessionId": "sess-new", "cwd": "/ws", "path": "a.go", "row": float64(10), "column": float64(3)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("code/goto-definition params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/code/find-references", `{"symbol":"Foo","contextPath":"a.go"}`, "_x.ai/code/find-references")
	want = map[string]any{"sessionId": "sess-new", "symbol": "Foo", "contextPath": "a.go"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("code/find-references params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/code/status", `{"cwd":"/ws"}`, "_x.ai/code/status")
	want = map[string]any{"sessionId": "sess-new", "cwd": "/ws"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("code/status params = %v, want %v", params, want)
	}

	// 评审 / 调试。
	params = recordedParams(t, s, recordPath, "/api/review/comment", `{"promptIndex":1,"comment":"nit","citation":{"path":"a.go","startLine":1,"endLine":2,"text":"x","side":"left"}}`, "_x.ai/review/comment")
	want = map[string]any{
		"sessionId": "sess-new", "promptIndex": float64(1), "comment": "nit",
		"citation": map[string]any{"path": "a.go", "startLine": float64(1), "endLine": float64(2), "text": "x", "side": "left"},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("review/comment params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/review/comment-delete", `{"commentId":"c-1"}`, "_x.ai/review/comment/delete")
	want = map[string]any{"sessionId": "sess-new", "commentId": "c-1"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("review/comment/delete params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/debug/trigger-feedback", `{"tier":"tier2","mode":"stars"}`, "_x.ai/debug/trigger_feedback")
	want = map[string]any{"sessionId": "sess-new", "tier": "tier2", "mode": "stars"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("debug/trigger_feedback params = %v, want %v", params, want)
	}

	params = recordedParams(t, s, recordPath, "/api/debug/arm-auto-compact", `{}`, "_x.ai/debug/arm_auto_compact")
	want = map[string]any{"sessionId": "sess-new"}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("debug/arm_auto_compact params = %v, want %v", params, want)
	}
}

// readRecordedLines polls the fake agent's record file and returns every
// recorded JSON-RPC line (notifications or requests).
func readRecordedLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(data)) != "" {
			var out []map[string]any
			for _, ln := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				var m map[string]any
				if json.Unmarshal([]byte(ln), &m) == nil {
					out = append(out, m)
				}
			}
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fake agent never recorded a line in %s", path)
	return nil
}
