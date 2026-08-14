package server

import (
	"path/filepath"
	"reflect"
	"testing"
)

// align_full_test.go — A 部分 wire 对齐（server 层）：initialize/authenticate
// `_meta` 种子、session/cancel `_meta`、session/list + session/resume +
// session/updates + suggest + workspaces/list 可选字段。全部经 fake agent
// 录制 host→agent 请求逐键核对。

// ── A1: initialize `_meta` / clientCapabilities.meta 上 wire ────────

func TestInitializeCarriesMetaAndCaps(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	t.Setenv("ACP_INIT_CLIENT_IDENTIFIER", "capri-fe")
	t.Setenv("ACP_INIT_SYSTEM_PROMPT_OVERRIDE", "BE HELPFUL")
	t.Setenv("ACP_INIT_MCP_APPS", "1")
	t.Setenv("ACP_CAP_CODE_NAVIGATION", "true")
	t.Setenv("ACP_CAP_FS_NOTIFY", "true")
	s, _ := newFakeAgentServer(t)

	// 触发 boot（/api/sessions 会 ensureBooted）。
	rec := postJSON(t, s, "/api/sessions", `{}`)
	if rec.Code != 200 {
		t.Fatalf("/api/sessions status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req := findRequest(t, readRecordedRequests(t, recordPath), "initialize")
	params, _ := req["params"].(map[string]any)

	// `_meta`：clientType/clientVersion 恒在；env 种子在。
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("initialize params carry no _meta: %v", params)
	}
	if meta["clientType"] != "grok-pager" || meta["clientVersion"] != "0.2.0" {
		t.Errorf("_meta clientType/clientVersion = %v/%v, want grok-pager/0.2.0",
			meta["clientType"], meta["clientVersion"])
	}
	if meta["clientIdentifier"] != "capri-fe" {
		t.Errorf("_meta clientIdentifier = %v, want capri-fe", meta["clientIdentifier"])
	}
	if meta["systemPromptOverride"] != "BE HELPFUL" {
		t.Errorf("_meta systemPromptOverride = %v, want BE HELPFUL", meta["systemPromptOverride"])
	}
	if meta["mcpApps"] != true {
		t.Errorf("_meta mcpApps = %v, want true", meta["mcpApps"])
	}

	// clientCapabilities.meta：既有 4 键 + env-opt-in 键。
	caps, _ := params["clientCapabilities"].(map[string]any)
	cmeta, ok := caps["meta"].(map[string]any)
	if !ok {
		t.Fatalf("clientCapabilities.meta missing: %v", caps)
	}
	for _, k := range []string{
		"x.ai/incrementalBashOutput", "x.ai/bashOutputNoColor",
		"x.ai/gitHeadChanged", "x.ai/hunkTracker",
		"x.ai/codeNavigation", "x.ai/fs_notify",
	} {
		if _, has := cmeta[k]; !has {
			t.Errorf("clientCapabilities.meta missing %s: %v", k, cmeta)
		}
	}
	if v, ok := cmeta["x.ai/codeNavigation"].(map[string]any); !ok || v["enabled"] != true {
		t.Errorf("x.ai/codeNavigation = %v, want {enabled:true}", cmeta["x.ai/codeNavigation"])
	}
	if v, ok := cmeta["x.ai/fs_notify"].(bool); !ok || !v {
		t.Errorf("x.ai/fs_notify = %v, want true", cmeta["x.ai/fs_notify"])
	}
}

func TestInitializeOmitsEnvSeedsWhenAbsent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	for _, env := range []string{
		"ACP_INIT_CLIENT_IDENTIFIER", "ACP_INIT_CLIENT_SOURCE",
		"ACP_INIT_SYSTEM_PROMPT_OVERRIDE", "ACP_INIT_RULES",
		"ACP_INIT_MCP_APPS", "ACP_INIT_BUFFERING_SETTINGS", "ACP_INIT_STARTUP_HINTS",
		"ACP_CAP_CODE_NAVIGATION", "ACP_CAP_FOLDER_TRUST_INTERACTIVE", "ACP_CAP_FS_NOTIFY",
	} {
		t.Setenv(env, "")
	}
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/sessions", `{}`)
	if rec.Code != 200 {
		t.Fatalf("/api/sessions status = %d", rec.Code)
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "initialize")
	params, _ := req["params"].(map[string]any)
	meta, _ := params["_meta"].(map[string]any)
	want := map[string]any{"clientType": "grok-pager", "clientVersion": "0.2.0"}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("_meta = %v, want %v (env seeds omitted when absent)", meta, want)
	}
	caps, _ := params["clientCapabilities"].(map[string]any)
	cmeta, _ := caps["meta"].(map[string]any)
	for _, k := range []string{"x.ai/codeNavigation", "x.ai/folderTrust", "x.ai/fs_notify"} {
		if _, has := cmeta[k]; has {
			t.Errorf("clientCapabilities.meta must omit %s by default: %v", k, cmeta)
		}
	}
}

// ── A2: authenticate `_meta` env 种子 ────────────────────────────────

func TestAuthenticateCarriesEnvMeta(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	t.Setenv("ACP_AUTH_REAUTH", "1")
	t.Setenv("ACP_AUTH_USE_OAUTH", "true")
	t.Setenv("ACP_AUTH_FORCE_INTERACTIVE", "1")
	t.Setenv("ACP_AUTH_REQUEST_SEQ", "42")
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/sessions", `{}`)
	if rec.Code != 200 {
		t.Fatalf("/api/sessions status = %d", rec.Code)
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "authenticate")
	params, _ := req["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("authenticate params carry no _meta: %v", params)
	}
	want := map[string]any{
		"headless":          true,
		"reauth":            true,
		"use_oauth":         true,
		"force_interactive": true,
		"request_seq":       float64(42),
	}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("authenticate _meta = %v, want %v", meta, want)
	}
}

// ── A3: POST /api/cancel `_meta` 直通（session/cancel 为无 id 通知）──

func TestCancelForwardsMeta(t *testing.T) {
	notifPath := filepath.Join(t.TempDir(), "notifs.jsonl")
	t.Setenv(ACPHostFakeAgentRecordNotifs, notifPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/cancel", `{
		"cancelTrigger": "ctrl_c",
		"cancelSubagents": false,
		"rewindIfNoOutput": true,
		"rewindIfPristine": true
	}`)
	if rec.Code != 200 {
		t.Fatalf("/api/cancel status = %d, body=%s", rec.Code, rec.Body.String())
	}

	var found map[string]any
	for _, m := range readRecordedLines(t, notifPath) {
		if m["method"] == "session/cancel" {
			found = m
			break
		}
	}
	if found == nil {
		t.Fatal("no session/cancel notification recorded")
	}
	params, _ := found["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/cancel params carry no _meta: %v", params)
	}
	want := map[string]any{
		"cancelTrigger":    "ctrl_c",
		"cancelSubagents":  false,
		"rewindIfNoOutput": true,
		"rewindIfPristine": true,
	}
	if !reflect.DeepEqual(meta, want) {
		t.Errorf("session/cancel _meta = %v, want %v", meta, want)
	}
	if params["sessionId"] != "sess-new" {
		t.Errorf("sessionId = %v, want sess-new", params["sessionId"])
	}
}

// 不带可选字段 → session/cancel wire 保持无 `_meta`。
func TestCancelOmitsMetaWhenAbsent(t *testing.T) {
	notifPath := filepath.Join(t.TempDir(), "notifs.jsonl")
	t.Setenv(ACPHostFakeAgentRecordNotifs, notifPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/cancel", `{}`)
	if rec.Code != 200 {
		t.Fatalf("/api/cancel status = %d", rec.Code)
	}
	var found map[string]any
	for _, m := range readRecordedLines(t, notifPath) {
		if m["method"] == "session/cancel" {
			found = m
			break
		}
	}
	if found == nil {
		t.Fatal("no session/cancel notification recorded")
	}
	params, _ := found["params"].(map[string]any)
	if _, ok := params["_meta"]; ok {
		t.Errorf("bare cancel must not carry _meta: %v", params)
	}
}

// ── A5: POST /api/sessions 可选字段 ─────────────────────────────────

func TestSessionsForwardsOpts(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/sessions", `{"cwd":"/ws","cursor":"c1","meta":{"clientType":"pager"}}`)
	if rec.Code != 200 {
		t.Fatalf("/api/sessions status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "session/list")
	params, _ := req["params"].(map[string]any)
	want := map[string]any{"cwd": "/ws", "cursor": "c1", "_meta": map[string]any{"clientType": "pager"}}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/list params = %v, want %v", params, want)
	}
}

// ── A4: POST /api/session-resume `_meta` ─────────────────────────────

func TestSessionResumeForwardsMeta(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session-resume", `{"sessionId":"hist-1","cwd":"/ws","meta":{"yoloMode":true}}`)
	if rec.Code != 200 {
		t.Fatalf("/api/session-resume status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "session/resume")
	params, _ := req["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/resume params carry no _meta: %v", params)
	}
	if !reflect.DeepEqual(meta, map[string]any{"yoloMode": true}) {
		t.Errorf("_meta = %v, want {yoloMode:true}", meta)
	}
	if _, ok := params["additionalDirectories"].([]any); !ok {
		t.Errorf("additionalDirectories = %v, want []", params["additionalDirectories"])
	}
}

func TestSessionResumeOmitsMetaWhenAbsent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session-resume", `{"sessionId":"hist-1","cwd":"/ws"}`)
	if rec.Code != 200 {
		t.Fatalf("/api/session-resume status = %d", rec.Code)
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "session/resume")
	params, _ := req["params"].(map[string]any)
	if _, ok := params["_meta"]; ok {
		t.Errorf("session/resume must not carry _meta when absent: %v", params)
	}
	want := map[string]any{
		"sessionId":             "hist-1",
		"cwd":                   "/ws",
		"mcpServers":            []any{},
		"additionalDirectories": []any{},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}

// ── x.ai/session/updates 可选字段 ───────────────────────────────────

func TestSessionUpdatesExtrasWire(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session-updates",
		`{"sessionId":"s1","cwd":"/ws","offset":-100,"limit":5,"stream":true,"chunkSize":32,"turnIndex":3}`)
	if rec.Code != 200 {
		t.Fatalf("/api/session-updates status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "_x.ai/session/updates")
	params, _ := req["params"].(map[string]any)
	want := map[string]any{
		"sessionId": "s1", "cwd": "/ws",
		"offset": float64(-100), "limit": float64(5),
		"stream": true, "chunkSize": float64(32), "turnIndex": float64(3),
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("session/updates params = %v, want %v", params, want)
	}
}

func TestSessionUpdatesOmitsExtrasWhenAbsent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session-updates", `{"sessionId":"s1","cwd":"/ws","offset":10,"limit":2}`)
	if rec.Code != 200 {
		t.Fatalf("/api/session-updates status = %d", rec.Code)
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "_x.ai/session/updates")
	params, _ := req["params"].(map[string]any)
	want := map[string]any{"sessionId": "s1", "cwd": "/ws", "offset": float64(10), "limit": float64(2)}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v (no stream/chunkSize/turnIndex keys)", params, want)
	}
}

// ── /api/suggest 与 /api/workspaces/list 可选字段 ────────────────────

func TestSuggestForwardsExtras(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/suggest", `{
		"text":"foo","cwd":"/ws","cursor":3,"limit":10,"generation":7,
		"includeAi":true,"aiModel":"grok-4","tokenOnly":true
	}`)
	if rec.Code != 200 {
		t.Fatalf("/api/suggest status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "_x.ai/suggest")
	params, _ := req["params"].(map[string]any)
	want := map[string]any{
		"text": "foo", "cwd": "/ws",
		"cursor": float64(3), "limit": float64(10), "generation": float64(7),
		"includeAi": true, "aiModel": "grok-4", "tokenOnly": true,
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("suggest params = %v, want %v", params, want)
	}
}

func TestSuggestOmitsExtrasWhenAbsent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/suggest", `{"text":"foo"}`)
	if rec.Code != 200 {
		t.Fatalf("/api/suggest status = %d", rec.Code)
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "_x.ai/suggest")
	params, _ := req["params"].(map[string]any)
	if !reflect.DeepEqual(params, map[string]any{"text": "foo"}) {
		t.Errorf("params = %v, want {text:foo} (extras omitted)", params)
	}
}

func TestWorkspacesListForwardsExtras(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/workspaces/list", `{"pageSize":20,"pageToken":"t1","query":"q","kind":"cloud"}`)
	if rec.Code != 200 {
		t.Fatalf("/api/workspaces/list status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "_x.ai/workspaces/list")
	params, _ := req["params"].(map[string]any)
	want := map[string]any{
		"pageSize": float64(20), "pageToken": "t1", "query": "q", "kind": "cloud",
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("workspaces/list params = %v, want %v", params, want)
	}
}

func TestWorkspacesListOmitsExtrasWhenAbsent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/workspaces/list", `{}`)
	if rec.Code != 200 {
		t.Fatalf("/api/workspaces/list status = %d", rec.Code)
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "_x.ai/workspaces/list")
	params, _ := req["params"].(map[string]any)
	if !reflect.DeepEqual(params, map[string]any{}) {
		t.Errorf("params = %v, want {} (extras omitted)", params)
	}
}
