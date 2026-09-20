package server

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ── POST /api/billing ───────────────────────────────────────────────

func TestBillingEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/billing", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	// The agent's ExtMethodResult envelope ({result:{plan, credits}}) is
	// passed through verbatim under "result".
	res, _ := m["result"].(map[string]any)
	inner, _ := res["result"].(map[string]any)
	if inner["plan"] != "pro" || inner["credits"] != "42.50" {
		t.Fatalf("result = %v, want {result:{plan:pro credits:42.50}}", m["result"])
	}

	// No sessionId and no active session → 404 (task/list convention).
	rec2 := postJSON(t, s, "/api/billing", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
	if m := decodeBody(t, rec2); !strings.Contains(m["error"].(string), "暂无活动会话") {
		t.Fatalf("resp = %s, want 暂无活动会话", rec2.Body.String())
	}
}

// ── POST /api/memory-flush ──────────────────────────────────────────

func TestMemoryFlushEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/memory-flush", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/memory-flush", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/memory-rewrite ────────────────────────────────────────

func TestMemoryRewriteEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/memory-rewrite", `{"sessionId":"sess-1","rawText":"new memory","contextSummary":"ctx"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// rawText is required by the agent contract — missing → 400.
	recMissing := postJSON(t, s, "/api/memory-rewrite", `{"sessionId":"sess-1"}`)
	if recMissing.Code != http.StatusBadRequest {
		t.Fatalf("missing-rawText status = %d, body=%s", recMissing.Code, recMissing.Body.String())
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/memory-rewrite", `{"rawText":"x"}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/memory-list ───────────────────────────────────────────

func TestMemoryListEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/memory-list", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	// The listing reaches the caller unmangled (snake_case keys, verbatim).
	res, _ := m["result"].(map[string]any)
	if res["enabled"] != true || res["capture_enabled"] != true || res["dream_enabled"] != true {
		t.Fatalf("result = %v, want the MemoryListing passthrough", m["result"])
	}
	files, _ := res["files"].([]any)
	if len(files) != 5 {
		t.Fatalf("files = %v, want the canned fixture's five entries", res["files"])
	}
	// `title` (new in the shell) and `generated` survive the passthrough — the
	// modal labels and gates deletion on both.
	byPath := map[string]map[string]any{}
	for _, f := range files {
		e, _ := f.(map[string]any)
		p, _ := e["path"].(string)
		byPath[p] = e
	}
	topic := byPath["/ws/.grok/memory/topics/deploy-conventions.md"]
	if topic == nil || topic["title"] != "部署约定" || topic["generated"] != false {
		t.Fatalf("topic entry = %v, want the title/generated passthrough", topic)
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/memory-list", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/memory-toggle ─────────────────────────────────────────

func TestMemoryToggleEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/memory-toggle", `{"sessionId":"sess-1","enabled":false}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	// The fake echoes the requested state back, so this also proves the
	// host forwarded `enabled:false` rather than defaulting it.
	res, _ := m["result"].(map[string]any)
	if res["enabled"] != false {
		t.Fatalf("result = %v, want enabled:false forwarded", m["result"])
	}

	// enabled is required by the agent contract — a body without it must not
	// be read as "off".
	recMissing := postJSON(t, s, "/api/memory-toggle", `{"sessionId":"sess-1"}`)
	if recMissing.Code != http.StatusBadRequest {
		t.Fatalf("missing-enabled status = %d, body=%s", recMissing.Code, recMissing.Body.String())
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/memory-toggle", `{"enabled":true}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/memory-dream ──────────────────────────────────────────

func TestMemoryDreamEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/memory-dream", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	res, _ := m["result"].(map[string]any)
	if res["disposition"] != "completed" {
		t.Fatalf("result = %v, want the dream disposition passthrough", m["result"])
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/memory-dream", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/memory-forget ─────────────────────────────────────────

func TestMemoryForgetEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	// The store accepts only the digest of the bytes the client previewed: the
	// fake agent's canned accept-digest stands in for that check.
	body := `{"sessionId":"sess-1","path":"/ws/.grok/memory/topics/deploy-conventions.md","expectedContentHash":"` + fakeMemoryForgetHash + `"}`
	rec := postJSON(t, s, "/api/memory-forget", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	res, _ := m["result"].(map[string]any)
	if res["outcome"] != "forgotten" {
		t.Fatalf("result = %v, want outcome:forgotten", m["result"])
	}

	// A stale digest is reported as the store's own rejection, not swallowed.
	stale := `{"sessionId":"sess-1","path":"/ws/.grok/memory/topics/deploy-conventions.md","expectedContentHash":"` + strings.Repeat("0", 64) + `"}`
	recStale := postJSON(t, s, "/api/memory-forget", stale)
	if recStale.Code != http.StatusOK {
		t.Fatalf("stale status = %d, body=%s", recStale.Code, recStale.Body.String())
	}
	if res := decodeBody(t, recStale)["result"].(map[string]any); res["outcome"] != "rejected" || res["reason"] != "changed" {
		t.Fatalf("stale result = %v, want the changed rejection", res)
	}

	// path is required, and the digest must be a BLAKE3 hex — a body without
	// either never reaches the agent.
	for name, bad := range map[string]string{
		"missing path": `{"sessionId":"sess-1","expectedContentHash":"` + fakeMemoryForgetHash + `"}`,
		"missing hash": `{"sessionId":"sess-1","path":"/ws/note.md"}`,
		"short hash":   `{"sessionId":"sess-1","path":"/ws/note.md","expectedContentHash":"abc123"}`,
		"non-hex hash": `{"sessionId":"sess-1","path":"/ws/note.md","expectedContentHash":"` + strings.Repeat("z", 64) + `"}`,
	} {
		if rec := postJSON(t, s, "/api/memory-forget", bad); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, body=%s", name, rec.Code, rec.Body.String())
		}
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/memory-forget", `{"path":"/ws/note.md","expectedContentHash":"`+fakeMemoryForgetHash+`"}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/toggle-plan-mode ──────────────────────────────────────

func TestTogglePlanModeEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/toggle-plan-mode", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	// The method is sent as a fire-and-forget notification — no planMode
	// reply exists; the frontend applies its local desired state.
	if m["planMode"] != nil {
		t.Fatalf("resp = %s, want no planMode (notification, no reply)", rec.Body.String())
	}

	var gotModesUpdate bool
	deadline := time.After(2 * time.Second)
	for !gotModesUpdate {
		select {
		case ev := <-ch:
			if ev["type"] == "modes_update" {
				gotModesUpdate = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for modes_update after toggle-plan-mode")
		}
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/toggle-plan-mode", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/permissions-reset ─────────────────────────────────────

func TestPermissionsResetEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/permissions-reset", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	var gotReset bool
	deadline := time.After(2 * time.Second)
	for !gotReset {
		select {
		case ev := <-ch:
			if ev["type"] == "permissions_reset" {
				gotReset = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for permissions_reset broadcast")
		}
	}

	// No sessionId and no active session → 404.
	rec2 := postJSON(t, s, "/api/permissions-reset", `{}`)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("no-session status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/set-mode: permission-mode notification dispatch ───────

// readRecordedNotifs polls the fake agent's notification record file until
// it holds at least n lines, then returns them as parsed maps.
func readRecordedNotifs(t *testing.T, path string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f, err := os.Open(path)
		if err == nil {
			var lines []map[string]any
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				var m map[string]any
				if json.Unmarshal(sc.Bytes(), &m) == nil {
					lines = append(lines, m)
				}
			}
			f.Close()
			if len(lines) >= n {
				return lines
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fake agent never recorded %d notifications in %s", n, path)
	return nil
}

// Permission modes MUST go to the agent as the _x.ai/yolo_mode_changed
// notification (the agent's ask/auto/always-approve channel) — session/
// set_mode would silently no-op on these ids. Pins the wire payload per
// mode, exactly as the TUI's persist_permission_mode_and_notify sends it.
func TestSetModePermissionModeNotification(t *testing.T) {
	notifPath := filepath.Join(t.TempDir(), "notifs.jsonl")
	t.Setenv(ACPHostFakeAgentRecordNotifs, notifPath)
	s, _ := newFakeAgentServer(t)
	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	cases := []struct {
		modeID     string
		yolo       bool
		auto       bool
		permission string
	}{
		{modeID: "normal", yolo: false, auto: false, permission: "ask"},
		{modeID: "auto", yolo: false, auto: true, permission: "auto"},
		{modeID: "yolo", yolo: true, auto: false, permission: "always-approve"},
	}
	for _, tc := range cases {
		rec := postJSON(t, s, "/api/set-mode",
			`{"modeId":`+strconv.Quote(tc.modeID)+`}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("set-mode %s status = %d, body=%s", tc.modeID, rec.Code, rec.Body.String())
		}
		if m := decodeBody(t, rec); m["ok"] != true {
			t.Fatalf("set-mode %s resp = %s, want ok:true", tc.modeID, rec.Body.String())
		}
	}

	notifs := readRecordedNotifs(t, notifPath, len(cases))
	for i, tc := range cases {
		m := notifs[i]
		if m["method"] != "_x.ai/yolo_mode_changed" {
			t.Fatalf("notif %d method = %v, want _x.ai/yolo_mode_changed", i, m["method"])
		}
		if m["id"] != nil {
			t.Fatalf("notif %d has id %v — must be a fire-and-forget notification", i, m["id"])
		}
		params, _ := m["params"].(map[string]any)
		if params["yolo_mode"] != tc.yolo || params["auto_mode"] != tc.auto ||
			params["permission_mode"] != tc.permission {
			t.Fatalf("notif %d (%s) params = %v, want {yolo_mode:%v auto_mode:%v permission_mode:%q}",
				i, tc.modeID, params, tc.yolo, tc.auto, tc.permission)
		}
	}

	// 验证广播事件给已连接的 FE 订阅者：每一个模式切换都推了 yolo_mode_changed 广播
	for i, tc := range cases {
		for {
			select {
			case ev := <-ch:
				if ev["type"] != "yolo_mode_changed" {
					continue
				}
				p, _ := ev["params"].(map[string]any)
				if p["yolo_mode"] != tc.yolo || p["auto_mode"] != tc.auto || p["permission_mode"] != tc.permission {
					t.Fatalf("broadcast %d params = %v, want {yolo_mode:%v auto_mode:%v permission_mode:%q}",
						i, p, tc.yolo, tc.auto, tc.permission)
				}
				goto nextCase
			case <-time.After(2 * time.Second):
				t.Fatalf("broadcast %d (%s) timeout waiting for yolo_mode_changed event", i, tc.modeID)
			}
		}
	nextCase:
	}
}

// Session-mode ids (plan) keep the legacy session/set_mode request path —
// the fake agent answers it, so the endpoint still returns the agent result.
func TestSetModePlanStillSessionSetMode(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	sid := createActiveSession(t, s)
	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/set-mode", `{"modeId":"plan","sessionId":"`+sid+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	select {
	case ev := <-ch:
		if ev["type"] != "modes_update" {
			t.Fatalf("type = %v, want modes_update", ev["type"])
		}
		modes, _ := ev["modes"].(map[string]any)
		if modes["currentModeId"] != "plan" {
			t.Fatalf("currentModeId = %v, want plan", modes["currentModeId"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for modes_update broadcast")
	}
	// The fake agent answers unknown methods with {} — a real agent answers
	// session/set_mode with its response, and the host passes it through.
	if _, hasResult := m["result"]; !hasResult {
		t.Fatalf("resp = %s, want a result passthrough for session/set_mode", rec.Body.String())
	}
}

// ── GET /api/mcp/list ───────────────────────────────────────────────

func TestMCPListEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	req := httptest.NewRequest("GET", "/api/mcp/list", nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	servers, ok := m["servers"].([]any)
	if !ok || len(servers) != 2 {
		t.Fatalf("servers = %v, want 2 entries", m["servers"])
	}
	// Object entries pass through; the bare-string entry is normalized
	// to {name: …}.
	first := servers[0].(map[string]any)
	if first["name"] != "fs" || first["enabled"] != true {
		t.Errorf("servers[0] = %v, want {name:fs enabled:true}", first)
	}
	second := servers[1].(map[string]any)
	if second["name"] != "bare-name" || len(second) != 1 {
		t.Errorf("servers[1] = %v, want {name:bare-name} only", second)
	}
}

func TestNormalizeMCPServers(t *testing.T) {
	cases := []struct {
		name string
		res  map[string]any
		want []any
	}{
		{
			name: "top-level servers key",
			res:  map[string]any{"servers": []any{map[string]any{"name": "a"}}},
			want: []any{map[string]any{"name": "a"}},
		},
		{
			name: "ExtMethodResult envelope result.servers",
			res:  map[string]any{"result": map[string]any{"servers": []any{map[string]any{"name": "b"}}}},
			want: []any{map[string]any{"name": "b"}},
		},
		{
			name: "result is a bare array",
			res:  map[string]any{"result": []any{map[string]any{"name": "c"}}},
			want: []any{map[string]any{"name": "c"}},
		},
		{
			name: "bare string entries become name objects",
			res:  map[string]any{"servers": []any{"plain", map[string]any{"name": "d"}}},
			want: []any{map[string]any{"name": "plain"}, map[string]any{"name": "d"}},
		},
		{
			name: "nil entries dropped",
			res:  map[string]any{"servers": []any{nil, "x"}},
			want: []any{map[string]any{"name": "x"}},
		},
		{
			name: "nil input",
			res:  nil,
			want: nil,
		},
		{
			name: "no servers anywhere",
			res:  map[string]any{"result": map[string]any{"foo": "bar"}},
			want: []any{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeMCPServers(c.res)
			if len(got) != len(c.want) {
				t.Fatalf("got %d servers, want %d: %+v", len(got), len(c.want), got)
			}
			for i := range got {
				gm, gok := got[i].(map[string]any)
				wm, wok := c.want[i].(map[string]any)
				if !gok || !wok || gm["name"] != wm["name"] {
					t.Errorf("servers[%d] = %v, want %v", i, got[i], c.want[i])
				}
			}
		})
	}
}

// ── POST /api/mcp-toggle ────────────────────────────────────────────

func TestMCPToggleEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/mcp-toggle", `{"name":"fs","enabled":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// enabled:false is legitimate — must not 400.
	recFalse := postJSON(t, s, "/api/mcp-toggle", `{"name":"fs","enabled":false}`)
	if recFalse.Code != http.StatusOK {
		t.Fatalf("enabled:false status = %d, body=%s", recFalse.Code, recFalse.Body.String())
	}

	// Missing name → 400.
	rec2 := postJSON(t, s, "/api/mcp-toggle", `{"enabled":true}`)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing-name status = %d, body=%s", rec2.Code, rec2.Body.String())
	}

	// Missing enabled → 400 (pointer distinguishes absent from false).
	rec3 := postJSON(t, s, "/api/mcp-toggle", `{"name":"fs"}`)
	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("missing-enabled status = %d, body=%s", rec3.Code, rec3.Body.String())
	}
}

// ── POST /api/mcp-add ───────────────────────────────────────────────

func TestMCPAddEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/mcp-add",
		`{"server":{"name":"fs","command":"npx","args":["-y","@modelcontextprotocol/server-fs"],"env":{"KEY":"v"}}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// Missing server → 400.
	rec2 := postJSON(t, s, "/api/mcp-add", `{}`)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing-server status = %d, body=%s", rec2.Code, rec2.Body.String())
	}

	// Missing server.name → 400 (server object without a name is useless).
	rec3 := postJSON(t, s, "/api/mcp-add", `{"server":{"command":"npx"}}`)
	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("missing-name status = %d, body=%s", rec3.Code, rec3.Body.String())
	}
}

// ── POST /api/mcp-remove ────────────────────────────────────────────

func TestMCPRemoveEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/mcp-remove", `{"name":"fs"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}

	// Missing name → 400.
	rec2 := postJSON(t, s, "/api/mcp-remove", `{}`)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing-name status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── POST /api/mcp-auth-trigger ──────────────────────────────────────

func TestMCPAuthTriggerEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/mcp-auth-trigger", `{"name":"github"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	res, _ := m["result"].(map[string]any)
	if res["authUrl"] != "https://example.com/auth" {
		t.Fatalf("result = %v, want authUrl passthrough", m["result"])
	}

	// Missing name → 400.
	rec2 := postJSON(t, s, "/api/mcp-auth-trigger", `{}`)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing-name status = %d, body=%s", rec2.Code, rec2.Body.String())
	}
}

// ── graceful degradation: agent says the method is unsupported ──────

// When the agent answers with a JSON-RPC error (e.g. -32601 for an
// unimplemented extension), the endpoint must still return a JSON body —
// 200 + {ok:false, error:"「<method>」不受支持: …"} — so the frontend can
// render an error row instead of a hard failure.
func TestAgentMethodUnsupportedDegradesTo200(t *testing.T) {
	t.Setenv(ACPHostFakeAgentErrorMethod, "_x.ai/billing")
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/billing", `{"sessionId":"sess-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s — unsupported must degrade to 200", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != false {
		t.Fatalf("resp = %s, want ok:false", rec.Body.String())
	}
	msg, _ := m["error"].(string)
	if !strings.Contains(msg, "不受支持") || !strings.Contains(msg, "_x.ai/billing") {
		t.Fatalf("error = %q, want 「_x.ai/billing」不受支持: …", msg)
	}
}
