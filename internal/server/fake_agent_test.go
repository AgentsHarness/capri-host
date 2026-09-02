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

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// ACPHostFakeAgentEnv is the env var that switches the test binary into
// fake-agent mode. The bridge spawns cfg.Bin with args "agent stdio"; when
// Bin points at the test binary itself and this env var is set, TestMain
// serves minimal JSON-RPC responses on stdin/stdout instead of running
// tests. This gives handler tests a real transport round-trip (boot +
// request/response) without a real grok binary.
const ACPHostFakeAgentEnv = "ACP_HOST_FAKE_AGENT"

// ACPHostFakeAgentErrorMethod, when set, makes the fake agent answer that
// exact method with a JSON-RPC error (method_not_found) so tests can
// exercise the graceful-degradation path.
const ACPHostFakeAgentErrorMethod = "ACP_HOST_FAKE_AGENT_ERROR_METHOD"

// ACPHostFakeAgentEmitPermission, when set, makes the fake agent emit a
// session/request_permission request right after answering session/new, so
// permission-response endpoints round-trip fully.
const ACPHostFakeAgentEmitPermission = "ACP_HOST_FAKE_AGENT_EMIT_PERMISSION"

// ACPHostFakeAgentRecord, when set, makes the fake agent append every
// host→agent response that answers its emitted permission request to this
// file, so tests can assert the response meta the host forwarded.
const ACPHostFakeAgentRecord = "ACP_HOST_FAKE_AGENT_RECORD"

// ACPHostFakeAgentRecordNotifs, when set, makes the fake agent append every
// host→agent notification line (no JSON-RPC id) to this file, so tests can
// assert fire-and-forget notifications like _x.ai/yolo_mode_changed.
const ACPHostFakeAgentRecordNotifs = "ACP_HOST_FAKE_AGENT_RECORD_NOTIFS"

// ACPHostFakeAgentRecordRequests, when set, makes the fake agent append
// every host→agent REQUEST line (has a method) to this file, so tests can
// assert the exact params the host forwarded — e.g. the session/new and
// session/load `_meta` permission-mode seeds.
const ACPHostFakeAgentRecordRequests = "ACP_HOST_FAKE_AGENT_RECORD_REQUESTS"

// ACPHostFakeAgentPromptDelayMs, when set, makes the fake agent sleep that
// many milliseconds before answering session/prompt — tests use it to hold
// a turn in flight (Busy=true) long enough to observe host behavior
// mid-turn (e.g. the SSE hello busy flag).
const ACPHostFakeAgentPromptDelayMs = "ACP_HOST_FAKE_AGENT_PROMPT_DELAY_MS"

// ACPHostFakeAgentPromptMeta / SessionNewMeta / SessionListCursor /
// SessionListMeta / AuthMeta: JSON strings injected into the fake agent's
// canned responses (`_meta` / pagination cursor) so tests can assert the
// host's meta/cursor passthrough to the browser. Unset = the canned
// response carries no such keys (absent key ≠ off).
const (
	ACPHostFakeAgentPromptMeta      = "ACP_HOST_FAKE_AGENT_PROMPT_META"
	ACPHostFakeAgentSessionNewMeta  = "ACP_HOST_FAKE_AGENT_SESSION_NEW_META"
	ACPHostFakeAgentSessionListCur  = "ACP_HOST_FAKE_AGENT_SESSION_LIST_CURSOR"
	ACPHostFakeAgentSessionListMeta = "ACP_HOST_FAKE_AGENT_SESSION_LIST_META"
	ACPHostFakeAgentAuthMeta        = "ACP_HOST_FAKE_AGENT_AUTH_META"
	// initialize 响应的 `_meta`（如 agentInfo._meta.modelState 模型目录），
	// 便于无会话 boot 状态下让 FE 拿到模型列表做 UI 验证。
	ACPHostFakeAgentInitMeta = "ACP_HOST_FAKE_AGENT_INIT_META"
)

// fakeAgentMeta parses an env var as a JSON object for the canned `_meta`
// response; nil when unset or invalid.
func fakeAgentMeta(env string) map[string]any {
	v := os.Getenv(env)
	if v == "" {
		return nil
	}
	var m map[string]any
	if json.Unmarshal([]byte(v), &m) != nil {
		return nil
	}
	return m
}

// fakeAgentSessionRows 是 E2E 浏览器验证用的 canned 会话摘要行：指向临时
// HOME 里预置的 sess-new 会话（updates.jsonl 由验证流程手工写入），FE
// 侧边栏据此列出会话并可点击恢复。
func fakeAgentSessionRows() []any {
	return []any{map[string]any{
		"info": map[string]any{
			"id":  "sess-new",
			"cwd": "/tmp",
		},
		"session_summary": "E2E 五轮会话",
		"num_messages":    float64(15),
	}}
}

// fakeAgentPermissionID is the JSON-RPC id the fake agent stamps on its
// emitted permission request.
const fakeAgentPermissionID float64 = 99

func TestMain(m *testing.M) {
	if os.Getenv(ACPHostFakeAgentEnv) == "1" {
		runFakeAgent()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runFakeAgent reads JSON-RPC lines from stdin and answers each request
// with a canned result keyed by method name.
func runFakeAgent() {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	out := bufio.NewWriter(os.Stdout)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg map[string]any
		if json.Unmarshal(line, &msg) != nil {
			continue
		}
		id := msg["id"]
		if id == nil {
			// Notification (no id): record it when asked — the fake agent
			// never answers notifications.
			if notifs := os.Getenv(ACPHostFakeAgentRecordNotifs); notifs != "" {
				appendRecordLine(notifs, line)
			}
			continue
		}
		// Capture the host's permission response: a line without a method
		// that answers our emitted permission request. Record it and skip
		// the request switch (it is not a request to answer).
		if record := os.Getenv(ACPHostFakeAgentRecord); record != "" {
			if rid, ok := id.(float64); ok && rid == fakeAgentPermissionID && msg["method"] == nil {
				appendPermissionRecord(record, line)
				continue
			}
		}
		// Record every host→agent request (has a method) verbatim.
		if record := os.Getenv(ACPHostFakeAgentRecordRequests); record != "" && msg["method"] != nil {
			appendRecordLine(record, line)
		}
		method, _ := msg["method"].(string)
		if em := os.Getenv(ACPHostFakeAgentErrorMethod); em != "" && method == em {
			resp, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"error":   map[string]any{"code": -32601, "message": "method not found: " + method},
			})
			out.Write(resp)
			out.WriteByte('\n')
			out.Flush()
			continue
		}
		var result map[string]any
		switch method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": 1,
				"authMethods":     []any{map[string]any{"id": "cached_token"}},
			}
			if m := fakeAgentMeta(ACPHostFakeAgentInitMeta); m != nil {
				result["_meta"] = m
			}
		case "authenticate":
			result = map[string]any{}
			if m := fakeAgentMeta(ACPHostFakeAgentAuthMeta); m != nil {
				result["_meta"] = m
			}
		case "session/new":
			result = map[string]any{"sessionId": "sess-new"}
			if m := fakeAgentMeta(ACPHostFakeAgentSessionNewMeta); m != nil {
				result["_meta"] = m
			}
			if os.Getenv(ACPHostFakeAgentEmitPermission) == "1" {
				emitPermissionRequest(out)
			}
		case "session/load":
			// 恢复历史会话（浏览器端 continueSession / 重启回锚）。真实
			// agent 会重放整段会话，fake agent 只回模型快照——历史内容由
			// FE 经 /api/session-updates 自行拉取。
			result = map[string]any{"busy": false}
			if m := fakeAgentMeta(ACPHostFakeAgentInitMeta); m != nil {
				if ms, ok := m["modelState"]; ok {
					result["models"] = ms
				}
			}
		case "_x.ai/session_summaries/workspace_list_recent":
			// E2E 浏览器验证用：把预置在临时 HOME 里的会话暴露给 FE 侧边栏。
			result = map[string]any{"result": fakeAgentSessionRows()}
		case "_x.ai/session_summaries/workspace_list":
			result = map[string]any{"result": map[string]any{
				"all_sessions": map[string]any{"/tmp": fakeAgentSessionRows()},
			}}
		case "session/list":
			result = map[string]any{"sessions": []any{}}
			if v := os.Getenv(ACPHostFakeAgentSessionListCur); v != "" {
				result["nextCursor"] = v
			}
			if m := fakeAgentMeta(ACPHostFakeAgentSessionListMeta); m != nil {
				result["_meta"] = m
			}
		case "session/prompt":
			if d := os.Getenv(ACPHostFakeAgentPromptDelayMs); d != "" {
				if ms, err := strconv.Atoi(d); err == nil && ms > 0 {
					time.Sleep(time.Duration(ms) * time.Millisecond)
				}
			}
			result = map[string]any{"stopReason": "end_turn"}
			if m := fakeAgentMeta(ACPHostFakeAgentPromptMeta); m != nil {
				result["_meta"] = m
			}
		case "_x.ai/session/delete":
			result = map[string]any{"ok": true}
		case "_x.ai/compact_conversation":
			result = map[string]any{"ok": true}
		case "_x.ai/rewind/points":
			// Wrapped ExtMethodResult envelope with a mix of snake_case
			// and camelCase point fields to exercise normalization.
			result = map[string]any{"result": map[string]any{"points": []any{
				map[string]any{"index": float64(0), "timestamp": float64(1000), "summary": "hello"},
				map[string]any{"prompt_index": float64(1), "ts": float64(2000)},
			}}}
		case "_x.ai/rewind/execute":
			result = map[string]any{"ok": true}
		case "_x.ai/scheduler/delete":
			result = map[string]any{"ok": true}
		case "_x.ai/billing":
			// ExtMethodResult envelope with billing/quota payload.
			result = map[string]any{"result": map[string]any{"plan": "pro", "credits": "42.50"}}
		case "_x.ai/memory/flush":
			result = map[string]any{"ok": true}
		case "_x.ai/memory/rewrite":
			result = map[string]any{"ok": true}
		case "_x.ai/toggle_plan_mode":
			// planMode nested in the result envelope.
			result = map[string]any{"result": map[string]any{"planMode": true}}
		case "_x.ai/permissions/reset":
			result = map[string]any{"ok": true}
		case "_x.ai/internal/reload_models":
			// Mirrors the agent's ExtMethodResult envelope
			// ({"models": count}); the host only checks success.
			result = map[string]any{"result": map[string]any{"models": 3}}
		case "_x.ai/mcp/list":
			// Top-level {servers:[…]} shape with a bare-string entry to
			// exercise name normalization.
			result = map[string]any{"servers": []any{
				map[string]any{"name": "fs", "command": "npx", "enabled": true},
				"bare-name",
			}}
		case "_x.ai/mcp/toggle":
			result = map[string]any{"ok": true}
		case "_x.ai/mcp/upsert":
			result = map[string]any{"ok": true}
		case "_x.ai/mcp/delete":
			result = map[string]any{"ok": true}
		case "_x.ai/mcp/auth_trigger":
			result = map[string]any{"ok": true, "authUrl": "https://example.com/auth"}
		default:
			result = map[string]any{}
		}
		resp, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
		out.Write(resp)
		out.WriteByte('\n')
		out.Flush()
	}
}

// newFakeAgentServer builds a Server whose bridge talks to the test binary
// re-executed as a fake agent, so agent-backed endpoints round-trip fully.
// LastSessionFile is redirected to a temp path: the fake agent process is
// killed at cleanup, and waitProcess would otherwise persist the test's
// active session over the real ~/.capri-host/last-session.json.
func newFakeAgentServer(t *testing.T) (*Server, *acp.Bridge) {
	t.Helper()
	t.Setenv(ACPHostFakeAgentEnv, "1")
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             os.Args[0],
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	t.Cleanup(b.Shutdown)
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)
	return s, b
}

// postJSON issues a POST against the server's mux and returns the recorder.
func postJSON(t *testing.T, s *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

// createActiveSession boots a session through /api/session (the fake agent
// answers session/new with "sess-new") so session-scoped endpoints like the
// MCP mutations can resolve an active sessionId.
func createActiveSession(t *testing.T, s *Server) string {
	t.Helper()
	rec := postJSON(t, s, "/api/session", `{"cwd":"/ws"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	sid, _ := m["sessionId"].(string)
	if sid == "" {
		t.Fatalf("create session resp = %s, want sessionId", rec.Body.String())
	}
	return sid
}

// emitPermissionRequest writes a session/request_permission request (id
// fakeAgentPermissionID) so tests can exercise the host's permission-response
// path end-to-end. Options include the reject-once row the TUI's followup
// dispatch resolves against.
func emitPermissionRequest(out *bufio.Writer) {
	req, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      fakeAgentPermissionID,
		"method":  "session/request_permission",
		"params": map[string]any{
			"sessionId": "sess-new",
			"toolCall":  map[string]any{"id": "tc-1", "fields": map[string]any{}},
			"options": []any{
				map[string]any{"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
				map[string]any{"optionId": "reject-once", "name": "Reject once", "kind": "reject_once"},
			},
		},
	})
	out.Write(req)
	out.WriteByte('\n')
	out.Flush()
}

// appendPermissionRecord appends one host→agent response line to the record
// file (created on first write).
func appendPermissionRecord(path string, line []byte) {
	appendRecordLine(path, line)
}

// appendRecordLine appends one raw line to a record file (created on first
// write). Shared by the permission-response and notification recorders.
func appendRecordLine(path string, line []byte) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(line)
	_, _ = f.Write([]byte("\n"))
}
