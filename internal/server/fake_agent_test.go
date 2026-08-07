package server

import (
	"bufio"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
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
		case "authenticate":
			result = map[string]any{}
		case "session/new":
			result = map[string]any{"sessionId": "sess-new"}
			if os.Getenv(ACPHostFakeAgentEmitPermission) == "1" {
				emitPermissionRequest(out)
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
// active session over the real ~/.acp-host/last-session.json.
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
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(line)
	_, _ = f.Write([]byte("\n"))
}
