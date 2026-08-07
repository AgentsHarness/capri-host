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
		method, _ := msg["method"].(string)
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
