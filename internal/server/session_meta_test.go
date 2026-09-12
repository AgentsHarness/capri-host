package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// readRecordedRequests polls the fake agent's request-record file until the
// host→agent requests arrive and returns every recorded line.
func readRecordedRequests(t *testing.T, path string) []map[string]any {
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
	t.Fatalf("fake agent never recorded a request in %s", path)
	return nil
}

// findRequest returns the first recorded request with the given method.
func findRequest(t *testing.T, lines []map[string]any, method string) map[string]any {
	t.Helper()
	for _, m := range lines {
		if m["method"] == method {
			return m
		}
	}
	t.Fatalf("no recorded %s request in %v", method, lines)
	return nil
}

// ── POST /api/session: meta → session/new `_meta` ─────────────────────

// A `meta` body field (FE permission-mode seeds) must reach the agent as
// the session/new params `_meta` — the TUI's SessionFlags.to_meta() analog.
func TestSessionNewForwardsMetaToAgent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session",
		`{"cwd":"/ws","meta":{"yoloMode":true,"autoMode":false}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req := findRequest(t, readRecordedRequests(t, recordPath), "session/new")
	params, _ := req["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/new params carry no _meta: %v", params)
	}
	// seeds 原样透传；会话级回显开关（clientUserMessageEcho）无条件带上。
	if !reflect.DeepEqual(meta, map[string]any{
		"yoloMode": true, "autoMode": false, "clientUserMessageEcho": true,
	}) {
		t.Errorf("_meta = %v, want yoloMode:true autoMode:false + echo", meta)
	}
}

// Without meta the session/new request still carries the echo switch —
// 缺了它 agent 只落盘不活推 user_message_chunk（正文 + 附图），实时看不见图。
func TestSessionNewSendsEchoMetaWithoutSeeds(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session", `{"cwd":"/ws"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req := findRequest(t, readRecordedRequests(t, recordPath), "session/new")
	params, _ := req["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/new params must carry _meta: %v", params)
	}
	if !reflect.DeepEqual(meta, map[string]any{"clientUserMessageEcho": true}) {
		t.Errorf("_meta = %v, want {clientUserMessageEcho:true}", meta)
	}
	want := map[string]any{
		"cwd":                   "/ws",
		"additionalDirectories": []any{},
		"mcpServers":            []any{},
		"_meta":                 map[string]any{"clientUserMessageEcho": true},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}

// ── POST /api/session-load: meta → session/load `_meta` ───────────────

func TestSessionLoadForwardsMetaToAgent(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session-load",
		`{"sessionId":"hist-1","cwd":"/ws","meta":{"yoloMode":false,"autoMode":true}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session-load status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req := findRequest(t, readRecordedRequests(t, recordPath), "session/load")
	params, _ := req["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/load params carry no _meta: %v", params)
	}
	// load 出来的会话同样要带回显开关（否则实时看不到用户附图）。
	if !reflect.DeepEqual(meta, map[string]any{
		"yoloMode": false, "autoMode": true, "clientUserMessageEcho": true,
	}) {
		t.Errorf("_meta = %v, want yoloMode:false autoMode:true + echo", meta)
	}
}

// Without meta the session/load request still carries the echo switch.
func TestSessionLoadSendsEchoMetaWithoutMeta(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session-load", `{"sessionId":"hist-1","cwd":"/ws"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session-load status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req := findRequest(t, readRecordedRequests(t, recordPath), "session/load")
	params, _ := req["params"].(map[string]any)
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("session/load params must carry _meta: %v", params)
	}
	if !reflect.DeepEqual(meta, map[string]any{"clientUserMessageEcho": true}) {
		t.Errorf("_meta = %v, want {clientUserMessageEcho:true}", meta)
	}
	want := map[string]any{
		"sessionId":  "hist-1",
		"cwd":        "/ws",
		"mcpServers": []any{},
		"_meta":      map[string]any{"clientUserMessageEcho": true},
	}
	if !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}
}
