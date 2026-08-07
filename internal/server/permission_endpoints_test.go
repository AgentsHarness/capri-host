package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
)

// waitPendingPermission polls the bridge until the fake agent's
// session/request_permission request shows up and returns its requestId.
func waitPendingPermission(t *testing.T, b *acp.Bridge) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range b.Snapshot().PendingRequests {
			if p.Method == "session/request_permission" {
				return p.RequestID
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("fake agent's permission request never became pending")
	return ""
}

// readRecordedResponses polls the fake agent's record file until the host's
// permission response arrives and returns every recorded line.
func readRecordedResponses(t *testing.T, path string) []map[string]any {
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
	t.Fatalf("fake agent never recorded a permission response in %s", path)
	return nil
}

// ── POST /api/permission-response: scope meta round-trip ─────────────

// A Selected reply with a bash scope must reach the agent as
// result = {outcome:{outcome:"selected",optionId}, _meta:{command_parts,
// is_glob}} — the exact wire shape the TUI sends for BashCommandSelectedTerms.
func TestPermissionResponseScopeMetaRoundTrip(t *testing.T) {
	t.Setenv(ACPHostFakeAgentEmitPermission, "1")
	recordPath := filepath.Join(t.TempDir(), "responses.jsonl")
	t.Setenv(ACPHostFakeAgentRecord, recordPath)
	s, b := newFakeAgentServer(t)

	// Boot + create a session; the fake agent emits a permission request
	// right after answering session/new.
	rec := postJSON(t, s, "/api/session", `{"cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	reqID := waitPendingPermission(t, b)

	rec2 := postJSON(t, s, "/api/permission-response",
		fmt.Sprintf(`{"requestId":%q,"optionId":"allow-once","scope":{"commandParts":["gh","api"],"isGlob":false}}`, reqID))
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec2.Code, rec2.Body.String())
	}

	lines := readRecordedResponses(t, recordPath)
	if len(lines) != 1 {
		t.Fatalf("recorded %d lines, want 1: %v", len(lines), lines)
	}
	res, _ := lines[0]["result"].(map[string]any)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "allow-once" {
		t.Fatalf("outcome = %v, want selected/allow-once", outcome)
	}
	meta, ok := res["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("result = %v, want _meta (wire key `_meta`)", res)
	}
	parts, _ := meta["command_parts"].([]any)
	if len(parts) != 2 || parts[0] != "gh" || parts[1] != "api" {
		t.Errorf("command_parts = %v, want [gh api]", meta["command_parts"])
	}
	if meta["is_glob"] != false {
		t.Errorf("is_glob = %v, want false", meta["is_glob"])
	}
}

// ── POST /api/permission-response: followup message round-trip ────────

// A cancelled reply with a followupMessage must reach the agent as the TUI's
// followup dispatch: Selected on the request's RejectOnce option with
// _meta:{followup_message} — the only branch the agent reads followup text
// from. The HTTP body's optional fields are parsed and forwarded untouched.
func TestPermissionResponseFollowupRoundTrip(t *testing.T) {
	t.Setenv(ACPHostFakeAgentEmitPermission, "1")
	recordPath := filepath.Join(t.TempDir(), "responses.jsonl")
	t.Setenv(ACPHostFakeAgentRecord, recordPath)
	s, b := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session", `{"cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	reqID := waitPendingPermission(t, b)

	rec2 := postJSON(t, s, "/api/permission-response",
		fmt.Sprintf(`{"requestId":%q,"cancelled":true,"followupMessage":"不要这么做"}`, reqID))
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec2.Code, rec2.Body.String())
	}

	lines := readRecordedResponses(t, recordPath)
	if len(lines) != 1 {
		t.Fatalf("recorded %d lines, want 1: %v", len(lines), lines)
	}
	res, _ := lines[0]["result"].(map[string]any)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "reject-once" {
		t.Fatalf("outcome = %v, want selected/reject-once (TUI followup semantics)", outcome)
	}
	meta, _ := res["_meta"].(map[string]any)
	if meta["followup_message"] != "不要这么做" {
		t.Errorf("followup_message = %v, want 不要这么做", meta["followup_message"])
	}
}

// Legacy compatibility: a response without scope/followupMessage must not
// gain a _meta key on the wire (byte-identical to the old behavior).
func TestPermissionResponseLegacyNoMeta(t *testing.T) {
	t.Setenv(ACPHostFakeAgentEmitPermission, "1")
	recordPath := filepath.Join(t.TempDir(), "responses.jsonl")
	t.Setenv(ACPHostFakeAgentRecord, recordPath)
	s, b := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/session", `{"cwd":"/tmp"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	reqID := waitPendingPermission(t, b)

	rec2 := postJSON(t, s, "/api/permission-response",
		fmt.Sprintf(`{"requestId":%q,"optionId":"allow-once"}`, reqID))
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec2.Code, rec2.Body.String())
	}

	lines := readRecordedResponses(t, recordPath)
	if len(lines) != 1 {
		t.Fatalf("recorded %d lines, want 1: %v", len(lines), lines)
	}
	res, _ := lines[0]["result"].(map[string]any)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "allow-once" {
		t.Fatalf("outcome = %v, want selected/allow-once", outcome)
	}
	if _, has := res["_meta"]; has {
		t.Errorf("result = %v, want no _meta without scope/followup", res)
	}
}
