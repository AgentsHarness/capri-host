package server

import (
	"path/filepath"
	"testing"
)

func TestInterjectHonorsExplicitSession(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s)
	for _, sid := range []string{"sess-B", "sess-new"} {
		rec := postJSON(t, s, "/api/session-load", `{"sessionId":"`+sid+`","cwd":"/tmp"}`)
		if rec.Code != 200 {
			t.Fatalf("load %s: %s", sid, rec.Body.String())
		}
	}
	params := recordedParams(t, s, recordPath, "/api/interject",
		`{"text":"only session B","sessionId":"sess-B"}`, "_x.ai/interject")
	if params["sessionId"] != "sess-B" {
		t.Fatalf("explicit session sess-B was lost; agent received %#v", params)
	}
	before := len(readRecordedRequests(t, recordPath))
	if rec := postJSON(t, s, "/api/interject", `{"text":"legacy active session"}`); rec.Code != 200 {
		t.Fatalf("legacy interject failed: %s", rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath)[before:], "_x.ai/interject")
	params = req["params"].(map[string]any)
	if params["sessionId"] != "sess-new" {
		t.Fatalf("legacy request did not use active session: %#v", params)
	}
	if rec := postJSON(t, s, "/api/interject", `{"sessionId":"sess-B"}`); rec.Code != 400 {
		t.Fatalf("empty interject text status = %d", rec.Code)
	}
}
