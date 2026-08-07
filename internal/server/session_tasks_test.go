package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
)

func writeFakeSession(t *testing.T, grokHome, cwd, sessionID string, lines []string) {
	t.Helper()
	dir := filepath.Join(grokHome, "sessions", acp.EncodeCwdDirname(cwd), sessionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for _, l := range lines {
		sb.WriteString(l)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "updates.jsonl"), []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestSessionRunningTasksEndpoint verifies POST /api/session-running-tasks
// end to end (no agent boot needed — pure file scan + open-fd probe).
func TestSessionRunningTasksEndpoint(t *testing.T) {
	home := t.TempDir()
	freshLog := filepath.Join(home, "held.log")
	held, err := os.OpenFile(freshLog, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	upd, _ := json.Marshal(map[string]any{
		"timestamp": 100,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": "sess-1",
			"update": map[string]any{
				"sessionUpdate": "task_backgrounded",
				"task_id":       "t-1",
				"command":       "npm run dev",
				"output_file":   freshLog,
			},
		},
	})
	writeFakeSession(t, home, "/Users/benin/ccwork", "sess-1", []string{string(upd)})

	b := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)

	req := httptest.NewRequest("POST", "/api/session-running-tasks",
		strings.NewReader(`{"sessionId":"sess-1","cwd":"/Users/benin/ccwork"}`))
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		OK     bool `json:"ok"`
		Events []struct {
			Kind    string `json:"kind"`
			TaskID  string `json:"taskId"`
			Command string `json:"command"`
			Running bool   `json:"running"`
		} `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK || len(out.Events) != 1 {
		t.Fatalf("resp = %s", rec.Body.String())
	}
	ev := out.Events[0]
	if ev.Kind != "task_backgrounded" || ev.TaskID != "t-1" || ev.Command != "npm run dev" || !ev.Running {
		t.Fatalf("event = %+v", ev)
	}

	// Unknown session → empty events, still ok.
	req2 := httptest.NewRequest("POST", "/api/session-running-tasks",
		strings.NewReader(`{"sessionId":"nope","cwd":"/Users/benin/ccwork"}`))
	rec2 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK || !strings.Contains(rec2.Body.String(), `"events":[]`) {
		t.Fatalf("missing session: %d %s", rec2.Code, rec2.Body.String())
	}

	// Missing cwd → 400.
	req3 := httptest.NewRequest("POST", "/api/session-running-tasks", strings.NewReader(`{}`))
	rec3 := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("bad body status = %d", rec3.Code)
	}
}
