package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
)

// prompt_accept_test.go — POST /api/prompt 受理即返回（async accept）的
// 失败面：回合级错误不再走 HTTP 响应，全部经 SSE error 事件送达。

// waitEvent drains ch until an event matching pred arrives, or fails the
// test on timeout.
func waitEvent(t *testing.T, ch chan acp.Event, pred func(acp.Event) bool) acp.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if pred(ev) {
				return ev
			}
		case <-deadline:
			t.Fatal("timed out waiting for a matching SSE event")
		}
	}
}

// agent 拒绝回合（RPCError）：HTTP 只回受理 {ok:true}，错误经 SSE error
// 事件（sessionId + source=agent）送达——前端据此渲染回合错误行。
func TestPromptAgentRejectionBroadcastsError(t *testing.T) {
	t.Setenv(ACPHostFakeAgentErrorMethod, "session/prompt")
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s)
	ch, unsub := b.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/prompt status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("accepted body = %v, want ok:true", m)
	}
	ev := waitEvent(t, ch, func(ev acp.Event) bool { return ev["type"] == "error" })
	if sid, _ := ev["sessionId"].(string); sid != "sess-new" {
		t.Errorf("error sessionId = %v, want sess-new", ev["sessionId"])
	}
	if src, _ := ev["source"].(string); src != "agent" {
		t.Errorf("error source = %v, want agent", ev["source"])
	}
	msg, _ := ev["message"].(string)
	if !strings.Contains(msg, "method not found") {
		t.Errorf("error message = %v, want method-not-found text", ev["message"])
	}
}

// 无活动会话且 last-session 恢复失败：受理已确认（无 HTTP 错误面），
// 失败经 SSE error 事件送达（会话尚不存在 → 无 sessionId，前端按
// host 级错误渲染）。
func TestPromptRestoreFailureBroadcastsError(t *testing.T) {
	lsPath := filepath.Join(t.TempDir(), "last-session.json")
	if err := os.WriteFile(lsPath, []byte(`{"sessionId":"sess-gone","cwd":"/ws"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(ACPHostFakeAgentEnv, "1")
	t.Setenv(ACPHostFakeAgentErrorMethod, "session/load")
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             os.Args[0],
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: lsPath,
	})
	t.Cleanup(b.Shutdown)
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)
	ch, unsub := b.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/prompt status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["ok"] != true {
		t.Fatalf("accepted body = %v, want ok:true", m)
	}
	ev := waitEvent(t, ch, func(ev acp.Event) bool { return ev["type"] == "error" })
	msg, _ := ev["message"].(string)
	if !strings.Contains(msg, "恢复会话失败") {
		t.Errorf("error message = %v, want 恢复会话失败", ev["message"])
	}
	if _, has := ev["sessionId"]; has {
		t.Errorf("restore-failure error must not carry sessionId (none exists): %v", ev)
	}
	if src, _ := ev["source"].(string); src != "agent" {
		t.Errorf("error source = %v, want agent", ev["source"])
	}
}
