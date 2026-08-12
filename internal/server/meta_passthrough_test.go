package server

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
)

// meta_passthrough_test.go — 响应 `_meta` / 分页游标透传给浏览器的
// HTTP 层断言：fake agent 的 canned 响应注入 `_meta`（经
// ACP_HOST_FAKE_AGENT_*_META env），host 必须原样透传；env 缺省时响应
// 保持原 wire 形状（absent key ≠ off）。

// ── POST /api/prompt：受理即返回；session/prompt 响应 `_meta` 经 SSE
// ── done 事件的 `meta` 透传（HTTP 响应只确认受理，不再携带回合结果）──

func TestPromptAcceptedImmediately(t *testing.T) {
	// 慢速回合：POST 必须在回合结束前就返回（受理即返回），回合结果走
	// live 通道的 done 事件。
	t.Setenv(ACPHostFakeAgentPromptDelayMs, "600")
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s)
	ch, unsub := b.Subscribe()
	defer unsub()

	start := time.Now()
	rec := postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"hi"}]}`)
	elapsed := time.Since(start)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/prompt status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Errorf("accepted body = %v, want ok:true", m)
	}
	if _, has := m["stopReason"]; has {
		t.Errorf("accept response must not carry stopReason: %v", m)
	}
	if _, has := m["meta"]; has {
		t.Errorf("accept response must not carry meta: %v", m)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("accept took %v with a 600ms turn — POST must return at accept time, not turn end", elapsed)
	}
	// 回合结果经 live 通道送达：done 事件带 stopReason。
	ev := waitEvent(t, ch, func(ev acp.Event) bool { return ev["type"] == "done" })
	if ev["stopReason"] != "end_turn" {
		t.Errorf("done stopReason = %v, want end_turn", ev["stopReason"])
	}
}

func TestPromptResponseMetaViaDoneEvent(t *testing.T) {
	t.Setenv(ACPHostFakeAgentPromptMeta, `{"turn_id":"t-1","cost":42}`)
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s)
	ch, unsub := b.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/prompt status = %d, body=%s", rec.Code, rec.Body.String())
	}
	ev := waitEvent(t, ch, func(ev acp.Event) bool { return ev["type"] == "done" })
	want := map[string]any{"turn_id": "t-1", "cost": float64(42)}
	if !reflect.DeepEqual(ev["meta"], want) {
		t.Errorf("done meta = %v, want %v", ev["meta"], want)
	}
}

func TestPromptDoneOmitsMetaWhenAbsent(t *testing.T) {
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s)
	ch, unsub := b.Subscribe()
	defer unsub()

	rec := postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/prompt status = %d, body=%s", rec.Code, rec.Body.String())
	}
	ev := waitEvent(t, ch, func(ev acp.Event) bool { return ev["type"] == "done" })
	if _, ok := ev["meta"]; ok {
		t.Errorf("done must not carry meta when the agent returned none: %v", ev)
	}
}

// ── POST /api/sessions：nextCursor + `_meta` → 响应透传 ──────────────

func TestSessionsResponseCarriesCursorAndMeta(t *testing.T) {
	t.Setenv(ACPHostFakeAgentSessionListCur, "c2")
	t.Setenv(ACPHostFakeAgentSessionListMeta, `{"has_more":true}`)
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/sessions", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/sessions status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["nextCursor"] != "c2" {
		t.Errorf("nextCursor = %v, want c2", m["nextCursor"])
	}
	if !reflect.DeepEqual(m["meta"], map[string]any{"has_more": true}) {
		t.Errorf("meta = %v, want {has_more:true}", m["meta"])
	}
}

func TestSessionsResponseOmitsCursorAndMetaWhenAbsent(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	rec := postJSON(t, s, "/api/sessions", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/sessions status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if _, ok := m["nextCursor"]; ok {
		t.Errorf("response must not carry nextCursor when absent: %v", m)
	}
	if _, ok := m["meta"]; ok {
		t.Errorf("response must not carry meta when absent: %v", m)
	}
}

// ── GET /api/status：authenticate / session 响应 `_meta` ─────────────

// getStatus fetches GET /api/status and decodes it.
func getStatus(t *testing.T, s *Server) map[string]any {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/status status = %d, body=%s", rec.Code, rec.Body.String())
	}
	return decodeBody(t, rec)
}

func TestStatusCarriesAuthMeta(t *testing.T) {
	t.Setenv(ACPHostFakeAgentAuthMeta, `{"email":"a@b.c","subscription_tier":"pro"}`)
	s, _ := newFakeAgentServer(t)

	// Boot the agent (authenticate returns the injected `_meta`).
	if rec := postJSON(t, s, "/api/sessions", `{}`); rec.Code != http.StatusOK {
		t.Fatalf("/api/sessions status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := getStatus(t, s)
	want := map[string]any{"email": "a@b.c", "subscription_tier": "pro"}
	if !reflect.DeepEqual(m["authMeta"], want) {
		t.Errorf("status authMeta = %v, want %v", m["authMeta"], want)
	}
}

func TestStatusCarriesSessionMeta(t *testing.T) {
	t.Setenv(ACPHostFakeAgentSessionNewMeta, `{"kind":"fresh"}`)
	s, _ := newFakeAgentServer(t)

	if rec := postJSON(t, s, "/api/session", `{"cwd":"/ws"}`); rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := getStatus(t, s)
	if !reflect.DeepEqual(m["sessionMeta"], map[string]any{"kind": "fresh"}) {
		t.Errorf("status sessionMeta = %v, want {kind:fresh}", m["sessionMeta"])
	}
}

func TestStatusOmitsMetaKeysWhenAbsent(t *testing.T) {
	s, _ := newFakeAgentServer(t)

	if rec := postJSON(t, s, "/api/session", `{"cwd":"/ws"}`); rec.Code != http.StatusOK {
		t.Fatalf("/api/session status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := getStatus(t, s)
	if _, ok := m["authMeta"]; ok {
		t.Errorf("status must not carry authMeta when absent: %v", m)
	}
	if _, ok := m["sessionMeta"]; ok {
		t.Errorf("status must not carry sessionMeta when absent: %v", m)
	}
}
