package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// newFakeAgentServerWithGrokHome builds a fake-agent server whose bridge
// writes config.toml into the temp grok home (never the real ~/.grok).
func newFakeAgentServerWithGrokHome(t *testing.T) (*Server, *acp.Bridge, string) {
	t.Helper()
	t.Setenv(ACPHostFakeAgentEnv, "1")
	grokHome := t.TempDir()
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             os.Args[0],
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
		GrokHome:        grokHome,
	})
	t.Cleanup(b.Shutdown)
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)
	return s, b, filepath.Join(grokHome, "config.toml")
}

func readFileStr(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func TestSetDefaultModelEndpoint(t *testing.T) {
	s, _, path := newFakeAgentServerWithGrokHome(t)
	sid := createActiveSession(t, s)

	rec := postJSON(t, s, "/api/set-default-model", fmt.Sprintf(`{"modelId":"deepseek-v4-flash-go","reasoningEffort":"max","sessionId":%q}`, sid))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["reloaded"] != true {
		t.Errorf("reloaded = %v, want true (agent catalog must refresh after config write)", m["reloaded"])
	}
	out := readFileStr(t, path)
	for _, want := range []string{`default = "deepseek-v4-flash-go"`, `default_reasoning_effort = "max"`} {
		if !strings.Contains(out, want) {
			t.Errorf("config.toml missing %q:\n%s", want, out)
		}
	}
}

func TestSetDefaultModelEndpointRejectsEmptyModel(t *testing.T) {
	s, _, _ := newFakeAgentServerWithGrokHome(t)
	rec := postJSON(t, s, "/api/set-default-model", `{"modelId":""}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rec.Code, rec.Body.String())
	}
}

func TestCustomModelsEndpoints(t *testing.T) {
	s, b, path := newFakeAgentServerWithGrokHome(t)

	// 初始为空。
	rec := postJSON(t, s, "/api/custom-models", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	models, _ := m["models"].([]any)
	if len(models) != 0 {
		t.Fatalf("initial list = %v, want empty", models)
	}

	// 必填校验。
	rec = postJSON(t, s, "/api/custom-model", `{"id":"m1","values":{"base_url":"https://x"}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing slug status = %d, want 400, body=%s", rec.Code, rec.Body.String())
	}

	// 新增。
	rec = postJSON(t, s, "/api/custom-model", `{"id":"m1","values":{"model":"m1","base_url":"https://api.example.com/v1","name":"M1","context_window":200000}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["reloaded"] != true {
		t.Errorf("reloaded = %v, want true after upsert", m["reloaded"])
	}

	// 列表包含新条目。
	rec = postJSON(t, s, "/api/custom-models", `{}`)
	m = decodeBody(t, rec)
	models, _ = m["models"].([]any)
	if len(models) != 1 {
		t.Fatalf("list = %v, want 1 entry", models)
	}
	row, _ := models[0].(map[string]any)
	if row["id"] != "m1" || row["model"] != "m1" {
		t.Errorf("row = %v, want id/model m1", row)
	}

	// 删除（非默认 → defaultCleared=false）。
	rec = postJSON(t, s, "/api/custom-model-delete", `{"id":"m1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["defaultCleared"] != false {
		t.Errorf("defaultCleared = %v, want false", m["defaultCleared"])
	}
	if m := decodeBody(t, rec); m["reloaded"] != true {
		t.Errorf("reloaded = %v, want true after delete", m["reloaded"])
	}
	if strings.Contains(readFileStr(t, path), "[model.m1]") {
		t.Errorf("model section still present:\n%s", readFileStr(t, path))
	}

	// 删除默认模型 → 自动清 default。
	if err := b.SetDefaultModelConfig("m2", "max"); err != nil {
		t.Fatalf("SetDefaultModelConfig: %v", err)
	}
	rec = postJSON(t, s, "/api/custom-model", `{"id":"m2","values":{"model":"m2","base_url":"https://api.example.com/v2"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert m2 status = %d, body=%s", rec.Code, rec.Body.String())
	}
	rec = postJSON(t, s, "/api/custom-model-delete", `{"id":"m2"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete m2 status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["defaultCleared"] != true {
		t.Errorf("defaultCleared = %v, want true", m["defaultCleared"])
	}
	if out := readFileStr(t, path); strings.Contains(out, "default =") {
		t.Errorf("default must be cleared after deleting the default model:\n%s", out)
	}
}

// ── POST /api/set-model: sessionId 隔离 ──────────────────────────────

// 无 sessionId 的切模型请求必须被拒绝——即使 host 侧存在 active 会话：
// 空状态（FE 未锚定）下发的切换会落到别的会话上，失去会话隔离。
func TestSetModelRejectsMissingSessionID(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	createActiveSession(t, s) // host 侧有 active 会话也不能回退

	rec := postJSON(t, s, "/api/set-model", `{"modelId":"grok-4"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("/api/set-model status = %d, body=%s", rec.Code, rec.Body.String())
	}
	// 绝不能转发给 agent：createActiveSession 只留了 session/new 一行。
	lines := readRecordedRequests(t, recordPath)
	for _, m := range lines {
		if m["method"] == "session/set_model" {
			t.Fatalf("missing sessionId must not reach the agent: %v", m)
		}
	}
}

// 带 sessionId 的切模型请求按原样转发 session/set_model（含 effort _meta）。
func TestSetModelForwardsSessionID(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _ := newFakeAgentServer(t)
	ch, unsub := s.bridge.Subscribe()
	defer unsub()
	sid := createActiveSession(t, s)

	rec := postJSON(t, s, "/api/set-model",
		fmt.Sprintf(`{"modelId":"grok-4","reasoningEffort":"high","sessionId":%q}`, sid))
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/set-model status = %d, body=%s", rec.Code, rec.Body.String())
	}
	req := findRequest(t, readRecordedRequests(t, recordPath), "session/set_model")
	params, _ := req["params"].(map[string]any)
	if params["sessionId"] != sid {
		t.Errorf("sessionId = %v, want %s", params["sessionId"], sid)
	}
	if params["modelId"] != "grok-4" {
		t.Errorf("modelId = %v, want grok-4", params["modelId"])
	}
	meta, _ := params["_meta"].(map[string]any)
	if meta["reasoningEffort"] != "high" {
		t.Errorf("_meta = %v, want reasoningEffort high", params["_meta"])
	}

	// 验证广播事件中包含 sessions_changed
	var gotSessionsChanged bool
	deadline := time.After(2 * time.Second)
	for !gotSessionsChanged {
		select {
		case ev := <-ch:
			if ev["type"] == "sessions_changed" {
				gotSessionsChanged = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for sessions_changed after set-model")
		}
	}
}
