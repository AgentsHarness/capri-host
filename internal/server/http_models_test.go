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

// 不带 sessionId 的默认模型写入只落盘：改模型列表（改名重指默认）不该牵动
// 任何会话当前的模型——host 侧有 active 会话也不许被切。
func TestSetDefaultModelWithoutSessionPersistsOnly(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, _, path := newFakeAgentServerWithGrokHome(t)
	createActiveSession(t, s)

	rec := postJSON(t, s, "/api/set-default-model", `{"modelId":"m1","reasoningEffort":"max"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if m := decodeBody(t, rec); m["reloaded"] != true {
		t.Errorf("reloaded = %v, want true (config write must refresh the catalog)", m["reloaded"])
	}
	out := readFileStr(t, path)
	for _, want := range []string{`default = "m1"`, `default_reasoning_effort = "max"`} {
		if !strings.Contains(out, want) {
			t.Errorf("config.toml missing %q:\n%s", want, out)
		}
	}
	for _, m := range readRecordedRequests(t, recordPath) {
		if m["method"] == "session/set_model" || m["method"] == "session/set_config_option" {
			t.Fatalf("no sessionId must not touch any session: %v", m)
		}
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
		if m["method"] == "session/set_model" || m["method"] == "session/set_config_option" {
			t.Fatalf("missing sessionId must not reach the agent: %v", m)
		}
	}
}

// 带 sessionId 的切模型请求规范转发 session/set_config_option。
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
	reqs := readRecordedRequests(t, recordPath)
	var reqModel, reqEffort map[string]any
	for _, r := range reqs {
		if r["method"] == "session/set_config_option" {
			p, _ := r["params"].(map[string]any)
			if p["configId"] == "model" {
				reqModel = r
			} else if p["configId"] == "reasoning_effort" {
				reqEffort = r
			}
		}
	}
	if reqModel == nil {
		t.Fatalf("no recorded session/set_config_option for model in %v", reqs)
	}
	pModel, _ := reqModel["params"].(map[string]any)
	if pModel["sessionId"] != sid || pModel["value"] != "grok-4" {
		t.Errorf("model config option = %v, want grok-4 on %s", pModel, sid)
	}
	if reqEffort == nil {
		t.Fatalf("no recorded session/set_config_option for reasoning_effort in %v", reqs)
	}
	pEffort, _ := reqEffort["params"].(map[string]any)
	if pEffort["sessionId"] != sid || pEffort["value"] != "high" {
		t.Errorf("effort config option = %v, want high on %s", pEffort, sid)
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

// 模型切换成功但档位被 agent 拒绝：HTTP 仍是 200（模型确实切了），但 body
// 必须带 warning，前端据此弹 toast 并回滚 optimistic 档位——静默成功会让
// caption 显示一个 agent 根本没采用的档位。
func TestSetModelReportsEffortWarning(t *testing.T) {
	t.Setenv(ACPHostFakeAgentRejectEffort, "1")
	s, _ := newFakeAgentServer(t)
	sid := createActiveSession(t, s)

	rec := postJSON(t, s, "/api/set-model",
		fmt.Sprintf(`{"modelId":"grok-4","reasoningEffort":"high","sessionId":%q}`, sid))
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/set-model status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body decode: %v (%s)", err, rec.Body.String())
	}
	if body["ok"] != true {
		t.Fatalf("ok = %v, want true (the model switch itself succeeded)", body["ok"])
	}
	warning, _ := body["warning"].(string)
	if warning == "" {
		t.Fatalf("warning missing; body = %s", rec.Body.String())
	}
	if !strings.Contains(warning, "high") || !strings.Contains(warning, "未生效") {
		t.Errorf("warning = %q, want it to name the effort and say it did not apply", warning)
	}
}

// 档位正常的切模型请求不带 warning（不能给成功路径加噪音）。
func TestSetModelNoWarningOnCleanSwitch(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	sid := createActiveSession(t, s)

	rec := postJSON(t, s, "/api/set-model",
		fmt.Sprintf(`{"modelId":"grok-4","reasoningEffort":"high","sessionId":%q}`, sid))
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/set-model status = %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body decode: %v", err)
	}
	if _, present := body["warning"]; present {
		t.Errorf("warning must be absent on a clean switch, got %v", body["warning"])
	}
}
