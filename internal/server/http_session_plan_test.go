package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// TestSessionPlanEndpoint — POST /api/session-plan 读会话的 plan.md（FE
// /view-plan 的权威正文来源）：没有 plan 文件时是 200 + 空串（FE 显示空态，
// 不是错误），有则原样返回；形状可疑的 sessionId（路径穿越）不读盘。
func TestSessionPlanEndpoint(t *testing.T) {
	s, _, cfgPath := newFakeAgentServerWithGrokHome(t)
	// 夹具返回的是 config.toml 路径，它的父目录才是 grok home。
	grokHome := filepath.Dir(cfgPath)
	const sid = "01a00000-0000-7000-8000-000000000001"

	content := func(body string) (bool, string, int) {
		rec := postJSON(t, s, "/api/session-plan", body)
		var out struct {
			OK      bool   `json:"ok"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("响应不是 JSON: %v (%s)", err, rec.Body.String())
		}
		return out.OK, out.Content, rec.Code
	}

	// 还没有 plan → 200 + 空 content。
	if ok, got, code := content(`{"sessionId":"` + sid + `","cwd":"/ws"}`); !ok || got != "" || code != 200 {
		t.Fatalf("无 plan = (ok=%v, %q, %d), want (true, \"\", 200)", ok, got, code)
	}

	dir := filepath.Join(grokHome, "sessions", acp.EncodeCwdDirname("/ws"), sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	const plan = "# 计划\n\n- 第一步\n"
	if err := os.WriteFile(filepath.Join(dir, "plan.md"), []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, got, code := content(`{"sessionId":"` + sid + `","cwd":"/ws"}`); !ok || got != plan || code != 200 {
		t.Errorf("plan 正文 = (ok=%v, %q, %d), want (true, %q, 200)", ok, got, code, plan)
	}

	// sessionId 里带路径分隔符 → 不越界读盘。
	if ok, got, _ := content(`{"sessionId":"../evil","cwd":"/ws"}`); !ok || got != "" {
		t.Errorf("穿越 sessionId = (ok=%v, %q), want (true, \"\")", ok, got)
	}
	// 缺 cwd / 缺 sessionId → 空 content。
	if _, got, _ := content(`{"sessionId":"` + sid + `"}`); got != "" {
		t.Errorf("缺 cwd 应返回空 content，got %q", got)
	}
}
