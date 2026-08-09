package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
)

// usageServer 构造一个直扫本地 grok home 的 Server（usage-report 纯本地
// 文件统计，不需要 agent 通信，无需 fake agent）。
func usageServer(t *testing.T, home string) *Server {
	t.Helper()
	b := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	return New(config.Config{Port: 0}, b)
}

// writeUsageSession 在临时 grok home 里落一个会话的 updates.jsonl。
func writeUsageSession(t *testing.T, home, cwd, sid, line string) {
	t.Helper()
	dir := filepath.Join(home, "sessions", acp.EncodeCwdDirname(cwd), sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "updates.jsonl"), []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestUsageReportEndpoint(t *testing.T) {
	home := t.TempDir()
	usageLine := func(ts int64, in int64) string {
		// 手写信封：与 agent 持久化格式一致（timestamp 秒 + turn_completed
		// 携带 camelCase usage + modelUsage 分组）。
		return `{"timestamp":` + itoa(ts) + `,"method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"turn_completed","usage":{"inputTokens":` + itoa(in) + `,"outputTokens":` + itoa(in/10) + `,"totalTokens":` + itoa(in+in/10) + `,"cachedReadTokens":` + itoa(in*8/10) + `,"modelCalls":1,"modelUsage":{"m1":{"inputTokens":` + itoa(in) + `,"outputTokens":` + itoa(in/10) + `,"totalTokens":` + itoa(in+in/10) + `,"cachedReadTokens":` + itoa(in*8/10) + `}}}}}}`
	}
	writeUsageSession(t, home, "/ws", "s1", usageLine(100, 1000))
	writeUsageSession(t, home, "/ws", "s2", usageLine(200, 2000))
	s := usageServer(t, home)

	// 时间窗口 [150, 250] → 只统计 s2 的 ts=200 事件。
	rec := postJSON(t, s, "/api/usage-report", `{"cwd":"/ws","from":150,"to":250}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	result, _ := m["result"].(map[string]any)
	total, _ := result["total"].(map[string]any)
	if total["inputTokens"] != float64(2000) || total["outputTokens"] != float64(200) ||
		total["totalTokens"] != float64(2200) || total["cachedReadTokens"] != float64(1600) {
		t.Fatalf("total = %v, want input=2000 output=200 total=2200 cachedRead=1600", total)
	}
	if total["cacheHitRate"] != float64(0.8) {
		t.Fatalf("cacheHitRate = %v, want 0.8", total["cacheHitRate"])
	}
	byModel, _ := result["byModel"].(map[string]any)
	m1, _ := byModel["m1"].(map[string]any)
	if m1["inputTokens"] != float64(2000) {
		t.Fatalf("byModel.m1 = %v, want input=2000", m1)
	}

	// 全量（无 cwd 无窗口）→ 两个会话都统计。
	recAll := postJSON(t, s, "/api/usage-report", `{}`)
	if recAll.Code != http.StatusOK {
		t.Fatalf("all status = %d, body=%s", recAll.Code, recAll.Body.String())
	}
	resultAll, _ := decodeBody(t, recAll)["result"].(map[string]any)
	if resultAll["sessions"] != float64(2) {
		t.Fatalf("sessions = %v, want 2", resultAll["sessions"])
	}
	totalAll, _ := resultAll["total"].(map[string]any)
	if totalAll["inputTokens"] != float64(3000) {
		t.Fatalf("all inputTokens = %v, want 3000", totalAll["inputTokens"])
	}

	// 坏 JSON → 400。
	recBad := postJSON(t, s, "/api/usage-report", `{`)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("bad-json status = %d, want 400", recBad.Code)
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
