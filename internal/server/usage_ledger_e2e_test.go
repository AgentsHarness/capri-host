package server

import (
	"encoding/json"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"os"
	"path/filepath"
	"testing"
)

// 端到端：源文件被删后 /api/usage-report 仍返回台账里的历史，
// 且响应带 coverageFrom/coverageTo。
func TestUsageReportEndpointLedgerSurvivesDeletion(t *testing.T) {
	home := t.TempDir()
	ledgerPath := filepath.Join(home, ".capri-host", "usage-ledger.jsonl")
	b := acp.NewBridge(acp.GrokConfig{
		Bin: "grok", HostID: "h", HostName: "host", GrokHome: home,
		UsageLedgerOn: true, UsageLedgerFile: ledgerPath,
	})
	s := New(config.Config{Port: 0}, b)

	writeUsageSession(t, home, "/ws", "s1",
		`{"timestamp":100,"method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"turn_completed","prompt_id":"p1","usage":{"inputTokens":1000,"outputTokens":100,"totalTokens":1100,"cachedReadTokens":800,"modelCalls":1,"modelUsage":{"m1":{"inputTokens":1000,"outputTokens":100,"totalTokens":1100,"cachedReadTokens":800,"modelCalls":1}}}}}}`)
	// 首次查询会惰性建台账。
	rec := postJSON(t, s, "/api/usage-report", `{"cwd":"/ws"}`)
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	// 删源文件后仍可查。
	if err := os.Remove(filepath.Join(home, "sessions", acp.EncodeCwdDirname("/ws"), "s1", "updates.jsonl")); err != nil {
		t.Fatal(err)
	}
	rec2 := postJSON(t, s, "/api/usage-report", `{"cwd":"/ws"}`)
	var m map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	result, _ := m["result"].(map[string]any)
	total, _ := result["total"].(map[string]any)
	if total["inputTokens"] != float64(1000) {
		t.Fatalf("删源文件后 input=%v，want 1000（台账兜底）", total["inputTokens"])
	}
	if result["coverageFrom"] != float64(100) || result["coverageTo"] != float64(100) {
		t.Fatalf("覆盖区间 = %v~%v，want 100~100", result["coverageFrom"], result["coverageTo"])
	}
}
