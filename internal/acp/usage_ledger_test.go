package acp

import (
	"os"
	"path/filepath"
	"testing"
)

// ledgerBridge 构造一个开启台账、指向临时 grok home 的 Bridge。
func ledgerBridge(t *testing.T, home string) *Bridge {
	t.Helper()
	return NewBridge(GrokConfig{
		Bin: "grok", HostID: "h", HostName: "host",
		GrokHome:        home,
		UsageLedgerOn:   true,
		UsageLedgerFile: filepath.Join(home, ".capri-host", usageLedgerFileName),
	})
}

// ledgerLine 生成一条带 prompt_id 的回合终态事件。
func ledgerLine(ts int64, promptID string, in, out, cached int64) string {
	return envLine(ts, map[string]any{
		"sessionUpdate": "turn_completed",
		"prompt_id":     promptID,
		"usage": map[string]any{
			"inputTokens":      in,
			"outputTokens":     out,
			"totalTokens":      in + out,
			"cachedReadTokens": cached,
			"modelCalls":       1,
			"modelUsage": map[string]any{
				"m1": map[string]any{
					"inputTokens": in, "outputTokens": out, "totalTokens": in + out,
					"cachedReadTokens": cached, "modelCalls": 1,
				},
			},
		},
	})
}

// 台账落盘后，即使源 updates.jsonl 被删（模拟 agent 的 30 天清理），
// 用量仍可查——这是本次改动的核心目的。
func TestUsageLedgerSurvivesSourceDeletion(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws", "s1", []string{
		ledgerLine(100, "p1", 1000, 100, 800),
		ledgerLine(200, "p2", 2000, 200, 1500),
	})
	b := ledgerBridge(t, home)
	if err := b.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}

	// 源文件存在时：盘上直扫 + 台账去重后，必须正好是一份（2 回合）。
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Turns != 2 || rep.Total.InputTokens != 3000 {
		t.Fatalf("源文件存在时 turns=%d input=%d，want 2/3000（不得双算）",
			rep.Total.Turns, rep.Total.InputTokens)
	}

	// 模拟 30 天清理：删掉源文件。
	if err := os.Remove(filepath.Join(home, "sessions", EncodeCwdDirname("/ws"), "s1", "updates.jsonl")); err != nil {
		t.Fatal(err)
	}
	rep2, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Total.Turns != 2 || rep2.Total.InputTokens != 3000 || rep2.Total.OutputTokens != 300 {
		t.Fatalf("源文件删除后 turns=%d input=%d output=%d，want 2/3000/300（台账兜底）",
			rep2.Total.Turns, rep2.Total.InputTokens, rep2.Total.OutputTokens)
	}
	if rep2.Total.CachedReadTokens != 2300 {
		t.Fatalf("缓存命中读 = %d，want 2300", rep2.Total.CachedReadTokens)
	}
	m1 := rep2.ByModel["m1"]
	if m1.InputTokens != 3000 || m1.Turns != 2 {
		t.Fatalf("byModel.m1 = %+v，want input=3000 turns=2", m1)
	}
}

// 去重按 (sessionId, promptId) 幂等键：同一 prompt 被重复读到只记一次。
func TestUsageLedgerIdempotentRescan(t *testing.T) {
	home := t.TempDir()
	path := writeSessionFile(t, home, "/ws", "s1", []string{
		ledgerLine(100, "p1", 1000, 100, 0),
	})
	b := ledgerBridge(t, home)
	for i := 0; i < 3; i++ {
		if err := b.syncUsageLedger(); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Turns != 1 || rep.Total.InputTokens != 1000 {
		t.Fatalf("重复同步后 turns=%d input=%d，want 1/1000", rep.Total.Turns, rep.Total.InputTokens)
	}

	// 追加一轮：mtime/size 变化后必须被增量收进来。
	writeSessionFile(t, home, "/ws", "s1", []string{
		ledgerLine(100, "p1", 1000, 100, 0),
		ledgerLine(200, "p2", 500, 50, 0),
	})
	if err := b.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	rep2, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Total.Turns != 2 || rep2.Total.InputTokens != 1500 {
		t.Fatalf("追加后 turns=%d input=%d，want 2/1500", rep2.Total.Turns, rep2.Total.InputTokens)
	}
	_ = path
}

// 台账支持任意时间窗口，且窗口外不计入。
func TestUsageLedgerArbitraryWindow(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws", "s1", []string{
		ledgerLine(1000, "p1", 100, 10, 0),
		ledgerLine(2000, "p2", 200, 20, 0),
		ledgerLine(3000, "p3", 400, 40, 0),
	})
	b := ledgerBridge(t, home)
	if err := b.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	// 删源文件，强制只走台账。
	if err := os.Remove(filepath.Join(home, "sessions", EncodeCwdDirname("/ws"), "s1", "updates.jsonl")); err != nil {
		t.Fatal(err)
	}
	rep, err := b.UsageReport(t.Context(), "/ws", "", 1500, 3000)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Turns != 2 || rep.Total.InputTokens != 600 {
		t.Fatalf("窗口 [1500,3000] turns=%d input=%d，want 2/600", rep.Total.Turns, rep.Total.InputTokens)
	}
	if rep.CoverageFrom != 2000 || rep.CoverageTo != 3000 {
		t.Fatalf("覆盖区间 = [%d,%d]，want [2000,3000]", rep.CoverageFrom, rep.CoverageTo)
	}
}

// 台账被手工删除后，游标必须一并失效，否则会跳过重扫留下永久缺口。
func TestUsageLedgerMetaInvalidatedWithEntries(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws", "s1", []string{ledgerLine(100, "p1", 1000, 100, 0)})
	b := ledgerBridge(t, home)
	if err := b.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	// 只删 entries、留下 meta（模拟用户清理或落盘中断）。
	if err := os.Remove(b.usageLedgerPath()); err != nil {
		t.Fatal(err)
	}
	fresh := ledgerBridge(t, home)
	if err := fresh.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	rep, err := fresh.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Turns != 1 || rep.Total.InputTokens != 1000 {
		t.Fatalf("entries 丢失后 turns=%d input=%d，want 1/1000（游标须作废重扫）",
			rep.Total.Turns, rep.Total.InputTokens)
	}
}

// cwd 范围过滤：台账记录带 cwd，必须能按工作区隔离。
func TestUsageLedgerScopeByCwdAndSession(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws1", "s1", []string{ledgerLine(100, "p1", 1000, 100, 0)})
	writeSessionFile(t, home, "/ws1", "s2", []string{ledgerLine(100, "p1", 2000, 200, 0)})
	writeSessionFile(t, home, "/ws2", "s3", []string{ledgerLine(100, "p1", 4000, 400, 0)})
	b := ledgerBridge(t, home)
	if err := b.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"/ws1", "/ws2"} {
		_ = os.Remove(filepath.Join(home, "sessions", EncodeCwdDirname(dir)))
	}
	// 删掉 s1/s3 的源文件，只留 s2：验证 session 级范围走台账。
	if err := os.Remove(filepath.Join(home, "sessions", EncodeCwdDirname("/ws1"), "s1", "updates.jsonl")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(home, "sessions", EncodeCwdDirname("/ws2"), "s3", "updates.jsonl")); err != nil {
		t.Fatal(err)
	}

	repWS1, err := b.UsageReport(t.Context(), "/ws1", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if repWS1.Total.InputTokens != 3000 {
		t.Fatalf("cwd=/ws1 input=%d，want 3000（s1 台账 + s2 盘上）", repWS1.Total.InputTokens)
	}

	repS2, err := b.UsageReport(t.Context(), "/ws1", "s2", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if repS2.Total.InputTokens != 2000 || repS2.Sessions != 1 {
		t.Fatalf("session=s2 input=%d sessions=%d，want 2000/1", repS2.Total.InputTokens, repS2.Sessions)
	}

	repAll, err := b.UsageReport(t.Context(), "", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if repAll.Total.InputTokens != 7000 {
		t.Fatalf("全范围 input=%d，want 7000", repAll.Total.InputTokens)
	}
}

// 台账关闭时行为与改动前一致（只扫盘上文件）。
func TestUsageLedgerDisabledFallsBackToScan(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws", "s1", []string{ledgerLine(100, "p1", 1000, 100, 0)})
	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	if err := b.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b.usageLedgerPath()); !os.IsNotExist(err) {
		t.Fatalf("台账关闭时不应写盘: %v", err)
	}
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Turns != 1 || rep.Total.InputTokens != 1000 {
		t.Fatalf("关闭台账 turns=%d input=%d，want 1/1000", rep.Total.Turns, rep.Total.InputTokens)
	}
}

// prompt_id 缺失的老数据用「文件内序号」派生键，重扫仍幂等。
func TestUsageLedgerKeyFallsBackWithoutPromptID(t *testing.T) {
	home := t.TempDir()
	line := envLine(100, map[string]any{
		"sessionUpdate": "turn_completed",
		"usage": map[string]any{
			"inputTokens": 700, "outputTokens": 70, "totalTokens": 770,
			"modelCalls": 1,
		},
	})
	writeSessionFile(t, home, "/ws", "s1", []string{line})
	b := ledgerBridge(t, home)
	for i := 0; i < 2; i++ {
		if err := b.syncUsageLedger(); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Turns != 1 || rep.Total.InputTokens != 700 {
		t.Fatalf("无 prompt_id 重复同步 turns=%d input=%d，want 1/700", rep.Total.Turns, rep.Total.InputTokens)
	}
	// 无 modelUsage → 归 unknown。
	if _, ok := rep.ByModel[unknownModel]; !ok {
		t.Fatalf("无 modelUsage 应归 unknown，实际 byModel=%v", rep.ByModel)
	}
}
