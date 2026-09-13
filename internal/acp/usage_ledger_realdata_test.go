package acp

import (
	"os"
	"path/filepath"
	"testing"
)

// TestVerifyRealDataLedgerParity 临时校验：在真实 ~/.grok 上，开启台账前后
// 的数字必须完全一致（台账只补缺口，不得改变已有数据）。需要真实数据，
// 默认跳过，用 LEDGER_VERIFY=1 手动跑。
func TestVerifyRealDataLedgerParity(t *testing.T) {
	if os.Getenv("LEDGER_VERIFY") != "1" {
		t.Skip("需要真实 ~/.grok 数据；设 LEDGER_VERIFY=1 运行")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	grokHome := filepath.Join(home, ".grok")
	ledgerPath := filepath.Join(t.TempDir(), "usage-ledger.jsonl")

	// 1) 台账关闭：纯盘上直扫（现状基线）。
	off := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: grokHome})
	base, err := off.UsageReport(t.Context(), "", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 2) 台账开启（空台账）：必须与基线逐字段一致。
	on := NewBridge(GrokConfig{
		Bin: "grok", HostID: "h", HostName: "host",
		GrokHome: grokHome, UsageLedgerOn: true, UsageLedgerFile: ledgerPath,
	})
	if err := on.syncUsageLedger(); err != nil {
		t.Fatal(err)
	}
	withLedger, err := on.UsageReport(t.Context(), "", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("基线(直扫)      sessions=%d turns=%d input=%d output=%d total=%d cached=%d",
		base.Sessions, base.Total.Turns, base.Total.InputTokens,
		base.Total.OutputTokens, base.Total.TotalTokens, base.Total.CachedReadTokens)
	t.Logf("台账 + 直扫     sessions=%d turns=%d input=%d output=%d total=%d cached=%d",
		withLedger.Sessions, withLedger.Total.Turns, withLedger.Total.InputTokens,
		withLedger.Total.OutputTokens, withLedger.Total.TotalTokens, withLedger.Total.CachedReadTokens)
	t.Logf("基线 byModel 数=%d  合并后 byModel 数=%d", len(base.ByModel), len(withLedger.ByModel))

	if base.Total != withLedger.Total {
		t.Errorf("总计不一致:\n  直扫 = %+v\n  合并 = %+v", base.Total, withLedger.Total)
	}
	if base.Sessions != withLedger.Sessions {
		t.Errorf("会话数不一致: 直扫=%d 合并=%d", base.Sessions, withLedger.Sessions)
	}
	for model, bs := range base.ByModel {
		ws, ok := withLedger.ByModel[model]
		if !ok {
			t.Errorf("模型 %s 在合并结果里缺失", model)
			continue
		}
		norm := func(s TokenUsageStat) TokenUsageStat { s.CacheHitRate = 0; return s }
		if norm(bs) != norm(ws) {
			t.Errorf("模型 %s 不一致:\n  直扫 = %+v\n  合并 = %+v", model, bs, ws)
		}
	}
	if len(base.ByModel) != len(withLedger.ByModel) {
		t.Errorf("模型数不一致: 直扫=%d 合并=%d", len(base.ByModel), len(withLedger.ByModel))
	}

	// 3) 台账文件规模（对照 1.28GB 源数据）。
	if st, err := os.Stat(ledgerPath); err == nil {
		t.Logf("台账文件大小 = %.2f MB", float64(st.Size())/(1<<20))
	}

	// 4) 覆盖区间：说明「全部」实际覆盖到哪。
	t.Logf("覆盖区间 = [%d, %d]", withLedger.CoverageFrom, withLedger.CoverageTo)
}
