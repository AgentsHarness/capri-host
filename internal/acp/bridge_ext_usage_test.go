package acp

import (
	"math"
	"testing"
)

// usageTurnUpdate 构造一个带 usage 的回合终态 update（camelCase 字段，
// 与真实数据一致）。modelUsage 为 nil 时省略（老版本/无分组）。
func usageTurnUpdate(in, out, tot, cr, cc, rk int64, modelUsage map[string]any) map[string]any {
	u := map[string]any{
		"inputTokens":         in,
		"outputTokens":        out,
		"totalTokens":         tot,
		"cachedReadTokens":    cr,
		"cacheCreationTokens": cc,
		"reasoningTokens":     rk,
		"modelCalls":          1,
	}
	if modelUsage != nil {
		u["modelUsage"] = modelUsage
	}
	return map[string]any{"sessionUpdate": "turn_completed", "usage": u}
}

// modelStat 构造 modelUsage 里的单个模型条目。
func modelStat(in, out, tot, cr int64) map[string]any {
	return map[string]any{
		"inputTokens":      in,
		"outputTokens":     out,
		"totalTokens":      tot,
		"cachedReadTokens": cr,
		"modelCalls":       1,
	}
}

func closeRate(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

// ── 时间窗口过滤 ────────────────────────────────────────────────────

func TestUsageReportWindowFilter(t *testing.T) {
	home := t.TempDir()
	lines := []string{
		envLine(100, usageTurnUpdate(1000, 100, 1100, 800, 0, 0, nil)),
		envLine(200, usageTurnUpdate(2000, 200, 2200, 1500, 0, 0, nil)),
		envLine(300, usageTurnUpdate(3000, 300, 3300, 2500, 0, 0, nil)),
	}
	writeSessionFile(t, home, "/ws", "s1", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	rep, err := b.UsageReport(t.Context(), "/ws", "", 150, 250)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Sessions != 1 || rep.Total.Turns != 1 {
		t.Fatalf("sessions=%d turns=%d, want 1/1", rep.Sessions, rep.Total.Turns)
	}
	if rep.Total.InputTokens != 2000 || rep.Total.OutputTokens != 200 || rep.Total.TotalTokens != 2200 {
		t.Fatalf("total = %+v, want input=2000 output=200 total=2200", rep.Total)
	}
	if rep.Total.CachedReadTokens != 1500 {
		t.Fatalf("cachedRead = %d, want 1500", rep.Total.CachedReadTokens)
	}
	if !closeRate(rep.Total.CacheHitRate, 1500.0/2000.0) {
		t.Fatalf("hitRate = %v, want 0.75", rep.Total.CacheHitRate)
	}
	// 全窗口（省略 from/to）→ 三个事件全进。
	repAll, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if repAll.Total.Turns != 3 || repAll.Total.InputTokens != 6000 {
		t.Fatalf("all-window total = %+v, want turns=3 input=6000", repAll.Total)
	}
}

// ── 按模型分组 + unknown 承接 ───────────────────────────────────────

func TestUsageReportByModelAndUnknown(t *testing.T) {
	home := t.TempDir()
	lines := []string{
		// 带 modelUsage：m1 + m2 完全覆盖顶层。
		envLine(100, usageTurnUpdate(1000, 100, 1100, 800, 100, 50, map[string]any{
			"m1": modelStat(600, 60, 660, 500),
			"m2": modelStat(400, 40, 440, 300),
		})),
		// 无 modelUsage：整体归 unknown。
		envLine(200, usageTurnUpdate(100, 10, 110, 90, 0, 0, nil)),
	}
	writeSessionFile(t, home, "/ws", "s1", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	m1, ok := rep.ByModel["m1"]
	if !ok || m1.InputTokens != 600 || m1.CachedReadTokens != 500 {
		t.Fatalf("m1 = %+v, want input=600 cachedRead=500", m1)
	}
	m2, ok := rep.ByModel["m2"]
	if !ok || m2.InputTokens != 400 {
		t.Fatalf("m2 = %+v, want input=400", m2)
	}
	unk, ok := rep.ByModel[unknownModel]
	if !ok || unk.InputTokens != 100 || unk.TotalTokens != 110 {
		t.Fatalf("unknown = %+v, want input=100 total=110", unk)
	}
	if rep.Total.InputTokens != 1100 || rep.Total.TotalTokens != 1210 {
		t.Fatalf("total = %+v, want input=1100 total=1210", rep.Total)
	}
	if !closeRate(m1.CacheHitRate, 500.0/600.0) {
		t.Fatalf("m1 hitRate = %v, want 5/6", m1.CacheHitRate)
	}
	// 各模型之和 == total（unknown 补齐差额）。
	var sumIn, sumTot int64
	for _, st := range rep.ByModel {
		sumIn += st.InputTokens
		sumTot += st.TotalTokens
	}
	if sumIn != rep.Total.InputTokens || sumTot != rep.Total.TotalTokens {
		t.Fatalf("byModel sums (%d/%d) != total (%d/%d)", sumIn, sumTot, rep.Total.InputTokens, rep.Total.TotalTokens)
	}
}

// ── snake_case 老版本兼容 ───────────────────────────────────────────

func TestUsageReportSnakeCaseCompat(t *testing.T) {
	home := t.TempDir()
	raw := `{"timestamp":100,"method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"response_completed","usage":{"input_tokens":500,"output_tokens":50,"total_tokens":550,"cached_read_tokens":400,"cache_creation_tokens":10,"reasoning_tokens":20,"model_calls":2}}}}`
	writeSessionFile(t, home, "/ws", "s1", []string{raw})

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.InputTokens != 500 || rep.Total.OutputTokens != 50 ||
		rep.Total.TotalTokens != 550 || rep.Total.CachedReadTokens != 400 ||
		rep.Total.CacheCreationTokens != 10 || rep.Total.ReasoningTokens != 20 ||
		rep.Total.ModelCalls != 2 {
		t.Fatalf("total = %+v, want snake_case fields honored", rep.Total)
	}
	if !closeRate(rep.Total.CacheHitRate, 0.8) {
		t.Fatalf("hitRate = %v, want 0.8", rep.Total.CacheHitRate)
	}
}

// ── rewind 死分支照常计入 ───────────────────────────────────────────

func TestUsageReportCountsRewindBranch(t *testing.T) {
	home := t.TempDir()
	lines := []string{
		userChunk(50, 0),
		envLine(100, usageTurnUpdate(1000, 100, 1100, 800, 0, 0, nil)),
		userChunk(150, 1),
		envLine(200, usageTurnUpdate(2000, 200, 2200, 1500, 0, 0, nil)), // 死分支
		rewindMarkerLine(1),
		userChunk(250, 2),
		envLine(300, usageTurnUpdate(3000, 300, 3300, 2500, 0, 0, nil)),
	}
	writeSessionFile(t, home, "/ws", "s1", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	rep, err := b.UsageReport(t.Context(), "/ws", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// rewind 掉的回合是真实消耗过的 token（与任务时间线的会话视图语义
	// 不同），全部计入：turns=3 input=1000+2000+3000。
	if rep.Total.Turns != 3 || rep.Total.InputTokens != 6000 {
		t.Fatalf("total = %+v, want turns=3 input=6000 (rewind branch counted)", rep.Total)
	}
}

// ── 扫描范围：cwd / sessionId / 全部 ────────────────────────────────

func TestUsageReportScope(t *testing.T) {
	home := t.TempDir()
	writeSessionFile(t, home, "/ws1", "s1", []string{envLine(100, usageTurnUpdate(1000, 100, 1100, 800, 0, 0, nil))})
	writeSessionFile(t, home, "/ws1", "s2", []string{envLine(100, usageTurnUpdate(2000, 200, 2200, 1500, 0, 0, nil))})
	writeSessionFile(t, home, "/ws2", "s3", []string{envLine(100, usageTurnUpdate(3000, 300, 3300, 2500, 0, 0, nil))})

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})

	// 仅 cwd → 该工作区所有会话。
	rep, err := b.UsageReport(t.Context(), "/ws1", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Sessions != 2 || rep.Total.InputTokens != 3000 {
		t.Fatalf("cwd scope = sessions=%d input=%d, want 2/3000", rep.Sessions, rep.Total.InputTokens)
	}

	// cwd + sessionId → 单个会话。
	rep1, err := b.UsageReport(t.Context(), "/ws1", "s2", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep1.Sessions != 1 || rep1.Total.InputTokens != 2000 {
		t.Fatalf("session scope = sessions=%d input=%d, want 1/2000", rep1.Sessions, rep1.Total.InputTokens)
	}

	// 全空 → 全部会话。
	repAll, err := b.UsageReport(t.Context(), "", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if repAll.Sessions != 3 || repAll.Total.InputTokens != 6000 {
		t.Fatalf("all scope = sessions=%d input=%d, want 3/6000", repAll.Sessions, repAll.Total.InputTokens)
	}

	// 不存在的 session → 空报告（不报错）。
	repNone, err := b.UsageReport(t.Context(), "/ws1", "nope", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if repNone.Sessions != 0 || repNone.Total.Turns != 0 {
		t.Fatalf("missing session = %+v, want empty", repNone)
	}
}

// ── 毫秒窗口归一化 ──────────────────────────────────────────────────

func TestUsageReportWindowMsCompat(t *testing.T) {
	home := t.TempDir()
	// 事件时间戳用真实 epoch 秒（1.7e9 级别），窗口用毫秒（1.7e12 级别）。
	lines := []string{
		envLine(1_700_000_100, usageTurnUpdate(1000, 100, 1100, 800, 0, 0, nil)),
		envLine(1_700_000_200, usageTurnUpdate(2000, 200, 2200, 1500, 0, 0, nil)),
		envLine(1_700_000_300, usageTurnUpdate(3000, 300, 3300, 2500, 0, 0, nil)),
	}
	writeSessionFile(t, home, "/ws", "s1", lines)

	b := NewBridge(GrokConfig{Bin: "grok", HostID: "h", HostName: "host", GrokHome: home})
	// from/to 传毫秒（…150_000 / …250_000 ms）→ 归一化为秒。
	rep, err := b.UsageReport(t.Context(), "/ws", "", 1_700_000_150_000, 1_700_000_250_000)
	if err != nil {
		t.Fatal(err)
	}
	if rep.From != 1_700_000_150 || rep.To != 1_700_000_250 {
		t.Fatalf("window = [%d, %d], want [1700000150, 1700000250]", rep.From, rep.To)
	}
	if rep.Total.Turns != 1 || rep.Total.InputTokens != 2000 {
		t.Fatalf("total = %+v, want the middle event only", rep.Total)
	}
}
