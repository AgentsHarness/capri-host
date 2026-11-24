package acp

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// 桥级回归：detail 缺省 / full / 未知值 → 与今天的响应逐字节相等，且响应
// 里不存在 lite 痕迹、不回显 projected。
func TestSessionUpdatesDetailFullIsByteIdentical(t *testing.T) {
	f := liteFixtureOf(t)
	home := t.TempDir()
	sid := "sess-1"
	writeSessionFile(t, home, "/ws", sid, f.lines)
	ctx := context.Background()

	b, w := historyBridge(t, home)
	base, err := b.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatalf("基线: %v", err)
	}
	if len(w.lines) != 0 {
		t.Fatal("基线应走本地分页（夹具带 _meta）")
	}
	baseRaw := mustJSON(t, base)
	for _, detail := range []string{"", DetailFull, "LITE", "none", "meta "} {
		b2, _ := historyBridge(t, home)
		got, err := b2.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: detail})
		if err != nil {
			t.Fatalf("detail=%q: %v", detail, err)
		}
		if got.Projected != "" || got.OmittedBytes != 0 {
			t.Errorf("detail=%q 回显了投影: %q/%d", detail, got.Projected, got.OmittedBytes)
		}
		raw := mustJSON(t, got)
		if !reflect.DeepEqual(raw, baseRaw) {
			t.Errorf("detail=%q 与今天的响应不逐字节相等（%d vs %d 字节）", detail, len(raw), len(baseRaw))
		}
		if strings.Contains(string(raw), `"lite"`) {
			t.Errorf("detail=%q 响应里出现了 lite 标记", detail)
		}
	}
}

// 本地分页 + lite：骨架字段与 full 完全一致，工具正文被裁。
func TestSessionUpdatesLiteLocalPageMatchesFull(t *testing.T) {
	f := liteFixtureOf(t)
	home := t.TempDir()
	sid := "sess-1"
	writeSessionFile(t, home, "/ws", sid, f.lines)
	ctx := context.Background()

	b, _ := historyBridge(t, home)
	full, err := b.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	b2, w2 := historyBridge(t, home)
	lite, err := b2.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: DetailLite, TurnIndex: intPtr(2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(w2.lines) != 0 {
		t.Error("lite 本地命中时不该问 agent")
	}
	if lite.Projected != DetailLite || lite.OmittedBytes <= 0 {
		t.Errorf("回显 = %q/%d, want lite/正数", lite.Projected, lite.OmittedBytes)
	}
	if len(lite.Updates) != len(full.Updates) {
		t.Fatalf("夹具条数 = %d, want %d（无合成对象）", len(lite.Updates), len(full.Updates))
	}
	if got, want := pageSeqs(t, lite), pageSeqs(t, full); !reflect.DeepEqual(got, want) {
		t.Errorf("msgSeq = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(lite.PromptStarts, full.PromptStarts) || lite.TotalCount != full.TotalCount || lite.HasMore != full.HasMore {
		t.Errorf("锚点被改: promptStarts %v vs %v, total %d vs %d, hasMore %v vs %v",
			lite.PromptStarts, full.PromptStarts, lite.TotalCount, full.TotalCount, lite.HasMore, full.HasMore)
	}
	if got, want := liteSkeleton(t, lite.Updates), liteSkeleton(t, full.Updates); !reflect.DeepEqual(got, want) {
		t.Error("lite 页的骨架与 full 页不同")
	}
	if n, want := len(mustJSON(t, lite.Updates)), len(mustJSON(t, full.Updates)); n >= want {
		t.Errorf("lite 页 %d 字节没有比 full 页 %d 小", n, want)
	}
	// 同一个 bridge 上再来一次全量：投影不影响后续请求（信封每次现读，
	// 归一化缓存里只有行级元数据）。
	again, err := b2.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if a, bRaw := mustJSON(t, full), mustJSON(t, again); !reflect.DeepEqual(a, bRaw) {
		t.Error("投影污染了后续的全量请求（信封不是现读的）")
	}
}

// 回退路径（_x.ai/session/updates 透传）同样投影，且 detail 不下发 agent。
func TestSessionUpdatesLitePassthroughAlsoProjected(t *testing.T) {
	home := t.TempDir()
	sid := "sess-1"
	// 唯一一行信封缺 params._meta.agentTimestampMs → 本地不可用 → 透传。
	writeSessionFile(t, home, "/ws", sid, []string{
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
	})
	lines := liteFixtureOf(t).lines
	page := map[string]any{
		"updates":      litePageOf(t, lines),
		"totalCount":   float64(len(lines)),
		"hasMore":      true,
		"promptStarts": []any{float64(0), float64(9)},
	}
	b, w := historyBridge(t, home)
	ctx := context.Background()
	var got UpdatesPage
	var err error
	off := int64(0)
	runResolved(t, b, w, page, func() error {
		got, err = b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: DetailLite, Offset: &off})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	method, params := lastRequestParams(t, w)
	if method != "_x.ai/session/updates" {
		t.Fatalf("method = %v, want 透传", method)
	}
	if _, ok := params["detail"]; ok {
		t.Errorf("detail 不该下发 agent: %v", params)
	}
	if got.Projected != DetailLite || got.OmittedBytes <= 0 {
		t.Errorf("透传路径回显 = %q/%d, want lite/正数", got.Projected, got.OmittedBytes)
	}
	if _, ok := liteStampOf(t, got.Updates, 2); !ok {
		t.Error("透传路径的工具体没被投影")
	}
	if want := liteSkeleton(t, litePageOf(t, lines)); !reflect.DeepEqual(liteSkeleton(t, got.Updates), want) {
		t.Error("透传路径的 lite 页骨架与 full 不同")
	}
	if !reflect.DeepEqual(got.PromptStarts, []int{0, 9}) || got.TotalCount != len(lines) || !got.HasMore {
		t.Errorf("锚点被改: %v %d %v", got.PromptStarts, got.TotalCount, got.HasMore)
	}
}

// meta 档：不回 updates（本地与透传两条出口都不回），锚点语义不变。
func TestSessionUpdatesMetaModeDropsUpdates(t *testing.T) {
	f := liteFixtureOf(t)
	home := t.TempDir()
	sid := "sess-1"
	writeSessionFile(t, home, "/ws", sid, f.lines)
	ctx := context.Background()

	b, w := historyBridge(t, home)
	meta, err := b.SessionUpdates(ctx, sid, "/ws", SessionUpdatesOpts{Detail: DetailMeta})
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Error("meta 档本地可用时不该问 agent（信封一行都不必读）")
	}
	if meta.Updates != nil {
		t.Errorf("meta 档仍回了 %d 条信封", len(meta.Updates))
	}
	if meta.Projected != DetailMeta {
		t.Errorf("projected = %q, want meta", meta.Projected)
	}
	b2, _ := historyBridge(t, home)
	full, err := b2.SessionUpdates(ctx, sid, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TotalCount != full.TotalCount || meta.HasMore != full.HasMore || !reflect.DeepEqual(meta.PromptStarts, full.PromptStarts) {
		t.Errorf("锚点不等: total %d vs %d, hasMore %v vs %v, promptStarts %v vs %v",
			meta.TotalCount, full.TotalCount, meta.HasMore, full.HasMore, meta.PromptStarts, full.PromptStarts)
	}
	if !reflect.DeepEqual(meta.Btw, full.Btw) {
		t.Errorf("btw 语义变了: %v vs %v", meta.Btw, full.Btw)
	}

	// 透传出口的 meta：agent 照旧回信封，host 丢弃。
	lines := liteFixtureOf(t).lines
	page := map[string]any{
		"updates":      litePageOf(t, lines),
		"totalCount":   float64(len(lines)),
		"hasMore":      false,
		"promptStarts": []any{float64(0)},
	}
	writeSessionFile(t, home, "/other", sid, []string{
		`{"timestamp":1,"method":"session/update","params":{"sessionId":"sess-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"b"}}}}`,
	})
	b3, w3 := historyBridge(t, home)
	var got UpdatesPage
	runResolved(t, b3, w3, page, func() error {
		var err error
		got, err = b3.SessionUpdates(ctx, sid, "/other", SessionUpdatesOpts{Detail: DetailMeta})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Updates != nil || got.Projected != DetailMeta {
		t.Errorf("透传 meta: updates=%v projected=%q", got.Updates, got.Projected)
	}
	if got.TotalCount != len(lines) || !reflect.DeepEqual(got.PromptStarts, []int{0}) {
		t.Errorf("透传 meta 锚点被改: %d %v", got.TotalCount, got.PromptStarts)
	}
	if method, _ := lastRequestParams(t, w3); method != "_x.ai/session/updates" {
		t.Errorf("meta 档仍该走同一条透传，method = %v", method)
	}
}

// stream=true 不参与投影：响应里没有可裁的页，也不回显 projected。
func TestSessionUpdatesStreamIgnoresDetail(t *testing.T) {
	b, w := metaReadyBridge(t)
	ctx := context.Background()
	var got UpdatesPage
	var err error
	runResolved(t, b, w, map[string]any{"totalCount": float64(3), "chunkCount": float64(1)}, func() error {
		got, err = b.SessionUpdates(ctx, "s1", "/ws", SessionUpdatesOpts{Stream: true, Detail: DetailLite})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Projected != "" || got.OmittedBytes != 0 {
		t.Errorf("stream 页回显了投影: %q/%d", got.Projected, got.OmittedBytes)
	}
	method, params := lastRequestParams(t, w)
	if method != "_x.ai/session/updates" || params["stream"] != true {
		t.Errorf("stream 请求被改: %v %v", method, params)
	}
	if _, ok := params["detail"]; ok {
		t.Errorf("detail 不该下发 agent: %v", params)
	}
}
