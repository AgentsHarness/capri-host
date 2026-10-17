package acp

import (
	"context"
	"encoding/json"
	"testing"
)

// sessionWithCatalog returns a ready bridge whose active session carries a
// model catalog, so resolveEffortToken has something to resolve against.
func sessionWithCatalog(avail []any, current string) (*Bridge, *recordingStdin) {
	b, w := readyBridge()
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  current,
		"availableModels": avail,
	}
	b.mu.Unlock()
	return b, w
}

func effortModel(id string, efforts ...map[string]any) map[string]any {
	list := make([]any, 0, len(efforts))
	for _, e := range efforts {
		list = append(list, e)
	}
	return map[string]any{"modelId": id, "_meta": map[string]any{"reasoningEfforts": list}}
}

// cachedModelMeta returns the cached catalog entry's `_meta` for modelID.
func cachedModelMeta(t *testing.T, b *Bridge, modelID string) map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	models, _ := b.sessions["s1"].models.(map[string]any)
	avail, _ := models["availableModels"].([]any)
	for _, raw := range avail {
		mm, _ := raw.(map[string]any)
		if id, _ := mm["modelId"].(string); id != modelID {
			continue
		}
		meta, _ := mm["_meta"].(map[string]any)
		return meta
	}
	t.Fatalf("model %s not in cached catalog", modelID)
	return nil
}

// 一次 session/set_model 带上模型与档位，成功时没有 warning。
func TestSetModelEffortSuccessNoWarning(t *testing.T) {
	b, w := sessionWithCatalog([]any{
		effortModel("m1", map[string]any{"id": "high", "value": "high"}),
	}, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "high", 0)
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("SetModel error: %v", cr.err)
	}
	if cr.res != "" {
		t.Errorf("warning = %q, want empty on full success", cr.res)
	}
}

// session/set_model 被拒绝是整次失败，不能当成「模型已切、档位未生效」的 warning。
func TestSetModelFailureIsError(t *testing.T) {
	b, w := sessionWithCatalog([]any{
		effortModel("m1", map[string]any{"id": "high", "value": "high"}),
	}, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "high", 0)
		done <- callResult{war, err}
	}()
	resolveLineWithError(t, b, w, 0, &RPCError{Code: -32602, Msg: "unknown model"})
	cr := <-done
	if cr.err == nil {
		t.Fatal("set_model failure must be an error")
	}
	if cr.res != "" {
		t.Errorf("warning = %q, want empty when the call failed", cr.res)
	}
}

// `_meta.reasoningEffort` 发目录里的 canonical value，不发 option id。
// contextWindow 是正整数 token 数。
func TestSetModelEffortTranslatesIDAndValue(t *testing.T) {
	avail := []any{effortModel("m1",
		map[string]any{"id": "med", "value": "medium"},
	)}
	b, w := sessionWithCatalog(avail, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "medium", 500000)
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{})
	if cr := <-done; cr.err != nil || cr.res != "" {
		t.Fatalf("SetModel = (%v, %v), want clean success", cr.res, cr.err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.lines) != 1 {
		t.Fatalf("wrote %d requests, want one session/set_model", len(w.lines))
	}
	var msg map[string]any
	if err := json.Unmarshal(w.lines[0], &msg); err != nil {
		t.Fatal(err)
	}
	if msg["method"] != "session/set_model" {
		t.Fatalf("method = %v, want session/set_model", msg["method"])
	}
	p, _ := msg["params"].(map[string]any)
	if p["modelId"] != "m1" || p["sessionId"] != "s1" {
		t.Fatalf("params = %v", p)
	}
	meta, _ := p["_meta"].(map[string]any)
	if meta["reasoningEffort"] != "medium" {
		t.Errorf("reasoningEffort = %v, want canonical value \"medium\"", meta["reasoningEffort"])
	}
	if got, _ := meta["contextWindow"].(float64); got != 500000 {
		t.Errorf("contextWindow = %v, want 500000", meta["contextWindow"])
	}
}

// 目录里没有该 id/value（未知档位）时原样透传：agent 的拒绝比静默改写更可信。
func TestResolveEffortTokenUnknownPassesThrough(t *testing.T) {
	b, _ := sessionWithCatalog([]any{
		effortModel("m1", map[string]any{"id": "high", "value": "high"}),
	}, "m1")
	id, value := b.resolveEffortToken("s1", "bogus")
	if id != "bogus" || value != "bogus" {
		t.Errorf("resolveEffortToken(bogus) = (%q, %q), want passthrough", id, value)
	}
}

// 切换窗口后，host 缓存的目录 meta 必须跟着走到会话选择上：FE 的菜单
// 与上下文芯片都以这份目录为唯一真源，不 patch 就会显示切换前的窗口
// （ModelState::get_context_window 优先读模型 meta 的 contextWindow）。
func TestSetModelPatchesCachedContextWindow(t *testing.T) {
	avail := []any{effortModel("m1", map[string]any{"id": "high", "value": "high"})}
	b, w := sessionWithCatalog(avail, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "", 500000)
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{})
	if cr := <-done; cr.err != nil {
		t.Fatalf("SetModel error: %v", cr.err)
	}

	if got := cachedModelMeta(t, b, "m1")["contextWindow"]; got != uint64(500000) {
		t.Errorf("cached meta contextWindow = %v, want 500000", got)
	}
}

// 不带窗口的切换（普通切模型）是 agent 的 Preserve 语义：缓存里已有的
// 窗口选择必须原样保留，不能被清成「默认」。
func TestSetModelWithoutWindowKeepsCachedSelection(t *testing.T) {
	avail := []any{map[string]any{
		"modelId": "m1",
		"_meta":   map[string]any{"contextWindow": uint64(500000)},
	}}
	b, w := sessionWithCatalog(avail, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "", 0)
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{})
	if cr := <-done; cr.err != nil {
		t.Fatalf("SetModel error: %v", cr.err)
	}

	if got := cachedModelMeta(t, b, "m1")["contextWindow"]; got != uint64(500000) {
		t.Errorf("cached meta contextWindow = %v, want the pre-switch 500000", got)
	}
}

// 上下文窗口只在带上时才进 `_meta`：0 表示不指定（保持会话选择），
// 不能写成 contextWindow:0 —— 那会被 agent 当成非法值而整次拒收。
func TestSetModelOmitsZeroContextWindow(t *testing.T) {
	b, w := sessionWithCatalog([]any{
		effortModel("m1", map[string]any{"id": "high", "value": "high"}),
	}, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "", 0)
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{})
	if cr := <-done; cr.err != nil {
		t.Fatalf("SetModel error: %v", cr.err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	var msg map[string]any
	if err := json.Unmarshal(w.lines[0], &msg); err != nil {
		t.Fatal(err)
	}
	p, _ := msg["params"].(map[string]any)
	if meta, ok := p["_meta"].(map[string]any); ok {
		if _, present := meta["contextWindow"]; present {
			t.Errorf("_meta = %v, want no contextWindow key when none was requested", meta)
		}
	}
}
