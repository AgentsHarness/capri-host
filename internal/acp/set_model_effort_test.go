package acp

import (
	"context"
	"encoding/json"
	"strings"
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

// 档位切换成功：两条 rail 都不产生 warning。
func TestSetModelEffortSuccessNoWarning(t *testing.T) {
	b, w := sessionWithCatalog([]any{
		effortModel("m1", map[string]any{"id": "high", "value": "high"}),
	}, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "high")
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{"configOptions": []any{}}) // model
	resolveLine(t, b, w, 1, map[string]any{"configOptions": []any{}}) // reasoning_effort
	cr := <-done
	if cr.err != nil {
		t.Fatalf("SetModel error: %v", cr.err)
	}
	if cr.res != "" {
		t.Errorf("warning = %q, want empty on full success", cr.res)
	}
}

// B：模型切换成功、档位被拒不能静默——必须返回非空 warning 且不返回 error
// （HTTP 层仍 200，前端据 warning 弹 toast）。
func TestSetModelEffortFailureReturnsWarning(t *testing.T) {
	b, w := sessionWithCatalog([]any{
		effortModel("m1", map[string]any{"id": "high", "value": "high"}),
	}, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "high")
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{"configOptions": []any{}}) // model 成功
	// reasoning_effort 返回 JSON-RPC 错误 → 档位失败但模型已切换
	resolveLineWithError(t, b, w, 1, &RPCError{Code: -32602, Msg: "unknown reasoning_effort value"})
	cr := <-done
	if cr.err != nil {
		t.Fatalf("effort failure must not be a hard error: %v", cr.err)
	}
	war, _ := cr.res.(string)
	if war == "" {
		t.Fatal("warning must be non-empty when the effort did not apply")
	}
	for _, want := range []string{"high", "未生效"} {
		if !strings.Contains(war, want) {
			t.Errorf("warning = %q, want it to mention %q", war, want)
		}
	}
}

// C：目录里 id 与 value 不同（{id:"med", value:"medium"}）时，档位 rail 必须
// 发 id；session/set_model 回退 rail 的 `_meta.reasoningEffort` 必须发 value。
func TestSetModelEffortTranslatesIDAndValue(t *testing.T) {
	avail := []any{effortModel("m1",
		map[string]any{"id": "med", "value": "medium"},
	)}
	b, w := sessionWithCatalog(avail, "m1")

	done := make(chan callResult, 1)
	go func() {
		war, err := b.SetModel(context.Background(), "s1", "m1", "medium")
		done <- callResult{war, err}
	}()
	resolveLine(t, b, w, 0, map[string]any{"configOptions": []any{}})
	resolveLine(t, b, w, 1, map[string]any{"configOptions": []any{}})
	if cr := <-done; cr.err != nil || cr.res != "" {
		t.Fatalf("SetModel = (%v, %v), want clean success", cr.res, cr.err)
	}

	// 第一条：configId=model；第二条：configId=reasoning_effort，value 必须是 id
	params := func(n int) map[string]any {
		w.mu.Lock()
		defer w.mu.Unlock()
		var msg map[string]any
		_ = json.Unmarshal(w.lines[n], &msg)
		p, _ := msg["params"].(map[string]any)
		return p
	}
	if got := params(1)["configId"]; got != "reasoning_effort" {
		t.Fatalf("line 1 configId = %v, want reasoning_effort", got)
	}
	if got := params(1)["value"]; got != "med" {
		t.Errorf("config_option value = %v, want the option id \"med\" (agent resolves by id)", got)
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
