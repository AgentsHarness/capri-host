package acp

import (
	"reflect"
	"testing"
	"time"
)

// model_changed kind：roster models 缓存更新（currentModelId +
// reasoningEffort）+ models_update / model / typed model_changed 事件广播；
// kind modeled → 无 generic session_notification。
func TestModelChangedKindUpdatesRoster(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId": "grok-3",
		"availableModels": []any{
			map[string]any{"modelId": "grok-3", "name": "Grok 3"},
			map[string]any{"modelId": "grok-4", "name": "Grok 4"},
		},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":    "model_changed",
		"model_id":         "grok-4",
		"reasoning_effort": "high",
	}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var sawModelsUpdate, sawModel, sawTyped bool
	var events []Event
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			events = append(events, ev)
			switch ev["type"] {
			case "session_notification":
				t.Errorf("unexpected generic session_notification for modeled kind model_changed: %v", ev)
			case "model_changed":
				sawTyped = true
				if !reflect.DeepEqual(ev["update"], update) {
					t.Errorf("typed update = %v, want the original update verbatim", ev["update"])
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
				}
			case "models_update":
				sawModelsUpdate = true
				models, _ := ev["params"].(map[string]any)
				if models["currentModelId"] != "grok-4" {
					t.Errorf("models_update currentModelId = %v, want grok-4", models["currentModelId"])
				}
				if models["reasoningEffort"] != "high" {
					t.Errorf("models_update reasoningEffort = %v, want high", models["reasoningEffort"])
				}
			case "model":
				sawModel = true
				if ev["modelId"] != "grok-4" || ev["modelName"] != "Grok 4" || ev["reasoningEffort"] != "high" {
					t.Errorf("model event = %v, want modelId grok-4 / modelName Grok 4 / reasoningEffort high", ev)
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("model sessionId = %v, want s1", ev["sessionId"])
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawModelsUpdate || !sawModel {
		t.Errorf("sawModelsUpdate=%v sawModel=%v", sawModelsUpdate, sawModel)
	}
	if !sawTyped {
		t.Error("model_changed kind must broadcast a typed event (the FE scrollback line's only source)")
	}
	b.mu.Lock()
	models, _ := b.sessions["s1"].models.(map[string]any)
	b.mu.Unlock()
	if models["currentModelId"] != "grok-4" {
		t.Errorf("roster cache currentModelId = %v, want grok-4", models["currentModelId"])
	}
}

// agent 的 model_changed 携带 context_window_selection（会话自己的窗口选择）
// 时，缓存目录与 model 事件都要跟上：FE 的窗口 chips 高亮与上下文芯片
// 都以这份目录为真源。全程没有该字段时不得伪造一个（Preserve 语义）。
func TestModelChangedKindCarriesContextWindow(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId": "grok-3",
		"availableModels": []any{
			map[string]any{"modelId": "grok-3", "name": "Grok 3"},
			map[string]any{"modelId": "grok-4", "name": "Grok 4", "_meta": map[string]any{}},
		},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()

	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": map[string]any{
		"sessionUpdate":            "model_changed",
		"model_id":                 "grok-4",
		"context_window_selection": float64(500000),
	}})

	var sawContextWindow bool
	deadline := time.Now().Add(300 * time.Millisecond)
	for !sawContextWindow && time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev["type"] == "model" && ev["contextWindow"] == uint64(500000) {
				sawContextWindow = true
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawContextWindow {
		t.Error("model event must carry contextWindow from context_window_selection")
	}

	b.mu.Lock()
	meta, _ := b.sessions["s1"].models.(map[string]any)["availableModels"].([]any)[1].(map[string]any)["_meta"].(map[string]any)
	got := meta["contextWindow"]
	b.mu.Unlock()
	if got != uint64(500000) {
		t.Errorf("cached meta contextWindow = %v, want 500000", got)
	}
}

// model_auto_switched kind（previous_model_id/new_model_id/reason）：
// currentModelId 更新；无 reasoningEffort 时保留原 effort（
// patchSessionModels 语义）。model 事件不带 reasoningEffort 键。
// kind modeled → 无 generic session_notification；另发一条 typed
// `model_auto_switched`（FE 提示行的唯一来源，见 HandleModelSwitchKind）。
func TestModelAutoSwitchedKindUpdatesRoster(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "grok-3",
		"reasoningEffort": "medium",
		"availableModels": []any{
			map[string]any{"modelId": "grok-3", "name": "Grok 3"},
			map[string]any{"modelId": "grok-5", "name": "Grok 5"},
		},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":     "model_auto_switched",
		"previous_model_id": "grok-3",
		"new_model_id":      "grok-5",
		"reason":            "grok-3 不可用",
	}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var sawModelsUpdate, sawModel, sawAutoSwitched bool
	var events []Event
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			events = append(events, ev)
			switch ev["type"] {
			case "session_notification":
				t.Errorf("unexpected generic session_notification for modeled kind model_auto_switched: %v", ev)
			case "model_auto_switched":
				sawAutoSwitched = true
				if !reflect.DeepEqual(ev["update"], update) {
					t.Errorf("typed update = %v, want the original update verbatim", ev["update"])
				}
				if ev["sessionId"] != "s1" {
					t.Errorf("typed sessionId = %v, want s1", ev["sessionId"])
				}
			case "models_update":
				sawModelsUpdate = true
				models, _ := ev["params"].(map[string]any)
				if models["currentModelId"] != "grok-5" {
					t.Errorf("models_update currentModelId = %v, want grok-5", models["currentModelId"])
				}
				if models["reasoningEffort"] != "medium" {
					t.Errorf("models_update reasoningEffort = %v, want medium preserved", models["reasoningEffort"])
				}
			case "model":
				sawModel = true
				if ev["modelId"] != "grok-5" || ev["modelName"] != "Grok 5" {
					t.Errorf("model event = %v, want modelId grok-5 / modelName Grok 5", ev)
				}
				if _, ok := ev["reasoningEffort"]; ok {
					t.Errorf("model event must not carry reasoningEffort: %v", ev)
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawModelsUpdate || !sawModel {
		t.Errorf("sawModelsUpdate=%v sawModel=%v", sawModelsUpdate, sawModel)
	}
	if !sawAutoSwitched {
		t.Error("model_auto_switched kind must broadcast a typed event (the FE notice's only source)")
	}
	b.mu.Lock()
	models, _ := b.sessions["s1"].models.(map[string]any)
	b.mu.Unlock()
	if models["currentModelId"] != "grok-5" {
		t.Errorf("roster cache currentModelId = %v, want grok-5", models["currentModelId"])
	}
}

// model_auto_switched 无同族兜底时 new_model_id 为空：handleModelSwitchKind
// 提前返回（不 patch 目录、不发 model），typed 事件仍要带上 reason，否则
// web 端只会看到「消息发出去、没有回复」。
func TestModelAutoSwitchedWithoutFallbackStillNotifies(t *testing.T) {
	b, _ := metaReadyBridge(t)
	b.mu.Lock()
	b.sessions["s1"].models = map[string]any{
		"currentModelId":  "gone-model",
		"availableModels": []any{map[string]any{"modelId": "grok-5", "name": "Grok 5"}},
	}
	b.mu.Unlock()
	ch, unsub := b.Subscribe()
	defer unsub()
	update := map[string]any{
		"sessionUpdate":     "model_auto_switched",
		"previous_model_id": "gone-model",
		"new_model_id":      "",
		"reason":            "Model \"gone-model\" is no longer available. Please start a new session.",
	}
	b.handleSessionUpdate(map[string]any{"sessionId": "s1", "update": update})
	var sawAutoSwitched bool
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev["type"] == "model_auto_switched" {
				sawAutoSwitched = true
				if ev["update"].(map[string]any)["reason"] == "" {
					t.Errorf("typed update lost reason: %v", ev["update"])
				}
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !sawAutoSwitched {
		t.Error("blocked-session auto-switch must still reach the client")
	}
}
