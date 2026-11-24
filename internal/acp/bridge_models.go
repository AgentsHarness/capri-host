package acp

import (
	"context"
	"errors"
	"time"
)

// handleModelSwitchKind applies model_auto_switched / model_changed
// (extensions/notification.rs SessionUpdate::ModelAutoSwitched /
// ModelChanged; 字段名以 capri-fe chat.ts:3242-3260 与 grok 源码为准):
//   - model_changed:       model_id/modelId + reasoning_effort/reasoningEffort
//   - model_auto_switched: previous_model_id/new_model_id/reason（无 effort）
//
// 更新 roster 会话的 models 缓存（currentModelId 置为新模型 id；有
// reasoningEffort 时同步，patchSessionModels 语义，bridge.go 2099-2187
// 附近），然后广播 `models_update`（形状与现有一致：{type:"models_update",
// params: <session 的 models>, sessionId}）与 `model`（形状与 SetModel
// 一致：{type:"model", modelId, modelName, reasoningEffort, sessionId}；
// modelName 用 modelDisplayName 解析，空字符串省略）。这两个广播即该
// kind 的 typed 语义事件（modeled → 无 generic session_notification）。
func (b *Bridge) handleModelSwitchKind(sid string, update map[string]any, tag func(Event) Event) {
	// 新模型 id：model_changed 走 model_id/modelId，model_auto_switched
	// 走 new_model_id/newModelId。
	id := ""
	for _, k := range []string{"model_id", "modelId", "new_model_id", "newModelId"} {
		if v, ok := update[k].(string); ok && v != "" {
			id = v
			break
		}
	}
	effort := ""
	for _, k := range []string{"reasoning_effort", "reasoningEffort"} {
		if v, ok := update[k].(string); ok && v != "" {
			effort = v
			break
		}
	}
	// The session's own window pick (0 = not named, keep the cached one).
	// `model_changed` carries it as context_window_selection, and the agent
	// only omits it when the session never picked one.
	var contextWindow uint64
	for _, k := range []string{"context_window_selection", "contextWindowSelection"} {
		if v, ok := update[k].(float64); ok && v > 0 {
			contextWindow = uint64(v)
			break
		}
	}
	if id == "" {
		// 字段缺失/为空：无可应用的状态（kind modeled，无 generic）。
		return
	}
	b.mu.Lock()
	var models any
	if s := b.sessions[sid]; s != nil {
		b.patchSessionModelsWindow(s, id, effort, contextWindow)
		models = s.models
	}
	b.mu.Unlock()
	if models != nil {
		b.Broadcast(tag(Event{kType: "models_update", kParams: models}))
	}
	ev := Event{kType: "model", "modelId": id}
	if name := modelDisplayName(models, id); name != "" {
		ev["modelName"] = name
	}
	if effort != "" {
		ev["reasoningEffort"] = effort
	}
	if contextWindow > 0 {
		ev["contextWindow"] = contextWindow
	}
	b.Broadcast(tag(ev))
}

// SetMode calls session/set_mode on the given session (empty sessionId
// resolves to the active one).
func (b *Bridge) SetMode(ctx context.Context, sessionID, modeID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, errors.New("没有活跃会话")
	}
	res, err := b.request(ctx, "session/set_mode", map[string]any{
		kSessionID: sessionID,
		"modeId":   modeID,
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	var modes any
	if act := b.sessions[sessionID]; act != nil {
		mm, ok := act.modes.(map[string]any)
		if !ok || mm == nil {
			mm = map[string]any{}
			act.modes = mm
		}
		mm["currentModeId"] = modeID
		modes = cloneAny(act.modes)
	} else {
		modes = map[string]any{"currentModeId": modeID}
	}
	b.mu.Unlock()
	b.Broadcast(Event{
		kType:      "modes_update",
		"modes":    modes,
		kSessionID: sessionID,
	})
	return res, nil
}

// SetConfigOption calls the official ACP session/set_config_option method.
// Supports configId="model" (value=modelId) and configId="reasoning_effort" (value=effort).
// Returns the updated configOptions array/object from the agent.
func (b *Bridge) SetConfigOption(ctx context.Context, sessionID, configID, value string) (any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if sessionID == "" {
		return nil, &HTTPError{Code: 400, Msg: "需要 sessionId"}
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return nil, errors.New("没有活跃会话")
	}
	params := map[string]any{
		kSessionID: sessionID,
		"configId": configID,
		"value":    value,
	}
	resp, err := b.request(ctx, "session/set_config_option", params, 30*time.Second)
	if err != nil {
		return nil, err
	}

	// Update cached configOptions and models for the session
	var configOpts any
	if resp != nil {
		if co, ok := resp[kConfigOptions]; ok {
			configOpts = co
		} else if co, ok := resp["options"]; ok {
			configOpts = co
		}
	}

	b.mu.Lock()
	var models any
	var name string
	var modelID string
	var effort string
	if act := b.sessions[sessionID]; act != nil {
		if configOpts != nil {
			act.configOpts = configOpts
		}
		if m, ok := act.models.(map[string]any); ok {
			if cm, ok := m["currentModelId"].(string); ok {
				modelID = cm
			}
			if re, ok := m["reasoningEffort"].(string); ok {
				effort = re
			}
		}
		if configID == "model" {
			modelID = value
			b.patchSessionModels(act, modelID, effort)
		} else if configID == "reasoning_effort" {
			effort = value
			b.patchSessionModels(act, modelID, effort)
		}
		models = act.models
		name = modelDisplayName(models, modelID)
	}
	b.mu.Unlock()

	if configOpts != nil {
		b.Broadcast(Event{
			kType:          "config_options_update",
			kConfigOptions: configOpts,
			kSessionID:     sessionID,
		})
	}
	if models != nil {
		b.Broadcast(Event{kType: "models_update", kParams: models, kSessionID: sessionID})
	}
	if modelID != "" {
		b.Broadcast(Event{
			kType:             "model",
			"modelId":         modelID,
			"modelName":       name,
			"reasoningEffort": effort,
			kSessionID:        sessionID,
		})
	}
	b.broadcastRosterChange()
	return resp, nil
}

// SetModel switches the session with the agent's own client call,
// session/set_model. Reasoning effort and context window ride in `_meta`
// (`reasoningEffort` is the catalog's canonical value; `contextWindow` is a
// positive token count). Omitting either leaves the session's current
// selection in place. contextWindow == 0 means "do not send".
//
// The warning return stays so HTTP callers keep a field for a non-fatal
// note. This path has nothing to warn about: the agent applies the model
// and the meta together, and a rejection fails the whole call.
func (b *Bridge) SetModel(ctx context.Context, sessionID, modelID, reasoningEffort string, contextWindow uint64) (string, error) {
	if err := b.Boot(ctx); err != nil {
		return "", err
	}
	// 无 sessionId 直接拒绝，绝不回退到 active 会话：FE 空状态（未锚定）
	// 下发的切换请求落到别的会话上就失去了会话隔离（同 handleSetDefaultModel
	// 的双动作语义）。
	if sessionID == "" {
		return "", &HTTPError{Code: 400, Msg: "需要 sessionId"}
	}
	if sessionID = b.resolveSessionID(sessionID); sessionID == "" {
		return "", errors.New("没有活跃会话")
	}

	params := map[string]any{
		kSessionID: sessionID,
		"modelId":  modelID,
	}
	meta := map[string]any{}
	effortValue := reasoningEffort
	if reasoningEffort != "" {
		// `_meta.reasoningEffort` parses the canonical value, not an option id.
		_, effortValue = b.resolveEffortToken(sessionID, reasoningEffort)
		meta["reasoningEffort"] = effortValue
	}
	if contextWindow > 0 {
		meta["contextWindow"] = contextWindow
	}
	if len(meta) > 0 {
		params[kMeta] = meta
	}
	if _, err := b.request(ctx, "session/set_model", params, 30*time.Second); err != nil {
		return "", err
	}
	// session/set_model does not reliably refresh the host's cached
	// SessionModelState (hello / ready / status snapshot). Patch it and
	// re-broadcast so every client converges without waiting for a later
	// catalog push.
	b.mu.Lock()
	var models any
	var name string
	if act := b.sessions[sessionID]; act != nil {
		b.patchSessionModelsWindow(act, modelID, effortValue, contextWindow)
		models = act.models
		name = modelDisplayName(models, modelID)
	}
	b.mu.Unlock()
	if models != nil {
		b.Broadcast(Event{kType: "models_update", kParams: models, kSessionID: sessionID})
	}
	b.Broadcast(Event{
		kType:             "model",
		"modelId":         modelID,
		"modelName":       name,
		"reasoningEffort": effortValue,
		kSessionID:        sessionID,
	})
	b.broadcastRosterChange()
	return "", nil
}

// resolveEffortToken maps a client-supplied reasoning effort to the two
// spellings the agent's two rails expect, using the session's cached model
// catalog (each `_meta.reasoningEfforts` entry carries both).
//
// session/set_model `_meta.reasoningEffort` parses the canonical **value**
// (sampling-types parse_reasoning_effort_meta). session/set_config_option
// with configId="reasoning_effort" resolves an option **id** instead.
// SetModel sends the value. The id is still returned for a caller that
// speaks set_config_option directly.
//
// Clients send the canonical value (what the caption shows). For every
// catalog seen so far id == value; a catalog that spells them differently
// (e.g. {id:"med", value:"medium"}) is why this translation exists.
//
// The token is matched against either spelling, so callers may pass whichever
// they hold. Unknown tokens pass through unchanged — the agent rejects them
// and that error is reported rather than silently rewritten.
func (b *Bridge) resolveEffortToken(sessionID, token string) (id, value string) {
	if token == "" {
		return "", ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	act := b.sessions[sessionID]
	if act == nil {
		return token, token
	}
	models, _ := act.models.(map[string]any)
	avail, _ := models["availableModels"].([]any)
	cur, _ := models["currentModelId"].(string)
	// Prefer the current model's menu; fall back to scanning all of them so a
	// switch in flight still resolves.
	for _, pass := range []bool{true, false} {
		for _, raw := range avail {
			mm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if pass {
				if mid, _ := mm["modelId"].(string); mid != cur {
					continue
				}
			}
			meta, _ := mm[kMeta].(map[string]any)
			if meta == nil {
				meta, _ = mm["meta"].(map[string]any)
			}
			list, _ := meta["reasoningEfforts"].([]any)
			for _, e := range list {
				eo, ok := e.(map[string]any)
				if !ok {
					continue
				}
				eid, _ := eo["id"].(string)
				eval, _ := eo["value"].(string)
				if eid == "" || eval == "" {
					continue
				}
				if token == eid || token == eval {
					return eid, eval
				}
			}
		}
	}
	return token, token
}

// patchSessionModels updates a session's cached SessionModelState after a
// successful session/set_model: the current model id, the matching catalog
// entry's default effort, and (host extension) the top-level reasoning
// effort the client selected.
func (b *Bridge) patchSessionModels(s *SessionState, modelID, effort string) {
	b.patchSessionModelsWindow(s, modelID, effort, 0)
}

// patchSessionModelsWindow is patchSessionModels plus the session's picked
// context window. contextWindow == 0 leaves the cached meta's `contextWindow`
// alone (a switch that did not name a window keeps the previous selection —
// the agent's SwitchContextWindow::Preserve).
//
// The FE offers only the windows a model declares in `_meta.contextWindows`,
// and `ModelState::get_context_window` prefers the model meta's
// `contextWindow` over the catalog default, so the two must move together or
// the picker would show the pre-switch window.
func (b *Bridge) patchSessionModelsWindow(s *SessionState, modelID, effort string, contextWindow uint64) {
	models, ok := s.models.(map[string]any)
	if !ok {
		return
	}
	models["currentModelId"] = modelID
	if effort != "" {
		models["reasoningEffort"] = effort
	}
	if avail, ok := models["availableModels"].([]any); ok {
		for _, raw := range avail {
			mm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if id, _ := mm["modelId"].(string); id == modelID {
				if meta, ok := mm[kMeta].(map[string]any); ok {
					if effort != "" {
						meta["reasoningEffort"] = effort
					}
					if contextWindow > 0 {
						meta["contextWindow"] = contextWindow
					}
				}
			}
		}
	}
}

// modelDisplayName resolves a model's display name from a SessionModelState
// catalog ("" when the id is unknown / no catalog).
func modelDisplayName(models any, modelID string) string {
	m, ok := models.(map[string]any)
	if !ok {
		return ""
	}
	avail, _ := m["availableModels"].([]any)
	for _, raw := range avail {
		mm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := mm["modelId"].(string); id == modelID {
			if n, ok := mm["name"].(string); ok {
				return n
			}
		}
	}
	return ""
}

// applyModelsCatalog refreshes a session's cached catalog from a
// machine-wide x.ai/models/update broadcast, keeping the session's current
// model when it is still offered (falling back to the broadcast's current
// otherwise, like the TUI's update_catalog). The session's chosen
// reasoningEffort also survives the refresh when the still-offered model
// keeps listing it — the broadcast carries each model's static default,
// which would otherwise clobber the user's in-session choice (e.g. high
// reverting to the catalog default low on a config.toml reload).
func (b *Bridge) applyModelsCatalog(s *SessionState, incoming map[string]any) {
	models, ok := s.models.(map[string]any)
	if !ok {
		if len(incoming) == 0 {
			return
		}
		models = map[string]any{}
		s.models = models
	}
	if inAvail, ok := incoming["availableModels"].([]any); ok {
		cur, _ := models["currentModelId"].(string)
		if cur != "" {
			still := false
			for _, raw := range inAvail {
				if mm, ok := raw.(map[string]any); ok {
					if id, _ := mm["modelId"].(string); id == cur {
						still = true
						break
					}
				}
			}
			if !still {
				cur = ""
			}
		}
		if cur == "" {
			cur, _ = incoming["currentModelId"].(string)
		}
		models["availableModels"] = inAvail
		models["currentModelId"] = cur
		// Preserve the session's chosen effort when the still-current model
		// still offers it: clear the incoming static-default effort on that
		// model's catalog entry (and any top-level effort copied from it by
		// the agent) so consumers see the session's choice, not the default.
		prevEffort, _ := models["reasoningEffort"].(string)
		if cur != "" && prevEffort != "" && effortStillOffered(inAvail, cur, prevEffort) {
			for _, raw := range inAvail {
				mm, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				if id, _ := mm["modelId"].(string); id != cur {
					continue
				}
				if meta, ok := mm[kMeta].(map[string]any); ok {
					delete(meta, "reasoningEffort")
				}
			}
		} else {
			// Model changed or the effort is no longer offered: the session
			// falls back to the new catalog's default for the current model.
			delete(models, "reasoningEffort")
		}
	} else if c, ok := incoming["currentModelId"].(string); ok {
		// Catalog-less payload: only the current id changed.
		models["currentModelId"] = c
	}
}

// effortStillOffered reports whether the model's refreshed catalog entry
// still lists effort among its reasoningEfforts values/ids.
func effortStillOffered(avail []any, modelID, effort string) bool {
	for _, raw := range avail {
		mm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := mm["modelId"].(string); id != modelID {
			continue
		}
		meta, _ := mm[kMeta].(map[string]any)
		list, _ := meta["reasoningEfforts"].([]any)
		for _, e := range list {
			em, ok := e.(map[string]any)
			if !ok {
				continue
			}
			if v, _ := em["value"].(string); v == effort {
				return true
			}
			if id, _ := em["id"].(string); id == effort {
				return true
			}
		}
		return false
	}
	return false
}
