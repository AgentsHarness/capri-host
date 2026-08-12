package server

import (
	"log"
	"net/http"
)

// ── 默认模型 / 自定义模型配置（~/.grok/config.toml 的 [models]/[model.*]）──
//
// FE 侧「设为默认」与「自定义模型可视化配置」的落盘端点。写入走 Bridge 的
// 原子读改写（models_config.go）。agent（stdio 模式）没有 config.toml
// watcher，所以每次写入后主动调 x.ai/internal/reload_models 让 agent 重读
// 配置并重建模型目录——新模型/新默认无需重启 host 即生效。

type setDefaultModelBody struct {
	ModelID         string `json:"modelId"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
	SessionID       string `json:"sessionId,omitempty"`
}

// handleSetDefaultModel persists `[models].default` (+ optional
// `default_reasoning_effort`) AND switches the current session to the model
// — the TUI `/model <name> [effort]` double-action (session switch + next
// session default), so a single FE call covers both. Session switch runs
// first (TUI parity: the switch is the primary action, persistence is the
// preference side-effect).
//
// 用户选择优先（TUI persist_models_default 语义）：写 config.toml 前先把
// 触及 models.default 的活动 campaign 加入 dismissed_ids——否则新建会话仍
// 会按活动值（如 grok-4.6 推广）选模型，「配置变了但 agent 没变」。dismiss
// 是 best-effort：失败只记日志，配置写入照常（与 TUI 一致）。
func (s *Server) handleSetDefaultModel(w http.ResponseWriter, r *http.Request) {
	var body setDefaultModelBody
	if err := readJSON(r, &body); err != nil || body.ModelID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modelId"})
		return
	}
	if err := s.bridge.DismissModelDefaultCampaigns(r.Context()); err != nil {
		log.Printf("[acp-host] campaign dismiss 失败（配置仍会写入，活动可能继续覆盖默认模型）: %v", err)
	}
	if err := s.bridge.SetModel(r.Context(), body.SessionID, body.ModelID, body.ReasoningEffort); err != nil {
		writeAgentError(w, "session/set-model", err)
		return
	}
	if err := s.bridge.SetDefaultModelConfig(body.ModelID, body.ReasoningEffort); err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "reloaded": s.reloadModels(r)})
}

// handleCustomModels lists every `[model.<id>]` entry from config.toml.
func (s *Server) handleCustomModels(w http.ResponseWriter, r *http.Request) {
	models, err := s.bridge.ListCustomModels()
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "models": models})
}

type customModelBody struct {
	ID     string         `json:"id"`
	Values map[string]any `json:"values"`
}

// handleCustomModelUpsert creates or fully replaces `[model.<id>]`, then
// asks the agent to reload the catalog.
func (s *Server) handleCustomModelUpsert(w http.ResponseWriter, r *http.Request) {
	var body customModelBody
	if err := readJSON(r, &body); err != nil || body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	if err := s.bridge.UpsertCustomModel(body.ID, body.Values); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "reloaded": s.reloadModels(r)})
}

type customModelDeleteBody struct {
	ID string `json:"id"`
}

// handleCustomModelDelete removes `[model.<id>]`; clears `models.default`
// when the deleted id was the configured default; then reloads the catalog.
func (s *Server) handleCustomModelDelete(w http.ResponseWriter, r *http.Request) {
	var body customModelDeleteBody
	if err := readJSON(r, &body); err != nil || body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	cleared, err := s.bridge.DeleteCustomModel(body.ID)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "defaultCleared": cleared, "reloaded": s.reloadModels(r)})
}

// reloadModels asks the agent to rebuild its model catalog from config.toml
// (x.ai/internal/reload_models). Best-effort: the config write already
// happened — a reload failure only means the new model/default appears on
// the next reload/restart, so it is logged rather than failing the request.
func (s *Server) reloadModels(r *http.Request) bool {
	if err := s.bridge.ReloadModels(r.Context()); err != nil {
		log.Printf("[acp-host] 模型目录重载失败（配置已写入，将在下次重载/重启生效）: %v", err)
		return false
	}
	return true
}
