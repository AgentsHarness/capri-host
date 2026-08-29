package server

import (
	"net/http"
)

// http_ext_queue.go — 提示队列管理端点（x.ai/queue/* 经 XaiNotify 即发即回，权威状态由 queue/changed 广播）。

// ── 队列（grok 侧为 ext_notification 型：只收 fire-and-forget 通知，无
//    ext_method 分支；以 _x.ai/queue/* 通知发送，sessionId 由宿主解析：
//    请求体可带可选 sessionId 显式指定目标会话，缺省用活动会话，无显式
//    id 且缺活动会话 404。结果经 x.ai/queue/changed 广播回传，端点即写
//    即回 {ok:true}。TUI 同款：xai-grok-pager app/effects/mod.rs 全以
//    ExtNotification 发送）────

// queueNotify fires one x.ai/queue/* mutation as a fire-and-forget
// notification and writes the immediate {ok:true} (or error) response.
// The params carry the sessionId key: an explicit id from the request body
// passes through untouched; "" lets bridge.XaiNotify resolve the active
// session (404 when none is active).
func (s *Server) queueNotify(w http.ResponseWriter, r *http.Request, method string, params map[string]any) {
	res, err := s.bridge.XaiNotify(r.Context(), method, params)
	if err != nil {
		writeAgentError(w, "_"+method, err)
		return
	}
	writeJSON(w, 200, res)
}

// handleQueueRemove — {sessionId?, id, expectedVersion?}：sessionId 可选，
// 显式指定目标会话（缺省由宿主解析活动会话）。
func (s *Server) handleQueueRemove(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID       string `json:"sessionId,omitempty"`
		ID              string `json:"id"`
		ExpectedVersion int64  `json:"expectedVersion"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	params := map[string]any{"sessionId": body.SessionID, "id": body.ID}
	if body.ExpectedVersion != 0 {
		params["expectedVersion"] = body.ExpectedVersion
	}
	s.queueNotify(w, r, "x.ai/queue/remove", params)
}

// handleQueueClear — {sessionId?}。
func (s *Server) handleQueueClear(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.queueNotify(w, r, "x.ai/queue/clear", map[string]any{"sessionId": body.SessionID})
}

// handleQueueReorder — {sessionId?, ids: []string}；wire 键为 orderedIds
// （grok 侧 parse_queue_edit_command 的约定）。
func (s *Server) handleQueueReorder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string   `json:"sessionId,omitempty"`
		IDs       []string `json:"ids"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{"sessionId": body.SessionID}
	if len(body.IDs) > 0 {
		params["orderedIds"] = body.IDs
	}
	s.queueNotify(w, r, "x.ai/queue/reorder", params)
}

// handleQueueEdit — {sessionId?, id, newText}。
func (s *Server) handleQueueEdit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		ID        string `json:"id"`
		NewText   string `json:"newText"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	if body.NewText == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 newText"})
		return
	}
	s.queueNotify(w, r, "x.ai/queue/edit", map[string]any{"sessionId": body.SessionID, "id": body.ID, "newText": body.NewText})
}

// handleQueueInterject — {sessionId?, id, newText?, expectedVersion?}。
func (s *Server) handleQueueInterject(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID       string `json:"sessionId,omitempty"`
		ID              string `json:"id"`
		NewText         string `json:"newText"`
		ExpectedVersion int64  `json:"expectedVersion"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	params := map[string]any{"sessionId": body.SessionID, "id": body.ID}
	if body.NewText != "" {
		params["newText"] = body.NewText
	}
	if body.ExpectedVersion != 0 {
		params["expectedVersion"] = body.ExpectedVersion
	}
	s.queueNotify(w, r, "x.ai/queue/interject", params)
}

// handleQueueHoldEdit — 编辑锁（客户端编辑期间组合保持）{sessionId?, id}。
// wire parse_queue_edit_command 只读 id（combine-hold 语义，TUI 同款）。
func (s *Server) handleQueueHoldEdit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		ID        string `json:"id"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	s.queueNotify(w, r, "x.ai/queue/hold_edit", map[string]any{"sessionId": body.SessionID, "id": body.ID})
}

// handleQueueReleaseEdit — 释放编辑锁 {sessionId?, id}（同 hold_edit，只读 id）。
func (s *Server) handleQueueReleaseEdit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		ID        string `json:"id"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 id"})
		return
	}
	s.queueNotify(w, r, "x.ai/queue/release_edit", map[string]any{"sessionId": body.SessionID, "id": body.ID})
}

// handleQueueStatus — 读端点 {sessionId}：返回该会话最近一次
// x.ai/queue/changed 的缓存快照（host 内存，不落盘；本进程存活期间
// 从未见过该会话的队列广播时 queue 为 null）。队列状态不随
// session/load 回放（agent 不持久化 pending_inputs），FE 在加载会话后
// 主动拉取以对齐本地镜像。与写端点不同：不发通知、不问 agent。
func (s *Server) handleQueueStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SessionID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId"})
		return
	}
	out := map[string]any{
		"ok":        true,
		"sessionId": body.SessionID,
	}
	if snap := s.bridge.QueueStatus(body.SessionID); snap != nil {
		out["queue"] = snap
	} else {
		out["queue"] = nil
	}
	writeJSON(w, 200, out)
}

// registerExtQueueRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtQueueRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/queue/remove", s.handleQueueRemove)
	mux.HandleFunc("POST /api/queue/clear", s.handleQueueClear)
	mux.HandleFunc("POST /api/queue/reorder", s.handleQueueReorder)
	mux.HandleFunc("POST /api/queue/edit", s.handleQueueEdit)
	mux.HandleFunc("POST /api/queue/interject", s.handleQueueInterject)
	mux.HandleFunc("POST /api/queue/hold-edit", s.handleQueueHoldEdit)
	mux.HandleFunc("POST /api/queue/release-edit", s.handleQueueReleaseEdit)
	mux.HandleFunc("POST /api/queue/status", s.handleQueueStatus)
}
