package server

import (
	"errors"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"net/http"
)

// x.ai/task/kill 的终止来源，即 agent 侧 TaskKillSource 的 wire 名（camelCase
// 由 serde 决定）。clientUi = 单条 UI 终止，agent 会在任务收尾时补一条
// "killed by the user" 唤醒；teardown = 静默，不再为这条任务唤醒模型。
const (
	taskKillSourceClientUI = "clientUi"
	taskKillSourceTeardown = "teardown"
)

// handleTaskKill kills a background task via x.ai/task/kill
// ({sessionId?} empty resolves to the active session).
//
// 可选的 {source} 原样转发给 agent：它决定这次终止要不要通知模型。只认
// 上表两个值——其余字符串 agent 的 serde 会直接拒掉整个请求（含 source
// 字段的 kill 请求整体失败），杀掉的是"终止"本身，比回一个 400 更糟。
func (s *Server) handleTaskKill(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		TaskID    string `json:"taskId"`
		Source    string `json:"source,omitempty"`
	}
	if err := readJSON(r, &body); err != nil || body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	switch body.Source {
	case "", taskKillSourceClientUI, taskKillSourceTeardown:
	default:
		writeJSON(w, 400, map[string]any{"ok": false, "error": "source 只支持 clientUi / teardown"})
		return
	}
	res, err := s.bridge.TaskKill(r.Context(), body.SessionID, body.TaskID, body.Source)
	if err != nil {
		writeAgentError(w, "x.ai/task/kill", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskList lists background tasks via x.ai/task/list. An explicit
// {sessionId} asks for that session's registry; without it the active
// session answers.
func (s *Server) handleTaskList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.TaskList(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "x.ai/task/list", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskOutput returns one task's stdout for the FE block viewer.
// Body: { taskId } — the ACTIVE session's live registry
// (x.ai/task/list); or { taskId, sessionId, cwd } — reconstructed from
// that session's persisted timeline + on-disk log, so history-replay
// rows and top-strip restored (TUI-held) tasks get their full log
// regardless of which history page is loaded.
func (s *Server) handleTaskOutput(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TaskID    string `json:"taskId"`
		SessionID string `json:"sessionId,omitempty"`
		Cwd       string `json:"cwd,omitempty"`
	}
	if err := readJSON(r, &body); err != nil || body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	if body.SessionID != "" && body.Cwd != "" {
		tl, err := s.bridge.TaskLog(body.SessionID, body.Cwd, body.TaskID)
		if err != nil {
			if errors.Is(err, acp.ErrTaskLogNotFound) {
				writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
				return
			}
			writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "task": tl})
		return
	}
	res, err := s.bridge.TaskList(r.Context())
	if err != nil {
		writeAgentError(w, "x.ai/task/list", err)
		return
	}
	// ExtMethodResult envelope: { result: { tasks: [...] } } or flat { tasks }.
	tasks := extractTaskList(res)
	var found map[string]any
	for _, t := range tasks {
		id, _ := t["task_id"].(string)
		if id == "" {
			id, _ = t["taskId"].(string)
		}
		if id == body.TaskID {
			found = t
			break
		}
	}
	if found == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "任务不存在或已清理"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "task": found})
}

// extractTaskList unwraps x.ai/task/list response shapes into a []map.
func extractTaskList(res map[string]any) []map[string]any {
	if res == nil {
		return nil
	}
	// Nested ExtMethodResult: { result: { tasks: [...] } }
	inner := res
	if r, ok := res["result"].(map[string]any); ok {
		inner = r
	}
	raw, ok := inner["tasks"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}
