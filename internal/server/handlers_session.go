package server

import (
	"github.com/AgentsHarness/capri-host/internal/acp"
	"net/http"
)

// listSessionsBody — POST /api/sessions. cwd/cursor/meta are OPTIONAL
// official session/list fields forwarded on the wire when present (absent
// = the existing `{}` request exactly); the response keeps the existing
// local enrichment (status/badges) untouched.
type listSessionsBody struct {
	Cwd    string         `json:"cwd,omitempty"`
	Cursor string         `json:"cursor,omitempty"`
	Meta   map[string]any `json:"meta,omitempty"`
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	var body listSessionsBody
	_ = readJSON(r, &body)
	var opts acp.ListSessionsOpts
	if body.Cwd != "" || body.Cursor != "" || len(body.Meta) > 0 {
		opts = acp.ListSessionsOpts{Cwd: body.Cwd, Cursor: body.Cursor, Meta: body.Meta}
	}
	sessions, nextCursor, meta, err := s.bridge.ListSessions(r.Context(), opts)
	if err != nil {
		writeAgentError(w, "session/list", err)
		return
	}
	out := map[string]any{"ok": true, "sessions": sessions}
	// 响应游标与 `_meta` 原样透传（仅非空才带；absent key ≠ off）。
	if nextCursor != "" {
		out["nextCursor"] = nextCursor
	}
	if len(meta) > 0 {
		out["meta"] = meta
	}
	writeJSON(w, 200, out)
}

type sessionStateBody struct {
	SessionID string `json:"sessionId"`
}

// handleSessionState — x.ai/session/state analog: host-side live state of
// one session (dashboard active/idle/awaiting classification).
func (s *Server) handleSessionState(w http.ResponseWriter, r *http.Request) {
	var body sessionStateBody
	_ = readJSON(r, &body)
	if body.SessionID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId"})
		return
	}
	st := s.bridge.SessionStateOf(body.SessionID)
	if st == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "会话不在线（未在本进程创建/加载）"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "session": st})
}

type sessionLoadBody struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	// Meta: client-supplied session seeds (permission-mode flags, the
	// TUI's yoloMode/autoMode analog) forwarded as the session/load
	// params `_meta`. Omitted = current behavior exactly.
	Meta map[string]any `json:"meta,omitempty"`
}

// handleSessionLoad switches the active session to a historical one
// (session/load), so subsequent prompts continue that conversation.
func (s *Server) handleSessionLoad(w http.ResponseWriter, r *http.Request) {
	var body sessionLoadBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	sessRes, err := s.bridge.LoadSession(r.Context(), body.SessionID, body.Cwd, body.Meta)
	if err != nil {
		writeAgentError(w, "session/load", err)
		return
	}
	// Echo models / busy so the FE can update caption + spinner even if the
	// SSE ready/busy events race with historyLoading.
	out := map[string]any{"ok": true, "sessionId": body.SessionID}
	if sessRes != nil {
		if m, ok := sessRes["models"]; ok && m != nil {
			out["models"] = m
		}
		if modes, ok := sessRes["modes"]; ok && modes != nil {
			out["modes"] = modes
		}
		if co, ok := sessRes["configOptions"]; ok && co != nil {
			out["configOptions"] = co
		}
		if busy, ok := sessRes["busy"].(bool); ok {
			out["busy"] = busy
		}
	}
	writeJSON(w, 200, out)
}

type sessionUpdatesBody struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	Offset    *int64 `json:"offset"`
	Limit     *int   `json:"limit"`
	// Optional x.ai/session/updates request fields (camelCase on the wire):
	// stream delivers updates as chunked notifications, chunkSize sets the
	// per-chunk size (default 64), turnIndex tails by user-message turn
	// count. Absent = the existing offset/limit behavior exactly.
	Stream    bool `json:"stream,omitempty"`
	ChunkSize *int `json:"chunkSize,omitempty"`
	TurnIndex *int `json:"turnIndex,omitempty"`
	// detail 是历史页的响应投影档位（见 acp/lite.go）："lite" 是首屏
	// 时间线（合成工具信封、thought 占位、丢掉正文），"meta" 只回分页
	// 锚点，"full"/缺省/未知值 = 信封逐字节原样。
	Detail string `json:"detail,omitempty"`
}

// handleSessionUpdates fetches a session's stored updates (message history)
// and returns them as raw envelopes. The frontend replays them locally
// (deterministic ordering, no SSE broadcast drops).
func (s *Server) handleSessionUpdates(w http.ResponseWriter, r *http.Request) {
	var body sessionUpdatesBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	opts := acp.SessionUpdatesOpts{Offset: body.Offset, Limit: body.Limit, Stream: body.Stream, Detail: body.Detail}
	if body.ChunkSize != nil {
		opts.ChunkSize = body.ChunkSize
	}
	if body.TurnIndex != nil {
		opts.TurnIndex = body.TurnIndex
	}
	page, err := s.bridge.SessionUpdates(r.Context(), body.SessionID, body.Cwd, opts)
	if err != nil {
		writeAgentError(w, "session/updates", err)
		return
	}
	out := map[string]any{
		"ok":         true,
		"sessionId":  body.SessionID,
		"totalCount": page.TotalCount,
		"hasMore":    page.HasMore,
	}
	// goal：该会话的 goal 快照（host 引擎内存态；null = 没有）。回放时
	// FE 直接用它恢复 GoalChip，不再为每个会话单独打 /api/goal/status。
	// 键恒存在（值可为 null），旧 FE 忽略未知键，新 FE 用它判别新 host。
	goal, goalErr := s.bridge.GoalStatus(body.SessionID)
	if goalErr != nil {
		goal = nil
	}
	out["goal"] = goal
	// 能力回显（契约 [B]）：projected/omittedBytes 只在投影真正生效时带，
	// 旧 host 不带该键 → FE 判定不支持 lite。meta 档省略 updates 键本身
	// （而不是回空数组）——FE 要能区分「没回信封」与「会话没有历史」。
	// detail 为 full/缺省/未知值时这两条分支都不进，响应与今天逐字节一致。
	if page.Projected != acp.DetailMeta {
		out["updates"] = page.Updates
	}
	if page.Projected != "" {
		out["projected"] = page.Projected
		out["omittedBytes"] = page.OmittedBytes
	}
	// promptStarts: pass through so FE can page history one user-turn at a
	// time (see chat.ts previousTurnWindow). Omit when empty — older agents
	// and pure offset/limit pages never set it.
	if len(page.PromptStarts) > 0 {
		out["promptStarts"] = page.PromptStarts
	}
	// promptPreviews：与 promptStarts 平行的轮次首行预览（用户消息目录用，
	// 本地归一化路径才有）。缺省 = 旧 host / 透传路径，FE 回退为目录只列
	// 已加载轮。
	if len(page.PromptPreviews) > 0 {
		out["promptPreviews"] = page.PromptPreviews
	}
	// btw：/btw 侧问回放记录（本地归一化路径才有；见 SessionBtw）。
	if len(page.Btw) > 0 {
		out["btw"] = page.Btw
	}
	if body.Stream {
		// stream=true: the agent does not return the updates in this
		// response — the real data arrives as session_updates_chunk SSE
		// notifications. Mark the response so the FE knows this is the
		// stream ack (updates is empty by design); totalCount/hasMore are
		// still meaningful from the stream start.
		out["streamed"] = true
	}
	writeJSON(w, 200, out)
}

// handleSessionRunningTasks returns the session's STILL-RUNNING tasks
// (task_backgrounded orphans whose output log was written recently) plus
// the detached subset — those the agent's own registry does not know and
// therefore cannot kill. The persisted timeline is only used to surface
// current work, not to replay history.
func (s *Server) handleSessionRunningTasks(w http.ResponseWriter, r *http.Request) {
	var body sessionUpdatesBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sessionId 和 cwd"})
		return
	}
	events, err := s.bridge.SessionRunningTasks(body.SessionID, body.Cwd)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	// Tasks the liveness probe still sees but the agent's registry does not
	// know: the frontend must NOT list them as running tasks (it could not
	// kill them) — it surfaces them as a one-off detached hint.
	detached := s.bridge.DetachedRunningTasks(r.Context(), body.SessionID, body.Cwd)
	if detached == nil {
		detached = []acp.TaskEvent{}
	}
	writeJSON(w, 200, map[string]any{
		"ok":        true,
		"sessionId": body.SessionID,
		"events":    events,
		"detached":  detached,
	})
}

// handleGitInfo returns the git branch/worktree state for a session cwd
// (x.ai/git/info + the agent's git_head_changed stash / local worktree
// probe), so the frontend can show the status-bar branch without waiting
// for a git_head_changed notification. The sessionId anchors the stash
// lookup (agent three-way worktree detection); cwd alone falls back to the
// local probe.
type gitInfoBody struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
}

func (s *Server) handleGitInfo(w http.ResponseWriter, r *http.Request) {
	var body gitInfoBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 cwd"})
		return
	}
	info, err := s.bridge.GitInfo(r.Context(), body.SessionID, body.Cwd)
	if err != nil {
		// Non-repo / agent error → empty state, not a hard failure.
		writeJSON(w, 200, map[string]any{
			"ok": true, "branch": "", "isWorktree": false, "mainRepo": "",
		})
		return
	}
	out := map[string]any{"ok": true, "sessionId": body.SessionID}
	for k, v := range info {
		out[k] = v
	}
	writeJSON(w, 200, out)
}

// handleSessionFork forks the given session via x.ai/session/fork
// ({sessionId?} empty resolves to the active one).
type forkBody struct {
	SessionID     string `json:"sessionId,omitempty"`
	SourceCwd     string `json:"sourceCwd"`
	NewCwd        string `json:"newCwd"`
	NewSessionID  string `json:"newSessionId"`
	SourceWorkDir string `json:"sourceWorkspaceDir"`
	// Fork truncation (agent ForkSessionRequest.target_prompt_index):
	// keep turns 0..=targetPromptIndex (0-based, inclusive). Nil → full copy.
	TargetPromptIndex *uint64 `json:"targetPromptIndex,omitempty"`
}

func (s *Server) handleSessionFork(w http.ResponseWriter, r *http.Request) {
	var body forkBody
	_ = readJSON(r, &body)
	params := map[string]any{}
	if body.SourceCwd != "" {
		params["sourceCwd"] = body.SourceCwd
	}
	if body.NewCwd != "" {
		params["newCwd"] = body.NewCwd
	}
	if body.NewSessionID != "" {
		params["newSessionId"] = body.NewSessionID
	}
	if body.SourceWorkDir != "" {
		params["sourceWorkspaceDir"] = body.SourceWorkDir
	}
	if body.TargetPromptIndex != nil {
		params["targetPromptIndex"] = *body.TargetPromptIndex
	}
	res, err := s.bridge.ForkSession(r.Context(), body.SessionID, params)
	if err != nil {
		writeAgentError(w, "session/fork", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionRename renames the given session via x.ai/session/rename
// ({sessionId?} empty resolves to the active one).
func (s *Server) handleSessionRename(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Title     string `json:"title"`
	}
	if err := readJSON(r, &body); err != nil || body.Title == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 title"})
		return
	}
	res, err := s.bridge.RenameSession(r.Context(), body.SessionID, body.Title)
	if err != nil {
		writeAgentError(w, "session/rename", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleRecap fires x.ai/recap (fire-and-forget "where was I" summary).
// {sessionId?} empty resolves to the active session.
func (s *Server) handleRecap(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId,omitempty"`
		Auto      bool   `json:"auto"`
	}
	_ = readJSON(r, &body)
	res, err := s.bridge.Recap(r.Context(), body.SessionID, body.Auto)
	if err != nil {
		writeAgentError(w, "session/recap", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionInfo serves the given session's details on demand
// (TUI /session-info analog) — no client-side state reconstruction.
// {sessionId?}: empty resolves to the active session.
func (s *Server) handleSessionInfo(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	_ = readJSON(r, &body)
	info := s.bridge.SessionInfo(body.SessionID)
	if info == nil {
		msg := "暂无活动会话"
		if body.SessionID != "" {
			msg = "未知会话"
		}
		writeJSON(w, 404, map[string]any{"ok": false, "error": msg})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "session": info})
}

// handleSessionPlan serves the session's plan.md body (plan 模式正文) —
// the file the TUI's /view-plan reads. {sessionId, cwd} → {ok, content};
// 没有 plan 文件时 content 为空串（FE 显示空态），不是错误。
func (s *Server) handleSessionPlan(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	content, ok := s.bridge.SessionPlan(body.SessionID, body.Cwd)
	if !ok {
		content = ""
	}
	writeJSON(w, 200, map[string]any{"ok": true, "content": content})
}

// handleSubagentCancel cancels a subagent via x.ai/subagent/cancel
// ({sessionId?} empty resolves to the active session).
func (s *Server) handleSubagentCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID  string `json:"sessionId,omitempty"`
		SubagentID string `json:"subagentId"`
	}
	if err := readJSON(r, &body); err != nil || body.SubagentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 subagentId"})
		return
	}
	res, err := s.bridge.SubagentCancel(r.Context(), body.SessionID, body.SubagentID)
	if err != nil {
		writeAgentError(w, "x.ai/subagent/cancel", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSubagentMessage sends a steering or queued message to an active child subagent
// via x.ai/subagent/message ({sessionId?, agentAddress/subagentId, text/content, queue?}).
func (s *Server) handleSubagentMessage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID    string `json:"sessionId,omitempty"`
		AgentAddress string `json:"agentAddress,omitempty"`
		SubagentID   string `json:"subagentId,omitempty"`
		Text         string `json:"text,omitempty"`
		Content      []any  `json:"content,omitempty"`
		Queue        bool   `json:"queue,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "请求体 JSON 格式错误"})
		return
	}
	addr := body.AgentAddress
	if addr == "" {
		addr = body.SubagentID
	}
	if addr == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 agentAddress 或 subagentId"})
		return
	}
	var content []any
	if len(body.Content) > 0 {
		content = body.Content
	} else if body.Text != "" {
		content = []any{
			map[string]any{
				"type": "text",
				"text": body.Text,
			},
		}
	} else {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 text 或 content"})
		return
	}

	res, err := s.bridge.SubagentMessage(r.Context(), body.SessionID, addr, body.Queue, content)
	if err != nil {
		writeAgentError(w, "x.ai/subagent/message", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionDelete deletes a session via x.ai/session/delete.
// Body: {sessionId, cwd} — sessionId defaults to the active session; cwd
// is accepted for symmetry with the other session endpoints but the wire
// method only carries {sessionId}.
func (s *Server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.SessionDelete(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "session/delete", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleCompact compacts a session's context via x.ai/compact_conversation.
// Body: {sessionId, note?} — sessionId defaults to the active session.
func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Note      string `json:"note"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.CompactConversation(r.Context(), body.SessionID, body.Note)
	if err != nil {
		writeAgentError(w, "session/compact", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleRewindPoints lists a session's rewindable points via
// x.ai/rewind/points. Body: {sessionId, cwd} — sessionId defaults to the
// active session. Points are normalized to {index, timestamp, summary?}
// regardless of the agent's snake_case/camelCase field names.
func (s *Server) handleRewindPoints(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Cwd       string `json:"cwd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.RewindPoints(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "session/rewind_points", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "points": normalizeRewindPoints(res)})
}

// handleRewindExecute rolls a session back to a rewind point via
// x.ai/rewind/execute. Body: {sessionId, targetIndex, mode?} — mode is
// optional ("all" | "conversation_only"); empty defaults to
// "conversation_only" (TUI /rewind behavior, conversation-only rollback).
func (s *Server) handleRewindExecute(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID   string `json:"sessionId"`
		TargetIndex *int   `json:"targetIndex"`
		Mode        string `json:"mode"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.TargetIndex == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 targetIndex"})
		return
	}
	if body.Mode != "" && body.Mode != "all" && body.Mode != "conversation_only" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "mode 必须是 all 或 conversation_only"})
		return
	}
	res, err := s.bridge.RewindExecute(r.Context(), body.SessionID, *body.TargetIndex, body.Mode)
	if err != nil {
		writeAgentError(w, "session/rewind", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSchedulerDelete deletes a scheduled task via x.ai/scheduler/delete.
// Body: {sessionId, taskId} — sessionId defaults to the active session.
func (s *Server) handleSchedulerDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		TaskID    string `json:"taskId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	res, err := s.bridge.SchedulerDelete(r.Context(), body.SessionID, body.TaskID)
	if err != nil {
		writeAgentError(w, "scheduler/delete", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// normalizeRewindPoints unwraps the x.ai/rewind/points response — flat
// {points:[…]}, ExtMethodResult {result:{points:[…]}}, or the agent's raw
// snake_case {rewind_points:[…]} — and normalizes each point's field names
// (snake_case or camelCase on the wire) into {index, timestamp, summary?}.
// Points without a usable index are dropped.
func normalizeRewindPoints(res map[string]any) []map[string]any {
	if res == nil {
		return nil
	}
	inner := res
	if r, ok := res["result"].(map[string]any); ok {
		inner = r
	}
	var raw []any
	if p, ok := inner["points"].([]any); ok {
		raw = p
	} else if p, ok := inner["rewindPoints"].([]any); ok {
		raw = p
	} else if p, ok := inner["rewind_points"].([]any); ok {
		raw = p
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		idx, hasIdx := asInt64(firstAny(m, "index", "prompt_index", "target_prompt_index", "targetIndex"))
		if !hasIdx {
			continue
		}
		pt := map[string]any{"index": idx}
		if ts := firstAny(m, "timestamp", "ts", "created_at"); ts != nil {
			pt["timestamp"] = ts
		}
		if sum := firstAny(m, "summary", "description", "prompt_preview"); sum != nil {
			pt["summary"] = sum
		}
		if hfc := firstAny(m, "has_file_changes", "hasFileChanges"); hfc != nil {
			pt["hasFileChanges"] = hfc
		}
		out = append(out, pt)
	}
	return out
}
