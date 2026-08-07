package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
)

type Server struct {
	cfg    config.Config
	bridge *acp.Bridge
	http   *http.Server
	// promptFn runs a turn. Defaults to bridge.Prompt; injectable in tests.
	// The handler runs it detached from the client connection so a browser
	// crash mid-turn does not cancel the turn (the grok agent keeps running).
	promptFn func(ctx context.Context, sessionID string, blocks []acp.ContentBlock) (string, error)
}

func New(cfg config.Config, bridge *acp.Bridge) *Server {
	s := &Server{cfg: cfg, bridge: bridge}
	s.promptFn = s.bridge.Prompt
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", s.handleSSE)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("POST /api/prompt", s.handlePrompt)
	mux.HandleFunc("POST /api/cancel", s.handleCancel)
	mux.HandleFunc("POST /api/permission-response", s.handlePermission)
	mux.HandleFunc("POST /api/client-response", s.handleClientResponse)
	mux.HandleFunc("POST /api/session", s.handleSession)
	mux.HandleFunc("POST /api/session-load", s.handleSessionLoad)
	mux.HandleFunc("POST /api/set-mode", s.handleSetMode)
	mux.HandleFunc("POST /api/set-model", s.handleSetModel)
	mux.HandleFunc("POST /api/sessions", s.handleListSessions)
	mux.HandleFunc("POST /api/session-state", s.handleSessionState)
	mux.HandleFunc("POST /api/session-updates", s.handleSessionUpdates)
	mux.HandleFunc("POST /api/session-running-tasks", s.handleSessionRunningTasks)
	mux.HandleFunc("POST /api/git-info", s.handleGitInfo)
	mux.HandleFunc("POST /api/session-fork", s.handleSessionFork)
	mux.HandleFunc("POST /api/session-rename", s.handleSessionRename)
	mux.HandleFunc("POST /api/recap", s.handleRecap)
	mux.HandleFunc("POST /api/session-info", s.handleSessionInfo)
	mux.HandleFunc("POST /api/subagent-cancel", s.handleSubagentCancel)
	mux.HandleFunc("POST /api/task-kill", s.handleTaskKill)
	mux.HandleFunc("POST /api/task-list", s.handleTaskList)
	mux.HandleFunc("POST /api/task-output", s.handleTaskOutput)
	mux.HandleFunc("POST /api/session-delete", s.handleSessionDelete)
	mux.HandleFunc("POST /api/compact", s.handleCompact)
	mux.HandleFunc("POST /api/rewind-points", s.handleRewindPoints)
	mux.HandleFunc("POST /api/rewind-execute", s.handleRewindExecute)
	mux.HandleFunc("POST /api/scheduler-delete", s.handleSchedulerDelete)
	mux.HandleFunc("POST /api/shell", s.handleShell)
	mux.HandleFunc("GET /api/hosts", s.handleHosts)
	mux.HandleFunc("POST /api/billing", s.handleBilling)
	mux.HandleFunc("POST /api/memory-flush", s.handleMemoryFlush)
	mux.HandleFunc("POST /api/memory-rewrite", s.handleMemoryRewrite)
	mux.HandleFunc("POST /api/toggle-plan-mode", s.handleTogglePlanMode)
	mux.HandleFunc("POST /api/permissions-reset", s.handlePermissionsReset)
	mux.HandleFunc("GET /api/mcp/list", s.handleMCPList)
	mux.HandleFunc("POST /api/mcp-toggle", s.handleMCPToggle)
	mux.HandleFunc("POST /api/mcp-add", s.handleMCPAdd)
	mux.HandleFunc("POST /api/mcp-remove", s.handleMCPRemove)
	mux.HandleFunc("POST /api/mcp-auth-trigger", s.handleMCPAuthTrigger)
	mux.HandleFunc("GET /api/extensions", s.handleExtensions)
	mux.HandleFunc("GET /api/settings", s.handleSettings)
	// CORS for Vite dev
	s.http = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           withCORS(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) ListenAndServe() error {
	log.Printf("[acp-host] listening on http://localhost:%d", s.cfg.Port)
	log.Printf("[acp-host] grok bin=%s hostId=%s name=%q", s.cfg.GrokBin, s.cfg.HostID, s.cfg.HostName)
	log.Printf("[acp-host] mode=agent-only (no client fs/terminal)")
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	snap := s.bridge.Snapshot()
	hello := map[string]any{
		"type":            "hello",
		"ready":           snap.Ready,
		"busy":            snap.Busy,
		"sessionId":       snap.SessionID,
		"cwd":             snap.Cwd,
		"text":            snap.Text,
		"error":           snap.BootError,
		"agentInfo":       snap.AgentInfo,
		"modes":           snap.Modes,
		"configOptions":   snap.ConfigOptions,
		"models":          snap.Models,
		"pendingRequests": snap.PendingRequests,
		"hostId":          snap.HostID,
		"hostName":        snap.HostName,
		"homeDir":         snap.HomeDir,
		"capabilities":    snap.Capabilities,
		"roster":          snap.Roster,
	}
	data, _ := json.Marshal(hello)
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.bridge.Snapshot())
}

func (s *Server) handleHosts(w http.ResponseWriter, r *http.Request) {
	// Local mode: single host (this process). Multi-host comes via Hub.
	snap := s.bridge.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"hosts": []map[string]any{
			{
				"hostId":   snap.HostID,
				"hostName": snap.HostName,
				"online":   true,
				"ready":    snap.Ready,
				"local":    true,
			},
		},
	})
}

type promptBody struct {
	Blocks []acp.ContentBlock `json:"blocks"`
	// Optional: target session (defaults to the active session).
	SessionID string `json:"sessionId,omitempty"`
}

func (s *Server) handlePrompt(w http.ResponseWriter, r *http.Request) {
	var body promptBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if len(body.Blocks) == 0 {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "blocks 不能为空"})
		return
	}
	ctx := r.Context()
	// Run the turn on an independent context, not r.Context(). Previously the
	// turn was driven by the client connection, so a browser crash mid-turn
	// cancelled the whole turn (Prompt() sent session/cancel) even though the
	// grok agent process itself was perfectly healthy. Now a client disconnect
	// just releases the HTTP handler while the turn keeps running — progress
	// and completion are observable via SSE, and only an explicit /api/cancel
	// stops it. For a live client, the handler still blocks and returns the
	// stopReason as before.
	type result struct {
		stopReason string
		err        error
	}
	done := make(chan result, 1)
	go func() {
		sr, err := s.promptFn(context.Background(), body.SessionID, body.Blocks)
		done <- result{sr, err}
	}()

	select {
	case <-ctx.Done():
		// Client went away mid-turn; the turn continues in the background.
		return
	case res := <-done:
		if res.err != nil {
			code := 500
			var he *acp.HTTPError
			if errors.As(res.err, &he) {
				code = he.Code
			}
			writeJSON(w, code, map[string]any{"ok": false, "error": res.err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "stopReason": res.stopReason})
	}
}

type cancelBody struct {
	// Optional: target session (defaults to the active session).
	SessionID string `json:"sessionId,omitempty"`
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var body cancelBody
	_ = readJSON(r, &body)
	s.bridge.Cancel(body.SessionID)
	writeJSON(w, 200, map[string]any{"ok": true})
}

type permBody struct {
	RequestID string `json:"requestId"`
	OptionID  string `json:"optionId"`
	Cancelled bool   `json:"cancelled"`
}

func (s *Server) handlePermission(w http.ResponseWriter, r *http.Request) {
	var body permBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RequestID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 requestId"})
		return
	}
	if err := s.bridge.RespondPermission(body.RequestID, body.OptionID, body.Cancelled); err != nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// handleClientResponse resolves a forwarded x.ai/* request (ask_user_question,
// exit_plan_mode, …) with a raw result object or an error message.
type clientResponseBody struct {
	RequestID string         `json:"requestId"`
	Result    map[string]any `json:"result"`
	Error     string         `json:"error"`
}

func (s *Server) handleClientResponse(w http.ResponseWriter, r *http.Request) {
	var body clientResponseBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RequestID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 requestId"})
		return
	}
	if err := s.bridge.RespondClientRequest(body.RequestID, body.Result, body.Error); err != nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	var sc acp.SessionConfig
	_ = readJSON(r, &sc)
	if err := s.bridge.NewSession(r.Context(), sc); err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	snap := s.bridge.Snapshot()
	writeJSON(w, 200, map[string]any{"ok": true, "sessionId": snap.SessionID})
}

type modeBody struct {
	ModeID string `json:"modeId"`
}

func (s *Server) handleSetMode(w http.ResponseWriter, r *http.Request) {
	var body modeBody
	if err := readJSON(r, &body); err != nil || body.ModeID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modeId"})
		return
	}
	res, err := s.bridge.SetMode(r.Context(), body.ModeID)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

type setModelBody struct {
	ModelID         string `json:"modelId"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
}

func (s *Server) handleSetModel(w http.ResponseWriter, r *http.Request) {
	var body setModelBody
	if err := readJSON(r, &body); err != nil || body.ModelID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 modelId"})
		return
	}
	if err := s.bridge.SetModel(r.Context(), body.ModelID, body.ReasoningEffort); err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.bridge.ListSessions(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "sessions": sessions})
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
	sessRes, err := s.bridge.LoadSession(r.Context(), body.SessionID, body.Cwd)
	if err != nil {
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
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
	page, err := s.bridge.SessionUpdates(r.Context(), body.SessionID, body.Cwd, body.Offset, body.Limit)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":         true,
		"sessionId":  body.SessionID,
		"totalCount": page.TotalCount,
		"hasMore":    page.HasMore,
		"updates":    page.Updates,
	})
}

// handleSessionRunningTasks returns the session's STILL-RUNNING tasks
// (task_backgrounded orphans whose output log was written recently) — the
// web equivalent of the TUI's live tasks pane. The persisted timeline is
// only used to surface current work, not to replay history.
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
	writeJSON(w, 200, map[string]any{
		"ok":        true,
		"sessionId": body.SessionID,
		"events":    events,
	})
}

// handleGitInfo returns the git branch/worktree state for a session cwd
// (x.ai/git/info + local worktree probe), so the frontend can show the
// status-bar branch without waiting for a git_head_changed notification.
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
	info, err := s.bridge.GitInfo(r.Context(), body.Cwd)
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

// handleSessionFork forks the current session via x.ai/session/fork.
type forkBody struct {
	SourceCwd     string `json:"sourceCwd"`
	NewCwd        string `json:"newCwd"`
	NewSessionID  string `json:"newSessionId"`
	SourceWorkDir string `json:"sourceWorkspaceDir"`
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
	res, err := s.bridge.ForkSession(r.Context(), params)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionRename renames the current session via x.ai/session/rename.
func (s *Server) handleSessionRename(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title string `json:"title"`
	}
	if err := readJSON(r, &body); err != nil || body.Title == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 title"})
		return
	}
	res, err := s.bridge.RenameSession(r.Context(), body.Title)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleRecap fires x.ai/recap (fire-and-forget "where was I" summary).
func (s *Server) handleRecap(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Auto bool `json:"auto"`
	}
	_ = readJSON(r, &body)
	res, err := s.bridge.Recap(r.Context(), body.Auto)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleSessionInfo serves the active session's details on demand
// (TUI /session-info analog) — no client-side state reconstruction.
func (s *Server) handleSessionInfo(w http.ResponseWriter, r *http.Request) {
	info := s.bridge.SessionInfo()
	if info == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "暂无活动会话"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "session": info})
}

// handleSubagentCancel cancels a subagent via x.ai/subagent/cancel.
func (s *Server) handleSubagentCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SubagentID string `json:"subagentId"`
	}
	if err := readJSON(r, &body); err != nil || body.SubagentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 subagentId"})
		return
	}
	res, err := s.bridge.SubagentCancel(r.Context(), body.SubagentID)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskKill kills a background task via x.ai/task/kill.
func (s *Server) handleTaskKill(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TaskID string `json:"taskId"`
	}
	if err := readJSON(r, &body); err != nil || body.TaskID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 taskId"})
		return
	}
	res, err := s.bridge.TaskKill(r.Context(), body.TaskID)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTaskList lists background tasks via x.ai/task/list.
func (s *Server) handleTaskList(w http.ResponseWriter, r *http.Request) {
	res, err := s.bridge.TaskList(r.Context())
	if err != nil {
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
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
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
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
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
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
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
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
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "points": normalizeRewindPoints(res)})
}

// handleRewindExecute rolls a session back to a rewind point via
// x.ai/rewind/execute. Body: {sessionId, targetIndex}.
func (s *Server) handleRewindExecute(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID   string `json:"sessionId"`
		TargetIndex *int   `json:"targetIndex"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.TargetIndex == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 targetIndex"})
		return
	}
	res, err := s.bridge.RewindExecute(r.Context(), body.SessionID, *body.TargetIndex)
	if err != nil {
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
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
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// shellBody is POST /api/shell — local command execution, NOT routed
// through the agent.
type shellBody struct {
	Command   string `json:"command"`
	Cwd       string `json:"cwd"`
	TimeoutMs int    `json:"timeoutMs"`
}

const (
	shellDefaultTimeout = 10 * time.Second
	shellMaxTimeout     = 60 * time.Second
)

// handleShell runs a command locally via os/exec (sh -c) — a pure host
// facility that never touches the agent. Body: {command, cwd?, timeoutMs?}
// (cwd defaults to the active session's cwd, timeout defaults to 10s and
// is capped at 60s). Like handlePrompt, the command runs on an independent
// context, so a browser disconnect just releases the handler while the
// command keeps running; only the timeout stops it. No stdin is attached.
func (s *Server) handleShell(w http.ResponseWriter, r *http.Request) {
	var body shellBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if strings.TrimSpace(body.Command) == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "command 不能为空"})
		return
	}
	cwd := body.Cwd
	if cwd == "" {
		cwd = s.bridge.Snapshot().Cwd // active session cwd
	}
	if cwd == "" {
		wd, err := os.Getwd()
		if err == nil {
			cwd = wd
		}
	}
	timeout := shellDefaultTimeout
	if body.TimeoutMs > 0 {
		t := time.Duration(body.TimeoutMs) * time.Millisecond
		if t > shellMaxTimeout {
			t = shellMaxTimeout
		}
		timeout = t
	}

	type shellResult struct {
		exitCode int
		stdout   string
		stderr   string
		timedOut bool
	}
	done := make(chan shellResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", body.Command)
		cmd.Dir = cwd
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		// No stdin by design.
		res := shellResult{}
		if err := cmd.Run(); err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				res.exitCode = ee.ExitCode()
			} else {
				res.exitCode = -1
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				res.timedOut = true
			}
		}
		res.stdout = stdout.String()
		res.stderr = stderr.String()
		done <- res
	}()

	select {
	case <-r.Context().Done():
		// Client went away mid-run; the command continues in the background.
		return
	case res := <-done:
		writeJSON(w, 200, map[string]any{
			"ok":       true,
			"exitCode": res.exitCode,
			"stdout":   res.stdout,
			"stderr":   res.stderr,
			"timedOut": res.timedOut,
		})
	}
}

// firstAny returns the first present value under any of the given keys.
func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

// asInt64 is the lenient int reader for JSON-decoded numbers.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	case int:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}

// normalizeRewindPoints unwraps the x.ai/rewind/points response — flat
// {points:[…]} or ExtMethodResult {result:{points:[…]}} — and normalizes
// each point's field names (snake_case or camelCase on the wire) into
// {index, timestamp, summary?}. Points without a usable index are dropped.
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
		if ts := firstAny(m, "timestamp", "ts"); ts != nil {
			pt["timestamp"] = ts
		}
		if sum := firstAny(m, "summary", "description"); sum != nil {
			pt["summary"] = sum
		}
		out = append(out, pt)
	}
	return out
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

// ── admin endpoints (billing / memory / plan / permissions / MCP) ────

// writeAgentError maps an agent-backed endpoint failure to a response.
// Host-side HTTPErrors keep their status code (e.g. 404 暂无活动会话 — the
// task/list convention); agent-side failures (unsupported method, agent
// error, timeout) degrade to 200 + {ok:false, error} so the frontend can
// render an error row for the operation instead of a hard failure.
func writeAgentError(w http.ResponseWriter, method string, err error) {
	var he *acp.HTTPError
	if errors.As(err, &he) {
		writeJSON(w, he.Code, map[string]any{"ok": false, "error": he.Msg})
		return
	}
	msg := err.Error()
	if methodUnsupported(msg) {
		writeJSON(w, 200, map[string]any{"ok": false, "error": fmt.Sprintf("「%s」不受支持: %s", method, msg)})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false, "error": msg})
}

// methodUnsupported reports whether an agent error message indicates the
// wire method is not implemented (-32601 method_not_found and friends).
func methodUnsupported(msg string) bool {
	lower := strings.ToLower(msg)
	for _, pat := range []string{
		"-32601", "method not found", "unknown method", "no such method",
		"not supported", "unsupported", "not implemented",
	} {
		if strings.Contains(lower, pat) {
			return true
		}
	}
	return false
}

// extractPlanMode pulls a planMode bool out of an x.ai/toggle_plan_mode
// reply, whether it sits at the top level or inside an ExtMethodResult
// envelope ({result:{planMode:…}}). Returns ok=false when absent so the
// caller can fall back to a bare ok response.
func extractPlanMode(res map[string]any) (bool, bool) {
	if res == nil {
		return false, false
	}
	inner := res
	if r, ok := res["result"].(map[string]any); ok {
		inner = r
	}
	for _, k := range []string{"planMode", "plan_mode"} {
		if v, ok := inner[k].(bool); ok {
			return v, true
		}
	}
	return false, false
}

// normalizeMCPServers unwraps an x.ai/mcp/list reply into a []any of server
// entries, accepting three shapes: {servers:[…]} at the top level, an
// ExtMethodResult envelope {result:{servers:[…]}}, or a bare result array
// ({result:[…]}). Bare-string entries (server names) become {name: …};
// object entries pass through untouched.
func normalizeMCPServers(res map[string]any) []any {
	if res == nil {
		return nil
	}
	var raw []any
	if arr, ok := res["servers"].([]any); ok {
		raw = arr
	} else if r, ok := res["result"].(map[string]any); ok {
		if arr, ok := r["servers"].([]any); ok {
			raw = arr
		}
	} else if arr, ok := res["result"].([]any); ok {
		raw = arr
	}
	out := make([]any, 0, len(raw))
	for _, item := range raw {
		if item == nil {
			continue
		}
		if name, ok := item.(string); ok {
			out = append(out, map[string]any{"name": name})
			continue
		}
		out = append(out, item)
	}
	return out
}

// handleBilling — POST /api/billing {sessionId?} → _x.ai/billing.
// sessionId defaults to the active session.
func (s *Server) handleBilling(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.Billing(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/billing", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryFlush — POST /api/memory-flush {sessionId?} → _x.ai/memory/flush.
func (s *Server) handleMemoryFlush(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.MemoryFlush(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/flush", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryRewrite — POST /api/memory-rewrite {sessionId?} → _x.ai/memory/rewrite.
func (s *Server) handleMemoryRewrite(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.MemoryRewrite(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/rewrite", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleTogglePlanMode — POST /api/toggle-plan-mode {sessionId?} →
// _x.ai/toggle_plan_mode. The resulting planMode bool is extracted from
// the reply when present; otherwise a bare ok is returned.
func (s *Server) handleTogglePlanMode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.TogglePlanMode(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/toggle_plan_mode", err)
		return
	}
	out := map[string]any{"ok": true, "result": res}
	if pm, ok := extractPlanMode(res); ok {
		out["planMode"] = pm
	}
	writeJSON(w, 200, out)
}

// handlePermissionsReset — POST /api/permissions-reset {sessionId?} →
// _x.ai/permissions/reset.
func (s *Server) handlePermissionsReset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.PermissionsReset(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/permissions/reset", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPList — GET /api/mcp/list → _x.ai/mcp/list. The agent's server
// registry is returned defensively normalized under {servers:[…]}.
func (s *Server) handleMCPList(w http.ResponseWriter, r *http.Request) {
	res, err := s.bridge.MCPList(r.Context())
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/list", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "servers": normalizeMCPServers(res)})
}

// handleMCPToggle — POST /api/mcp-toggle {name, enabled} → _x.ai/mcp/toggle.
func (s *Server) handleMCPToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Name == "" || body.Enabled == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name 和 enabled"})
		return
	}
	res, err := s.bridge.MCPToggle(r.Context(), body.Name, *body.Enabled)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/toggle", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPAdd — POST /api/mcp-add {server:{name, command, args?, env?}} →
// _x.ai/mcp/upsert. The server object is passed through verbatim — the
// agent is the authority on its schema — only server.name is validated
// host-side.
func (s *Server) handleMCPAdd(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Server map[string]any `json:"server"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Server == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 server"})
		return
	}
	if name, _ := body.Server["name"].(string); name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 server.name"})
		return
	}
	res, err := s.bridge.MCPUpsert(r.Context(), body.Server)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/upsert", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPRemove — POST /api/mcp-remove {name} → _x.ai/mcp/delete.
func (s *Server) handleMCPRemove(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &body); err != nil || body.Name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	res, err := s.bridge.MCPDelete(r.Context(), body.Name)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/delete", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMCPAuthTrigger — POST /api/mcp-auth-trigger {name} →
// _x.ai/mcp/auth_trigger.
func (s *Server) handleMCPAuthTrigger(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &body); err != nil || body.Name == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	res, err := s.bridge.MCPAuthTrigger(r.Context(), body.Name)
	if err != nil {
		writeAgentError(w, "_x.ai/mcp/auth_trigger", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleExtensions — GET /api/extensions: the LOCAL extension inventory
// (~/.grok on disk), never routed through the agent. Missing/unreadable
// paths are skipped silently; the response is always 200 with
// {hooks:[], plugins:[], skills:[]}.
func (s *Server) handleExtensions(w http.ResponseWriter, r *http.Request) {
	home, err := os.UserHomeDir()
	if err != nil {
		writeJSON(w, 200, map[string]any{"hooks": []any{}, "plugins": []any{}, "skills": []any{}})
		return
	}
	writeJSON(w, 200, scanExtensions(home))
}

// handleSettings — GET /api/settings: the SAFE, read-only subset of
// ~/.grok/config.toml ({ui, session, models, cli} sections, scalar values
// only). The file is never written; a missing file yields {}.
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	home, err := os.UserHomeDir()
	if err != nil {
		writeJSON(w, 200, map[string]any{})
		return
	}
	sections := parseConfigTOML(filepath.Join(home, ".grok", "config.toml"))
	out := map[string]any{}
	for _, name := range []string{"ui", "session", "models", "cli"} {
		if kv, ok := sections[name]; ok && len(kv) > 0 {
			out[name] = kv
		}
	}
	writeJSON(w, 200, out)
}
