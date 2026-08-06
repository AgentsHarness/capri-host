package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
)

type Server struct {
	cfg    config.Config
	bridge *acp.Bridge
	http   *http.Server
}

func New(cfg config.Config, bridge *acp.Bridge) *Server {
	s := &Server{cfg: cfg, bridge: bridge}
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
	mux.HandleFunc("POST /api/sessions", s.handleListSessions)
	mux.HandleFunc("POST /api/session-updates", s.handleSessionUpdates)
	mux.HandleFunc("POST /api/session-fork", s.handleSessionFork)
	mux.HandleFunc("POST /api/session-rename", s.handleSessionRename)
	mux.HandleFunc("POST /api/recap", s.handleRecap)
	mux.HandleFunc("POST /api/subagent-cancel", s.handleSubagentCancel)
	mux.HandleFunc("POST /api/task-kill", s.handleTaskKill)
	mux.HandleFunc("GET /api/hosts", s.handleHosts)
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
		"text":            snap.Text,
		"error":           snap.BootError,
		"agentInfo":       snap.AgentInfo,
		"modes":           snap.Modes,
		"configOptions":   snap.ConfigOptions,
		"pendingRequests": snap.PendingRequests,
		"hostId":          snap.HostID,
		"hostName":        snap.HostName,
		"capabilities":    snap.Capabilities,
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
	sr, err := s.bridge.Prompt(ctx, body.Blocks)
	if err != nil {
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "stopReason": sr})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.bridge.Cancel()
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

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.bridge.ListSessions(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "sessions": sessions})
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
	if err := s.bridge.LoadSession(r.Context(), body.SessionID, body.Cwd); err != nil {
		code := 500
		var he *acp.HTTPError
		if errors.As(err, &he) {
			code = he.Code
		}
		writeJSON(w, code, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "sessionId": body.SessionID})
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
