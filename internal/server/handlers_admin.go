package server

import (
	"errors"
	"fmt"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"net/http"
	"strings"
)

// ── admin endpoints (billing / memory / plan / permissions / MCP) ────

// writeAgentError maps an agent-backed endpoint failure to a response.
// Three-tier contract (the frontend classifies on exactly this):
//   - host-side HTTPError          → keep its status code (404 会话不存在
//     等；goal 端点仍用 409 表示状态冲突);
//   - agent JSON-RPC rejection (RPCError) → 200 + {ok:false, error, code?} —
//     the agent is alive and answered; the FE renders an operation/turn
//     error row, never a connection error;
//   - transport failure (timeout / write error / boot failure) → 502
//     (bad gateway) — the host is fine, the upstream agent is not; the FE
//     still renders an operation error row but can tell the two apart.
//
// Unsupported-method replies keep their friendly 200 + ok:false form.
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
	var rpcErr *acp.RPCError
	if errors.As(err, &rpcErr) {
		body := map[string]any{"ok": false, "error": msg}
		if rpcErr.Code != 0 {
			body["code"] = rpcErr.Code
		}
		writeJSON(w, 200, body)
		return
	}
	writeJSON(w, 502, map[string]any{"ok": false, "error": msg})
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

// handleMemoryRewrite — POST /api/memory-rewrite {sessionId?, rawText,
// contextSummary?} → _x.ai/memory/rewrite. rawText is required by the
// agent's request contract (rawText + contextSummary).
func (s *Server) handleMemoryRewrite(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID      string `json:"sessionId"`
		RawText        string `json:"rawText"`
		ContextSummary string `json:"contextSummary"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.RawText == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 rawText"})
		return
	}
	res, err := s.bridge.MemoryRewrite(r.Context(), body.SessionID, body.RawText, body.ContextSummary)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/rewrite", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryList — POST /api/memory-list {sessionId?} → _x.ai/memory/list.
// Returns the /memory modal's listing; the agent requires a resident session
// (a session it has not loaded answers "session not found" as invalid params).
func (s *Server) handleMemoryList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.MemoryList(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/list", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryToggle — POST /api/memory-toggle {sessionId?, enabled} →
// _x.ai/memory/toggle. `enabled` is required by the agent's request contract,
// so a body without it is a 400 rather than a silent false (which would read
// as "turn memory off").
func (s *Server) handleMemoryToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
		Enabled   *bool  `json:"enabled"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Enabled == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 enabled"})
		return
	}
	res, err := s.bridge.MemoryToggle(r.Context(), body.SessionID, *body.Enabled)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/toggle", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryDream — POST /api/memory-dream {sessionId?} → _x.ai/memory/dream
// (manual consolidation, the /dream command). The result carries a
// disposition the caller renders; the host does not interpret it.
func (s *Server) handleMemoryDream(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	res, err := s.bridge.MemoryDream(r.Context(), body.SessionID)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/dream", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// handleMemoryForget — POST /api/memory-forget {sessionId?, path,
// expectedContentHash} → _x.ai/memory/forget. `expectedContentHash` is the
// BLAKE3 hex the CALLER computed over the note text it previewed; the host
// forwards it verbatim (it never reads the file itself — the agent owns file
// access). The agent refuses on a mismatch ("changed"), which is what keeps a
// note edited after the preview from being deleted unnoticed.
func (s *Server) handleMemoryForget(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID           string `json:"sessionId"`
		Path                string `json:"path"`
		ExpectedContentHash string `json:"expectedContentHash"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Path == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 path"})
		return
	}
	if !isBlake3Hex(body.ExpectedContentHash) {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 expectedContentHash（BLAKE3 十六进制）"})
		return
	}
	res, err := s.bridge.MemoryForget(r.Context(), body.SessionID, body.Path, body.ExpectedContentHash)
	if err != nil {
		writeAgentError(w, "_x.ai/memory/forget", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": res})
}

// isBlake3Hex reports whether s is a 32-byte BLAKE3 digest in hex — the only
// form the memory store compares against. Case-insensitive: `blake3::Hash::to_hex`
// emits lowercase, but rejecting an uppercase digest would be a needless 400.
func isBlake3Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// handleTogglePlanMode — POST /api/toggle-plan-mode {sessionId?} →
// _x.ai/toggle_plan_mode (sent as a fire-and-forget notification; the
// agent has no request branch for it). The frontend applies its local
// desired planMode, so a bare ok is the contract.
func (s *Server) handleTogglePlanMode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"sessionId"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if _, err := s.bridge.TogglePlanMode(r.Context(), body.SessionID); err != nil {
		writeAgentError(w, "_x.ai/toggle_plan_mode", err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
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
