package acp

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// permissionParams builds a session/request_permission params map with the
// given options (wire shape: {optionId, name, kind} — options arrive as a
// JSON array, hence []any).
func permissionParams(options ...map[string]any) map[string]any {
	opts := make([]any, 0, len(options))
	for _, o := range options {
		opts = append(opts, o)
	}
	return map[string]any{
		"sessionId": "s1",
		"toolCall":  map[string]any{},
		"options":   opts,
	}
}

func allowAlwaysOption() map[string]any {
	return map[string]any{"optionId": "allow-always-command", "name": "Always allow", "kind": "allow_always"}
}

func rejectOnceOption() map[string]any {
	return map[string]any{"optionId": "reject-once", "name": "Reject once", "kind": "reject_once"}
}

// pushPermission feeds an agent session/request_permission request into the
// bridge (as if it arrived on the agent stdin) and returns the requestId
// the browser-facing client gets. The bridge answers on the recordingStdin
// asynchronously; waitForWireLine polls for the JSON-RPC response.
func pushPermission(t *testing.T, b *Bridge, w *recordingStdin, params map[string]any, agentID float64) string {
	t.Helper()
	// The bridge stamps client request ids from nextClientReqID with
	// Add(1) (post-increment), so the next id is counter+1. Read it first
	// so the caller knows the requestId.
	next := b.nextClientReqID.Load()
	b.onAgentMessage(map[string]any{
		"jsonrpc": "2.0",
		"id":      agentID,
		"method":  "session/request_permission",
		"params":  params,
	})
	return fmt.Sprintf("acp_cr_%d", next+1)
}

// waitForWireResponse polls the recording stdin until a JSON-RPC response
// with the given id appears and returns its result object.
func waitForWireResponse(t *testing.T, b *Bridge, w *recordingStdin, id float64) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msg := w.last()
		if msg != nil {
			if mid, ok := msg["id"].(float64); ok && mid == id {
				res, _ := msg["result"].(map[string]any)
				return res
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("bridge never wrote the permission response (id=%v) to stdin", id)
	return nil
}

// A Selected reply with a bash scope must carry the ACP `_meta` exactly as
// the TUI sends it: BashCommandSelectedTerms {command_parts, is_glob} as a
// sibling of outcome — and the outcome object must stay byte-compatible.
func TestRespondPermissionWithMetaScope(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption(), rejectOnceOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "allow-always-command", false, &PermissionScope{
		CommandParts: []string{"gh", "api"},
		IsGlob:       false,
	}, ""); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}

	res := waitForWireResponse(t, b, w, 7)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "allow-always-command" {
		t.Fatalf("outcome = %v, want selected/allow-always-command", outcome)
	}
	meta, ok := res["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("result = %v, want _meta present", res)
	}
	parts, _ := meta["command_parts"].([]any)
	if len(parts) != 2 || parts[0] != "gh" || parts[1] != "api" {
		t.Errorf("command_parts = %v, want [gh api]", meta["command_parts"])
	}
	if meta["is_glob"] != false {
		t.Errorf("is_glob = %v, want false", meta["is_glob"])
	}
}

// A glob-scoped selection keeps is_glob true and the authored pattern as
// the single command part.
func TestRespondPermissionWithMetaGlob(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "allow-always-command", false, &PermissionScope{
		CommandParts: []string{"rm -rf /tmp/*"},
		IsGlob:       true,
	}, ""); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}

	res := waitForWireResponse(t, b, w, 7)
	meta, _ := res["_meta"].(map[string]any)
	parts, _ := meta["command_parts"].([]any)
	if len(parts) != 1 || parts[0] != "rm -rf /tmp/*" {
		t.Errorf("command_parts = %v, want [rm -rf /tmp/*]", meta["command_parts"])
	}
	if meta["is_glob"] != true {
		t.Errorf("is_glob = %v, want true", meta["is_glob"])
	}
}

// A cancelled reply with a followup message must resolve with the request's
// RejectOnce option + {followup_message} meta (TUI dispatch_permission_followup:
// the agent only reads followup text off the RejectOnce branch).
func TestRespondPermissionWithMetaFollowupUsesRejectOnce(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption(), rejectOnceOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "", true, nil, "  不要这么做  "); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}

	res := waitForWireResponse(t, b, w, 7)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "reject-once" {
		t.Fatalf("outcome = %v, want selected/reject-once (TUI followup semantics)", outcome)
	}
	meta, _ := res["_meta"].(map[string]any)
	if meta["followup_message"] != "不要这么做" {
		t.Errorf("followup_message = %v, want trimmed text", meta["followup_message"])
	}
}

// A followup on a request without a RejectOnce option falls back to a plain
// Cancelled outcome (no meta) — exactly the TUI's fallback.
func TestRespondPermissionFollowupFallsBackToCancelled(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "", true, nil, "不要这么做"); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}

	res := waitForWireResponse(t, b, w, 7)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "cancelled" {
		t.Fatalf("outcome = %v, want cancelled", outcome)
	}
	if _, has := res["_meta"]; has {
		t.Errorf("result = %v, want no _meta on the Cancelled fallback", res)
	}
}

// Plain cancelled without a followup message, and Selected without a scope,
// must not gain meta — no scope / followup means the old wire shape.
func TestRespondPermissionNoMetaWireShape(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "", true, nil, ""); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}
	res := waitForWireResponse(t, b, w, 7)
	outcome, _ := res["outcome"].(map[string]any)
	if outcome["outcome"] != "cancelled" {
		t.Fatalf("outcome = %v, want cancelled", outcome)
	}
	if _, has := res["_meta"]; has {
		t.Errorf("result = %v, want no _meta", res)
	}

	// Selected without scope: same shape, no meta.
	reqID = pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 8)
	if err := b.RespondPermissionWithMeta(reqID, "allow-always-command", false, nil, ""); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}
	res = waitForWireResponse(t, b, w, 8)
	outcome, _ = res["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "allow-always-command" {
		t.Fatalf("outcome = %v, want selected/allow-always-command", outcome)
	}
	if _, has := res["_meta"]; has {
		t.Errorf("result = %v, want no _meta", res)
	}
}

// A nil/empty scope must not attach meta; unknown request ids still error.
func TestRespondPermissionMetaEdgeCases(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "allow-always-command", false, nil, ""); err != nil {
		t.Fatalf("RespondPermissionWithMeta(nil scope) = %v", err)
	}
	res := waitForWireResponse(t, b, w, 7)
	if _, has := res["_meta"]; has {
		t.Errorf("result = %v, want no _meta for nil scope", res)
	}

	if err := b.RespondPermissionWithMeta("acp_cr_missing", "x", false, nil, ""); err == nil {
		t.Error("RespondPermissionWithMeta on unknown requestId = nil, want error")
	}
}

// The full response line must be valid ACP: result = {outcome, _meta} with
// the `_meta` key spelled exactly as the protocol expects (snake_case wire).
func TestRespondPermissionMetaSerializesAsMetaKey(t *testing.T) {
	b, w := readyBridge()
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption(), rejectOnceOption()), 7)

	if err := b.RespondPermissionWithMeta(reqID, "allow-always-command", false, &PermissionScope{
		CommandParts: []string{"cargo", "test"},
	}, ""); err != nil {
		t.Fatalf("RespondPermissionWithMeta = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var raw string
	for time.Now().Before(deadline) {
		w.mu.Lock()
		if len(w.lines) > 0 {
			raw = string(w.lines[len(w.lines)-1])
		}
		w.mu.Unlock()
		if raw != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	var msg map[string]any
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("response line is not JSON: %v (%q)", err, raw)
	}
	res, _ := msg["result"].(map[string]any)
	if _, has := res["_meta"]; !has {
		t.Fatalf("result = %v, want the wire key `_meta`", res)
	}
}
