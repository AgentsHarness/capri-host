package acp

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// handleAgentRequest — minimal client: only session/request_permission is interactive.
// fs/* and terminal/* are rejected (capabilities declared false; agent should not call them).
func (b *Bridge) handleAgentRequest(id any, method string, params map[string]any) {
	switch method {
	case "session/request_permission":
		b.forwardPermission(id, method, params)
	case "fs/read_text_file", "fs/write_text_file",
		"terminal/create", "terminal/output", "terminal/wait_for_exit",
		"terminal/kill", "terminal/release":
		b.respondError(id, fmt.Sprintf("客户端未提供 %s（纯 Agent 执行模式）", method), -32601)
	default:
		b.respondError(id, fmt.Sprintf("客户端不支持方法 %s", method), -32601)
	}
}

func (b *Bridge) forwardPermission(id any, method string, params map[string]any) {
	b.clientReqMu.Lock()
	defer b.clientReqMu.Unlock()
	b.mu.Lock()
	stdin := b.stdin
	b.mu.Unlock()
	reqID := fmt.Sprintf("acp_cr_%d", b.nextClientReqID.Add(1))
	cr := &clientRequest{
		stdin:        stdin,
		AgentID:      id,
		SessionID:    b.sessionIdFrom(params),
		Method:       method,
		Params:       params,
		done:         make(chan struct{}),
		isPermission: true,
		receivedAt:   time.Now().UnixMilli(),
	}
	b.clientReqs.Store(reqID, cr)
	b.setSessionAwaiting(cr.SessionID, true)
	b.Broadcast(Event{
		kType:        "client_request",
		"requestId":  reqID,
		kMethod:      method,
		kParams:      params,
		kSessionID:   cr.SessionID,
		"receivedAt": cr.receivedAt,
	})

	go b.waitClientResolution(reqID, cr)
}

// forwardXaiRequest forwards an agent → client extension request
// (x.ai/ask_user_question, x.ai/exit_plan_mode, x.ai/mcp/sdk_call, …) to
// the browser as a generic client_request; the browser's response is passed
// back verbatim as the JSON-RPC result.
func (b *Bridge) forwardXaiRequest(id any, method string, params map[string]any) {
	b.clientReqMu.Lock()
	defer b.clientReqMu.Unlock()
	b.mu.Lock()
	stdin := b.stdin
	b.mu.Unlock()
	reqID := fmt.Sprintf("acp_cr_%d", b.nextClientReqID.Add(1))
	cr := &clientRequest{
		stdin:      stdin,
		AgentID:    id,
		SessionID:  b.sessionIdFrom(params),
		Method:     method,
		Params:     params,
		done:       make(chan struct{}),
		receivedAt: time.Now().UnixMilli(),
	}
	b.clientReqs.Store(reqID, cr)
	b.setSessionAwaiting(cr.SessionID, true)
	b.Broadcast(Event{
		kType:        "client_request",
		"requestId":  reqID,
		kMethod:      method,
		kParams:      params,
		kSessionID:   cr.SessionID,
		"receivedAt": cr.receivedAt,
	})

	go b.waitClientResolution(reqID, cr)
}

// waitClientResolution blocks until the browser resolves the request,
// the request's own budget elapses, or the agent connection dies, then
// writes the JSON-RPC response back to the agent.
func (b *Bridge) waitClientResolution(reqID string, cr *clientRequest) {
	budget, deadline := b.clientRequestBudget(cr)
	if !deadline {
		// No host-side deadline: only the browser or the agent's own
		// timeout can end this request.
		<-cr.done
		b.resolveClientRequest(reqID, cr)
		return
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-cr.done:
		b.resolveClientRequest(reqID, cr)
	case <-timer.C:
		b.expireClientRequest(reqID, cr)
	}
}

// clientRequestBudget is how long the host holds a forwarded request open
// waiting for the browser, and whether it has a host-side deadline at all.
// A permission prompt uses approvalTimeout (the host's own constant). An
// x.ai/ask_user_question uses the budget the user configured for it —
// [toolset.ask_user_question] timeout_secs, the same value the agent
// enforces and the question card counts down from — because expiring it
// early closes a card the user is still looking at (with the FE default of
// 1800s the host's 900s would cut every long question in half).
// timeout_enabled=false means no host-side deadline: the agent still owns
// its own budget, so the host waits rather than inventing one.
func (b *Bridge) clientRequestBudget(cr *clientRequest) (time.Duration, bool) {
	if cr == nil || cr.Method != methodAskUserQuestion {
		return approvalTimeout, true
	}
	return b.askUserQuestionBudget()
}

// askUserQuestionBudget reads [toolset.ask_user_question] from config.toml.
// The file is re-read per question (questions are rare and the settings
// pane writes it live); anything unusable — unreadable file, missing key,
// non-positive value — falls back to the agent's own default.
func (b *Bridge) askUserQuestionBudget() (time.Duration, bool) {
	path, err := b.ConfigTOMLPath()
	if err != nil {
		return askUserQuestionTimeoutDefault, true
	}
	table, err := readConfigTable(path)
	if err != nil {
		return askUserQuestionTimeoutDefault, true
	}
	ask, _ := readConfigSection(table, "toolset", "ask_user_question")
	if enabled, ok := ask["timeout_enabled"].(bool); ok && !enabled {
		return 0, false
	}
	secs, ok := asInt(ask["timeout_secs"])
	if !ok || secs <= 0 {
		return askUserQuestionTimeoutDefault, true
	}
	return time.Duration(secs) * time.Second, true
}

func (b *Bridge) expireClientRequest(reqID string, cr *clientRequest) {
	b.clientReqMu.Lock()
	select {
	case <-cr.done: // An accepted answer wins over the timer.
	default:
		cr.errMsg = "审批超时"
		cr.errCode = -32002
		if cr.Method == methodAskUserQuestion {
			cr.errMsg = "提问超时"
		}
		cr.cancel = cr.isPermission
		close(cr.done)
	}
	msg := b.resolveClientRequestLocked(reqID, cr)
	b.clientReqMu.Unlock()
	if msg != nil {
		_ = writeAgentMessage(cr.stdin, msg)
	}
}

// broadcastClientRequestResolved notifies every SSE subscriber that a
// forwarded client request (permission / x.ai question / …) is no longer
// pending. Clients remove matching pending / xaiRequests rows by requestId.
// method / cancelled / error / outcome ride along so a sibling tab can
// apply side effects (e.g. leave plan mode after exit_plan_mode) without
// waiting on a later CurrentModeUpdate that the shell may drop.
func (b *Bridge) broadcastClientRequestResolved(reqID string, cr *clientRequest) {
	ev := Event{
		kType:       "client_request_resolved",
		"requestId": reqID,
	}
	if cr == nil {
		b.Broadcast(ev)
		return
	}
	if cr.SessionID != "" {
		ev[kSessionID] = cr.SessionID
	}
	if cr.Method != "" {
		ev[kMethod] = cr.Method
	}
	if cr.cancel {
		ev["cancelled"] = true
	}
	if cr.errMsg != "" {
		ev["error"] = cr.errMsg
	}
	if cr.result != nil {
		ev["result"] = cr.result
		if o, ok := cr.result["outcome"].(string); ok && o != "" {
			ev["outcome"] = o
		}
	}
	b.Broadcast(ev)
}

// resolveClientRequest writes the JSON-RPC response for a client request
// the browser resolved before the approval timeout (cancel / error /
// permission outcome / raw result).
func (b *Bridge) resolveClientRequest(reqID string, cr *clientRequest) {
	b.clientReqMu.Lock()
	msg := b.resolveClientRequestLocked(reqID, cr)
	b.clientReqMu.Unlock()
	if msg != nil {
		_ = writeAgentMessage(cr.stdin, msg)
	}
}

// Bookkeeping finishes before I/O: a blocked old pipe must not block restart.
func (b *Bridge) resolveClientRequestLocked(reqID string, cr *clientRequest) map[string]any {
	if !b.clientReqs.CompareAndDelete(reqID, cr) {
		return nil
	}
	defer b.broadcastClientRequestResolved(reqID, cr)
	// Another request in the same session may still be awaiting input.
	awaiting := false
	b.clientReqs.Range(func(_, value any) bool {
		if value.(*clientRequest).SessionID == cr.SessionID {
			awaiting = true
		}
		return !awaiting
	})
	b.setSessionAwaiting(cr.SessionID, awaiting)
	if cr.retired {
		return nil
	}
	msg := map[string]any{kJSONRPC: "2.0", kID: cr.AgentID}
	switch {
	case cr.cancel && cr.isPermission:
		msg[kResult] = permissionResult(map[string]any{"outcome": "cancelled"}, cr.meta)
	case cr.cancel:
		msg[kError] = map[string]any{"code": -32800, "message": "已取消"}
	case cr.errMsg != "":
		code := cr.errCode
		if code == 0 {
			code = -32001
		}
		msg[kError] = map[string]any{"code": code, "message": cr.errMsg}
	case cr.isPermission:
		msg[kResult] = permissionResult(cr.outcome, cr.meta)
	default:
		msg[kResult] = cr.result
	}
	return msg
}

// permissionResult wraps an ACP permission outcome with the optional
// response `_meta` — the ACP wire shape is result = {outcome, _meta?}
// (RequestPermissionResponse; `_meta` reserved for client/agent extension
// metadata). The meta object carries exactly the keys the TUI puts there:
// BashCommandSelectedTerms {command_parts, is_glob} or {followup_message}.
func permissionResult(outcome map[string]any, meta map[string]any) map[string]any {
	if len(meta) == 0 {
		return map[string]any{"outcome": outcome}
	}
	return map[string]any{
		"outcome": outcome,
		kMeta:     meta,
	}
}

// RespondPermissionWithMeta resolves a pending session/request_permission,
// forwarding the client's bash scope and/or followup message to the agent
// via the ACP response `_meta` — the same wire shape the TUI sends:
//
//   - scope (Selected replies): {command_parts: [...], is_glob: bool}
//     (BashCommandSelectedTerms, word-scope or authored glob).
//   - followupMessage on a cancelled reply: resolved with the request's
//     RejectOnce option + {followup_message: text} — the TUI
//     dispatch_permission_followup semantics, because the agent only reads
//     followup text off the RejectOnce branch. Without a RejectOnce option
//     it falls back to a plain Cancelled outcome (TUI does the same).
//
// Both fields are optional; with none set the response carries no `_meta`.
func (b *Bridge) RespondPermissionWithMeta(requestID, optionID string, cancelled bool, scope *PermissionScope, followupMessage string) error {
	b.clientReqMu.Lock()
	defer b.clientReqMu.Unlock()
	v, ok := b.clientReqs.Load(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	select {
	case <-cr.done:
		return errors.New("审批请求不存在或已过期")
	default:
	}
	if cancelled {
		if msg := strings.TrimSpace(followupMessage); msg != "" {
			if rejectOnce := rejectOnceOptionID(cr.Params); rejectOnce != "" {
				cr.outcome = map[string]any{
					"outcome":  "selected",
					"optionId": rejectOnce,
				}
				cr.meta = map[string]any{"followup_message": msg}
				close(cr.done)
				return nil
			}
		}
		cr.cancel = true
		close(cr.done)
		return nil
	}
	cr.outcome = map[string]any{
		"outcome":  "selected",
		"optionId": optionID,
	}
	if scope != nil && len(scope.CommandParts) > 0 {
		cr.meta = map[string]any{
			"command_parts": scope.CommandParts,
			"is_glob":       scope.IsGlob,
		}
	}
	close(cr.done)
	return nil
}

// rejectOnceOptionID returns the optionId of the request's RejectOnce
// option ("" when absent). The agent serializes PermissionOptionKind as
// snake_case ("reject_once"); the camelCase form is tolerated defensively.
func rejectOnceOptionID(params map[string]any) string {
	raw, _ := params["options"].([]any)
	for _, item := range raw {
		opt, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := opt["kind"].(string)
		if kind != "reject_once" && kind != "rejectOnce" {
			continue
		}
		id, _ := opt["optionId"].(string)
		return id
	}
	return ""
}

// RespondClientRequest resolves a forwarded x.ai/* request with either a
// raw result object (passed through as the JSON-RPC result) or an error
// message.
func (b *Bridge) RespondClientRequest(requestID string, result map[string]any, errMsg string) error {
	b.clientReqMu.Lock()
	defer b.clientReqMu.Unlock()
	v, ok := b.clientReqs.Load(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	select {
	case <-cr.done:
		return errors.New("审批请求不存在或已过期")
	default:
	}
	if errMsg != "" {
		cr.errMsg = errMsg
	} else {
		cr.result = result
	}
	close(cr.done)
	return nil
}
