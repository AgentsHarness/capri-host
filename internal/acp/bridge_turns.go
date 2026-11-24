package acp

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"
)

// ── roster helpers ────────────────────────────────────────────────

// openTurnLocked arms the agent-observed turn for sid at now (unix ms).
// Callers hold b.mu.
func (b *Bridge) openTurnLocked(sid string, now int64) {
	if sid == "" {
		return
	}
	if b.turns == nil {
		b.turns = make(map[string]*observedTurn)
	}
	w := b.turns[sid]
	if w == nil {
		w = &observedTurn{}
		b.turns[sid] = w
	}
	w.open = true
	w.seenAt = now
}

// closeTurnLocked disarms the agent-observed turn for sid, keeping the
// last-activity stamp for the session-list report. Callers hold b.mu.
func (b *Bridge) closeTurnLocked(sid string) {
	if w := b.turns[sid]; w != nil {
		w.open = false
	}
}

// syncBusyLocked re-projects a roster session's Busy from the two legs the
// host can observe — its own in-flight session/prompt count and the
// agent-observed turn — and reports whether the flag flipped (the caller
// announces the change). Callers hold b.mu.
func (b *Bridge) syncBusyLocked(s *SessionState) bool {
	w := b.turns[s.SessionID]
	busy := s.busyCount > 0 || (w != nil && w.open)
	if s.Busy == busy {
		return false
	}
	s.Busy = busy
	return true
}

// noteTurnActivity records that the agent is working on a turn for sid: it
// opens the observed turn, refreshes LastActiveAt and re-projects Busy.
//
// Called from the event stream on every turn-scoped update, so it must stay
// lock-cheap and broadcast only on the idle→busy / busy→idle flip (a flip
// happens at most once per turn; the update itself is never re-broadcast).
// No `busy` event is synthesized here: that event means "this frontend's
// turn started" (it arms the Waiting-for-response window), and a turn the
// host did not drive is already rendered from its own chunk/tool updates.
// Clients learn the new badge from sessions_changed + the next list fetch.
func (b *Bridge) noteTurnActivity(sid string) {
	if sid == "" {
		return
	}
	now := time.Now().UnixMilli()
	b.mu.Lock()
	b.openTurnLocked(sid, now)
	flipped := false
	if s := b.sessions[sid]; s != nil {
		if now > s.LastActiveAt {
			s.LastActiveAt = now
		}
		flipped = b.syncBusyLocked(s)
	}
	b.mu.Unlock()
	if flipped {
		b.broadcastRosterRows(sid)
	}
}

// noteTurnEnd clears the agent-observed turn for sid (the agent reported
// turn_completed, or a turn the host drove came to a
// definitive end) and re-projects Busy. See noteTurnActivity for the
// broadcast discipline.
func (b *Bridge) noteTurnEnd(sid string) {
	if sid == "" {
		return
	}
	b.mu.Lock()
	b.closeTurnLocked(sid)
	flipped := false
	if s := b.sessions[sid]; s != nil {
		flipped = b.syncBusyLocked(s)
	}
	b.mu.Unlock()
	if flipped {
		b.broadcastRosterRows(sid)
	}
}

// settleTurnsLocked drops observed turns that have gone silent for longer
// than turnStaleAfter without ever reporting a terminal update, so a lost
// update cannot pin a finished session to "active" forever (turns the host
// drives with a prompt in flight are never touched by the observed leg's
// expiry — busyCount keeps them busy on its own). Awaiting-input turns are
// exempt: the agent is legitimately silent while it waits for the user.
// Stale closed entries are pruned to keep the map bounded. Reports whether
// any session's Busy flipped; the caller broadcasts once. Callers hold b.mu.
func (b *Bridge) settleTurnsLocked(now int64) bool {
	changed := false
	for sid, w := range b.turns {
		age := now - w.seenAt
		if !w.open {
			if age > turnStaleAfter.Milliseconds() {
				delete(b.turns, sid)
			}
			continue
		}
		if age <= turnStaleAfter.Milliseconds() {
			continue
		}
		s := b.sessions[sid]
		if s != nil && s.AwaitingInput {
			continue
		}
		w.open = false
		if s != nil && b.syncBusyLocked(s) {
			changed = true
		}
	}
	return changed
}

// turnEvidenceKinds are the sessionUpdate kinds that prove the agent is
// executing a turn: they arm the observed leg of Busy, which
// turn_completed then clears.
//
// Deliberately a whitelist rather than "everything except the terminal
// kinds": a false "active" is sticky (only a terminal update or the
// turnStaleAfter expiry clears it) while a false "idle" self-heals on the
// next update, so only kinds that cannot arrive outside a running turn are
// admitted. Excluded on purpose: session metadata (title / modes / config /
// commands / model switches — those fire while a session sits idle),
// usage_update (it also rides along at turn end), the background-task and
// monitor rails (a backgrounded shell can finish long after its turn did),
// subagent_finished (a detached subagent can report after its parent
// turn ended), the memory system (memory_flush_started /
// memory_flush_completed run as turn-end housekeeping and arrive AFTER
// the turn_completed that closed the turn, so admitting them re-arms a
// turn nothing will ever close again — the session reports "active"
// until the turnStaleAfter expiry), and workflow_updated (the workflow
// panel's state is re-broadcast after the parent turn ended, and can
// arrive days later for a run that finished long ago; measured over the
// local session history it never once opened a turn, while it re-armed
// finished ones in nine sessions).
//
// retry_state is the one kind whose evidence depends on its payload, so it
// is handled in observeUpdateKind rather than this map.
var turnEvidenceKinds = map[string]bool{
	"agent_message_chunk":     true,
	"agent_thought_chunk":     true,
	"user_message_chunk":      true,
	"tool_call":               true,
	"tool_call_update":        true,
	"tool_call_delta_chunk":   true,
	"plan":                    true,
	"plan_update":             true,
	"diff_review":             true,
	"response_started":        true,
	"reasoning_completed":     true,
	"pending_interaction":     true,
	"interaction_resolved":    true,
	"subagent_spawned":        true,
	"subagent_progress":       true,
	"goal_updated":            true,
	"scheduled_task_fired":    true,
	"auto_compact_started":    true,
	"auto_compact_completed":  true,
	"auto_compact_failed":     true,
	"auto_compact_cancelled":  true,
	"auto_continue_completed": true,
}

// retryStateIsEvidence reports whether one retry_state update proves the
// agent is executing a turn. `retrying` means an inference attempt is in
// flight (the agent is retrying the very request a turn is waiting on), so
// it opens the observed turn like the streaming kinds do. `failed` and
// `exhausted` are terminal for the attempt — the agent emits them while
// unwinding a turn that already ended, and admitting them re-armed finished
// sessions (measured: 33 turns in the local history carried a terminal
// retry_state after their own turn_completed, the last one 69s late).
func retryStateIsEvidence(update map[string]any) bool {
	t, _ := update[kType].(string)
	return t != "failed" && t != "exhausted"
}

// observeUpdateKind maintains the observed turn from one sessionUpdate kind:
// execution evidence opens it, a terminal kind closes it. Unknown/unmodelled
// kinds are ignored — a future kind does not get to decide session state
// until it is classified here. The update payload is needed for the kinds
// whose evidence depends on their own fields (see retryStateIsEvidence).
func (b *Bridge) observeUpdateKind(sid, kind string, update map[string]any) {
	switch kind {
	case "turn_completed":
		b.noteTurnEnd(sid)
	case "session_ended":
		// grok 1.0.89 adds SessionUpdate::SessionEnded. The session is gone,
		// so any turn it was running can never emit its own terminal update;
		// clear the observed leg here instead of waiting for turnStaleAfter.
		b.noteTurnEnd(sid)
	case "retry_state":
		if retryStateIsEvidence(update) {
			b.noteTurnActivity(sid)
		}
	default:
		if turnEvidenceKinds[kind] {
			b.noteTurnActivity(sid)
		}
	}
}

// activeSession returns the current active session (nil if none).
// Callers must hold b.mu.
func (b *Bridge) activeSessionLocked() *SessionState {
	if id := b.activeSessionID; id != "" {
		return b.sessions[id]
	}
	return nil
}

// ActiveSessionID returns the session clients are currently attached to
// ("" when none). Callers that must attribute agent-side state to a
// specific session use this to refuse guessing when the question is about
// some other (read-only, historical) session.
func (b *Bridge) ActiveSessionID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.activeSessionID
}

// releaseBusy drops one in-flight prompt's claim on a session and, when that
// leaves the session with nothing in flight, flips it idle and notifies
// clients (busy → idle transition only — a turn that resolves while a
// sibling is still running must not report an idle the session never
// had). It only releases when the roster still holds the SAME session
// object the turn started on: resetRoster wipes and rebuilds the roster
// (and a user-requested restart may restore the session), so a stale
// turn must not clear the busy flag of a newer turn on the restored
// session.
//
// turnEnded says whether the agent actually finished the turn: a prompt that
// was answered (with a result or with an agent-side JSON-RPC error — the
// agent answers session/prompt at turn end) did, as did one the host itself
// aborted (client disconnect → session/cancel) or never delivered. A plain
// transport failure did not — on the 30-minute promptTimeout of a long turn
// the agent is usually still working, so the observed turn stays armed and
// Busy keeps following the update stream instead of falsely reporting idle
// for the rest of that turn.
func (b *Bridge) releaseBusy(s *SessionState, sessionID string, turnEnded bool) {
	b.mu.Lock()
	changed := false
	if cur := b.sessions[sessionID]; cur == s && s.busyCount > 0 {
		s.busyCount--
		if turnEnded {
			b.closeTurnLocked(sessionID)
		}
		changed = b.syncBusyLocked(s)
	}
	b.mu.Unlock()
	if changed {
		b.broadcastRosterRows(sessionID)
	}
}

// setSessionAwaiting flips a session's awaiting-input state (pending
// permission / x.ai question) and notifies clients on transition. A pending
// interaction is itself proof that a turn is in flight — and State() only
// reports "awaiting" while the session is busy — so flipping to true also
// arms the observed turn and refreshes LastActiveAt, which a turn the host
// no longer tracks with a prompt would otherwise lose.
func (b *Bridge) setSessionAwaiting(id string, awaiting bool) {
	now := time.Now().UnixMilli()
	b.mu.Lock()
	s := b.sessions[id]
	changed := false
	if s != nil && s.AwaitingInput != awaiting {
		s.AwaitingInput = awaiting
		changed = true
	}
	if awaiting {
		b.openTurnLocked(id, now)
		if s != nil {
			if now > s.LastActiveAt {
				s.LastActiveAt = now
			}
			if b.syncBusyLocked(s) {
				changed = true
			}
		}
	}
	b.mu.Unlock()
	if changed {
		b.broadcastRosterRows(id)
	}
}

// promptIDOf returns the FE-minted per-turn id used for retry idempotency
// (wire: meta.promptId), falling back to messageId when a client sends that
// instead. Both are client-generated and stable across a retry.
func promptIDOf(opts PromptOpts) string {
	if v, ok := opts.Meta["promptId"].(string); ok && v != "" {
		return v
	}
	return opts.MessageID
}

// promptedIDCap bounds the retry-id memory. A turn's id only needs to
// survive the reconnect window it could be retried in, and the FE retries
// seconds to minutes later; 512 turns is far more than that per process.
const promptedIDCap = 512

// claimPromptIDLocked records pid and reports whether it was already seen.
// Caller holds b.mu.
func (b *Bridge) claimPromptIDLocked(pid string) (dup bool) {
	if b.promptedIDs == nil {
		b.promptedIDs = make(map[string]struct{}, 32)
	}
	if _, ok := b.promptedIDs[pid]; ok {
		return true
	}
	b.promptedIDs[pid] = struct{}{}
	b.promptedFIFO = append(b.promptedFIFO, pid)
	if len(b.promptedFIFO) > promptedIDCap {
		old := b.promptedFIFO[0]
		b.promptedFIFO = b.promptedFIFO[1:]
		delete(b.promptedIDs, old)
	}
	return false
}

// releasePromptIDLocked forgets pid so a caller that failed before the
// prompt reached the agent can retry. Caller holds b.mu.
func (b *Bridge) releasePromptIDLocked(pid string) {
	if _, ok := b.promptedIDs[pid]; !ok {
		return
	}
	delete(b.promptedIDs, pid)
	for i, id := range b.promptedFIFO {
		if id == pid {
			b.promptedFIFO = append(b.promptedFIFO[:i], b.promptedFIFO[i+1:]...)
			break
		}
	}
}

// PromptOpts carries the optional official session/prompt fields: the
// unstable `messageId` (UUID) and an extension `_meta` map. Empty fields
// are omitted from the wire (absent key ≠ off, matching the TUI).
type PromptOpts struct {
	MessageID string         // wire: messageId (UUID; 非空才发)
	Meta      map[string]any // wire: _meta (非空才发)
}

// StopReasonDuplicate is returned with no error when the promptId was
// already accepted, so a retried send is observable as a no-op instead of
// looking like a completed turn with an empty stop reason.
const StopReasonDuplicate = "duplicate"

// PromptWithOpts sends session/prompt to the given session (default:
// active) and blocks until the turn finishes. When no session exists yet
// (the host never auto-creates one at boot), the first prompt restores the
// last known session if one was remembered; only a machine with no
// last-session pointer starts a brand-new conversation. A failed restore is
// an error — never silently open a blank chat. Busy is per-session: other
// sessions can keep running turns in parallel (the agent process is
// multi-session), and a busy session is NOT rejected — the agent accepts
// mid-turn session/prompt and queues it in its own authoritative
// pending_inputs, so the host forwards and the turns run in submission
// order. The optional fields (messageId / _meta) are forwarded on the
// session/prompt params when set; empty opts omit them from the wire
// entirely. Returns the turn's stopReason plus the response `_meta` (the
// agent's prompt-result meta, nil when absent) so the HTTP layer can pass
// it through to the browser. A retried promptId returns
// StopReasonDuplicate without sending anything.
func (b *Bridge) PromptWithOpts(ctx context.Context, sessionID string, blocks []ContentBlock, opts PromptOpts) (stopReason string, meta map[string]any, err error) {
	// Retry idempotency: the FE mints one promptId per turn and reuses it
	// when it re-sends a prompt whose acceptance it never saw. Claiming the
	// id before the turn starts means a duplicate send is a no-op instead of
	// a second turn — the failure mode this guards ("message showed up but
	// never ran") is symmetric: after a fix that retries automatically, the
	// original may well have been accepted, and running it twice is worse
	// than the dropped message.
	claimedID := promptIDOf(opts)
	// issued flips once session/prompt actually goes to the agent. A call
	// that dies before that (agent down, unknown session) never consumed
	// the id: releasing it keeps the FE's retry meaningful.
	issued := false
	if claimedID != "" {
		b.mu.Lock()
		dup := b.claimPromptIDLocked(claimedID)
		b.mu.Unlock()
		if dup {
			log.Printf("[Capri-host] duplicate promptId %s ignored (retry of an accepted turn)", claimedID)
			return StopReasonDuplicate, nil, nil
		}
		defer func() {
			if issued {
				return
			}
			b.mu.Lock()
			b.releasePromptIDLocked(claimedID)
			b.mu.Unlock()
		}()
	}
	b.mu.Lock()
	if sessionID == "" {
		sessionID = b.activeSessionID
	}
	s := b.sessions[sessionID]
	if s == nil && sessionID == "" {
		// No active conversation yet. Prefer restoring the last one
		// (survives agent crash / host restart). Only create a fresh
		// session when nothing was ever remembered. An explicitly
		// targeted unknown sessionId below stays a 404.
		hasLast := b.lastSessionID != ""
		b.mu.Unlock()
		if hasLast {
			if err := b.restoreLastSession(ctx); err != nil {
				// 受理即返回后 prompt 的 HTTP 响应不再携带回合级错误：
				// 恢复失败必须经 live 通道送达。会话尚不存在 → 无
				// sessionId，前端按 host 级错误渲染（横幅 + conn error）。
				b.broadcastPromptError("", err)
				return "", nil, err
			}
		} else if err := b.NewSession(ctx, SessionConfig{}); err != nil {
			b.broadcastPromptError("", err)
			return "", nil, err
		}
		b.mu.Lock()
		sessionID = b.activeSessionID
		s = b.sessions[sessionID]
	}
	if s == nil {
		b.mu.Unlock()
		// 显式未知会话通常被 handlePrompt 的同步检查拦成 HTTP 404；
		// 走到这里说明是竞态（受理后会话被删）或 bridge 直连调用——
		// 补一条 live 事件，前端按带 sessionId 的回合级错误渲染。
		b.broadcastPromptError(sessionID, &HTTPError{Code: 404, Msg: "会话不存在"})
		return "", nil, &HTTPError{Code: 404, Msg: "会话不存在"}
	}
	// Idle-unload 只把 grok 侧 actor 卸掉，名册行还在（HasSession 仍
	// 为 true，避免 FE 点空闲会话发 prompt 吃 404）。prompt 前先
	// session/load 灌回去。
	if s.unloaded {
		cwd := s.Cwd
		b.mu.Unlock()
		if _, err := b.LoadSession(ctx, sessionID, cwd); err != nil {
			b.broadcastPromptError(sessionID, err)
			return "", nil, err
		}
		b.mu.Lock()
		s = b.sessions[sessionID]
		if s == nil {
			b.mu.Unlock()
			b.broadcastPromptError(sessionID, &HTTPError{Code: 404, Msg: "会话不存在"})
			return "", nil, &HTTPError{Code: 404, Msg: "会话不存在"}
		}
	}
	// A busy session no longer 409s: the agent accepts mid-turn
	// session/prompt and queues it in its own pending_inputs (popped when
	// the current turn ends), so we just forward. Busy stays the "any turn
	// in flight" projection via busyCount — an overlapping prompt must not
	// let the first resolver flip the session idle while the second is
	// still running.
	//
	// A prompt this host sends starts an agent turn, so the observed leg is
	// armed too: both legs then agree, and the release at promptTimeout
	// cannot strand a still-working session in "idle" (the update stream
	// keeps the turn open until turn_completed).
	first := s.busyCount == 0
	s.busyCount++
	s.LastActiveAt = time.Now().UnixMilli()
	b.openTurnLocked(sessionID, s.LastActiveAt)
	flipped := b.syncBusyLocked(s)
	// Keep last-session pointer fresh even if the user only talks to this
	// session without re-loading it (multi-session focus via prompt).
	b.rememberSessionLocked(sessionID, s.Cwd)
	b.mu.Unlock()
	if first {
		// 0→1 transition: announce the busy turn exactly once. A second
		// in-flight prompt must not re-announce — the session never left
		// busy between the two.
		b.Broadcast(Event{kType: "busy", kSessionID: sessionID})
	}
	if flipped {
		// The row now reads active/busy; patch it so clients that render
		// the list from sessions_changed skip the fetch. flipped is false
		// when first is true but the observed turn already held Busy: that
		// flip went out from noteTurnActivity.
		b.broadcastRosterRows(sessionID)
	}

	// turnEnded records whether the agent actually finished this turn. It
	// stays false on transport failures (timeout / write error / process
	// death), where the agent may still be working.
	turnEnded := false
	defer func() {
		b.releaseBusy(s, sessionID, turnEnded)
	}()

	if err := b.ensureBooted(ctx); err != nil {
		// The prompt never reached the agent — this turn does not exist, so
		// release both legs instead of leaving a phantom running session.
		turnEnded = true
		return "", nil, err
	}

	// convert blocks
	prompt := make([]any, 0, len(blocks))
	for _, bl := range blocks {
		prompt = append(prompt, map[string]any(bl))
	}

	params := map[string]any{
		kSessionID: sessionID,
		"prompt":   prompt,
	}
	if opts.MessageID != "" {
		params["messageId"] = opts.MessageID
	}
	if len(opts.Meta) > 0 {
		params[kMeta] = opts.Meta
	}

	sentAt := time.Now().UnixMilli()
	promptCtx, cancelPrompt := context.WithCancel(ctx)
	defer cancelPrompt()

	var stalled atomic.Bool
	stallTimeout := promptStallTimeout
	if stallTimeout > 0 {
		stallDone := make(chan struct{})
		defer close(stallDone)
		checkInterval := 1 * time.Second
		if stallTimeout < checkInterval {
			checkInterval = stallTimeout / 3
			if checkInterval < 5*time.Millisecond {
				checkInterval = 5 * time.Millisecond
			}
		}
		go func() {
			ticker := time.NewTicker(checkInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stallDone:
					return
				case <-promptCtx.Done():
					return
				case now := <-ticker.C:
					nowMs := now.UnixMilli()
					if nowMs-sentAt < stallTimeout.Milliseconds() {
						continue
					}
					b.mu.Lock()
					w := b.turns[sessionID]
					lastActivity := int64(0)
					if w != nil {
						lastActivity = w.seenAt
					}
					b.mu.Unlock()

					// 如果自发出 prompt 以来没有任何活动 update（seenAt 未推进）
					if lastActivity <= sentAt {
						log.Printf("[Capri-host] prompt stall detected (session=%s): 持续无活动超过 %v，判定底层可能死锁，提前熔断", sessionID, stallTimeout)
						stalled.Store(true)
						cancelPrompt()
						return
					}
					// 已经有活动 update 到达，解除看门狗
					return
				}
			}
		}()
	}

	res, err := b.request(promptCtx, "session/prompt", params, promptTimeout)
	issued = true
	if err != nil {
		if stalled.Load() {
			err = ErrPromptStalled
		}
		// 错误要不要上事件流、怎么留痕，由 reportPromptFailure 判定：回合
		// 还在正常输出时，prompt 预算耗尽不是回合失败。
		b.reportPromptFailure(sessionID, err)
		if stalled.Load() {
			b.Cancel(sessionID)
			turnEnded = true
			return "", nil, err
		}
		// The client (browser) went away mid-turn. The agent process may be
		// perfectly healthy — and other sessions may be running parallel
		// turns in the same process — so nothing is killed or cancelled
		// process-wide: cancel the orphaned turn so the session does not
		// stay busy for the full promptTimeout, and return; the client can
		// simply resend.
		if errors.Is(err, context.Canceled) {
			b.Cancel(sessionID)
			b.Broadcast(Event{kType: "status", kText: "连接已断开，本次回复已取消，请重新发送", kSessionID: sessionID})
			// The host just ordered this turn cancelled, so it stops
			// claiming the session is working. If the agent keeps streaming
			// anyway, its next update re-arms the observed turn — closing
			// here cannot hide a live session for longer than one update.
			turnEnded = true
			return "", nil, err
		}
		// A plain JSON-RPC error is the AGENT rejecting the turn (e.g. the
		// model API's 400 "Internal Error: …") — the process answered and is
		// healthy; the error event above already surfaced the failure.
		// Transport-level failures (timeout / write error) are logged and
		// surfaced by reportPromptFailure — the host never kills or restarts
		// the agent on its own (assume the agent is reliable; the process
		// lifecycle belongs to the user via the restart endpoint). The
		// failed turn is not retried.
		var rpcErr *RPCError
		if errors.As(err, &rpcErr) {
			// The agent answered: this turn is over, both legs release.
			turnEnded = true
		}
		// Otherwise the RPC died on the way, not on the turn: the agent may
		// still be working it, so the observed turn stays armed.
		return "", nil, err
	}
	turnEnded = true
	sr, _ := res["stopReason"].(string)
	if sr == "" {
		sr = "unknown"
	}
	// 响应 `_meta`（agent 下发的 prompt-result meta）原样透传：done 事件
	// 带 `meta`、返回值给 HTTP 层；仅非空才带（absent key ≠ off）。
	meta, _ = res[kMeta].(map[string]any)
	ev := Event{kType: "done", "stopReason": sr, kSessionID: sessionID}
	if len(meta) > 0 {
		ev[kMetaOut] = meta
	}
	b.Broadcast(ev)
	return sr, meta, nil
}

// turnStillStreaming reports whether the agent is provably working on a turn
// for this session right now: an observed turn whose last update landed
// within turnLivenessWindow. A merely-open-but-silent turn does not qualify —
// that is the shape a wedged agent leaves behind.
func (b *Bridge) turnStillStreaming(sessionID string) bool {
	now := time.Now().UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()
	w := b.turns[sessionID]
	return w != nil && w.open && now-w.seenAt <= turnLivenessWindow.Milliseconds()
}

// reportPromptFailure decides how a failed session/prompt reaches clients and
// what it leaves in the log. Returns whether the error rode the live channel.
//
// An agent JSON-RPC error means the turn itself failed (the process answered)
// and a canceled caller context means the host just cancelled the turn — both
// always surface, as before. A transport failure is different: the common one
// is a turn that outlived the 30-minute promptTimeout while the agent kept
// streaming it. Reporting that as a turn error made every client viewing the
// session seal the live stream, drop an error row with a "restart agent"
// button into the scrollback (restarting would kill the healthy turn it was
// warning about) and reset its turn timer — mid-turn. While the update stream
// proves work is ongoing, the failure is a host-side budget event: it stays in
// the log. Clients still see the truth through the session's busy state, which
// the observed turn keeps until turn_completed.
func (b *Bridge) reportPromptFailure(sessionID string, err error) bool {
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) || errors.Is(err, context.Canceled) || errors.Is(err, ErrPromptStalled) {
		b.broadcastPromptError(sessionID, err)
		return true
	}
	// 留痕底层错误：区分超时 / 写失败 / 进程退出，事后才能还原事故。
	log.Printf("[Capri-host] prompt transport failure (session=%s): %v", sessionID, err)
	if b.turnStillStreaming(sessionID) {
		log.Printf("[Capri-host] prompt error not surfaced (session=%s): agent turn still streaming, session stays active", sessionID)
		return false
	}
	b.broadcastPromptError(sessionID, err)
	return true
}

// broadcastPromptError surfaces a prompt-turn failure on the live channel.
// POST /api/prompt accepts immediately and no longer carries turn-level
// error bodies, so every turn failure the host reports must ride the
// SSE/WS event stream — see reportPromptFailure for which failures the host
// does NOT report (a prompt-budget timeout on a turn that is still streaming).
// source 标记（前端据此渲染）：agent 报错（RPCError — 进程活着、拒绝了
// 回合）vs 传输失败（超时/写失败 — agent 可能不可达）；老版本客户端忽略
// 该字段，仅按带 sessionId 的回合错误处理。sessionId 为空（如恢复/新建
// 会话失败，会话尚不存在）时省略该键，前端按 host 级错误渲染。
func (b *Bridge) broadcastPromptError(sessionID string, err error) {
	source := "transport"
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		source = "agent"
	}
	ev := Event{kType: kError, "message": err.Error(), "source": source}
	if sessionID != "" {
		ev[kSessionID] = sessionID
	}
	b.Broadcast(ev)
}

// Cancel sends session/cancel for the given session (default: active) and
// cancels its pending client requests.
func (b *Bridge) Cancel(sessionID string) {
	b.CancelWithMeta(sessionID, nil)
}

// CancelWithMeta is Cancel with an optional `_meta` forwarded on the
// session/cancel params. The agent reads cancelTrigger ("esc"|"ctrl_c"),
// cancelSubagents (default true), and rewindIfNoOutput / rewindIfPristine
// from that meta (mvp_agent/acp_agent.rs:2079-2108); an empty meta keeps
// the wire byte-identical to Cancel.
func (b *Bridge) CancelWithMeta(sessionID string, meta map[string]any) {
	b.clientReqMu.Lock()
	b.mu.Lock()
	if sessionID == "" {
		sessionID = b.activeSessionID
	}
	s := b.sessions[sessionID]
	sid := ""
	if s != nil {
		sid = s.SessionID
	}
	stdin := b.stdin
	b.mu.Unlock()
	if sid == "" {
		b.clientReqMu.Unlock()
		return
	}
	var responses []map[string]any
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		if cr.SessionID != sid {
			return true
		}
		select {
		case <-cr.done:
		default:
			cr.cancel = true
			close(cr.done)
		}
		if msg := b.resolveClientRequestLocked(key.(string), cr); msg != nil {
			responses = append(responses, msg)
		}
		return true
	})
	b.Broadcast(Event{kType: "cancelled", kSessionID: sid})
	b.clientReqMu.Unlock()
	params := map[string]any{kSessionID: sid}
	if len(meta) > 0 {
		params[kMeta] = meta
	}
	_ = writeAgentMessage(stdin, map[string]any{
		kJSONRPC: "2.0", kMethod: "session/cancel", kParams: params,
	})
	for _, msg := range responses {
		_ = writeAgentMessage(stdin, msg)
	}
}
