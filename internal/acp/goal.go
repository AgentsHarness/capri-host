package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────
// Goal engine — host-side port of the TUI's /goal (goal_tracker).
//
// The ACP wire defines the `goal_updated` session notification but NO
// goal control methods, and the TUI's goal engine lives in the client
// process. This host therefore owns the engine:
//
//   - control: POST /api/goal/{set,status,pause,resume,clear} (server layer)
//   - progress: the agent's own `goal_updated` notifications (when the
//     agent session has goal mode on) OR the host's continuation loop
//     with reply-text / update_goal tool-call parsing (always works)
//   - loop: while Active and the session is idle, the host automatically
//     sends continuation turns until complete / blocked / paused /
//     budget exhausted / cleared — mirroring the TUI's goal loop
//   - budget: token budget enforced from `usage` events (best effort)
//   - verification: a claimed completion triggers a dedicated evidence
//     round; the goal only completes when the evidence round backs it
//
// State values follow the TUI wire so the frontend's goal chip renders
// them directly: active / user_paused / blocked / complete / cleared /
// budget_limited.
// ─────────────────────────────────────────────────────────────────────

// GoalInstruction is the host-side port of the TUI's goal_instruction()
// (xai-grok-tools-api slash_commands.rs). Injected as a text block at the
// front of the first goal turn (the ACP wire has no system blocks).
func goalInstruction(objective string) string {
	return fmt.Sprintf(`# /goal -- pursue an objective

A goal has been set: %s

Work directly on this goal and carry it as far as you can. Deliver
everything the user asked for yourself: no follow-up questions, no
manual steps left for the user. If the conversation continues, keep
pursuing the goal until it is complete.

TRACKING: break the objective into concrete steps and track them
(use your todo tool if one is available), marking each done as you
finish it.

VERIFY AS YOU GO: test each change on the real path before moving on.
A completion claim must be backed by evidence produced in this
session, not assumptions.

If the goal is fully achieved, say so explicitly with a summary of
concrete evidence (files changed, commands run, results). If you are
truly stuck after 3+ consecutive failed attempts at the same problem,
say so with the reason. If you have an update_goal tool available, use
it to report progress, completion (completed: true) or blockers
(blocked_reason); if it errors or is unavailable, report status in your
reply instead.

Start now.`, objective)
}

// goalContinueInstruction is injected at the front of every continuation
// turn after the first.
func goalContinueInstruction(objective string) string {
	return fmt.Sprintf(`Continue working toward the goal: %s

If the goal is now fully achieved, respond with a completion summary and
concrete evidence (files changed, commands run, their outputs). If you are
blocked, explain the blocker. Otherwise keep working.`, objective)
}

// goalVerifyInstruction is injected for the evidence round after the
// model claims completion.
func goalVerifyInstruction(objective string) string {
	return fmt.Sprintf(`Independent verification: the goal "%s" was claimed complete.

Provide concrete evidence for that claim: list the files changed, the
commands run, and their results. If the claim cannot be backed by
evidence from this session, or work remains, say so explicitly and
continue working.`, objective)
}

// GoalState is the wire shape of the host-side goal. Field names mirror
// the TUI's goal_updated payload so the frontend renders it unchanged.
type GoalState struct {
	Objective    string `json:"objective"`
	Status       string `json:"status"` // active|user_paused|blocked|complete|cleared|budget_limited
	Phase        string `json:"phase"`  // planning|executing
	Planning     bool   `json:"planning,omitempty"`
	Verifying    bool   `json:"verifying_completion,omitempty"`
	TokenBudget  int64  `json:"token_budget,omitempty"`
	TokensUsed   int64  `json:"tokens_used"`
	TokenBaseline int64 `json:"token_baseline"`
	StartedAt    int64  `json:"started_at"` // unix ms
	ElapsedMs    int64  `json:"elapsed_ms"`
	TotalDeliverables   int `json:"total_deliverables"`
	CompletedDeliverables int `json:"completed_deliverables"`
	Message      string `json:"message,omitempty"`

	// sessionID is the session the goal runs in (not serialized; used to
	// tag broadcasts and pick the prompt target).
	sessionID string
	// verifyingRound marks the current loop iteration as the evidence
	// round (not serialized directly; Verifying mirrors it on the wire).
	verifyingRound bool
	// claimedCompletion latches a completion claim until the evidence
	// round resolves it (not serialized).
	claimedCompletion bool
}

// Goal status values (wire).
const (
	goalActive        = "active"
	goalUserPaused    = "user_paused"
	goalBlocked       = "blocked"
	goalComplete      = "complete"
	goalCleared       = "cleared"
	goalBudgetLimited = "budget_limited"
)

// goalLoopIdlePoll is how often the goal loop re-checks whether the
// session is idle before sending the next continuation turn.
const goalLoopIdlePoll = 2 * time.Second

// goalLoopTurnDelay is the grace period after a user turn ends before the
// loop sends its own continuation (lets the user's reply render first).
const goalLoopTurnDelay = 3 * time.Second

// ── control ──────────────────────────────────────────────────────────

// GoalSet starts (or restarts) an autonomous goal on the active session.
// Returns the new state. The first turn is fired by the loop goroutine,
// which injects the goal instruction.
func (b *Bridge) GoalSet(ctx context.Context, objective string, tokenBudget int64) (map[string]any, error) {
	objective = strings.TrimSpace(objective)
	if objective == "" {
		return nil, &HTTPError{Code: 400, Msg: "缺少目标描述"}
	}
	b.mu.Lock()
	sid := b.activeSessionID
	b.mu.Unlock()
	if sid == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	now := time.Now().UnixMilli()
	b.goalMu.Lock()
	// Stop any previous loop before replacing the state.
	b.stopGoalLoopLocked()
	b.goal = &GoalState{
		Objective:   objective,
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: tokenBudget,
		StartedAt:   now,
		ElapsedMs:   0,
		sessionID:   sid,
	}
	b.goalLoopOn = true
	b.goalStop = make(chan struct{})
	stop := b.goalStop
	b.goalMu.Unlock()

	b.broadcastGoal()
	go b.goalLoop(stop)
	return b.goalSnapshot(), nil
}

// GoalStatus returns the current goal state (nil when no goal is set).
func (b *Bridge) GoalStatus() map[string]any {
	return b.goalSnapshot()
}

// GoalPause pauses an active goal (user_paused). The current in-flight
// continuation turn finishes; the loop stops scheduling further turns.
func (b *Bridge) GoalPause() (map[string]any, error) {
	b.goalMu.Lock()
	if b.goal == nil || b.goal.Status != goalActive {
		b.goalMu.Unlock()
		return b.goalSnapshot(), &HTTPError{Code: 409, Msg: "当前没有进行中的目标"}
	}
	b.goal.Status = goalUserPaused
	b.goal.Message = ""
	b.stopGoalLoopLocked()
	b.goalMu.Unlock()
	b.broadcastGoal()
	return b.goalSnapshot(), nil
}

// GoalResume resumes a paused goal.
func (b *Bridge) GoalResume() (map[string]any, error) {
	b.goalMu.Lock()
	if b.goal == nil {
		b.goalMu.Unlock()
		return nil, &HTTPError{Code: 404, Msg: "当前没有目标"}
	}
	if b.goal.Status != goalUserPaused && b.goal.Status != goalBlocked {
		b.goalMu.Unlock()
		return b.goalSnapshot(), &HTTPError{Code: 409, Msg: "目标未处于暂停状态"}
	}
	b.goal.Status = goalActive
	b.goal.Verifying = false
	b.goal.verifyingRound = false
	b.goal.claimedCompletion = false
	b.goal.Message = ""
	b.goalLoopOn = true
	b.goalStop = make(chan struct{})
	stop := b.goalStop
	b.goalMu.Unlock()
	b.broadcastGoal()
	go b.goalLoop(stop)
	return b.goalSnapshot(), nil
}

// GoalClear clears the goal (cleared). Stops the loop.
func (b *Bridge) GoalClear() (map[string]any, error) {
	b.goalMu.Lock()
	if b.goal == nil {
		b.goalMu.Unlock()
		return nil, &HTTPError{Code: 404, Msg: "当前没有目标"}
	}
	b.goal.Status = goalCleared
	b.goal.Message = ""
	b.stopGoalLoopLocked()
	b.goalMu.Unlock()
	b.broadcastGoal()
	return b.goalSnapshot(), nil
}

// ── loop ─────────────────────────────────────────────────────────────

// goalLoop drives continuation turns while the goal is Active. It owns a
// single goroutine per goal (stop channel closes to end it).
//
// The loop captures the goal pointer it owns at entry. /goal set may
// replace b.goal while this loop is blocked (e.g. in a long
// PromptWithOpts, up to promptTimeout); every state write below is
// guarded by goalLoopOwns so a stale loop never pollutes the
// replacement goal and exits as soon as it notices the swap.
func (b *Bridge) goalLoop(stop chan struct{}) {
	b.goalMu.Lock()
	g := b.goal
	b.goalMu.Unlock()
	if g == nil {
		return
	}
	for {
		// 1. State check: only Active goals continue.
		st := b.goalStruct()
		if st == nil || st.Status != goalActive {
			return
		}
		// 1b. Ownership: the goal was replaced by /goal set while this
		//     loop was in flight — exit without touching the new goal.
		if !b.goalLoopOwns(g) {
			return
		}

		// 2. First turn injects the full instruction; continuation and
		//    evidence turns inject their own. The evidence round is
		//    decided by the previous iteration's analysis.
		var instruction string
		b.goalMu.Lock()
		if !b.goalLoopOwns(g) {
			b.goalMu.Unlock()
			return
		}
		first := b.goal.TokensUsed == 0 && !b.goal.claimedCompletion && !b.goal.verifyingRound
		if b.goal.verifyingRound {
			instruction = goalVerifyInstruction(st.Objective)
		} else if first {
			instruction = goalInstruction(st.Objective)
		} else {
			instruction = goalContinueInstruction(st.Objective)
		}
		sid := st.sessionID
		b.goalMu.Unlock()

		// 3. Wait until the session is idle (user turns win; the loop
		//    yields while a user message is in flight).
		if !b.goalWaitIdle(stop, sid) {
			return
		}

		// 4. Re-check state and ownership (pause/clear/set may have
		//    landed while waiting).
		b.goalMu.Lock()
		owned := b.goalLoopOwns(g)
		active := owned && b.goal.Status == goalActive
		b.goalMu.Unlock()
		if !active {
			return
		}

		// 5. Run the turn, collecting events for analysis. The drainer
		//    goroutine consumes the subscription during the whole turn, so
		//    long turns never overflow the 64-slot Subscribe buffer and
		//    lose update_goal / usage / summary events to Broadcast drops.
		evCh, unsub := b.Subscribe()
		drain := b.goalDrainConcurrent(evCh)
		blocks := []ContentBlock{{"type": "text", "text": instruction}}
		ctx, cancel := context.WithTimeout(context.Background(), promptTimeout)
		stopReason, _, err := b.PromptWithOpts(ctx, sid, blocks, PromptOpts{})
		cancel()
		// Stop the drainer and collect the turn's events (in order).
		collected := drain()
		unsub()

		if err != nil {
			// Transport failure (agent rebooted mid-goal) — stop the loop;
			// the goal stays active and only /goal resume restarts it (a
			// user message does not restart the loop). A stale loop
			// (goal replaced while the prompt was in flight) must not
			// write its failure into the new goal.
			b.goalMu.Lock()
			owned := b.goalLoopOwns(g)
			if owned && b.goal.Status == goalActive {
				b.goal.Message = "回合失败: " + err.Error()
			}
			b.goalMu.Unlock()
			if owned {
				b.broadcastGoal()
			}
			return
		}

		// The user cancelled the continuation turn (or the agent hit its
		// own stop gate) — stop the loop rather than immediately firing
		// another turn; the goal stays active and only /goal resume
		// restarts it.
		if stopReason == "cancelled" {
			b.goalMu.Lock()
			owned := b.goalLoopOwns(g)
			if owned && b.goal.Status == goalActive {
				b.goal.Message = "回合已取消（goal 保持活动，可用 /goal resume 继续）"
			}
			b.goalMu.Unlock()
			if owned {
				b.broadcastGoal()
			}
			return
		}

		// 6. Analyze the turn (agent goal_updated / update_goal tool call
		//    / reply text / usage). goalAnalyze checks goal ownership
		//    before every state write and returns true when this loop is
		//    stale (goal replaced) — the loop then exits without touching
		//    the new goal.
		if done := b.goalAnalyze(g, collected); done {
			return
		}
	}
}

// goalWaitIdle polls the session busy flag until idle, or until the stop
// channel closes (returns false when stopped).
func (b *Bridge) goalWaitIdle(stop chan struct{}, sessionID string) bool {
	// Grace period so the user's previous turn renders before the loop
	// fires its continuation.
	select {
	case <-time.After(goalLoopTurnDelay):
	case <-stop:
		return false
	}
	for {
		b.mu.Lock()
		var busy bool
		if s := b.sessions[sessionID]; s != nil {
			busy = s.Busy
		}
		b.mu.Unlock()
		if !busy {
			return true
		}
		select {
		case <-time.After(goalLoopIdlePoll):
		case <-stop:
			return false
		}
	}
}

// goalDrainConcurrent starts a background drainer that consumes a
// subscription channel continuously, so long turns (dozens to hundreds of
// chunk/thought/usage events) never overflow the Subscribe buffer —
// Broadcast drops events for slow consumers, and the goal analysis must
// see the FULL turn (update_goal / usage / summary events near the end
// would otherwise be lost to the old 64-slot buffer). The returned stop
// function terminates the drainer (with a short grace sweep for events
// still in flight, mirroring the old post-turn drain) and returns the
// collected events in turn order. The drainer only collects events; it
// never touches goal state, so the goalLoopOwns identity guard in
// goalAnalyze is unaffected.
func (b *Bridge) goalDrainConcurrent(evCh chan Event) func() []Event {
	var events []Event
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case ev := <-evCh:
				events = append(events, ev)
			case <-stop:
				// Final sweep: catch events still in flight, then exit.
				deadline := time.After(300 * time.Millisecond)
				for {
					select {
					case ev := <-evCh:
						events = append(events, ev)
					case <-deadline:
						return
					}
				}
			}
		}
	}()
	return func() []Event {
		close(stop)
		<-drained
		return events
	}
}

// goalAnalyze applies the collected turn events to the tracker and
// decides whether the loop should stop. g is the goal pointer the
// calling loop captured at entry: before every write to b.goal the
// function checks goalLoopOwns(g), so a stale loop whose goal was
// replaced by /goal set returns true immediately without touching the
// new goal. Returns true when the loop must exit (complete / blocked /
// budget limited / cleared / paused mid-loop / loop stale).
func (b *Bridge) goalAnalyze(g *GoalState, events []Event) bool {
	var text strings.Builder
	tokensUsed := int64(0)
	sawAgentGoalUpdate := false
	sawUpdateGoalCall := false
	var toolCompleted, toolBlocked bool
	var toolMessage string

	for _, ev := range events {
		switch ev["type"] {
		case "chunk":
			if t, ok := ev["text"].(string); ok {
				text.WriteString(t)
			}
		case "usage":
			if u := toInt64(ev["used"]); u > 0 {
				tokensUsed = u
			}
		case "tool_call":
			if tc, ok := ev["toolCall"].(map[string]any); ok {
				if isUpdateGoalToolCall(tc) {
					sawUpdateGoalCall = true
					ri := toolRawInput(tc)
					if m, ok := ri.(map[string]any); ok {
						if b, _ := m["completed"].(bool); b {
							toolCompleted = true
						}
						if r, _ := m["blocked_reason"].(string); r != "" {
							toolBlocked = true
							toolMessage = r
						}
						if s, _ := m["message"].(string); s != "" && toolMessage == "" {
							toolMessage = s
						}
					}
				}
			}
		case "goal_updated":
			if u, ok := ev["update"].(map[string]any); ok {
				sawAgentGoalUpdate = true
				// The agent session has goal mode on and reports its own
				// state — mirror it (agent is authoritative for status).
				b.goalMu.Lock()
				if !b.goalLoopOwns(g) {
					b.goalMu.Unlock()
					return true // loop stale: goal replaced
				}
				if s, _ := u["status"].(string); s != "" {
					b.goal.Status = s
				}
				if v, _ := u["verifying_completion"].(bool); v {
					b.goal.Verifying = true
				}
				if m, _ := u["message"].(string); m != "" {
					b.goal.Message = m
				}
				if n, ok := u["total_deliverables"].(float64); ok {
					b.goal.TotalDeliverables = int(n)
				}
				if n, ok := u["completed_deliverables"].(float64); ok {
					b.goal.CompletedDeliverables = int(n)
				}
				b.goalMu.Unlock()
			}
		}
	}

	// Budget enforcement (best effort — only when a budget was set).
	b.goalMu.Lock()
	if !b.goalLoopOwns(g) {
		b.goalMu.Unlock()
		return true // loop stale: goal replaced
	}
	if b.goal != nil && b.goal.TokenBudget > 0 && tokensUsed > 0 {
		b.goal.TokensUsed = tokensUsed
		if tokensUsed > b.goal.TokenBudget {
			b.goal.Status = goalBudgetLimited
			b.goal.Message = fmt.Sprintf("token 预算 %d 已耗尽", b.goal.TokenBudget)
			b.stopGoalLoopLocked()
			done := true
			b.goalMu.Unlock()
			b.broadcastGoal()
			return done
		}
	}
	b.goalMu.Unlock()

	// Agent-driven state (goal mode on the agent side): mirror terminal
	// states and stop the loop.
	if sawAgentGoalUpdate {
		b.goalMu.Lock()
		if !b.goalLoopOwns(g) {
			b.goalMu.Unlock()
			return true // loop stale: goal replaced
		}
		terminal := b.goal != nil && (b.goal.Status == goalComplete || b.goal.Status == goalCleared ||
			b.goal.Status == goalBudgetLimited || b.goal.Status == goalUserPaused ||
			b.goal.Status == goalBlocked)
		b.goalMu.Unlock()
		if terminal {
			b.broadcastGoal()
			return true
		}
	}

	// Tool-driven state (update_goal call with completed/blocked_reason).
	if sawUpdateGoalCall {
		b.goalMu.Lock()
		if !b.goalLoopOwns(g) {
			b.goalMu.Unlock()
			return true // loop stale: goal replaced
		}
		if b.goal != nil {
			if toolCompleted {
				b.goal.claimedCompletion = true
				b.goal.Verifying = true
				b.goal.verifyingRound = true
			} else if toolBlocked {
				b.goal.Status = goalBlocked
				b.goal.Message = toolMessage
			} else if toolMessage != "" {
				b.goal.Message = toolMessage
			}
		}
		b.goalMu.Unlock()
		b.broadcastGoal()
		if toolBlocked {
			return true
		}
		if toolCompleted {
			// Next loop iteration is the evidence round.
			return false
		}
	}

	// Text-driven analysis.
	claim, blocked, evidence := analyzeGoalText(text.String())
	b.goalMu.Lock()
	if !b.goalLoopOwns(g) {
		b.goalMu.Unlock()
		return true // loop stale: goal replaced
	}
	if b.goal == nil {
		b.goalMu.Unlock()
		return true
	}
	if b.goal.verifyingRound {
		// Evidence round resolution.
		b.goal.verifyingRound = false
		b.goal.Verifying = false
		switch {
		case claim && evidence:
			b.goal.Status = goalComplete
			b.goal.Message = ""
			b.goal.CompletedDeliverables = b.goal.TotalDeliverables
			if b.goal.TotalDeliverables == 0 {
				b.goal.TotalDeliverables = 1
				b.goal.CompletedDeliverables = 1
			}
			b.goalMu.Unlock()
			b.broadcastGoal()
			return true
		case claim && !evidence:
			b.goal.Status = goalBlocked
			b.goal.Message = "完成声明缺少可验证证据（无变更文件/命令/输出）"
			b.goalMu.Unlock()
			b.broadcastGoal()
			return true
		case blocked:
			b.goal.Status = goalBlocked
			b.goal.Message = "模型报告受阻"
			b.goalMu.Unlock()
			b.broadcastGoal()
			return true
		default:
			// Model kept working instead of defending the claim.
			b.goal.claimedCompletion = false
			b.goalMu.Unlock()
			return false
		}
	}
	// Normal round: latch a completion claim for the evidence round.
	if claim && !b.goal.claimedCompletion {
		b.goal.claimedCompletion = true
		b.goal.Verifying = true
		b.goal.verifyingRound = true
		b.goal.Message = "完成声明待验证…"
		b.goalMu.Unlock()
		b.broadcastGoal()
		return false
	}
	if blocked {
		b.goal.Status = goalBlocked
		b.goal.Message = "模型报告受阻"
		b.goalMu.Unlock()
		b.broadcastGoal()
		return true
	}
	b.goalMu.Unlock()
	return false
}

// ── helpers ──────────────────────────────────────────────────────────

// stopGoalLoopLocked closes the stop channel so an in-flight goalLoop
// exits at its next checkpoint. Callers must hold b.goalMu.
func (b *Bridge) stopGoalLoopLocked() {
	if !b.goalLoopOn {
		return
	}
	if b.goalStop != nil {
		close(b.goalStop)
	}
	b.goalLoopOn = false
	b.goalStop = nil
}

// goalLoopOwns reports whether g — the goal pointer a loop captured at
// entry — is still the current goal. A /goal set that replaced b.goal
// invalidates every older loop: a stale loop must stop writing state so
// its old turn's events never pollute the new goal. Callers must hold
// b.goalMu.
func (b *Bridge) goalLoopOwns(g *GoalState) bool {
	return g != nil && b.goal == g
}

// goalStruct returns a copy of the goal state as a struct (nil when no
// goal is set). Loop logic that needs struct fields uses this instead of
// the wire-shaped snapshot map.
func (b *Bridge) goalStruct() *GoalState {
	b.goalMu.Lock()
	defer b.goalMu.Unlock()
	if b.goal == nil {
		return nil
	}
	g := *b.goal
	return &g
}

// goalSnapshot returns a serializable copy of the current state (nil when
// no goal is set). Elapsed is recomputed at snapshot time.
func (b *Bridge) goalSnapshot() map[string]any {
	b.goalMu.Lock()
	defer b.goalMu.Unlock()
	return b.goalSnapshotLocked()
}

func (b *Bridge) goalSnapshotLocked() map[string]any {
	if b.goal == nil {
		return nil
	}
	g := *b.goal
	g.ElapsedMs = time.Now().UnixMilli() - g.StartedAt
	if g.ElapsedMs < 0 {
		g.ElapsedMs = 0
	}
	m := map[string]any{
		"objective":             g.Objective,
		"status":                g.Status,
		"phase":                 g.Phase,
		"tokens_used":           g.TokensUsed,
		"token_baseline":        g.TokenBaseline,
		"started_at":            g.StartedAt,
		"elapsed_ms":            g.ElapsedMs,
		"total_deliverables":    g.TotalDeliverables,
		"completed_deliverables": g.CompletedDeliverables,
	}
	if g.Planning {
		m["planning"] = true
	}
	if g.Verifying {
		m["verifying_completion"] = true
	}
	if g.TokenBudget > 0 {
		m["token_budget"] = g.TokenBudget
	}
	if g.Message != "" {
		m["message"] = g.Message
	}
	return m
}

// broadcastGoal pushes a goal_updated notification carrying the current
// state — same event shape the agent's own goal_updated uses
// ({type, update, sessionId}).
func (b *Bridge) broadcastGoal() {
	b.goalMu.Lock()
	st := b.goalSnapshotLocked()
	var sid string
	if b.goal != nil {
		sid = b.goal.sessionID
	}
	b.goalMu.Unlock()
	if st == nil {
		return
	}
	ev := Event{"type": "goal_updated", "update": st}
	if sid != "" {
		ev["sessionId"] = sid
	}
	b.Broadcast(ev)
}

// isUpdateGoalToolCall detects the update_goal tool call on the wire
// (title "update_goal", kind "UpdateGoal", or a "Goal:" prefixed title).
func isUpdateGoalToolCall(tc map[string]any) bool {
	title, _ := tc["title"].(string)
	kind, _ := tc["kind"].(string)
	if title == "update_goal" || strings.HasPrefix(title, "Goal:") {
		return true
	}
	return kind == "UpdateGoal" || kind == "WorkflowSignal"
}

// toolRawInput returns the rawInput of a tool call, handling both a JSON
// string and an already-parsed object.
func toolRawInput(tc map[string]any) any {
	ri, ok := tc["rawInput"]
	if !ok {
		return nil
	}
	if s, ok := ri.(string); ok {
		var m map[string]any
		if json.Unmarshal([]byte(s), &m) == nil {
			return m
		}
		return s
	}
	return ri
}

// analyzeGoalText heuristically classifies a turn's reply text into a
// completion claim, a blocked claim, and presence of concrete evidence.
func analyzeGoalText(text string) (claimComplete, claimBlocked, hasEvidence bool) {
	lower := strings.ToLower(text)
	// Blocked claims win over completion claims ("I'm stuck, can't
	// complete").
	for _, kw := range []string{"受阻", "卡住", "无法继续", "无法完成", "不能继续", "blocked", "stuck", "cannot continue"} {
		if strings.Contains(lower, kw) {
			claimBlocked = true
			break
		}
	}
	// Completion claims: explicit phrasing, excluding "not complete" /
	// "未完成".
	for _, kw := range []string{"目标已完成", "目标完成", "已完成", "全部完成", "都完成了", "任务完成", "goal completed", "goal achieved", "all done", "fully complete", "completed the goal"} {
		if strings.Contains(lower, kw) {
			claimComplete = true
			break
		}
	}
	if !claimComplete {
		for _, kw := range []string{"完成", "done", "completed", "finished"} {
			if strings.Contains(lower, kw) &&
				!strings.Contains(lower, "未完成") &&
				!strings.Contains(lower, "not complete") &&
				!strings.Contains(lower, "not done") &&
				!strings.Contains(lower, "not finished") &&
				!strings.Contains(lower, "尚未完成") {
				claimComplete = true
				break
			}
		}
	}
	// Evidence: concrete artifacts — file paths, commands, test output.
	if len(strings.TrimSpace(text)) >= 200 {
		hasEvidence = true
	}
	for _, kw := range []string{"/", "npm ", "pnpm ", "yarn ", "cargo ", "go test", "go build", "npm run", "tsc ", "python", "pytest", "jest", "vitest", "测试通过", "构建成功", "运行成功", "passed", "success", "✅"} {
		if strings.Contains(lower, kw) {
			hasEvidence = true
			break
		}
	}
	return claimComplete, claimBlocked, hasEvidence
}
