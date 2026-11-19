package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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
//   - per-session: one goal run per session (runs map), mirroring the TUI's
//     per-SessionActor GoalTracker — a new /goal on one session never
//     replaces another session's goal
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
	Objective             string `json:"objective"`
	Status                string `json:"status"` // active|user_paused|blocked|complete|cleared|budget_limited
	Phase                 string `json:"phase"`  // planning|executing
	Planning              bool   `json:"planning,omitempty"`
	Verifying             bool   `json:"verifying_completion,omitempty"`
	TokenBudget           int64  `json:"token_budget,omitempty"`
	TokensUsed            int64  `json:"tokens_used"`
	TokenBaseline         int64  `json:"token_baseline"`
	StartedAt             int64  `json:"started_at"` // unix ms
	ElapsedMs             int64  `json:"elapsed_ms"`
	TotalDeliverables     int    `json:"total_deliverables"`
	CompletedDeliverables int    `json:"completed_deliverables"`
	Message               string `json:"message,omitempty"`

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

// ─────────────────────────────────────────────────────────────────────
// goalEngine — 独立的 goal 组件。原先 15 个 (b *Bridge) 方法与
// goalMu/goal/goalStop/goalLoopOn 四个字段收敛于此；Bridge 保留
// Goal{Set,Status,Pause,Resume,Clear} 五个门面方法（server 调用面），
// 进程死亡清理路径经 Bridge.goal 触达。
//
// goalHost 是引擎对 Bridge 的消费者侧窄接口：引擎只订阅事件、查会话
// 忙闲、发回合、广播——除此之外不触碰 Bridge 任何状态。
// ─────────────────────────────────────────────────────────────────────
type goalHost interface {
	Subscribe() (ch chan Event, unsubscribe func())
	// resolveSessionID maps "" to the active session (goal control verbs).
	resolveSessionID(sessionID string) string
	// sessionBusy reports whether the session currently has turns in flight.
	sessionBusy(sessionID string) bool
	PromptWithOpts(ctx context.Context, sessionID string, blocks []ContentBlock, opts PromptOpts) (stopReason string, meta map[string]any, err error)
	Broadcast(ev Event)
}

// goalRun is one session's goal: its state plus the stop channel of the
// continuation loop driving it. The engine keeps one run per session, so
// different sessions can pursue goals at the same time and every control
// verb (status/pause/resume/clear) only touches its own session.
type goalRun struct {
	g      *GoalState
	stop   chan struct{} // closed to end the goal loop
	loopOn bool
}

type goalEngine struct {
	host goalHost

	mu   sync.Mutex
	runs map[string]*goalRun // sessionID → goal run
}

// runLocked returns the session's run, or nil. Callers hold ge.mu.
func (ge *goalEngine) runLocked(sessionID string) *goalRun {
	if ge.runs == nil {
		return nil
	}
	return ge.runs[sessionID]
}

// goalLoopTurnDelay is the grace period after a user turn ends before the
// loop sends its own continuation (lets the user's reply render first).
const goalLoopTurnDelay = 3 * time.Second

// ── control ──────────────────────────────────────────────────────────

// GoalSet starts (or restarts) an autonomous goal on the given session
// (empty sessionId resolves to the active one). Returns the new state.
// The goal binds to the resolved session — the loop prompts that session
// and broadcasts are tagged with it. Other sessions' goals are untouched.
// The first turn is fired by the loop goroutine, which injects the goal
// instruction.
func (ge *goalEngine) GoalSet(ctx context.Context, sessionID, objective string, tokenBudget int64) (map[string]any, error) {
	objective = strings.TrimSpace(objective)
	if objective == "" {
		return nil, &HTTPError{Code: 400, Msg: "缺少目标描述"}
	}
	if sessionID = ge.host.resolveSessionID(sessionID); sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	now := time.Now().UnixMilli()
	ge.mu.Lock()
	if ge.runs == nil {
		ge.runs = make(map[string]*goalRun)
	}
	// Stop this session's previous loop before replacing its state.
	ge.stopLoopLocked(sessionID)
	stop := make(chan struct{})
	ge.runs[sessionID] = &goalRun{
		g: &GoalState{
			Objective:   objective,
			Status:      goalActive,
			Phase:       "executing",
			TokenBudget: tokenBudget,
			StartedAt:   now,
			ElapsedMs:   0,
			sessionID:   sessionID,
		},
		stop:   stop,
		loopOn: true,
	}
	ge.mu.Unlock()

	ge.broadcastGoal(sessionID)
	go ge.goalLoop(sessionID, stop)
	return ge.goalSnapshot(sessionID), nil
}

// GoalStatus returns the current goal state of the given session (empty
// sessionId resolves to the active one). nil state, no error when that
// session has no goal — goals on other sessions are independent.
func (ge *goalEngine) GoalStatus(sessionID string) (map[string]any, error) {
	sessionID = ge.host.resolveSessionID(sessionID)
	ge.mu.Lock()
	defer ge.mu.Unlock()
	return ge.goalSnapshotLocked(sessionID), nil
}

// GoalPause pauses an active goal (user_paused) on the given session. The
// current in-flight continuation turn finishes; that session's loop stops
// scheduling further turns. Other sessions keep running.
func (ge *goalEngine) GoalPause(sessionID string) (map[string]any, error) {
	sessionID = ge.host.resolveSessionID(sessionID)
	ge.mu.Lock()
	run := ge.runLocked(sessionID)
	if run == nil || run.g == nil {
		ge.mu.Unlock()
		return nil, &HTTPError{Code: 409, Msg: "当前没有进行中的目标"}
	}
	if run.g.Status != goalActive {
		st := ge.goalSnapshotLocked(sessionID)
		ge.mu.Unlock()
		return st, &HTTPError{Code: 409, Msg: "当前没有进行中的目标"}
	}
	run.g.Status = goalUserPaused
	run.g.Message = ""
	ge.stopLoopLocked(sessionID)
	ge.mu.Unlock()
	ge.broadcastGoal(sessionID)
	return ge.goalSnapshot(sessionID), nil
}

// GoalResume resumes a paused goal on the given session (empty sessionId
// resolves to the active one). Other sessions' goals are independent.
func (ge *goalEngine) GoalResume(sessionID string) (map[string]any, error) {
	sessionID = ge.host.resolveSessionID(sessionID)
	ge.mu.Lock()
	run := ge.runLocked(sessionID)
	if run == nil || run.g == nil {
		ge.mu.Unlock()
		return nil, &HTTPError{Code: 404, Msg: "当前没有目标"}
	}
	if run.g.Status != goalUserPaused && run.g.Status != goalBlocked {
		st := ge.goalSnapshotLocked(sessionID)
		ge.mu.Unlock()
		return st, &HTTPError{Code: 409, Msg: "目标未处于暂停状态"}
	}
	run.g.Status = goalActive
	run.g.Verifying = false
	run.g.verifyingRound = false
	run.g.claimedCompletion = false
	run.g.Message = ""
	stop := make(chan struct{})
	run.stop = stop
	run.loopOn = true
	ge.mu.Unlock()
	ge.broadcastGoal(sessionID)
	go ge.goalLoop(sessionID, stop)
	return ge.goalSnapshot(sessionID), nil
}

// GoalClear clears the goal on the given session (cleared; empty sessionId
// resolves to the active one). Stops that session's loop.
func (ge *goalEngine) GoalClear(sessionID string) (map[string]any, error) {
	sessionID = ge.host.resolveSessionID(sessionID)
	ge.mu.Lock()
	run := ge.runLocked(sessionID)
	if run == nil || run.g == nil {
		ge.mu.Unlock()
		return nil, &HTTPError{Code: 404, Msg: "当前没有目标"}
	}
	run.g.Status = goalCleared
	run.g.Message = ""
	ge.stopLoopLocked(sessionID)
	ge.mu.Unlock()
	ge.broadcastGoal(sessionID)
	return ge.goalSnapshot(sessionID), nil
}

// ── loop ─────────────────────────────────────────────────────────────

// goalLoop drives continuation turns for one session while its goal is
// Active. Each session's goal owns one goroutine (stop channel closes to
// end it); goals on other sessions are unrelated and keep running.
//
// The loop captures the goal pointer it owns at entry. A later /goal set
// on the same session replaces that run while this loop may be blocked
// (e.g. in a long PromptWithOpts, up to promptTimeout); every state write
// below is guarded by currentLocked so a stale loop never pollutes the
// replacement goal and exits as soon as it notices the swap.
func (ge *goalEngine) goalLoop(sessionID string, stop chan struct{}) {
	ge.mu.Lock()
	run := ge.runLocked(sessionID)
	ge.mu.Unlock()
	if run == nil || run.g == nil {
		return
	}
	g := run.g
	for {
		// 1. State check: only Active goals continue.
		st := ge.goalStruct(sessionID)
		if st == nil || st.Status != goalActive {
			return
		}
		// 1b. Ownership: the goal was replaced by /goal set while this
		//     loop was in flight — exit without touching the new goal.
		ge.mu.Lock()
		owned := ge.currentLocked(sessionID, g) != nil
		ge.mu.Unlock()
		if !owned {
			return
		}

		// 2. First turn injects the full instruction; continuation and
		//    evidence turns inject their own. The evidence round is
		//    decided by the previous iteration's analysis.
		var instruction string
		ge.mu.Lock()
		cur := ge.currentLocked(sessionID, g)
		if cur == nil {
			ge.mu.Unlock()
			return
		}
		first := cur.TokensUsed == 0 && !cur.claimedCompletion && !cur.verifyingRound
		if cur.verifyingRound {
			instruction = goalVerifyInstruction(st.Objective)
		} else if first {
			instruction = goalInstruction(st.Objective)
		} else {
			instruction = goalContinueInstruction(st.Objective)
		}
		ge.mu.Unlock()

		// 3. Wait until the session is idle (user turns win; the loop
		//    yields while a user message is in flight).
		if !ge.goalWaitIdle(stop, sessionID) {
			return
		}

		// 4. Re-check state and ownership (pause/clear/set may have
		//    landed while waiting).
		ge.mu.Lock()
		cur = ge.currentLocked(sessionID, g)
		active := cur != nil && cur.Status == goalActive
		ge.mu.Unlock()
		if !active {
			return
		}

		// 5. Run the turn, collecting events for analysis. The drainer
		//    goroutine consumes the subscription during the whole turn, so
		//    long turns never overflow the 512-slot Subscribe buffer and
		//    lose update_goal / usage / summary events to Broadcast drops.
		evCh, unsub := ge.host.Subscribe()
		drain := ge.goalDrainConcurrent(evCh)
		blocks := []ContentBlock{{kType: "text", "text": instruction}}
		ctx, cancel := context.WithTimeout(context.Background(), promptTimeout)
		stopReason, _, err := ge.host.PromptWithOpts(ctx, sessionID, blocks, PromptOpts{})
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
			ge.mu.Lock()
			cur = ge.currentLocked(sessionID, g)
			if cur != nil && cur.Status == goalActive {
				cur.Message = "回合失败: " + err.Error()
			}
			ge.mu.Unlock()
			if cur != nil {
				ge.broadcastGoal(sessionID)
			}
			return
		}

		// The user cancelled the continuation turn (or the agent hit its
		// own stop gate) — stop the loop rather than immediately firing
		// another turn; the goal stays active and only /goal resume
		// restarts it.
		if stopReason == "cancelled" {
			ge.mu.Lock()
			cur = ge.currentLocked(sessionID, g)
			if cur != nil && cur.Status == goalActive {
				cur.Message = "回合已取消（goal 保持活动，可用 /goal resume 继续）"
			}
			ge.mu.Unlock()
			if cur != nil {
				ge.broadcastGoal(sessionID)
			}
			return
		}

		// 6. Analyze the turn (agent goal_updated / update_goal tool call
		//    / reply text / usage). goalAnalyze checks goal ownership
		//    before every state write and returns true when this loop is
		//    stale (goal replaced) — the loop then exits without touching
		//    the new goal.
		if done := ge.goalAnalyze(sessionID, g, collected); done {
			return
		}
	}
}

// goalWaitIdle polls the session busy flag until idle, or until the stop
// channel closes (returns false when stopped).
func (ge *goalEngine) goalWaitIdle(stop chan struct{}, sessionID string) bool {
	// Grace period so the user's previous turn renders before the loop
	// fires its continuation.
	select {
	case <-time.After(goalLoopTurnDelay):
	case <-stop:
		return false
	}
	for {
		if !ge.host.sessionBusy(sessionID) {
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
// would otherwise be lost to the old post-turn drain / smaller buffer).
// The returned stop function terminates the drainer (with a short grace
// sweep for events still in flight, mirroring the old post-turn drain)
// and returns the collected events in turn order. The drainer only
// collects events; it never touches goal state, so the goalLoopOwns
// identity guard in goalAnalyze is unaffected.
func (ge *goalEngine) goalDrainConcurrent(evCh chan Event) func() []Event {
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
// calling loop captured at entry: before every write to ge.g the
// function checks goalLoopOwns(g), so a stale loop whose goal was
// replaced by /goal set returns true immediately without touching the
// new goal. Returns true when the loop must exit (complete / blocked /
// budget limited / cleared / paused mid-loop / loop stale).
func (ge *goalEngine) goalAnalyze(sessionID string, g *GoalState, events []Event) bool {
	var text strings.Builder
	tokensUsed := int64(0)
	sawAgentGoalUpdate := false
	sawUpdateGoalCall := false
	var toolCompleted, toolBlocked bool
	var toolMessage string

	for _, ev := range events {
		switch ev[kType] {
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
			if u, ok := ev[kUpdate].(map[string]any); ok {
				sawAgentGoalUpdate = true
				// The agent session has goal mode on and reports its own
				// state — mirror it (agent is authoritative for status).
				ge.mu.Lock()
				cur := ge.currentLocked(sessionID, g)
				if cur == nil {
					ge.mu.Unlock()
					return true // loop stale: goal replaced
				}
				if s, _ := u["status"].(string); s != "" {
					cur.Status = s
				}
				if v, _ := u["verifying_completion"].(bool); v {
					cur.Verifying = true
				}
				if m, _ := u["message"].(string); m != "" {
					cur.Message = m
				}
				if n, ok := u["total_deliverables"].(float64); ok {
					cur.TotalDeliverables = int(n)
				}
				if n, ok := u["completed_deliverables"].(float64); ok {
					cur.CompletedDeliverables = int(n)
				}
				ge.mu.Unlock()
			}
		}
	}

	// Budget enforcement (best effort — only when a budget was set).
	ge.mu.Lock()
	cur := ge.currentLocked(sessionID, g)
	if cur == nil {
		ge.mu.Unlock()
		return true // loop stale: goal replaced
	}
	if cur.TokenBudget > 0 && tokensUsed > 0 {
		cur.TokensUsed = tokensUsed
		if tokensUsed > cur.TokenBudget {
			cur.Status = goalBudgetLimited
			cur.Message = fmt.Sprintf("token 预算 %d 已耗尽", cur.TokenBudget)
			ge.stopLoopLocked(sessionID)
			ge.mu.Unlock()
			ge.broadcastGoal(sessionID)
			return true
		}
	}
	ge.mu.Unlock()

	// Agent-driven state (goal mode on the agent side): mirror terminal
	// states and stop the loop.
	if sawAgentGoalUpdate {
		ge.mu.Lock()
		cur = ge.currentLocked(sessionID, g)
		if cur == nil {
			ge.mu.Unlock()
			return true // loop stale: goal replaced
		}
		terminal := cur.Status == goalComplete || cur.Status == goalCleared ||
			cur.Status == goalBudgetLimited || cur.Status == goalUserPaused ||
			cur.Status == goalBlocked
		ge.mu.Unlock()
		if terminal {
			ge.broadcastGoal(sessionID)
			return true
		}
	}

	// Tool-driven state (update_goal call with completed/blocked_reason).
	if sawUpdateGoalCall {
		ge.mu.Lock()
		cur = ge.currentLocked(sessionID, g)
		if cur == nil {
			ge.mu.Unlock()
			return true // loop stale: goal replaced
		}
		if toolCompleted {
			cur.claimedCompletion = true
			cur.Verifying = true
			cur.verifyingRound = true
		} else if toolBlocked {
			cur.Status = goalBlocked
			cur.Message = toolMessage
		} else if toolMessage != "" {
			cur.Message = toolMessage
		}
		ge.mu.Unlock()
		ge.broadcastGoal(sessionID)
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
	ge.mu.Lock()
	cur = ge.currentLocked(sessionID, g)
	if cur == nil {
		ge.mu.Unlock()
		return true // loop stale: goal replaced
	}
	if cur.verifyingRound {
		// Evidence round resolution.
		cur.verifyingRound = false
		cur.Verifying = false
		switch {
		case claim && evidence:
			cur.Status = goalComplete
			cur.Message = ""
			cur.CompletedDeliverables = cur.TotalDeliverables
			if cur.TotalDeliverables == 0 {
				cur.TotalDeliverables = 1
				cur.CompletedDeliverables = 1
			}
			ge.mu.Unlock()
			ge.broadcastGoal(sessionID)
			return true
		case claim && !evidence:
			cur.Status = goalBlocked
			cur.Message = "完成声明缺少可验证证据（无变更文件/命令/输出）"
			ge.mu.Unlock()
			ge.broadcastGoal(sessionID)
			return true
		case blocked:
			cur.Status = goalBlocked
			cur.Message = "模型报告受阻"
			ge.mu.Unlock()
			ge.broadcastGoal(sessionID)
			return true
		default:
			// Model kept working instead of defending the claim.
			cur.claimedCompletion = false
			ge.mu.Unlock()
			return false
		}
	}
	// Normal round: latch a completion claim for the evidence round.
	if claim && !cur.claimedCompletion {
		cur.claimedCompletion = true
		cur.Verifying = true
		cur.verifyingRound = true
		cur.Message = "完成声明待验证…"
		ge.mu.Unlock()
		ge.broadcastGoal(sessionID)
		return false
	}
	if blocked {
		cur.Status = goalBlocked
		cur.Message = "模型报告受阻"
		ge.mu.Unlock()
		ge.broadcastGoal(sessionID)
		return true
	}
	ge.mu.Unlock()
	return false
}

// ── helpers ──────────────────────────────────────────────────────────

// stopLoopLocked closes the session's stop channel so its in-flight
// goalLoop exits at the next checkpoint. Other sessions' loops are
// untouched. Callers must hold ge.mu.
func (ge *goalEngine) stopLoopLocked(sessionID string) {
	run := ge.runLocked(sessionID)
	if run == nil {
		return
	}
	if run.loopOn && run.stop != nil {
		close(run.stop)
	}
	run.loopOn = false
	run.stop = nil
}

// stopAllLoopsLocked closes every session's stop channel (shutdown).
// Callers must hold ge.mu.
func (ge *goalEngine) stopAllLoopsLocked() {
	for _, run := range ge.runs {
		if run == nil {
			continue
		}
		if run.loopOn && run.stop != nil {
			close(run.stop)
		}
		run.loopOn = false
		run.stop = nil
	}
}

// currentLocked returns the live goal a loop still owns: the session's
// run must still point at the same g. A /goal set that replaced the run
// invalidates every older loop, so a stale loop must stop writing state
// and exit. Callers must hold ge.mu.
func (ge *goalEngine) currentLocked(sessionID string, g *GoalState) *GoalState {
	run := ge.runLocked(sessionID)
	if run == nil || run.g != g {
		return nil
	}
	return run.g
}

// goalStruct returns a copy of the session's goal state as a struct (nil
// when that session has no goal). Loop logic that needs struct fields
// uses this instead of the wire-shaped snapshot map.
func (ge *goalEngine) goalStruct(sessionID string) *GoalState {
	ge.mu.Lock()
	defer ge.mu.Unlock()
	run := ge.runLocked(sessionID)
	if run == nil || run.g == nil {
		return nil
	}
	g := *run.g
	return &g
}

// goalSnapshot returns a serializable copy of the session's current state
// (nil when it has no goal). Elapsed is recomputed at snapshot time.
func (ge *goalEngine) goalSnapshot(sessionID string) map[string]any {
	ge.mu.Lock()
	defer ge.mu.Unlock()
	return ge.goalSnapshotLocked(sessionID)
}

func (ge *goalEngine) goalSnapshotLocked(sessionID string) map[string]any {
	run := ge.runLocked(sessionID)
	if run == nil || run.g == nil {
		return nil
	}
	g := *run.g
	g.ElapsedMs = time.Now().UnixMilli() - g.StartedAt
	if g.ElapsedMs < 0 {
		g.ElapsedMs = 0
	}
	m := map[string]any{
		"objective":              g.Objective,
		"status":                 g.Status,
		"phase":                  g.Phase,
		"tokens_used":            g.TokensUsed,
		"token_baseline":         g.TokenBaseline,
		"started_at":             g.StartedAt,
		"elapsed_ms":             g.ElapsedMs,
		"total_deliverables":     g.TotalDeliverables,
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

// broadcastGoal pushes a goal_updated notification carrying the session's
// current state — same event shape the agent's own goal_updated uses
// ({type, update, sessionId}).
func (ge *goalEngine) broadcastGoal(sessionID string) {
	ge.mu.Lock()
	st := ge.goalSnapshotLocked(sessionID)
	ge.mu.Unlock()
	if st == nil {
		return
	}
	ev := Event{kType: "goal_updated", kUpdate: st}
	if sessionID != "" {
		ev[kSessionID] = sessionID
	}
	ge.host.Broadcast(ev)
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

// ── Bridge 门面（server 的调用面；实现全部在 goalEngine）─────────────

// GoalSet starts (or restarts) an autonomous goal on the given session.
func (b *Bridge) GoalSet(ctx context.Context, sessionID, objective string, tokenBudget int64) (map[string]any, error) {
	return b.goal.GoalSet(ctx, sessionID, objective, tokenBudget)
}

// GoalStatus returns the current goal state of the given session.
func (b *Bridge) GoalStatus(sessionID string) (map[string]any, error) {
	return b.goal.GoalStatus(sessionID)
}

// GoalPause pauses an active goal on the given session.
func (b *Bridge) GoalPause(sessionID string) (map[string]any, error) {
	return b.goal.GoalPause(sessionID)
}

// GoalResume resumes a paused goal on the given session.
func (b *Bridge) GoalResume(sessionID string) (map[string]any, error) {
	return b.goal.GoalResume(sessionID)
}

// GoalClear clears the goal on the given session.
func (b *Bridge) GoalClear(sessionID string) (map[string]any, error) {
	return b.goal.GoalClear(sessionID)
}
