package acp

import (
	"context"
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
