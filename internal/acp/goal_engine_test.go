package acp

import (
	"fmt"
	"testing"
	"time"
)

// TestGoalDrainConcurrentKeepsLongTurns — the 64-slot Subscribe buffer
// must not drop events on long turns: the continuous drainer collects
// every broadcast event (>64, the old post-turn drain would have lost the
// tail — update_goal / usage / summary events) in order. Events are
// streamed at a realistic turn pace (a real turn's chunks arrive over
// seconds), after the drainer goroutine has parked in its select.
func TestGoalDrainConcurrentKeepsLongTurns(t *testing.T) {
	b := NewBridge(GrokConfig{})
	evCh, unsub := b.Subscribe()
	drain := b.goalDrainConcurrent(evCh)

	time.Sleep(20 * time.Millisecond) // drainer parks in its select
	const n = 200                     // well beyond the 64-slot buffer
	for i := 0; i < n; i++ {
		b.Broadcast(Event{"type": "chunk", "text": fmt.Sprintf("%d", i)})
		time.Sleep(500 * time.Microsecond)
	}
	events := drain()
	unsub()

	if len(events) != n {
		t.Fatalf("drained %d events, want %d (buffer overflow must not drop goal events)", len(events), n)
	}
	if events[0]["text"] != "0" || events[n-1]["text"] != fmt.Sprintf("%d", n-1) {
		t.Fatalf("event order broken: first=%v last=%v", events[0]["text"], events[n-1]["text"])
	}
}


// TestGoalAnalyzeCompletesAfterEvidenceRound — a bare completion claim
// latches a verifying round; the evidence round must back the claim with
// concrete artifacts before the goal completes.
func TestGoalAnalyzeCompletesAfterEvidenceRound(t *testing.T) {
	b := NewBridge(GrokConfig{})
	b.goalMu.Lock()
	b.goal = &GoalState{
		Objective: "修复登录模块",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	g := b.goal
	b.goalMu.Unlock()

	// Round 1: bare claim without evidence → latch, keep looping.
	done := b.goalAnalyze(g, []Event{{"type": "chunk", "text": "目标已完成。"}})
	if done {
		t.Fatal("bare claim must not terminate the goal")
	}
	b.goalMu.Lock()
	if !b.goal.verifyingRound || !b.goal.Verifying {
		b.goalMu.Unlock()
		t.Fatal("expected verifying round to be latched after claim")
	}
	b.goalMu.Unlock()

	// Round 2: evidence round backs the claim → complete.
	done = b.goalAnalyze(g, []Event{{
		"type": "chunk",
		"text": "目标已完成。证据：修改了 internal/acp/goal.go 和 internal/server/http_goal.go，运行 go test ./... 全部通过，npm run build 成功。",
	}})
	if !done {
		t.Fatal("evidence-backed claim must terminate the goal")
	}
	st, _ := b.GoalStatus("s1")
	if st["status"] != goalComplete {
		t.Fatalf("status = %v, want complete", st["status"])
	}
}

// TestGoalAnalyzeBlocked — an explicit blocker pauses the goal with
// status blocked.
func TestGoalAnalyzeBlocked(t *testing.T) {
	b := NewBridge(GrokConfig{})
	b.goalMu.Lock()
	b.goal = &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	g := b.goal
	b.goalMu.Unlock()

	done := b.goalAnalyze(g, []Event{{"type": "chunk", "text": "我卡住了：第三方 SDK 不兼容，无法继续。"}})
	if !done {
		t.Fatal("blocked claim must terminate the goal")
	}
	st, _ := b.GoalStatus("s1")
	if st["status"] != goalBlocked {
		t.Fatalf("status = %v, want blocked", st["status"])
	}
}

// TestGoalAnalyzeBudget — usage above the token budget ends the goal
// with budget_limited.
func TestGoalAnalyzeBudget(t *testing.T) {
	b := NewBridge(GrokConfig{})
	b.goalMu.Lock()
	b.goal = &GoalState{
		Objective:   "x",
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: 100,
		StartedAt:   time.Now().UnixMilli(),
		sessionID:   "s1",
	}
	g := b.goal
	b.goalMu.Unlock()

	done := b.goalAnalyze(g, []Event{{"type": "usage", "used": float64(150)}})
	if !done {
		t.Fatal("budget overrun must terminate the goal")
	}
	st, _ := b.GoalStatus("s1")
	if st["status"] != goalBudgetLimited {
		t.Fatalf("status = %v, want budget_limited", st["status"])
	}
	if st["tokens_used"] != int64(150) {
		t.Fatalf("tokens_used = %v, want 150", st["tokens_used"])
	}
	// Under budget stays active.
	b2 := NewBridge(GrokConfig{})
	b2.goalMu.Lock()
	b2.goal = &GoalState{
		Objective:   "x",
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: 100,
		StartedAt:   time.Now().UnixMilli(),
		sessionID:   "s1",
	}
	g2 := b2.goal
	b2.goalMu.Unlock()
	if done := b2.goalAnalyze(g2, []Event{{"type": "usage", "used": float64(40)}}); done {
		t.Fatal("under-budget usage must not terminate")
	}
}

// TestGoalAnalyzeAgentNotification — an agent-side goal_updated carrying
// a terminal status (agent goal mode on) is mirrored and ends the loop.
func TestGoalAnalyzeAgentNotification(t *testing.T) {
	b := NewBridge(GrokConfig{})
	b.goalMu.Lock()
	b.goal = &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	g := b.goal
	b.goalMu.Unlock()

	done := b.goalAnalyze(g, []Event{{
		"type":   "goal_updated",
		"update": map[string]any{"status": "complete", "objective": "x"},
	}})
	if !done {
		t.Fatal("agent terminal notification must end the loop")
	}
	st, _ := b.GoalStatus("s1")
	if st["status"] != goalComplete {
		t.Fatalf("status = %v, want complete", st["status"])
	}
}

// TestGoalAnalyzeUpdateGoalTool — an update_goal tool call with
// completed:true latches the evidence round; blocked_reason pauses.
func TestGoalAnalyzeUpdateGoalTool(t *testing.T) {
	b := NewBridge(GrokConfig{})
	b.goalMu.Lock()
	b.goal = &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	g := b.goal
	b.goalMu.Unlock()

	done := b.goalAnalyze(g, []Event{{
		"type": "tool_call",
		"toolCall": map[string]any{
			"title":    "update_goal",
			"rawInput": map[string]any{"completed": true, "message": "全部搞定"},
		},
	}})
	if done {
		t.Fatal("completed:true must latch verification, not terminate")
	}
	b.goalMu.Lock()
	verifying := b.goal.Verifying
	b.goalMu.Unlock()
	if !verifying {
		t.Fatal("expected verifying flag after update_goal completed")
	}

	// Blocked reason terminates.
	b2 := NewBridge(GrokConfig{})
	b2.goalMu.Lock()
	b2.goal = &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	g2 := b2.goal
	b2.goalMu.Unlock()
	done = b2.goalAnalyze(g2, []Event{{
		"type": "tool_call",
		"toolCall": map[string]any{
			"title":    "update_goal",
			"rawInput": map[string]any{"blocked_reason": "API 挂了"},
		},
	}})
	if !done {
		t.Fatal("blocked_reason must terminate")
	}
	st, _ := b2.GoalStatus("s1")
	if st["status"] != goalBlocked {
		t.Fatalf("status = %v, want blocked", st["status"])
	}
}

// TestGoalAnalyzeStaleLoopDoesNotPolluteReplacement — the race behind
// /goal set: while the old goal's loop is blocked in a long
// PromptWithOpts, the user replaces the goal (GoalSet swaps b.goal and
// starts a new loop). When the old loop finally analyzes its stale
// turn's events, none of them may land on the replacement goal — the
// analysis must be dropped and the old loop must terminate.
func TestGoalAnalyzeStaleLoopDoesNotPolluteReplacement(t *testing.T) {
	// Scenario 1: the stale turn carries a usage reading and a
	// completion claim. Without the ownership guard these would mark B
	// budget_limited / complete and overwrite B's tokens.
	b1 := NewBridge(GrokConfig{})
	b1.goalMu.Lock()
	b1.goal = &GoalState{
		Objective: "目标A",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	goalA := b1.goal // what the old loop captured at entry
	// GoalSet replaces b.goal with a brand-new state (new loop owns B).
	b1.goal = &GoalState{
		Objective:   "目标B",
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: 100,
		StartedAt:   time.Now().UnixMilli(),
		sessionID:   "s1",
	}
	b1.goalMu.Unlock()

	done := b1.goalAnalyze(goalA, []Event{
		{"type": "usage", "used": float64(150)},
		{"type": "chunk", "text": "目标A已完成。证据：修改了 a.go，运行 go test 通过。"},
	})
	if !done {
		t.Fatal("stale loop analysis must terminate the old loop")
	}
	b1.goalMu.Lock()
	if b1.goal.Objective != "目标B" {
		b1.goalMu.Unlock()
		t.Fatalf("objective = %q, want 目标B", b1.goal.Objective)
	}
	if b1.goal.Status != goalActive {
		b1.goalMu.Unlock()
		t.Fatalf("status = %v, want active (B must not be marked terminal by A's events)", b1.goal.Status)
	}
	if b1.goal.TokensUsed != 0 {
		b1.goalMu.Unlock()
		t.Fatalf("tokens_used = %v, want 0 (B must not inherit A's usage)", b1.goal.TokensUsed)
	}
	if b1.goal.verifyingRound || b1.goal.Verifying || b1.goal.claimedCompletion {
		b1.goalMu.Unlock()
		t.Fatal("B must not inherit A's verification latch")
	}
	if b1.goal.Message != "" {
		b1.goalMu.Unlock()
		t.Fatalf("message = %q, want empty", b1.goal.Message)
	}
	b1.goalMu.Unlock()

	// Scenario 2: the stale turn carries an agent-side goal_updated with
	// a terminal status for A — it must not mark B complete either.
	b2 := NewBridge(GrokConfig{})
	b2.goalMu.Lock()
	b2.goal = &GoalState{
		Objective: "目标A",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	goalA2 := b2.goal
	b2.goal = &GoalState{
		Objective: "目标B",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	}
	b2.goalMu.Unlock()

	done = b2.goalAnalyze(goalA2, []Event{{
		"type":   "goal_updated",
		"update": map[string]any{"status": "complete", "objective": "目标A"},
	}})
	if !done {
		t.Fatal("stale loop analysis must terminate the old loop")
	}
	b2.goalMu.Lock()
	if b2.goal.Objective != "目标B" {
		b2.goalMu.Unlock()
		t.Fatalf("objective = %q, want 目标B", b2.goal.Objective)
	}
	if b2.goal.Status != goalActive {
		b2.goalMu.Unlock()
		t.Fatalf("status = %v, want active (B must not be marked complete by A's notification)", b2.goal.Status)
	}
	b2.goalMu.Unlock()
}
