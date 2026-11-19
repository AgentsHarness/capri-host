package acp

import (
	"fmt"
	"testing"
	"time"
)

// seedGoal installs a session's goal run directly; these unit tests drive
// goalAnalyze without starting GoalSet's continuation loop.
func seedGoal(b *Bridge, sid string, g *GoalState) *GoalState {
	b.goal.mu.Lock()
	defer b.goal.mu.Unlock()
	if b.goal.runs == nil {
		b.goal.runs = make(map[string]*goalRun)
	}
	b.goal.runs[sid] = &goalRun{g: g}
	return g
}

// seededGoal returns the session's current goal state for assertions.
func seededGoal(b *Bridge, sid string) *GoalState {
	b.goal.mu.Lock()
	defer b.goal.mu.Unlock()
	if run := b.goal.runs[sid]; run != nil {
		return run.g
	}
	return nil
}

// TestGoalDrainConcurrentKeepsLongTurns — the 64-slot Subscribe buffer
// must not drop events on long turns: the continuous drainer collects
// every broadcast event (>64, the old post-turn drain would have lost the
// tail — update_goal / usage / summary events) in order. Events are
// streamed at a realistic turn pace (a real turn's chunks arrive over
// seconds), after the drainer goroutine has parked in its select.
func TestGoalDrainConcurrentKeepsLongTurns(t *testing.T) {
	b := NewBridge(GrokConfig{})
	evCh, unsub := b.Subscribe()
	drain := b.goal.goalDrainConcurrent(evCh)

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
	g := seedGoal(b, "s1", &GoalState{
		Objective: "修复登录模块",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})

	// Round 1: bare claim without evidence → latch, keep looping.
	done := b.goal.goalAnalyze("s1", g, []Event{{"type": "chunk", "text": "目标已完成。"}})
	if done {
		t.Fatal("bare claim must not terminate the goal")
	}
	if !g.verifyingRound || !g.Verifying {
		t.Fatal("expected verifying round to be latched after claim")
	}

	// Round 2: evidence round backs the claim → complete.
	done = b.goal.goalAnalyze("s1", g, []Event{{
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
	g := seedGoal(b, "s1", &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})

	done := b.goal.goalAnalyze("s1", g, []Event{{"type": "chunk", "text": "我卡住了：第三方 SDK 不兼容，无法继续。"}})
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
	g := seedGoal(b, "s1", &GoalState{
		Objective:   "x",
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: 100,
		StartedAt:   time.Now().UnixMilli(),
		sessionID:   "s1",
	})

	done := b.goal.goalAnalyze("s1", g, []Event{{"type": "usage", "used": float64(150)}})
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
	g2 := seedGoal(b2, "s1", &GoalState{
		Objective:   "x",
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: 100,
		StartedAt:   time.Now().UnixMilli(),
		sessionID:   "s1",
	})
	if done := b2.goal.goalAnalyze("s1", g2, []Event{{"type": "usage", "used": float64(40)}}); done {
		t.Fatal("under-budget usage must not terminate")
	}
}

// TestGoalAnalyzeAgentNotification — an agent-side goal_updated carrying
// a terminal status (agent goal mode on) is mirrored and ends the loop.
func TestGoalAnalyzeAgentNotification(t *testing.T) {
	b := NewBridge(GrokConfig{})
	g := seedGoal(b, "s1", &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})

	done := b.goal.goalAnalyze("s1", g, []Event{{
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
	g := seedGoal(b, "s1", &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})

	done := b.goal.goalAnalyze("s1", g, []Event{{
		"type": "tool_call",
		"toolCall": map[string]any{
			"title":    "update_goal",
			"rawInput": map[string]any{"completed": true, "message": "全部搞定"},
		},
	}})
	if done {
		t.Fatal("completed:true must latch verification, not terminate")
	}
	if !g.Verifying {
		t.Fatal("expected verifying flag after update_goal completed")
	}

	// Blocked reason terminates.
	b2 := NewBridge(GrokConfig{})
	g2 := seedGoal(b2, "s1", &GoalState{
		Objective: "x",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})
	done = b2.goal.goalAnalyze("s1", g2, []Event{{
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
// PromptWithOpts, the user replaces the same session's goal. When the old
// loop finally analyzes its stale turn's events, none of them may land on
// the replacement goal — the analysis must be dropped and the old loop
// must terminate.
func TestGoalAnalyzeStaleLoopDoesNotPolluteReplacement(t *testing.T) {
	// Scenario 1: the stale turn carries a usage reading and a
	// completion claim. Without the ownership guard these would mark B
	// budget_limited / complete and overwrite B's tokens.
	b1 := NewBridge(GrokConfig{})
	goalA := seedGoal(b1, "s1", &GoalState{
		Objective: "目标A",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})
	// A replacement /goal set on the same session swaps the run.
	goalB := seedGoal(b1, "s1", &GoalState{
		Objective:   "目标B",
		Status:      goalActive,
		Phase:       "executing",
		TokenBudget: 100,
		StartedAt:   time.Now().UnixMilli(),
		sessionID:   "s1",
	})

	done := b1.goal.goalAnalyze("s1", goalA, []Event{
		{"type": "usage", "used": float64(150)},
		{"type": "chunk", "text": "目标A已完成。证据：修改了 a.go，运行 go test 通过。"},
	})
	if !done {
		t.Fatal("stale loop analysis must terminate the old loop")
	}
	if goalB.Objective != "目标B" {
		t.Fatalf("objective = %q, want 目标B", goalB.Objective)
	}
	if goalB.Status != goalActive {
		t.Fatalf("status = %v, want active (B must not be marked terminal by A's events)", goalB.Status)
	}
	if goalB.TokensUsed != 0 {
		t.Fatalf("tokens_used = %v, want 0 (B must not inherit A's usage)", goalB.TokensUsed)
	}
	if goalB.verifyingRound || goalB.Verifying || goalB.claimedCompletion {
		t.Fatal("B must not inherit A's verification latch")
	}
	if goalB.Message != "" {
		t.Fatalf("message = %q, want empty", goalB.Message)
	}

	// Scenario 2: the stale turn carries an agent-side goal_updated with
	// a terminal status for A — it must not mark B complete either.
	b2 := NewBridge(GrokConfig{})
	goalA2 := seedGoal(b2, "s1", &GoalState{
		Objective: "目标A",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})
	goalB2 := seedGoal(b2, "s1", &GoalState{
		Objective: "目标B",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})

	done = b2.goal.goalAnalyze("s1", goalA2, []Event{{
		"type":   "goal_updated",
		"update": map[string]any{"status": "complete", "objective": "目标A"},
	}})
	if !done {
		t.Fatal("stale loop analysis must terminate the old loop")
	}
	if goalB2.Objective != "目标B" {
		t.Fatalf("objective = %q, want 目标B", goalB2.Objective)
	}
	if goalB2.Status != goalActive {
		t.Fatalf("status = %v, want active (B must not be marked complete by A's notification)", goalB2.Status)
	}
}

// TestGoalSessionsAreIndependent — goals are per session: a new goal on
// s2 does not replace s1's, and pause/clear on one leaves the other alone.
func TestGoalSessionsAreIndependent(t *testing.T) {
	b := NewBridge(GrokConfig{})
	seedGoal(b, "s1", &GoalState{
		Objective: "目标1",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s1",
	})
	seedGoal(b, "s2", &GoalState{
		Objective: "目标2",
		Status:    goalActive,
		Phase:     "executing",
		StartedAt: time.Now().UnixMilli(),
		sessionID: "s2",
	})

	if st, _ := b.GoalStatus("s1"); st["objective"] != "目标1" {
		t.Fatalf("s1 objective = %v, want 目标1", st["objective"])
	}
	if st, _ := b.GoalStatus("s2"); st["objective"] != "目标2" {
		t.Fatalf("s2 objective = %v, want 目标2", st["objective"])
	}

	if _, err := b.GoalPause("s1"); err != nil {
		t.Fatalf("pause s1: %v", err)
	}
	if st, _ := b.GoalStatus("s1"); st["status"] != goalUserPaused {
		t.Fatalf("s1 status = %v, want user_paused", st["status"])
	}
	if st, _ := b.GoalStatus("s2"); st["status"] != goalActive {
		t.Fatalf("s2 status = %v, want active after pausing s1", st["status"])
	}

	if _, err := b.GoalClear("s1"); err != nil {
		t.Fatalf("clear s1: %v", err)
	}
	if st, _ := b.GoalStatus("s1"); st["status"] != goalCleared {
		t.Fatalf("s1 status = %v, want cleared", st["status"])
	}
	if st, _ := b.GoalStatus("s2"); st["status"] != goalActive {
		t.Fatalf("s2 status = %v, want active after clearing s1", st["status"])
	}

	// No goal on a third session: status is null, not another session's
	// goal and not an error.
	if st, err := b.GoalStatus("s3"); err != nil || st != nil {
		t.Fatalf("s3 status = %v, %v; want nil, nil", st, err)
	}
}
