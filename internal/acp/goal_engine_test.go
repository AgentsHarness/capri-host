package acp

import (
	"testing"
	"time"
)

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
	b.goalMu.Unlock()

	// Round 1: bare claim without evidence → latch, keep looping.
	done := b.goalAnalyze([]Event{{"type": "chunk", "text": "目标已完成。"}})
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
	done = b.goalAnalyze([]Event{{
		"type": "chunk",
		"text": "目标已完成。证据：修改了 internal/acp/goal.go 和 internal/server/http_goal.go，运行 go test ./... 全部通过，npm run build 成功。",
	}})
	if !done {
		t.Fatal("evidence-backed claim must terminate the goal")
	}
	if st := b.GoalStatus(); st["status"] != goalComplete {
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
	b.goalMu.Unlock()

	done := b.goalAnalyze([]Event{{"type": "chunk", "text": "我卡住了：第三方 SDK 不兼容，无法继续。"}})
	if !done {
		t.Fatal("blocked claim must terminate the goal")
	}
	if st := b.GoalStatus(); st["status"] != goalBlocked {
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
	b.goalMu.Unlock()

	done := b.goalAnalyze([]Event{{"type": "usage", "used": float64(150)}})
	if !done {
		t.Fatal("budget overrun must terminate the goal")
	}
	st := b.GoalStatus()
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
	b2.goalMu.Unlock()
	if done := b2.goalAnalyze([]Event{{"type": "usage", "used": float64(40)}}); done {
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
	b.goalMu.Unlock()

	done := b.goalAnalyze([]Event{{
		"type":   "goal_updated",
		"update": map[string]any{"status": "complete", "objective": "x"},
	}})
	if !done {
		t.Fatal("agent terminal notification must end the loop")
	}
	if st := b.GoalStatus(); st["status"] != goalComplete {
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
	b.goalMu.Unlock()

	done := b.goalAnalyze([]Event{{
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
	b2.goalMu.Unlock()
	done = b2.goalAnalyze([]Event{{
		"type": "tool_call",
		"toolCall": map[string]any{
			"title":    "update_goal",
			"rawInput": map[string]any{"blocked_reason": "API 挂了"},
		},
	}})
	if !done {
		t.Fatal("blocked_reason must terminate")
	}
	if st := b2.GoalStatus(); st["status"] != goalBlocked {
		t.Fatalf("status = %v, want blocked", st["status"])
	}
}
