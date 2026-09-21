package acp

import (
	"context"
	"log"
	"sort"
	"time"
)

// DefaultResidentCap is how many sessions stay loaded in grok when
// GrokConfig.ResidentCap is left at zero. Capri-host is a long-lived
// stdio client, so grok never idle-unloads on disconnect; the host
// enforces the cap instead.
const DefaultResidentCap = 4

const idleUnloadSweepInterval = 30 * time.Second

type unloadCandidate struct {
	id         string
	cwd        string
	lastActive int64
	created    int64
}

func (b *Bridge) residentCap() int {
	if b.cfg.ResidentCap < 0 {
		return 0
	}
	if b.cfg.ResidentCap == 0 {
		return DefaultResidentCap
	}
	return b.cfg.ResidentCap
}

// StartIdleUnload runs the LRU supervisor until ctx is done. A non-positive
// cap (ResidentCap < 0, or RESIDENT_CAP=0) is a no-op. The first sweep
// waits one interval so boot / last-session restore can settle.
func (b *Bridge) StartIdleUnload(ctx context.Context) {
	if b.residentCap() <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(idleUnloadSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n := b.SweepIdleUnload(ctx); n > 0 {
					log.Printf("[Capri-host] idle-unload: 卸掉 %d 个空闲会话（grok resident 上限 %d）", n, b.residentCap())
				}
			}
		}
	}()
}

// SweepIdleUnload closes grok-resident actors for idle sessions over the
// cap. Roster rows stay so session/list still shows them as idle; a later
// load/prompt session/load's them back. Returns how many were unloaded.
func (b *Bridge) SweepIdleUnload(ctx context.Context) int {
	if ctx.Err() != nil {
		return 0
	}
	capN := b.residentCap()
	if capN <= 0 {
		return 0
	}
	b.mu.Lock()
	ready := (b.ready || b.bootOK) && b.stdin != nil
	b.mu.Unlock()
	if !ready {
		return 0
	}

	goalSID := b.pinnedGoalSessionID()
	b.mu.Lock()
	nResident, unpinned := b.idleUnloadPoolLocked(goalSID)
	b.mu.Unlock()
	if nResident <= capN {
		return 0
	}
	need := nResident - capN

	var ok []unloadCandidate
	for _, c := range unpinned {
		if b.sessionHasLiveBg(c.cwd, c.id) {
			continue
		}
		ok = append(ok, c)
	}
	sort.Slice(ok, func(i, j int) bool {
		if ok[i].lastActive != ok[j].lastActive {
			return ok[i].lastActive < ok[j].lastActive
		}
		if ok[i].created != ok[j].created {
			return ok[i].created < ok[j].created
		}
		return ok[i].id < ok[j].id
	})
	if len(ok) > need {
		ok = ok[:need]
	}

	n := 0
	for _, c := range ok {
		if ctx.Err() != nil {
			break
		}
		if err := b.unloadResident(ctx, c.id); err != nil {
			log.Printf("[Capri-host] idle-unload %s: %v", c.id, err)
			continue
		}
		n++
	}
	if n > 0 {
		b.broadcastRosterChange()
	}
	return n
}

func (b *Bridge) pinnedGoalSessionID() string {
	b.goal.mu.Lock()
	defer b.goal.mu.Unlock()
	if !b.goal.loopOn || b.goal.g == nil {
		return ""
	}
	switch b.goal.g.Status {
	case goalComplete, goalCleared, goalBudgetLimited:
		return ""
	}
	return b.goal.g.sessionID
}

// idleUnloadPoolLocked returns the current grok-resident count and the
// sessions that are eligible to close (not busy / awaiting / focused /
// last-session / goal / a queue that still holds work). Caller holds b.mu.
func (b *Bridge) idleUnloadPoolLocked(goalSID string) (resident int, unpinned []unloadCandidate) {
	for id, s := range b.sessions {
		if s == nil || s.unloaded {
			continue
		}
		resident++
		if b.sessionPinnedLocked(id, s, goalSID) {
			continue
		}
		last := s.LastActiveAt
		if last == 0 {
			last = s.CreatedAt
		}
		unpinned = append(unpinned, unloadCandidate{
			id:         id,
			cwd:        s.Cwd,
			lastActive: last,
			created:    s.CreatedAt,
		})
	}
	return resident, unpinned
}

func (b *Bridge) sessionPinnedLocked(id string, s *SessionState, goalSID string) bool {
	if s.Busy || s.AwaitingInput {
		return true
	}
	if w := b.turns[id]; w != nil && w.open {
		return true
	}
	if id == b.activeSessionID || id == b.lastSessionID {
		return true
	}
	if goalSID != "" && id == goalSID {
		return true
	}
	if queueHoldsWork(b.queueSnapshots[id]) {
		return true
	}
	return false
}

// queueHoldsWork reports whether a cached x.ai/queue/changed snapshot still
// has work that must stay grok-resident: pending entries, or a running
// prompt id. A post-turn empty snapshot ({entries:[], sessionId}) has
// keys, so len(q)>0 would pin every session that ever prompted — that is
// not work.
func queueHoldsWork(q map[string]any) bool {
	if len(q) == 0 {
		return false
	}
	for _, k := range []string{"runningPromptId", "running_prompt_id"} {
		if s, ok := q[k].(string); ok && s != "" {
			return true
		}
	}
	for _, k := range []string{"entries", "items"} {
		if arr, ok := q[k].([]any); ok && len(arr) > 0 {
			return true
		}
	}
	return false
}

func (b *Bridge) sessionHasLiveBg(cwd, sessionID string) bool {
	if cwd == "" || sessionID == "" {
		return false
	}
	tasks, err := b.SessionRunningTasks(sessionID, cwd)
	return err == nil && len(tasks) > 0
}

// unloadResident asks grok to session/close one idle session and demotes
// the host row (keep listed, HasSession stays true). It refuses if the
// session became pinned between pick and close.
func (b *Bridge) unloadResident(ctx context.Context, sessionID string) error {
	goalSID := b.pinnedGoalSessionID()
	b.mu.Lock()
	s := b.sessions[sessionID]
	if s == nil || s.unloaded || b.sessionPinnedLocked(sessionID, s, goalSID) {
		b.mu.Unlock()
		return nil
	}
	cwd := s.Cwd
	b.mu.Unlock()
	if b.sessionHasLiveBg(cwd, sessionID) {
		return nil
	}

	if _, err := b.request(ctx, "session/close", map[string]any{
		kSessionID: sessionID,
	}, 30*time.Second); err != nil {
		return err
	}

	b.mu.Lock()
	b.markUnloadedLocked(sessionID)
	b.mu.Unlock()
	return nil
}

// markUnloadedLocked demotes a roster row after grok session/close.
// Does not forget the session, last-session pointer, or active id.
func (b *Bridge) markUnloadedLocked(sessionID string) {
	s := b.sessions[sessionID]
	if s == nil {
		return
	}
	s.unloaded = true
	s.Busy = false
	s.busyCount = 0
	s.AwaitingInput = false
	b.closeTurnLocked(sessionID)
	if b.queueSnapshots != nil {
		delete(b.queueSnapshots, sessionID)
	}
	if b.usageLastUsed != nil {
		delete(b.usageLastUsed, sessionID)
	}
	if b.liveTools != nil {
		delete(b.liveTools, sessionID)
	}
	b.genRate.discard(sessionID)
}
