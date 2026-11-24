package acp

import (
	"strings"
)

// liveToolResolver 实时 SSE 注入。ID 与历史主路径同一公式；首次非重放
// 事件用当前历史视图的仍开放调用做种子，这样「先拉历史再接 live」对得上。
type liveToolResolver struct {
	seeded        bool
	openCalls     []openCall
	pendingStarts []openCall
	tsCount       map[int64]int
	naCount       int
}

func (r *liveToolResolver) seedFrom(view *normalizedHistory) {
	if r.seeded {
		return
	}
	r.seeded = true
	r.tsCount = make(map[int64]int)
	if view == nil || len(view.order) == 0 {
		return
	}
	w := walkSyntheticToolCalls(view.order, view.lines)
	r.openCalls = append([]openCall(nil), w.openCalls...)
	r.pendingStarts = append([]openCall(nil), w.openCalls...)
	r.tsCount = w.tsCount
}

func (r *liveToolResolver) takeSeededStart(tm *toolLineMeta, tsMs int64) (string, bool) {
	for i := range r.pendingStarts {
		c := &r.pendingStarts[i]
		if tsMs > 0 && c.tsMs != tsMs {
			continue
		}
		meta := c.meta
		if !sameStartIdentity(&meta, tm) {
			continue
		}
		id := c.id
		r.pendingStarts = append(r.pendingStarts[:i], r.pendingStarts[i+1:]...)
		return id, true
	}
	return "", false
}

func (r *liveToolResolver) handleLive(sessionUpdate string, update map[string]any, tsMs int64, eventID string) {
	if sessionUpdate == "turn_completed" {
		r.openCalls = nil
		r.pendingStarts = nil
		return
	}
	if update == nil {
		return
	}
	tm := parseToolLineMeta(update)
	if tm == nil {
		return
	}
	if r.tsCount == nil {
		r.tsCount = make(map[int64]int)
	}

	if sessionUpdate == "tool_call" {
		if strings.TrimSpace(tm.toolCallID) != "" {
			return
		}
		if id, ok := r.takeSeededStart(tm, tsMs); ok {
			update["toolCallId"] = id
			return
		}
		id := assignStartID(r.tsCount, tsMs, eventID, &r.naCount)
		update["toolCallId"] = id
		r.openCalls = append(r.openCalls, openCall{id: id, tsMs: tsMs, meta: *tm})
		return
	}

	if sessionUpdate != "tool_call_update" {
		return
	}
	if strings.TrimSpace(tm.toolCallID) != "" {
		if isTerminalStatus(tm.status) {
			for i := range r.openCalls {
				if r.openCalls[i].id == tm.toolCallID {
					r.openCalls = append(r.openCalls[:i], r.openCalls[i+1:]...)
					break
				}
			}
		}
		return
	}
	if len(r.openCalls) == 0 {
		return
	}
	matchIdx := matchUpdateToCall(tm, r.openCalls)
	if matchIdx < 0 {
		return
	}
	update["toolCallId"] = r.openCalls[matchIdx].id
	if isTerminalStatus(tm.status) {
		r.openCalls = append(r.openCalls[:matchIdx], r.openCalls[matchIdx+1:]...)
	}
}
