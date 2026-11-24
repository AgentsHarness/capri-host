package acp

import (
	"fmt"
	"log"
	"os/exec"
	"sort"
	"time"
)

// broadcastRosterChange pushes sessions_changed so clients refresh the
// dashboard list whenever the roster or a session's live state changed.
//
// No payload: the roster membership or a row's non-live fields changed
// (create / delete / load / rename / rewind), which clients can only learn
// by re-reading the list.
func (b *Bridge) broadcastRosterChange() {
	b.Broadcast(Event{kType: "sessions_changed", kParams: map[string]any{}})
}

// rosterRow builds the `sessions[]` entry for one session — the same shape
// /api/sessions produces for live rows, minus the [bg] census fields (those
// come from the on-disk scan and are unaffected by a busy flip).
//
// Callers must NOT hold b.mu.
func (b *Bridge) rosterRow(sid string) map[string]any {
	b.mu.Lock()
	s := b.sessions[sid]
	if s == nil {
		b.mu.Unlock()
		return nil
	}
	row := map[string]any{
		kSessionID: sid,
		"status": map[string]any{
			"state":         s.State(),
			"busy":          s.Busy,
			"awaitingInput": s.AwaitingInput,
			"lastActiveAt":  s.LastActiveAt,
		},
	}
	if s.Cwd != "" {
		row["cwd"] = s.Cwd
	}
	if s.Title != "" {
		row[kTitle] = s.Title
	}
	if s.UpdatedAt != "" {
		row[kUpdatedAt] = s.UpdatedAt
	}
	b.mu.Unlock()
	return row
}

// broadcastRosterRows pushes sessions_changed carrying the affected rows, so
// clients patch their list instead of re-fetching it. Used for live-state
// transitions only (busy / awaiting flips): those are the high-frequency
// ones — a turn boundary fires two of them — and the list read behind them
// costs a full census scan.
//
// Rows omit the [bg] census fields by design; a client merging in place must
// keep the values it already has (absent ≠ zero).
func (b *Bridge) broadcastRosterRows(sids ...string) {
	upserted := make([]any, 0, len(sids))
	for _, sid := range sids {
		if row := b.rosterRow(sid); row != nil {
			upserted = append(upserted, row)
		}
	}
	if len(upserted) == 0 {
		return
	}
	b.Broadcast(Event{kType: "sessions_changed", kParams: map[string]any{"upserted": upserted}})
}

// sessionIdExplicit returns the sessionId explicitly carried by
// notification params — camelCase "sessionId" (the standard carrier key)
// or snake_case "session_id" (some x.ai rails) — and "" when absent. NO
// active-session fallback: the official session/update carrier always
// carries the id, so handleSessionUpdate uses this directly — falling back
// to the host's active session there would mis-tag events in multi-client
// setups.
func sessionIdExplicit(params map[string]any) string {
	if sid, ok := params[kSessionID].(string); ok && sid != "" {
		return sid
	}
	if sid, ok := params[kSessionIDS].(string); ok && sid != "" {
		return sid
	}
	return ""
}

// sessionIdFrom returns the sessionId carried by an agent notification
// envelope: the explicit params id wins (sessionIdExplicit); otherwise the
// active session is the defensive fallback. Only rails that genuinely
// never carry an explicit id (machine-wide broadcasts such as
// x.ai/announcements/update / x.ai/models/update / x.ai/leader_reconnected)
// hit the fallback — for those the active-session tag is the host's best
// guess. Session-scoped carriers always carry the id and never fall back.
func (b *Bridge) sessionIdFrom(params map[string]any) string {
	if sid := sessionIdExplicit(params); sid != "" {
		return sid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if act := b.activeSessionLocked(); act != nil {
		return act.SessionID
	}
	return ""
}

// cloneAny deep-copies JSON-like structures (maps/slices) so snapshots
// handed out for lock-free serialization never share storage with live
// bridge state (a concurrent in-place map write during json.Marshal is a
// fatal race — "concurrent map read and map write").
func cloneAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneAny(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneAny(val)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(t))
		for k, val := range t {
			out[k] = val
		}
		return out
	default:
		return v
	}
}

func (b *Bridge) Snapshot() Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Read-time settle: hello / /api-status roster rows and the enriched
	// session/list must agree about which observed turns already went stale.
	// No sessions_changed here — the push rides on the ListSessions settle,
	// and every client that reads this snapshot gets the fresh value anyway.
	b.settleTurnsLocked(time.Now().UnixMilli())
	var pending []PendingReq
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		pending = append(pending, PendingReq{
			RequestID:  key.(string),
			Method:     cr.Method,
			Params:     cr.Params,
			SessionID:  cr.SessionID,
			ReceivedAt: cr.receivedAt,
		})
		return true
	})
	roster := make([]SessionState, 0, len(b.sessions))
	busy := false
	for _, s := range b.sessions {
		row := *s
		// Deep-copy the shared map fields: the roster rows are serialized
		// outside b.mu (SSE hello / /api/status / /api/session-state) while
		// readStdout may still mutate them under the lock (model changes,
		// usage updates). Sharing the maps would race the serializer.
		row.modes = cloneAny(s.modes)
		row.configOpts = cloneAny(s.configOpts)
		row.models = cloneAny(s.models)
		row.sessionMeta = cloneAny(s.sessionMeta)
		roster = append(roster, row)
		if s.Busy {
			busy = true
		}
	}
	// Most recently active first — dashboard ordering.
	sort.Slice(roster, func(i, j int) bool {
		if roster[i].LastActiveAt != roster[j].LastActiveAt {
			return roster[i].LastActiveAt > roster[j].LastActiveAt
		}
		return roster[i].CreatedAt > roster[j].CreatedAt
	})
	act := b.activeSessionLocked()
	var sid, cwd string
	var modes, configOpts, models, sessionMeta any
	if act != nil {
		sid = act.SessionID
		cwd = act.Cwd
		modes = cloneAny(act.modes)
		configOpts = cloneAny(act.configOpts)
		models = cloneAny(act.models)
		sessionMeta = cloneAny(act.sessionMeta)
	}
	// AuthMeta/SessionMeta 是 any 字段：有类型的 nil map 装进 any 后
	// omitempty 会失效（输出 null），所以 nil 时不装。
	var authMeta any
	if b.authMeta != nil {
		authMeta = b.authMeta
	}
	return Status{
		// ready 对外的含义是「agent 可用」，与 idle_unload 的判据一致。
		// b.ready 只在会话建立/加载后置位，boot 完成但一台会话都没有时
		// 恒为 false——只报它会让空机在客户端一直停在「连接中/启动中」。
		Ready:             b.ready || b.bootOK,
		Busy:              busy,
		Booting:           b.booting,
		SessionID:         sid,
		Cwd:               cwd,
		HostID:            b.hostID,
		HostName:          b.hostName,
		HomeDir:           b.homeDir,
		AgentInfo:         b.agentInfo,
		AgentCapabilities: b.initAgentCapabilities,
		AuthMeta:          authMeta,
		Modes:             modes,
		ConfigOptions:     configOpts,
		SessionMeta:       sessionMeta,
		Models:            models,
		BootError:         b.bootError,
		PendingRequests:   pending,
		Capabilities:      DefaultClientCaps(),
		Roster:            roster,
		AgentStartedAt:    b.agentStartedAt,
		// b.mu is already held (Snapshot locks at entry) — read the field
		// directly, never via PermissionMode() (it would self-deadlock).
		PermissionMode: b.permMode,
	}
}

// broadcastScheduledTaskCreated normalizes the scheduled_task_created wire
// shapes — the SessionUpdate::ScheduledTaskCreated carrier（snake_case
// task_id/prompt/human_schedule/next_fire_at，extensions/notification.rs）
// or the standalone x.ai notification（camelCase task 对象 / 顶层
// taskId）— into ONE typed event. The normalized `task` keeps the FE
// contract (taskId/prompt/interval/nextFireAt); `rawTask`/`rawParams`/
// `meta` preserve every remaining original field (humanSchedule, status,
// enabled, createdAt, lastFiredAt, timezone, _meta.eventId /
// x.ai/schedulerGeneration / x.ai/schedulerRevision, …) so nothing is
// dropped. Shared by the x.ai method channel (x.ai/scheduled_task_created)
// and the sessionUpdate kind dispatch.
func (b *Bridge) broadcastScheduledTaskCreated(sid string, params map[string]any, tag func(Event) Event) {
	task, _ := params["task"].(map[string]any)
	update, _ := params[kUpdate].(map[string]any)
	ev := Event{
		kType: "scheduled_task_created",
		"task": map[string]any{
			"taskId":     pick([]map[string]any{task, update, params}, "taskId", "task_id"),
			"prompt":     pick([]map[string]any{task, update, params}, "prompt"),
			"interval":   pick([]map[string]any{task, update, params}, "interval", "humanSchedule", "human_schedule"),
			"nextFireAt": pick([]map[string]any{task, update, params}, "nextFireAt", "next_fire_at"),
		},
	}
	// Full original payloads ride along — nothing is dropped anymore.
	ev["rawParams"] = params
	if len(update) > 0 {
		ev["rawTask"] = update
	}
	if meta, ok := params[kMeta].(map[string]any); ok {
		ev[kMetaOut] = meta
	}
	b.Broadcast(tag(ev))
}

// broadcastScheduledTaskDeleted normalizes the scheduled_task_deleted wire
// shapes (SessionUpdate::ScheduledTaskDeleted {task_id} or the standalone
// x.ai notification) into ONE typed event. rawParams preserves the full
// original params (e.g. session_id + _meta.eventId / scheduler
// generation-revision stamps). Shared by the x.ai method channel and the
// sessionUpdate kind dispatch.
func (b *Bridge) broadcastScheduledTaskDeleted(sid string, params map[string]any, tag func(Event) Event) {
	task, _ := params["task"].(map[string]any)
	update, _ := params[kUpdate].(map[string]any)
	ev := Event{
		kType:    "scheduled_task_deleted",
		"taskId": pick([]map[string]any{task, update, params}, "taskId", "task_id"),
	}
	// reason 归一化（1.0.4 起 SessionUpdate::ScheduledTaskDeleted 带
	// reason：completed/expired/deleted/shutdown）：依次从 update →
	// params → task 提取，无来源时缺省 unknown（旧宿主/旧版本数据）。
	if reason, _ := pick([]map[string]any{update, params, task}, "reason").(string); reason != "" {
		ev["reason"] = reason
	} else {
		ev["reason"] = "unknown"
	}
	// rawParams preserves the full original params (e.g. session_id +
	// _meta.eventId / scheduler generation-revision stamps).
	ev["rawParams"] = params
	if meta, ok := params[kMeta].(map[string]any); ok {
		ev[kMetaOut] = meta
	}
	b.Broadcast(tag(ev))
}

// resetRoster 清理 agent 进程死亡后的 in-memory 状态：清空 roster 与
// 队列快照缓存、保留 lastSession 指针、让所有在飞 RPC 立即失败返回。
// 只做清理，绝不杀进程 —— 2026-08-14 起 host 假设 agent 可靠，进程
// 生命周期完全交给外部，host 只在进程退出后把状态收拾干净并报错。
// 返回清理前快照的 lastSession（active 优先），供调用方打日志。
func (b *Bridge) resetRoster(reason string) (string, string) {
	return b.resetProcessRoster(reason, nil)
}

func (b *Bridge) resetProcessRoster(reason string, expected *exec.Cmd) (string, string) {
	b.agentMessageMu.Lock()
	defer b.agentMessageMu.Unlock()
	b.mu.Lock()
	superseded := expected != nil && b.cmd != expected
	b.mu.Unlock()
	if superseded {
		return "", ""
	}
	b.clientReqMu.Lock()
	defer b.clientReqMu.Unlock()
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		cr.retired = true
		cr.cancel = true
		select {
		case <-cr.done:
		default:
			close(cr.done)
		}
		b.resolveClientRequestLocked(key.(string), cr)
		return true
	})
	b.mu.Lock()
	if act := b.activeSessionLocked(); act != nil {
		b.rememberSessionLocked(act.SessionID, act.Cwd)
	}
	lastID, lastCwd := b.lastSessionID, b.lastSessionCwd
	b.cmd = nil
	b.stdin = nil
	b.ready = false
	b.bootOK = false
	b.sessions = make(map[string]*SessionState)
	// Agent 进程已死：观察到的回合全部作废（没有进程还会发来
	// turn_completed 收口）。
	b.turns = make(map[string]*observedTurn)
	b.activeSessionID = ""
	// Agent 进程已死：队列是 agent 内存态，重启即清空 —— 快照缓存
	// 一并作废，避免 /api/queue/status 回放过期队列。
	b.queueSnapshots = nil
	// 重试去重记忆随 agent 进程一起作废：上一个进程里的回合已经不存在，
	// 此时 FE 的重试应当真的跑一次，而不是被当成重复静默吞掉。
	b.promptedIDs = nil
	b.promptedFIFO = nil
	b.liveTools = make(map[string]*liveToolResolver)
	// lastSessionID / lastSessionCwd intentionally survive.
	if b.cancelRd != nil {
		b.cancelRd()
		b.cancelRd = nil
	}
	b.mu.Unlock()
	log.Printf("[Capri-host] agent state reset (reason=%s)", reason)
	b.failAllPending(fmt.Errorf("grok 进程已退出或不可用"))
	b.broadcastRosterChange()
	return lastID, lastCwd
}
