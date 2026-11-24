package acp

import (
	"strings"
)

// dispatchSessionUpdateKind routes one sessionUpdate `update` to its typed
// events. Returns whether the kind is modeled (handled): true → the caller
// must NOT emit the generic session_notification (FE 消费 typed 事件);
// false → the kind is unmodeled and the caller emits the generic
// session_notification as the forward-compat carrier (compaction_checkpoint
// / rewind_marker / unknown 及未来任何新 kind). Shared by BOTH sessionUpdate
// carriers — the official session/update envelope (handleSessionUpdate) and
// the x.ai channel (x.ai/session_notification / x.ai/session/update,
// handleXaiNotification) — so every kind behaves identically regardless of
// which rail it rides.
//
//   - tag: sessionId 归属约定（官方载体恒打 sessionId；x.ai 载体空则省略，
//     withSid 约定）。
//
// Typed kind 事件（{type: <kind>, update, sessionId, meta?}）在
// params._meta 非空时携带 `meta`（官方与 x.ai 载体统一，两个载体都可能有
// _meta——eventId 等）；仅非空才带（absent key ≠ off）。
func (b *Bridge) dispatchSessionUpdateKind(sid string, params map[string]any, tag func(Event) Event) bool {
	update, _ := params[kUpdate].(map[string]any)
	if update == nil {
		return false
	}
	kind, _ := update[kSessionUpdate].(string)
	// 回合观察（Busy 的观察腿）：两个载体（官方 session/update 与
	// x.ai/session_notification）都经过这里，是唯一收口点。
	b.observeUpdateKind(sid, kind, update)
	// meta 保全：params._meta 非空时随 typed kind 事件携带。
	var kindMeta any
	if m, ok := params[kMeta].(map[string]any); ok && len(m) > 0 {
		kindMeta = m
	}
	// image 事件不经 tag 闭包（imageEvent 自带 sessionId），重放标记只能在
	// 这里单独补上。整个 dispatch 的广播点都过 tag，所以在这一处包一层即
	// 覆盖两条载体（官方 session/update 与 x.ai/session/update）——漏掉任一
	// 条都会让 session/load 的重放只有一半不上总线，进而把同一批历史里的
	// 图片与文字事件劈成两半。
	replay := paramsIsReplay(params)
	if replay {
		inner := tag
		tag = func(ev Event) Event {
			ev[kReplayInternal] = true
			return inner(ev)
		}
	}
	switch kind {
	case "agent_message_chunk":
		if text := contentText(update[kContent]); text != "" {
			ev := attachStreamMeta(Event{kType: "chunk", kText: text}, params)
			if mid, ok := update["messageId"]; ok {
				ev["messageId"] = mid
			}
			// Mirror user_message_chunk: forward the chunk + content-block
			// meta (wire shape: update._meta = ContentChunk.meta →
			// hideFromScrollback; update.content._meta = TextContent.meta →
			// displayText / displayAsCron) so live rendering classifies
			// system-injected prompts exactly like the replay path does.
			if meta, ok := update[kMeta].(map[string]any); ok {
				if v, ok := meta["hideFromScrollback"]; ok {
					ev["hideFromScrollback"] = v
				}
			}
			if content, ok := update[kContent].(map[string]any); ok {
				if meta, ok := content[kMeta].(map[string]any); ok {
					for _, k := range []string{"displayText", "displayAsCron"} {
						if v, ok := meta[k]; ok {
							ev[k] = v
						}
					}
				}
			}
			// 不携带 fullUpdate（整份 update）：typed 字段即 wire 契约，
			// 两条出口（SSE / hub）本就剥离该键，且 FE 无消费者。
			b.Broadcast(tag(ev))
			// 生成输出速率（按流式字符估算，见 genrate.go）：与 chunk 同轨
			// 广播，节流后每个 session 每 ≥250ms 至多一条 active。时间源
			// 优先 _meta.agentTimestampMs + streamStartMs（agent 累计口径：
			// 速率 = 流起点之后的全部字符 / 墙钟，含首包）。
			now, streamStartMs, agentClock := metaAgentTs(params)
			if rate, ok := b.genRate.observe(sid, text, now, agentClock, streamStartMs); ok {
				b.Broadcast(tag(Event{kType: "gen_rate", kRate: rate, kActive: true}))
			}
		}
		// Image blocks ride the same chunk carrier: emit one typed image
		// event per block (text parts still go through the chunk event).
		for _, img := range contentImages(update[kContent]) {
			b.broadcastImage(sid, img, replay)
		}
		return true
	case "user_message_chunk":
		// 用户输入打断生成段：静默复位速率估算器（不发任何事件，
		// 客户端显示值在下一段 live 更新前保留）。
		b.genRate.reset(sid)
		if text := contentText(update[kContent]); text != "" {
			ev := attachStreamMeta(Event{kType: "user_chunk", kText: text}, params)
			// Forward the chunk + content-block meta (wire shape:
			// update._meta = ContentChunk.meta → hideFromScrollback;
			// update.content._meta = TextContent.meta → displayText /
			// displayAsCron) so live rendering classifies system-injected
			// prompts exactly like the replay path does.
			if meta, ok := update[kMeta].(map[string]any); ok {
				if v, ok := meta["hideFromScrollback"]; ok {
					ev["hideFromScrollback"] = v
				}
			}
			if content, ok := update[kContent].(map[string]any); ok {
				if meta, ok := content[kMeta].(map[string]any); ok {
					for _, k := range []string{"displayText", "displayAsCron"} {
						if v, ok := meta[k]; ok {
							ev[k] = v
						}
					}
				}
			}
			// 同 agent_message_chunk：不携带 fullUpdate（typed 字段即 wire 契约）。
			b.Broadcast(tag(ev))
		}
		for _, img := range contentImages(update[kContent]) {
			b.broadcastImage(sid, img, replay)
		}
		return true
	case "agent_thought_chunk":
		// content is typically { "type":"text", "text":"..." }; accept a few shapes
		if text := contentText(update[kContent]); text != "" {
			ev := attachStreamMeta(Event{kType: "thought", kText: text}, params)
			// Forward the chunk meta verbatim (hideFromScrollback etc.)
			// so live rendering treats it like the replay path does.
			if meta, ok := update[kMeta].(map[string]any); ok {
				ev[kMetaOut] = meta
			}
			b.Broadcast(tag(ev))
			// 思考文本同样更新生成速率（见 genrate.go）。
			now, streamStartMs, agentClock := metaAgentTs(params)
			if rate, ok := b.genRate.observe(sid, text, now, agentClock, streamStartMs); ok {
				b.Broadcast(tag(Event{kType: "gen_rate", kRate: rate, kActive: true}))
			}
		}
		return true
	case "tool_call":
		// 工具执行打断生成段：复位段状态（新段从首包重新计时）并广播
		// active:false（不带 rate）——前端收到后清除速率显示（只在输
		// 出过程中显示）。
		b.genRate.reset(sid)
		b.liveToolResolve(sid, "tool_call", update, params, replay)
		b.Broadcast(tag(Event{kType: "gen_rate", kActive: false}))
		b.Broadcast(tag(Event{kType: "tool_call", kToolCall: update}))
		return true
	case "tool_call_update":
		b.liveToolResolve(sid, "tool_call_update", update, params, replay)
		b.Broadcast(tag(Event{kType: "tool_call_update", kToolCallUpdate: update}))
		return true
	case "plan":
		b.Broadcast(tag(Event{kType: "plan", kEntries: update[kEntries]}))
		return true
	case "usage_update":
		b.trackUsage(sid, toInt64(update[kUsed]), toInt64(update[kSize]))
		b.Broadcast(tag(Event{
			kType: "usage",
			kUsed: update[kUsed],
			kSize: update[kSize],
			kCost: update[kCost],
		}))
		return true
	case "current_mode_update":
		// ACP CurrentModeUpdate is {currentModeId} (enter_plan_mode /
		// exit_plan_mode and session/set_mode). Looking only for modeState
		// left s.modes stale, so the FE composer never followed the agent.
		b.mu.Lock()
		modes := b.applyCurrentModeUpdateLocked(sid, update)
		b.mu.Unlock()
		b.Broadcast(tag(Event{kType: "modes_update", "modes": modes}))
		return true
	case "config_option_update":
		b.mu.Lock()
		if co := pick([]map[string]any{update}, kConfigOptions, "options"); co != nil {
			if s := b.sessions[sid]; s != nil {
				s.configOpts = co
			}
		}
		var co any
		if s := b.sessions[sid]; s != nil {
			co = s.configOpts
		}
		b.mu.Unlock()
		b.Broadcast(tag(Event{kType: "config_options_update", kConfigOptions: co}))
		return true
	case "available_commands_update":
		// ACP AvailableCommandsUpdate is {availableCommands}, not {commands}.
		cmds := pick([]map[string]any{update}, kAvailableCommands, "available_commands", kCommands)
		b.Broadcast(tag(Event{kType: "commands_update", kCommands: cmds}))
		return true
	case "session_info_update", "session_info":
		// session_info_update 是官方 ACP SessionUpdate 的 kind（serde tag
		// "sessionUpdate"，agent-client-protocol-schema client.rs:106）；
		// 旧版 x.ai 载体曾用裸 "session_info" 形式——两者等价合并。
		// title/updatedAt roster 跟踪两个载体都要有。
		b.mu.Lock()
		if s := b.sessions[sid]; s != nil {
			if t, ok := update[kTitle].(string); ok && t != "" {
				s.Title = t
			}
			if u, ok := update[kUpdatedAt].(string); ok && u != "" {
				s.UpdatedAt = u
			}
		}
		b.mu.Unlock()
		ev := Event{
			kType:      "session_info",
			kTitle:     update[kTitle],
			kUpdatedAt: update[kUpdatedAt],
		}
		if v, ok := titleIsManualFrom(params, update); ok {
			ev["titleIsManual"] = v
		}
		b.Broadcast(tag(ev))
		return true
	case "scheduled_task_created":
		// 复用 x.ai 通道的归一化 helper（task/rawTask/rawParams/meta 保全，
		// 见 broadcastScheduledTaskCreated）。
		b.broadcastScheduledTaskCreated(sid, params, tag)
		return true
	case "scheduled_task_deleted":
		b.broadcastScheduledTaskDeleted(sid, params, tag)
		return true
	case "background_tasks":
		// SessionUpdate::BackgroundTasks 原生全量快照：
		// 权威替换原 updates.jsonl 扫描与 lsof 探测。
		var rawTasks []any
		if t, ok := update["tasks"].([]any); ok {
			rawTasks = t
		}
		b.mu.Lock()
		if s := b.sessions[sid]; s != nil {
			s.backgroundTasks = rawTasks
		}
		b.mu.Unlock()
		ev := Event{
			kType:       "background_tasks",
			"tasks":     rawTasks,
			"truncated": update["truncated"],
		}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	case "task_backgrounded", "task_completed", "monitor_event":
		// 形状与其它 kind typed 事件统一：{type, update, sessionId}
		// （update = 原始 update 对象）。x.ai standalone 通知通道
		// （x.ai/task_backgrounded / x.ai/task_completed /
		// x.ai/monitor_event 方法）保持 {type, params, sessionId} 不动
		// —— 不同载体，形状跟随 wire（见 handleXaiNotification）。
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	case "model_auto_switched", "model_changed":
		// 先发原样 typed 事件再改缓存：FE 的提示行（notifMemory.ts /
		// notifApps.ts）靠「上一个名称 ≠ 新名称」判断要不要落一行，而
		// models_update / model 会先把 modelName 写成新值——顺序反过来，
		// prevName 已等于新名称，提示行永远不会出现。
		//  - model_auto_switched：两个其它事件只换 caption / 目录，不带
		//    previous_model_id / reason，web 端无法据此说明「原模型已下线」。
		//    new_model_id 为空（同族也无兜底、prompt 被拦）时同样要发——
		//    那种情况下 handleModelSwitchKind 提前返回，只能靠 reason 告知
		//    用户。
		//  - model_changed：别的客户端切模型时这一端没有「模型已从 X 切换
		//    到 Y」滚动行；自己的切换有 setModel 的乐观回声行，回合内
		//    prevName 已等于新名称，同名守卫会去重。
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
		}
		b.Broadcast(tag(ev))
		// roster models 缓存更新 + models_update / model 广播（typed 语义
		// 事件，见 handleModelSwitchKind）；kind modeled → 无 generic，
		// FE 改用 typed 消费。
		b.handleModelSwitchKind(sid, update, tag)
		return true
	case "turn_completed", "response_completed":
		// Both boundaries end a generation segment; only turn_completed
		// ends the tool loop and the frontend turn.
		b.genRate.reset(sid)
		b.Broadcast(tag(Event{kType: "gen_rate", kActive: false}))
		if kind == "response_completed" {
			return true
		}
		b.liveToolResolve(sid, "turn_completed", nil, params, replay)
		// typed 事件是 FE 回合封口语义（update 原样含 stop_reason /
		// prompt_id / usage 等字段）；usage 提取保留在两个载体
		// （handleSessionUpdate / handleXaiNotification）。
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	case "compaction_checkpoint", "rewind_marker", "unknown":
		// 仅持久化、不发 typed UI（grokbuild-acp-protocol.md §13：
		// compaction checkpoint / rewind marker 不上 live 通道）；unknown
		// 前向兼容忽略。未建模 → 由调用方发 generic（保持不变）。
		return false
	case "diff_review", "retry_state", "auto_compact_started",
		"auto_compact_completed", "auto_compact_failed", "auto_compact_cancelled",
		"auto_continue_completed", "memory_flush_started", "memory_flush_completed",
		"memory_dream_completed", "memory_session_saved", "memory_files",
		"feedback_request", "relay_sync_status", "auto_recovery_started",
		"auto_recovery_exhausted", "hook_annotation", "hook_execution",
		"hook_run_started", "hooks_changed", "plugins_changed", "plugin_updates_installed",
		"session_status", "session_summary_generated", "session_recap", "session_recap_unavailable",
		"last_turn_summary", "subagent_spawned", "subagent_progress",
		"subagent_finished",
		"scheduled_task_fired", "tool_call_delta_chunk", "image_compressed",
		"image_dropped", "workflow_updated", "goal_updated",
		"pending_interaction", "interaction_resolved", "response_started",
		"reasoning_completed":
		// 单发 typed 事件：{type: <kind>, update: <原始 update>, sessionId,
		// meta?}（sessionId 空则省略，withSid 约定；params._meta 非空时带
		// meta），字段原样透传不归一化。载荷形状以 grok 源码
		// extensions/notification.rs 的 SessionUpdate 枚举（tag =
		// "sessionUpdate"，snake_case）为准。modeled → 无 generic。
		ev := Event{kType: kind, kUpdate: update}
		if kindMeta != nil {
			ev[kMetaOut] = kindMeta
		}
		b.Broadcast(tag(ev))
		return true
	default:
		// 未建模的 kind（未来任何新 kind）：generic session_notification
		// 由调用方发出（前向兼容，不新增 typed 事件）。
		return false
	}
}

func (b *Bridge) liveToolLocked(sid string) *liveToolResolver {
	if b.liveTools == nil {
		b.liveTools = make(map[string]*liveToolResolver)
	}
	r := b.liveTools[sid]
	if r == nil {
		r = &liveToolResolver{}
		b.liveTools[sid] = r
	}
	return r
}

// liveToolResolve 给实时工具事件打上与历史主路径同一套合成 ID。
// session/load 重放（isReplay）整段跳过：HTTP 历史已经按 synth:call:<ts>:<k>
// 注入过，重放再跑会把 live 计数器/open 集冲掉，后续真 live 对不上。
func (b *Bridge) liveToolResolve(sid, su string, update, params map[string]any, replay bool) {
	if replay {
		return
	}
	ts, eventID := paramsSynthMeta(params)

	b.mu.Lock()
	r := b.liveToolLocked(sid)
	if r.seeded {
		r.handleLive(su, update, ts, eventID)
		b.mu.Unlock()
		return
	}
	cwd := ""
	if s := b.sessions[sid]; s != nil {
		cwd = s.Cwd
	}
	b.mu.Unlock()

	var view *normalizedHistory
	if cwd != "" && sid != "" {
		if path := sessionUpdatesFile(b.grokHome(), cwd, sid); path != "" {
			view, _ = b.normalizedSessionHistory(path)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	r = b.liveToolLocked(sid)
	if !r.seeded {
		r.seedFrom(view)
	}
	r.handleLive(su, update, ts, eventID)
}

// unwrapExtMethod normalizes the two x.ai wire forms:
//   - direct:  {"method":"x.ai/foo", "params":{...}}          -> x.ai/foo
//   - wrapped: {"method":"_x.ai/foo","params":{"method":"x.ai/foo","params":{...}}} -> x.ai/foo
//
// For the wrapped leader form, an explicit sessionId carried on the OUTER
// envelope params (leader routing meta) is preserved into the inner params
// when the inner ones lack both id keys — so sessionIdFrom prefers the
// explicit id instead of falling back to the host's active session.
func unwrapExtMethod(method string, params map[string]any) (string, map[string]any) {
	if !strings.HasPrefix(method, "_x.ai/") {
		return method, params
	}
	if inner, ok := params[kMethod].(string); ok && strings.HasPrefix(inner, "x.ai/") {
		if innerParams, ok := params[kParams].(map[string]any); ok {
			_, hasCamel := innerParams[kSessionID]
			_, hasSnake := innerParams[kSessionIDS]
			if !hasCamel && !hasSnake {
				if sid, ok := params[kSessionID].(string); ok && sid != "" {
					innerParams[kSessionID] = sid
				} else if sid, ok := params[kSessionIDS].(string); ok && sid != "" {
					innerParams[kSessionIDS] = sid
				}
			}
			return inner, innerParams
		}
		return inner, map[string]any{}
	}
	return strings.TrimPrefix(method, "_"), params
}

// handleXaiNotification forwards x.ai/* extension notifications to browsers
// as typed SSE events. Unknown methods fall back to a generic
// ext_notification event so nothing is silently dropped.
func (b *Bridge) handleXaiNotification(method string, params map[string]any) {
	// Tag every extension notification with its originating session so the
	// dashboard can bucket multi-session activity. session/load 的重放走
	// dispatchSessionUpdateKind 的 tag 包装拦（两条载体共用）；这里的 generic
	// 兜底广播不过 dispatch，必须自己盖章，否则未建模 kind 的重放仍会上总线。
	sid := b.sessionIdFrom(params)
	replay := paramsIsReplay(params)
	withSid := func(ev Event) Event {
		if sid != "" {
			ev[kSessionID] = sid
		}
		if replay {
			ev[kReplayInternal] = true
		}
		return ev
	}
	// withExplicitSid 是 withSid 的严格变体：只在 params 里**显式**带了非空
	// sessionId 时才盖章，不回退到活动会话。给"null 表示尚无会话"的轨道用
	// （见 x.ai/session/setup）：sessionIdFrom 的活动会话回退会把它们错标到
	// 上一个会话。
	withExplicitSid := func(ev Event) Event {
		if explicit := sessionIdExplicit(params); explicit != "" {
			ev[kSessionID] = explicit
		}
		if replay {
			ev[kReplayInternal] = true
		}
		return ev
	}
	switch method {
	case "x.ai/session_notification", "x.ai/session/update":
		// Kind 分发（含 session_info 的 title/updatedAt roster 跟踪）在
		// dispatchSessionUpdateKind —— 与官方 session/update 载体一致。
		// modeled kind 只发 typed；仅当 dispatch 返回 false（未建模）时
		// generic 照发，携带完整 params 使 `_meta`（eventId 等）保全
		// 方式保持现状。
		if !b.dispatchSessionUpdateKind(sid, params, withSid) {
			b.Broadcast(withSid(Event{kType: "session_notification", kMethod: method, kParams: params}))
		}
		// grok ships usage inside response_completed / turn_completed
		// notifications instead of the standard usage_update carrier —
		// surface the context-window `used` (_meta.totalTokens, the TUI
		// ⇣ counter source) as a normal usage event. x.ai notifications
		// usually don't carry it (keep the last known usage on turn end,
		// same as the TUI). 实测 FE 只 merge used/size，usage 账本对象
		// 是死字段——不再透传。typed 事件（dispatch 已发）是 FE 回合
		// 封口语义；usage 提取保留在此（x.ai 载体不带 size:nil，官方
		// 载体带 —— 保持原状，不统一）。
		if up, ok := params[kUpdate].(map[string]any); ok {
			if kind, _ := up[kSessionUpdate].(string); kind == "response_completed" || kind == "turn_completed" {
				ev := Event{kType: "usage"}
				if meta, ok := params[kMeta].(map[string]any); ok {
					if used, ok := asInt(meta["totalTokens"]); ok && used > 0 {
						ev[kUsed] = used
						b.trackUsage(sid, used, 0)
					}
				}
				// Only broadcast when there is something to update —
				// an empty usage event (no _meta.totalTokens) would
				// otherwise null the client's `used`.
				if len(ev) > 1 {
					b.Broadcast(withSid(ev))
				}
			}
		}
	case "x.ai/task_backgrounded":
		b.Broadcast(withSid(Event{kType: "task_backgrounded", kParams: params}))
	case "x.ai/task_completed":
		b.Broadcast(withSid(Event{kType: "task_completed", kParams: params}))
	case "x.ai/monitor_event":
		b.Broadcast(withSid(Event{kType: "monitor_event", kParams: params}))
	case "x.ai/git_head_changed":
		// Stash the head into the roster so /api/session-info can serve it.
		b.mu.Lock()
		if s := b.sessions[sid]; s != nil {
			if v, ok := params["branch"].(string); ok {
				s.gitBranch = v
			}
			if v, ok := params["isWorktree"].(bool); ok {
				s.gitWorktree = v
			}
			if v, ok := params["mainRepo"].(string); ok {
				s.gitMainRepo = v
			}
		}
		b.mu.Unlock()
		b.Broadcast(withSid(Event{kType: "git_head_changed", kParams: params}))
	case "x.ai/yolo_mode_changed":
		// Permission-mode change: CLIENT-SCOPED on the agent side (applies
		// to every resident session of the sending client), so the broadcast
		// deliberately carries NO sessionId — withSid's active-session
		// fallback would mis-tag it in multi-session setups and mislead
		// frontends into treating a global toggle as session-scoped.
		// Also mirror the agent's (echoed) mode into the host record so
		// later hello snapshots carry the agent's real state.
		b.recordPermissionMode(permissionModeFromParams(params))
		b.Broadcast(Event{kType: "yolo_mode_changed", kParams: params})
	case "x.ai/mcp/server_status":
		b.Broadcast(withSid(Event{kType: "mcp_server_status", kParams: params}))
	case "x.ai/mcp/tools_changed", "x.ai/mcp_initialized":
		b.Broadcast(withSid(Event{kType: "mcp_tools_changed", kParams: params}))
	case "x.ai/mcp/servers_updated":
		b.Broadcast(withSid(Event{kType: "mcp_servers_updated", kParams: params}))
	case "x.ai/sessions/changed":
		b.Broadcast(Event{kType: "sessions_changed", kParams: params})
	case "x.ai/models/update":
		// Machine-wide catalog refresh (config.toml changed) — update the
		// active session's catalog while preserving its current selection
		// when still available (TUI update_catalog semantics). The raw
		// payload is a SessionModelState; forwarding it verbatim would
		// clobber the session's model with the machine-wide current.
		b.mu.Lock()
		if act := b.activeSessionLocked(); act != nil {
			b.applyModelsCatalog(act, params)
			models := act.models
			b.mu.Unlock()
			if models != nil {
				ev := Event{kType: "models_update", kParams: models}
				// raw carries the ORIGINAL notification params so no field
				// of the machine-wide broadcast is lost to the merge.
				ev["raw"] = params
				b.Broadcast(withSid(ev))
			}
		} else {
			b.mu.Unlock()
		}
	case "x.ai/announcements/update":
		b.Broadcast(Event{kType: "announcements_update", kParams: params})
	case "x.ai/scheduled_task_fired":
		b.Broadcast(withSid(Event{kType: "scheduled_task_fired", kParams: params}))
	case "x.ai/scheduled_task_inject_prompt":
		b.Broadcast(withSid(Event{kType: "scheduled_task_inject_prompt", kParams: params}))
	case "x.ai/scheduled_task_created":
		// 归一化逻辑抽到 broadcastScheduledTaskCreated（与 sessionUpdate
		// kind 分发共享）：task/rawTask/rawParams/meta 保全，见该 helper。
		b.broadcastScheduledTaskCreated(sid, params, withSid)
	case "x.ai/scheduled_task_deleted":
		b.broadcastScheduledTaskDeleted(sid, params, withSid)
	case "x.ai/session/prompt_complete":
		b.Broadcast(withSid(Event{kType: "prompt_complete", kParams: params}))
	case "x.ai/session/updates/chunk":
		// 分块拉取会话更新（extensions/session_updates.rs:290-330
		// send_streamed_chunks）：{sessionId, index, updates: [原始
		// updates.jsonl 行], done}，可选 routing meta。原样透传。
		b.Broadcast(withSid(Event{kType: "session_updates_chunk", kParams: params}))
	case "x.ai/queue/changed":
		// prompt 队列变更（session/prompt_queue.rs QUEUE_CHANGED_METHOD）。
		// 队列状态不落盘、load 不回放 —— 缓存最近一次快照，供
		// /api/queue/status 在 FE load 会话后主动拉取对齐镜像
		// （断线期间错过的广播靠它补上）。cloneAny 隔离副本，快照
		// 与后续 broadcast 序列化互不共享存储。
		if sid != "" {
			b.mu.Lock()
			if b.queueSnapshots == nil {
				b.queueSnapshots = map[string]map[string]any{}
			}
			if snap, ok := cloneAny(params).(map[string]any); ok {
				b.queueSnapshots[sid] = snap
			}
			b.mu.Unlock()
		}
		b.Broadcast(withSid(Event{kType: "queue_changed", kParams: params}))
	case "x.ai/config_changed":
		// 配置变更（MCP 初始化取消 / agent/app.rs leader 广播）。
		b.Broadcast(withSid(Event{kType: "config_changed", kParams: params}))
	case "x.ai/settings/update":
		// 设置热更新推送（agent/mvp_agent/mod.rs:2042）。
		b.Broadcast(withSid(Event{kType: "settings_update", kParams: params}))
	case "x.ai/fs_notify":
		// 文件系统变更（session/fs_watch.rs:342）：{sessionId,
		// event:{kind, paths}}。
		b.Broadcast(withSid(Event{kType: "fs_notify", kParams: params}))
	case "x.ai/fs/index":
		// 全量文件索引（session/fs_watch.rs:428）。
		b.Broadcast(withSid(Event{kType: "fs_index", kParams: params}))
	case "x.ai/fs/index/delta":
		// 增量文件索引（session/fs_watch.rs:368）。
		b.Broadcast(withSid(Event{kType: "fs_index_delta", kParams: params}))
	case "x.ai/search/fuzzy/status":
		// 模糊搜索进度（extensions/search.rs:158）。
		b.Broadcast(withSid(Event{kType: "search_fuzzy_status", kParams: params}))
	case "x.ai/search/content/status":
		// 内容搜索进度（extensions/search.rs:222）。
		b.Broadcast(withSid(Event{kType: "search_content_status", kParams: params}))
	case "x.ai/git/worktree/status":
		// worktree 创建进度（extensions/worktree.rs:37）。
		b.Broadcast(withSid(Event{kType: "git_worktree_status", kParams: params}))
	case "x.ai/mcp/init_progress":
		// MCP 初始化进度（extensions/mcp.rs INIT_PROGRESS）。
		b.Broadcast(withSid(Event{kType: "mcp_init_progress", kParams: params}))
	case "x.ai/terminal/pty/notification":
		// PTY 输出通知（terminal/pty_session.rs:23 NOTIFICATION_METHOD）。
		b.Broadcast(withSid(Event{kType: "pty_notification", kParams: params}))
	case "x.ai/session/interjection":
		// 回合中插话（session/acp_session_impl/interjection.rs:171）：
		// {sessionId, text, interjectionId?}。
		b.Broadcast(withSid(Event{kType: "session_interjection", kParams: params}))
	case "x.ai/follow_ups":
		// 回合结束后的建议 chips（TUI app/acp_handler/follow_ups.rs 渲染；
		// params 含 response_id / follow_ups 列表，原样透传）。
		b.Broadcast(withSid(Event{kType: "follow_ups", kParams: params}))
	case "x.ai/leader/version_mismatch":
		// leader 与 client 版本不一致横幅（TUI xai-grok-pager/src/acp/
		// version_mismatch.rs 渲染；wire params 为 {clientVersion,
		// leaderVersion, message}——message 被 TUI 忽略，原样透传）。
		b.Broadcast(withSid(Event{kType: "leader_version_mismatch", kParams: params}))
	case "x.ai/leader_reconnected":
		// leader 重连信号（xai-grok-pager-bin/src/main.rs:1354 等，
		// params 可为空）。
		b.Broadcast(withSid(Event{kType: "leader_reconnected", kParams: params}))
	case "x.ai/session/setup":
		// 新建会话的阻塞步骤进度（grok session_setup.rs：session/new 期间
		// 每个阶段一条，{method:"session/new", phase, sessionId?}）。会话 id
		// 铸造**之前**的阶段（Auth / ResolveWorkspace / FolderTrust /
		// LocalWorkspace）sessionId 是显式 null——必须用 withExplicitSid，否则
		// 这些阶段会被错标到新建之前那个活动会话上。TUI 客户端同口径
		// （xai-grok-pager acp_handler/mod.rs 要求 sessionId 非空才处理）。
		// 载体保持 generic：host 未建模阶段 UI，typed 事件留给 FE 有渲染需求时。
		b.Broadcast(withExplicitSid(Event{kType: "ext_notification", kMethod: method, kParams: params}))
	default:
		b.Broadcast(withSid(Event{kType: "ext_notification", kMethod: method, kParams: params}))
	}
}
