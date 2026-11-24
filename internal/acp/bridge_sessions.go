package acp

import (
	"context"
	"errors"
	"log"
	"time"
)

// NewSession creates a brand-new session in the (already booted) agent
// process and makes it the active session. Other sessions keep running —
// this is what powers the parallel dashboard.
func (b *Bridge) NewSession(ctx context.Context, sc SessionConfig) error {
	if err := b.ensureBooted(ctx); err != nil {
		return err
	}
	return b.createSession(ctx, sc)
}

// createSession calls session/new and registers the session in the roster.
func (b *Bridge) createSession(ctx context.Context, sc SessionConfig) error {
	cwd := sc.Cwd
	if cwd == "" {
		cwd = mustCwd()
	}
	cwd = normCwd(cwd)
	addDirs := sc.AdditionalDirectories
	if addDirs == nil {
		addDirs = []string{}
	}
	mcp := sc.MCPServers
	if mcp == nil {
		mcp = []map[string]any{}
	}
	params := map[string]any{
		"cwd":                   cwd,
		"additionalDirectories": addDirs,
		"mcpServers":            mcp,
	}
	// Client-supplied session seeds (permission mode flags etc.) ride the
	// params `_meta`, exactly like the TUI's SessionFlags.to_meta().
	// 回显开关（clientUserMessageEcho）无条件带上，与 seeds 合并。
	params[kMeta] = withUserMessageEcho(sc.Meta)

	sessRes, err := b.request(ctx, "session/new", params, bootTimeout)
	if err != nil {
		// 假设 agent 可靠：创建失败直接报错，不杀进程、不自愈。
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
		return err
	}

	sid, _ := sessRes[kSessionID].(string)
	if sid == "" {
		// 协议级异常：agent 应答了但没给 sessionId —— 报错返回，不杀
		// 进程；是否需要重启由用户决定。
		err := errors.New("session/new 未返回 sessionId")
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
		return err
	}

	now := time.Now().UnixMilli()
	b.mu.Lock()
	s := b.sessions[sid]
	if s == nil {
		s = &SessionState{SessionID: sid, CreatedAt: now}
		b.sessions[sid] = s
	}
	s.Cwd = cwd
	s.unloaded = false
	s.modes = sessRes["modes"]
	s.configOpts = sessRes[kConfigOptions]
	s.models = sessRes["models"]
	// session/new 响应 `_meta`（agent 下发）原样存下，ready 事件 / Status
	// 透传；缺省为 nil（absent key ≠ off）。
	s.sessionMeta = sessRes[kMeta]
	b.activeSessionID = sid
	b.rememberSessionLocked(sid, cwd)
	b.ready = true
	b.bootError = ""
	modes := s.modes
	configOpts := s.configOpts
	models := s.models
	sessionMeta := s.sessionMeta
	agentInfo := b.agentInfo
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := s.Cwd
	b.mu.Unlock()

	ev := Event{
		kType:          "ready",
		kSessionID:     sid,
		"cwd":          sessCwd,
		"agentInfo":    agentInfo,
		"modes":        modes,
		kConfigOptions: configOpts,
		"models":       models,
		"hostId":       hostID,
		"hostName":     hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
	b.broadcastRosterChange()
	return nil
}

func (b *Bridge) authenticate(ctx context.Context, init map[string]any) error {
	methodsRaw, _ := init["authMethods"].([]any)
	var methodIDs []string
	for _, m := range methodsRaw {
		mm, _ := m.(map[string]any)
		if id, _ := mm[kID].(string); id != "" {
			methodIDs = append(methodIDs, id)
		}
	}
	// 透传 agent 的认证信息：客户端不做能力判断（不检查 XAI_API_KEY
	// 等环境变量），完全采用 agent 广告的方法——agent 只在有可用凭证
	// 时才广告对应方法（env key / BYOK / 登录态），认证端还会按
	// `[auth] preferred_method` 兜底校验，客户端自行推导 api_key vs
	// session 曾导致 OIDC refresh 回归。
	// 优先采用 agent 声明的 defaultAuthMethodId（agent 是权威），
	// 未声明时取广告列表的第一个方法。
	// 注意：initialize 响应的认证字段在 `_meta`（带下划线，与请求侧
	// 一致）；读 `meta` 会永远落空而回退到第一个方法（xai.api_key），
	// 导致 session 认证（OIDC/cached_token）失效、模型请求 401。
	var methodID string
	if meta, ok := init[kMeta].(map[string]any); ok {
		if id, _ := meta["defaultAuthMethodId"].(string); id != "" {
			for _, m := range methodIDs {
				if m == id {
					methodID = id
					break
				}
			}
		}
	}
	if methodID == "" && len(methodIDs) > 0 {
		methodID = methodIDs[0]
	}
	if methodID == "" {
		return errors.New("没有可用的认证方式：agent 未广告任何认证方法（请先运行 `grok login`，或设置 XAI_API_KEY / per-model api_key）")
	}
	// The agent's AuthRequestMeta (mvp_agent/mod.rs:1203) reads
	// headless/reauth/use_oauth/force_interactive/request_seq from the
	// authenticate `_meta`. headless stays true; the rest are env-driven
	// and omitted when absent (absent key = current behavior exactly).
	meta := map[string]any{"headless": true}
	if boolEnv("ACP_AUTH_REAUTH") {
		meta["reauth"] = true
	}
	if boolEnv("ACP_AUTH_USE_OAUTH") {
		meta["use_oauth"] = true
	}
	if boolEnv("ACP_AUTH_FORCE_INTERACTIVE") {
		meta["force_interactive"] = true
	}
	if v, ok := intEnv("ACP_AUTH_REQUEST_SEQ"); ok {
		meta["request_seq"] = v
	}
	res, err := b.request(ctx, "authenticate", map[string]any{
		"methodId": methodID,
		kMeta:      meta,
	}, bootTimeout)
	if err != nil {
		return err
	}
	// AuthMeta: 响应 `_meta`（email/auth_mode/team_id/team_name/is_zdr/
	// team_role/coding_data_retention_opt_out/show_resolved_model/gate/
	// subscription_tier）原样存下，Status / ready 事件透传；缺省为 nil
	// （absent key ≠ off）。显式归 nil：类型断言的失败值是有类型的 nil
	// map，直接存入 any 字段会让 omitempty 失效。
	b.mu.Lock()
	b.authMeta = nil
	if m, ok := res[kMeta].(map[string]any); ok {
		b.authMeta = m
	}
	b.mu.Unlock()
	return nil
}

// HasSession reports whether the named session is in the roster — a pure
// in-memory check with no agent interaction. handlePrompt uses it to 404
// an explicitly-targeted unknown session synchronously, before accepting
// the prompt.
func (b *Bridge) HasSession(sessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.sessions[sessionID]
	return ok
}

// LoadSession switches the active session to a historical one (session/load),
// so subsequent prompts and live updates belong to that session.
//
// The load response carries the restored session's SessionModelState (the
// model that was active when the session was saved, after availability
// fallbacks). We surface it on the ready event so the UI caption matches
// the restored session instead of the process-global boot model.
//
// If the target session is already live in this host and has a turn in
// flight (Busy), we only re-focus the UI onto it — calling agent
// session/load would conflict with the running turn (previously 409).
//
// An optional meta map (client-supplied permission-mode seeds, e.g.
// yoloMode/autoMode) is forwarded as the session/load params `_meta` —
// the TUI's SessionFlags.to_meta() analog. Omitted = current behavior
// exactly (no `_meta` key on the wire).
func (b *Bridge) LoadSession(ctx context.Context, sessionID, cwd string, meta ...map[string]any) (map[string]any, error) {
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil && s.Busy {
		// Focus-only path for an in-flight session.
		if cwd != "" {
			s.Cwd = cwd
		}
		b.activeSessionID = sessionID
		b.rememberSessionLocked(sessionID, s.Cwd)
		b.ready = true
		b.bootError = ""
		sessRes := map[string]any{"busy": true}
		if s.models != nil {
			sessRes["models"] = s.models
		}
		if s.modes != nil {
			sessRes["modes"] = s.modes
		}
		if s.configOpts != nil {
			sessRes[kConfigOptions] = s.configOpts
		}
		agentInfo := b.agentInfo
		modes := s.modes
		configOpts := s.configOpts
		models := s.models
		sessionMeta := s.sessionMeta
		authMeta := b.authMeta
		hostID := b.hostID
		hostName := b.hostName
		sessCwd := s.Cwd
		b.mu.Unlock()

		ev := Event{
			kType:          "ready",
			kSessionID:     sessionID,
			"cwd":          sessCwd,
			"agentInfo":    agentInfo,
			"modes":        modes,
			kConfigOptions: configOpts,
			"models":       models,
			"hostId":       hostID,
			"hostName":     hostName,
		}
		// 已存 roster 的 sessionMeta / authenticate 的 authMeta 原样透传，
		// 仅非空才带。
		if sessionMeta != nil {
			ev["sessionMeta"] = sessionMeta
		}
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
		// Re-announce busy so the client can attach the spinner after history load.
		b.Broadcast(Event{kType: "busy", kSessionID: sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	// Capture the session's last-known reasoning effort BEFORE the agent
	// call. Agent session/load remaps persisted model ids to current
	// catalog keys (e.g. deepseek-v4-flash → deepseek-v4-flash-go) and
	// answers WITHOUT a reasoningEffort — the FE would then fall back to
	// the mapped model's default effort (e.g. low), silently discarding
	// the user's choice (e.g. max). The remap broadcast can also race in
	// while the request is in flight, so read the cache up-front rather
	// than after the response.
	prevEffort := ""
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil {
		if m, ok := s.models.(map[string]any); ok {
			if e, ok := m["reasoningEffort"].(string); ok {
				prevEffort = e
			}
		}
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		kSessionID:   sessionID,
		"cwd":        cwd,
		"mcpServers": []any{},
	}
	if len(meta) > 0 && len(meta[0]) > 0 {
		params[kMeta] = withUserMessageEcho(meta[0])
	} else {
		// 回显开关走会话 meta（见 clientUserMessageEchoMeta）：load 出来的
		// 会话也要能实时回显用户附图，客户端没给 meta 时也要带上。
		params[kMeta] = withUserMessageEcho(nil)
	}
	// Multi-tab: agent session/load REPLAYS the full conversation as
	// session/update over the shared SSE bus. The tab that called
	// /api/session-load drops those via historyLoading + HTTP history;
	// every OTHER tab that is viewing the same session would otherwise
	// APPEND the replay onto its existing scrollback (doubled timeline).
	// Announce before the agent call so peers arm the drop gate first.
	b.Broadcast(Event{
		kType:      "session_load_started",
		kSessionID: sessionID,
		"cwd":      cwd,
	})
	sessRes, err := b.request(ctx, "session/load", params, bootTimeout)
	if err != nil {
		// Peers that armed on started must unstick (reload or clear gate).
		b.Broadcast(Event{
			kType:      "session_load_finished",
			kSessionID: sessionID,
			"cwd":      cwd,
			"ok":       false,
		})
		return nil, err
	}

	b.mu.Lock()
	act := b.sessions[sessionID]
	if act == nil {
		act = &SessionState{SessionID: sessionID, CreatedAt: time.Now().UnixMilli()}
		b.sessions[sessionID] = act
	}
	act.Cwd = cwd
	act.unloaded = false
	b.ready = true
	b.bootError = ""
	// Prefer fields from the load response — they reflect the restored session.
	if m, ok := sessRes["models"]; ok && m != nil {
		act.models = m
		// 保留用户原选的 reasoningEffort：agent 在 load 时把持久化的
		// 模型 id 映射到当前 catalog 键（如 deepseek-v4-flash →
		// deepseek-v4-flash-go），响应 models 缺省 effort 时用缓存值
		// 补上，避免前端回落到新模型的默认档（如 low）静默覆盖用户
		// 原选的 max。同一 map 同时进 act.models / sessRes / ready 事件。
		if prevEffort != "" {
			if mm, ok := m.(map[string]any); ok {
				if cur, _ := mm["reasoningEffort"].(string); cur == "" {
					mm["reasoningEffort"] = prevEffort
				}
			}
		}
	}
	if modes, ok := sessRes["modes"]; ok && modes != nil {
		act.modes = modes
	}
	if co, ok := sessRes[kConfigOptions]; ok && co != nil {
		act.configOpts = co
	}
	// session/load 响应 `_meta` 原样存下（缺省保留 roster 旧值，与
	// models/modes 的处理一致），ready 事件 / Status 透传。
	if m, ok := sessRes[kMeta]; ok && m != nil {
		act.sessionMeta = m
	}
	b.activeSessionID = sessionID
	b.rememberSessionLocked(sessionID, cwd)
	agentInfo := b.agentInfo
	modes := act.modes
	configOpts := act.configOpts
	models := act.models
	sessionMeta := act.sessionMeta
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := act.Cwd
	b.mu.Unlock()

	if sessRes == nil {
		sessRes = map[string]any{}
	}
	// Cold load is never mid-turn.
	sessRes["busy"] = false

	ev := Event{
		kType:          "ready",
		kSessionID:     sessionID,
		"cwd":          sessCwd,
		"agentInfo":    agentInfo,
		"modes":        modes,
		kConfigOptions: configOpts,
		"models":       models,
		"hostId":       hostID,
		"hostName":     hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
	// Replay stream is complete once session/load returns — peers can
	// rebuild from HTTP history now (initiator already does via its own
	// loadHistory; this flag is for multi-tab viewers of the same sid).
	if n := b.replayDropped.Swap(0); n > 0 {
		log.Printf("[Capri-host] session/load %s：拦下 %d 条重放事件（历史走 HTTP，不上总线）", sessionID[:min(8, len(sessionID))], n)
	}
	b.Broadcast(Event{
		kType:      "session_load_finished",
		kSessionID: sessionID,
		"cwd":      sessCwd,
		"ok":       true,
	})
	b.broadcastRosterChange()
	return sessRes, nil
}

// ResumeSession switches the active session to a paused one (session/resume),
// mirroring LoadSession: the resume response's models/modes/configOptions
// win over the roster cache, the ready event is broadcast in the same shape,
// and a busy in-flight target takes the same focus-only path (re-focus +
// re-announce busy, no agent call — session/resume would conflict with the
// running turn).
//
// An optional meta map (client-supplied seeds, e.g. permission-mode flags)
// is forwarded as the session/resume params `_meta` — the LoadSession
// analog. Omitted = current behavior exactly (no `_meta` key on the wire;
// additionalDirectories stays []).
func (b *Bridge) ResumeSession(ctx context.Context, sessionID, cwd string, meta ...map[string]any) (map[string]any, error) {
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil && s.Busy {
		// Focus-only path for an in-flight session (mirror LoadSession).
		if cwd != "" {
			s.Cwd = cwd
		}
		b.activeSessionID = sessionID
		b.rememberSessionLocked(sessionID, s.Cwd)
		b.ready = true
		b.bootError = ""
		sessRes := map[string]any{"busy": true}
		if s.models != nil {
			sessRes["models"] = s.models
		}
		if s.modes != nil {
			sessRes["modes"] = s.modes
		}
		if s.configOpts != nil {
			sessRes[kConfigOptions] = s.configOpts
		}
		agentInfo := b.agentInfo
		modes := s.modes
		configOpts := s.configOpts
		models := s.models
		sessionMeta := s.sessionMeta
		authMeta := b.authMeta
		hostID := b.hostID
		hostName := b.hostName
		sessCwd := s.Cwd
		b.mu.Unlock()

		ev := Event{
			kType:          "ready",
			kSessionID:     sessionID,
			"cwd":          sessCwd,
			"agentInfo":    agentInfo,
			"modes":        modes,
			kConfigOptions: configOpts,
			"models":       models,
			"hostId":       hostID,
			"hostName":     hostName,
		}
		if sessionMeta != nil {
			ev["sessionMeta"] = sessionMeta
		}
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
		// Re-announce busy so the client can attach the spinner after history load.
		b.Broadcast(Event{kType: "busy", kSessionID: sessionID})
		b.broadcastRosterChange()
		return sessRes, nil
	}
	b.mu.Unlock()

	// Same effort-preservation rule as LoadSession: capture the cache
	// up-front (the remap broadcast can race in during the request), then
	// fill a missing reasoningEffort in the resume response models with
	// the session's last-known value.
	prevEffort := ""
	b.mu.Lock()
	if s := b.sessions[sessionID]; s != nil {
		if m, ok := s.models.(map[string]any); ok {
			if e, ok := m["reasoningEffort"].(string); ok {
				prevEffort = e
			}
		}
	}
	b.mu.Unlock()

	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	params := map[string]any{
		kSessionID:              sessionID,
		"cwd":                   cwd,
		"mcpServers":            []any{},
		"additionalDirectories": []any{},
	}
	if len(meta) > 0 && len(meta[0]) > 0 {
		params[kMeta] = withUserMessageEcho(meta[0])
	} else {
		// 同 session/load：回显开关必须随会话请求带上。
		params[kMeta] = withUserMessageEcho(nil)
	}
	sessRes, err := b.request(ctx, "session/resume", params, bootTimeout)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	act := b.sessions[sessionID]
	if act == nil {
		act = &SessionState{SessionID: sessionID, CreatedAt: time.Now().UnixMilli()}
		b.sessions[sessionID] = act
	}
	act.Cwd = cwd
	act.unloaded = false
	b.ready = true
	b.bootError = ""
	// Prefer fields from the resume response — they reflect the session.
	if m, ok := sessRes["models"]; ok && m != nil {
		act.models = m
		// 同 LoadSession：响应 models 缺 reasoningEffort 时用会话已知
		// 档位补上（agent 模型 id 映射场景），避免静默重置为用户未
		// 选过的默认档。
		if prevEffort != "" {
			if mm, ok := m.(map[string]any); ok {
				if cur, _ := mm["reasoningEffort"].(string); cur == "" {
					mm["reasoningEffort"] = prevEffort
				}
			}
		}
	}
	if modes, ok := sessRes["modes"]; ok && modes != nil {
		act.modes = modes
	}
	if co, ok := sessRes[kConfigOptions]; ok && co != nil {
		act.configOpts = co
	}
	// session/resume 响应 `_meta` 原样存下（缺省保留 roster 旧值），
	// ready 事件 / Status 透传。
	if m, ok := sessRes[kMeta]; ok && m != nil {
		act.sessionMeta = m
	}
	b.activeSessionID = sessionID
	b.rememberSessionLocked(sessionID, cwd)
	agentInfo := b.agentInfo
	modes := act.modes
	configOpts := act.configOpts
	models := act.models
	sessionMeta := act.sessionMeta
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	sessCwd := act.Cwd
	b.mu.Unlock()

	if sessRes == nil {
		sessRes = map[string]any{}
	}
	// A resumed session is never mid-turn.
	sessRes["busy"] = false

	ev := Event{
		kType:          "ready",
		kSessionID:     sessionID,
		"cwd":          sessCwd,
		"agentInfo":    agentInfo,
		"modes":        modes,
		kConfigOptions: configOpts,
		"models":       models,
		"hostId":       hostID,
		"hostName":     hostName,
	}
	if sessionMeta != nil {
		ev["sessionMeta"] = sessionMeta
	}
	if authMeta != nil {
		ev["authMeta"] = authMeta
	}
	b.Broadcast(ev)
	b.broadcastRosterChange()
	return sessRes, nil
}

// CloseSession is the explicit client close: session/close then drop the
// row from the live roster (forgetSessionLocked). Disk history is left
// intact. Idle-unload (SweepIdleUnload) is a different path — it also
// calls session/close but keeps the roster row so FE can still list and
// later session/load the conversation.
func (b *Bridge) CloseSession(ctx context.Context, sessionID string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	act := b.activeSessionLocked()
	if sessionID == "" && act != nil {
		sessionID = act.SessionID
	}
	b.mu.Unlock()
	if sessionID == "" {
		return nil, &HTTPError{Code: 404, Msg: "暂无活动会话"}
	}
	sessRes, err := b.request(ctx, "session/close", map[string]any{
		kSessionID: sessionID,
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	// 与 SessionDelete 同一套 per-session 清理：名册、队列快照、用量
	// 去重缓存、生成速率分桶与 last-session 指针。
	b.forgetSessionLocked(sessionID)
	b.mu.Unlock()
	b.broadcastRosterChange()
	return sessRes, nil
}

// forgetSessionLocked drops every per-session index entry for sid: the
// roster row, the observed-turn entry, queue snapshot, usage dedup cache,
// live tool resolver and gen-rate bucket, plus the active-session /
// last-session pointers when they pointed at it. The caller holds b.mu.
func (b *Bridge) forgetSessionLocked(sessionID string) {
	delete(b.sessions, sessionID)
	if b.turns != nil {
		delete(b.turns, sessionID)
	}
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
	if b.activeSessionID == sessionID {
		b.activeSessionID = ""
	}
	if b.lastSessionID == sessionID {
		b.lastSessionID = ""
		b.lastSessionCwd = ""
	}
}
