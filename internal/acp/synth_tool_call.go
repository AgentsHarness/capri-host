package acp

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────
// synth_tool_call.go — 缺失 toolCallId 的确定性合成与注入
//
// 部分 LLM（或兼容 OpenAI 格式的第三方网关）返回的工具调用缺失 call_id，
// agent 原样写入 updates.jsonl 时 toolCallId 为空。FE 全量补全、toolIndex、
// live↔快照去重都按 toolCallId 工作，空 ID 会让正文串台或行一直 Running。
//
// 身份在 host 赋一次，三条出口共用同一套 ID：
//   synth:call:<agentTimestampMs>:<k>   k = 该毫秒内第几个匿名 start（0 起）
//   synth:call:e:<eventId>              无时间戳时退到信封 eventId
//   synth:call:h:<fnv64>[-n]            透传页既无时间戳也无 eventId 的兜底
//
// 匹配纪律：每一档只在候选唯一时才认；多候选落到下一档更强信号，绝不
// first-match。有 ID 的调用不进匿名 open 集。歧义时宁可不打 ID，也不把
// A 的正文贴到 B 上。
// ─────────────────────────────────────────────────────────────────────

const synthLineArrow = "→"

// updateWirePayload 用于扫描 JSONL 时提取信封中的轻量字段。
type updateWirePayload struct {
	SessionUpdate     string          `json:"sessionUpdate"`
	ToolCallId        string          `json:"toolCallId"`
	TargetPromptIndex *int64          `json:"target_prompt_index"`
	Kind              string          `json:"kind"`
	Status            string          `json:"status"`
	Title             string          `json:"title"`
	RawInput          json.RawMessage `json:"rawInput"`
	RawOutput         json.RawMessage `json:"rawOutput"`
	Content           json.RawMessage `json:"content"`
	Meta              *struct {
		PromptIndex *int64          `json:"promptIndex"`
		HostTurn    bool            `json:"hostTurn"`
		Tool        *map[string]any `json:"x.ai/tool"`
	} `json:"_meta"`
}

// toolLineMeta 记录一行工具信封在扫描阶段提取的时序与特征元数据。
type toolLineMeta struct {
	sessionUpdate string // "tool_call" | "tool_call_update"
	toolCallID    string
	name          string
	kind          string
	status        string
	title         string

	command   string
	path      string
	offset    int
	hasOffset bool
	url       string
	query     string
	taskID    string

	outputType    string
	outputCmd     string
	contentPrefix string
	errorMsg      string
	contentText   string
}

func parseToolLineMetaFromWire(up *updateWirePayload) *toolLineMeta {
	su := up.SessionUpdate
	if su != "tool_call" && su != "tool_call_update" {
		return nil
	}
	tm := &toolLineMeta{
		sessionUpdate: su,
		toolCallID:    strings.TrimSpace(up.ToolCallId),
		kind:          strings.ToLower(strings.TrimSpace(up.Kind)),
		status:        strings.ToLower(strings.TrimSpace(up.Status)),
		title:         strings.TrimSpace(up.Title),
	}
	fillToolNameKind(tm, su)
	if up.Meta != nil && up.Meta.Tool != nil {
		applyXAITool(*up.Meta.Tool, tm)
	}
	if len(up.RawInput) > 0 {
		var ri map[string]any
		if json.Unmarshal(up.RawInput, &ri) == nil {
			parseRawInputFingerprint(ri, tm)
		}
	}
	if len(up.RawOutput) > 0 {
		var ro map[string]any
		if json.Unmarshal(up.RawOutput, &ro) == nil {
			parseRawOutputFeatures(ro, tm)
		}
	}
	parseContentRaw(up.Content, tm)
	return tm
}

func parseToolLineMeta(upd map[string]any) *toolLineMeta {
	if upd == nil {
		return nil
	}
	su, _ := upd["sessionUpdate"].(string)
	if su != "tool_call" && su != "tool_call_update" {
		return nil
	}
	tm := &toolLineMeta{sessionUpdate: su}
	if tid, ok := upd["toolCallId"].(string); ok {
		tm.toolCallID = strings.TrimSpace(tid)
	}
	if kind, ok := upd["kind"].(string); ok {
		tm.kind = strings.ToLower(strings.TrimSpace(kind))
	}
	if status, ok := upd["status"].(string); ok {
		tm.status = strings.ToLower(strings.TrimSpace(status))
	}
	if title, ok := upd["title"].(string); ok {
		tm.title = strings.TrimSpace(title)
	}
	fillToolNameKind(tm, su)
	if meta, ok := upd["_meta"].(map[string]any); ok {
		if xai, ok := meta["x.ai/tool"].(map[string]any); ok {
			applyXAITool(xai, tm)
		}
	}
	if ri, ok := upd["rawInput"].(map[string]any); ok {
		parseRawInputFingerprint(ri, tm)
	}
	if ro, ok := upd["rawOutput"].(map[string]any); ok {
		parseRawOutputFeatures(ro, tm)
	}
	switch c := upd["content"].(type) {
	case []any:
		parseContentText(c, tm)
	case map[string]any:
		parseContentText([]any{c}, tm)
	}
	return tm
}

func fillToolNameKind(tm *toolLineMeta, su string) {
	if su == "tool_call" && tm.title != "" {
		tm.name = tm.title
	}
}

func applyXAITool(xai map[string]any, tm *toolLineMeta) {
	if n, ok := xai["name"].(string); ok && n != "" {
		tm.name = n
	}
	if tm.kind == "" {
		if k, ok := xai["kind"].(string); ok && k != "" {
			tm.kind = strings.ToLower(strings.TrimSpace(k))
		}
	}
}

func parseRawInputFingerprint(ri map[string]any, tm *toolLineMeta) {
	if cmd, ok := ri["command"].(string); ok && cmd != "" {
		tm.command = cmd
	}
	for _, pKey := range []string{"target_file", "path", "filePath", "file_path", "targetFile"} {
		if p, ok := ri[pKey].(string); ok && p != "" {
			tm.path = p
			break
		}
	}
	if n, ok := asInt(ri["offset"]); ok {
		tm.offset = int(n)
		tm.hasOffset = true
	}
	if u, ok := ri["url"].(string); ok && u != "" {
		tm.url = u
	}
	if q, ok := ri["query"].(string); ok && q != "" {
		tm.query = q
	}
	for _, tKey := range []string{"task_id", "taskId"} {
		if tid, ok := ri[tKey].(string); ok && tid != "" {
			tm.taskID = tid
			break
		}
	}
	if tm.taskID == "" {
		for _, tKey := range []string{"task_ids", "taskIds"} {
			if arr, ok := ri[tKey].([]any); ok && len(arr) > 0 {
				if first, ok := arr[0].(string); ok && first != "" {
					tm.taskID = first
					break
				}
			}
		}
	}
}

func parseRawOutputFeatures(ro map[string]any, tm *toolLineMeta) {
	if t, ok := ro["type"].(string); ok && t != "" {
		tm.outputType = t
	}
	if cmd, ok := ro["command"].(string); ok && cmd != "" {
		tm.outputCmd = cmd
	}
	if errMsg, ok := ro["message"].(string); ok && errMsg != "" {
		tm.errorMsg = errMsg
	}
	if fc, ok := ro["FileContent"].(map[string]any); ok {
		if cnt, ok := fc["content"].(string); ok && cnt != "" {
			tm.contentPrefix = clipPrefix(cnt, 80)
		}
	}
}

func parseContentRaw(raw json.RawMessage, tm *toolLineMeta) {
	if len(raw) == 0 {
		return
	}
	var arr []any
	if json.Unmarshal(raw, &arr) == nil {
		parseContentText(arr, tm)
		return
	}
	var one any
	if json.Unmarshal(raw, &one) == nil {
		parseContentText([]any{one}, tm)
	}
}

func parseContentText(content []any, tm *toolLineMeta) {
	var sb strings.Builder
	for _, item := range content {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := m["text"].(string); ok && t != "" {
			sb.WriteString(t)
			sb.WriteString(" ")
		}
		if nested, ok := m["content"].(map[string]any); ok {
			if t, ok := nested["text"].(string); ok && t != "" {
				sb.WriteString(t)
				sb.WriteString(" ")
			}
		}
		if sb.Len() > 500 {
			break
		}
	}
	tm.contentText = sb.String()
	if tm.contentPrefix == "" && tm.contentText != "" {
		tm.contentPrefix = clipPrefix(tm.contentText, 80)
	}
}

func clipPrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := s[:n]
	for len(cut) > 0 && cut[len(cut)-1]&0xc0 == 0x80 {
		cut = cut[:len(cut)-1]
	}
	return cut
}

// openCall 是当前回合里尚未收口的匿名工具调用。
type openCall struct {
	id   string
	tsMs int64
	meta toolLineMeta
}

type synthWalk struct {
	ids       map[int]string
	openCalls []openCall
	tsCount   map[int64]int
}

func walkSyntheticToolCalls(order []int, lines []updateLineMeta) synthWalk {
	w := synthWalk{
		ids:     make(map[int]string),
		tsCount: make(map[int64]int),
	}
	for seq, lineIdx := range order {
		if lineIdx < 0 || lineIdx >= len(lines) {
			continue
		}
		m := &lines[lineIdx]
		if m.isTurnCompleted {
			w.openCalls = nil
			continue
		}
		if m.tool == nil {
			continue
		}
		w.observe(seq, m.agentTsMs, "", m.tool)
	}
	return w
}

func (w *synthWalk) observe(seq int, tsMs int64, eventID string, tm *toolLineMeta) {
	if tm.sessionUpdate == "tool_call" {
		if tm.toolCallID != "" {
			return
		}
		id := assignStartID(w.tsCount, tsMs, eventID, nil)
		if id == "" {
			id = formatSynthCallID(tsMs, w.tsCount[tsMs])
			w.tsCount[tsMs]++
		}
		w.ids[seq] = id
		w.openCalls = append(w.openCalls, openCall{id: id, tsMs: tsMs, meta: *tm})
		return
	}
	if tm.sessionUpdate != "tool_call_update" {
		return
	}
	if tm.toolCallID != "" {
		return
	}
	matchIdx := matchUpdateToCall(tm, w.openCalls)
	if matchIdx < 0 {
		return
	}
	w.ids[seq] = w.openCalls[matchIdx].id
	if isTerminalStatus(tm.status) {
		w.openCalls = append(w.openCalls[:matchIdx], w.openCalls[matchIdx+1:]...)
	}
}

func resolveSyntheticToolCallIDs(order []int, lines []updateLineMeta) map[int]string {
	return walkSyntheticToolCalls(order, lines).ids
}

func formatSynthCallID(tsMs int64, k int) string {
	return fmt.Sprintf("synth:call:%d:%d", tsMs, k)
}

func assignStartID(tsCount map[int64]int, tsMs int64, eventID string, na *int) string {
	if tsMs > 0 {
		k := tsCount[tsMs]
		tsCount[tsMs] = k + 1
		return formatSynthCallID(tsMs, k)
	}
	if eventID != "" {
		return "synth:call:e:" + eventID
	}
	if na != nil {
		*na++
		return fmt.Sprintf("synth:call:na:%d", *na)
	}
	return ""
}

func isTerminalStatus(status string) bool {
	return status == "completed" || status == "failed"
}

func hasFingerprint(u *toolLineMeta) bool {
	return u.command != "" || u.path != "" || u.url != "" || u.query != "" || u.taskID != ""
}

func fingerprintMatch(u, sig *toolLineMeta) bool {
	matched := false
	if u.command != "" {
		matched = true
		if sig.command != u.command {
			return false
		}
	}
	if u.path != "" {
		matched = true
		if sig.path != u.path {
			return false
		}
		if u.hasOffset && (!sig.hasOffset || sig.offset != u.offset) {
			return false
		}
	}
	if u.url != "" {
		matched = true
		if sig.url != u.url {
			return false
		}
	}
	if u.query != "" {
		matched = true
		if sig.query != u.query {
			return false
		}
	}
	if u.taskID != "" {
		matched = true
		if sig.taskID != u.taskID {
			return false
		}
	}
	return matched
}

func sameStartIdentity(a, b *toolLineMeta) bool {
	if a.command != b.command {
		return false
	}
	if a.path != b.path || a.hasOffset != b.hasOffset || a.offset != b.offset {
		return false
	}
	if a.url != b.url || a.query != b.query || a.taskID != b.taskID {
		return false
	}
	if a.name != "" && b.name != "" && a.name != b.name {
		return false
	}
	return true
}

func filterOpen(open []openCall, pred func(*openCall) bool) []int {
	var idx []int
	for i := range open {
		if pred(&open[i]) {
			idx = append(idx, i)
		}
	}
	return idx
}

func uniqueOpen(open []openCall, pred func(*openCall) bool) int {
	hits := filterOpen(open, pred)
	if len(hits) == 1 {
		return hits[0]
	}
	return -1
}

func parseLeadingLineNum(s string) (int, bool) {
	s = strings.TrimLeft(s, " \t\r\n")
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 || !strings.HasPrefix(s[i:], synthLineArrow) {
		return 0, false
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil {
		return 0, false
	}
	return n, true
}

func readLineNum(u *toolLineMeta) (int, bool) {
	if n, ok := parseLeadingLineNum(u.contentPrefix); ok {
		return n, true
	}
	return parseLeadingLineNum(u.contentText)
}

func familyNamesForOutput(outputType string) []string {
	switch outputType {
	case "Bash":
		return []string{"run_terminal_command"}
	case "ReadFile":
		return []string{"read_file"}
	case "WebSearch":
		return []string{"web_search"}
	case "SearchTool":
		return []string{"search_tool"}
	case "TaskOutput":
		return []string{"get_command_or_subagent_output"}
	case "TodosUpdated", "Todo":
		return []string{"todo_write"}
	default:
		return nil
	}
}

func familyNamesForKind(kind string) []string {
	switch kind {
	case "execute":
		return []string{"run_terminal_command"}
	case "read":
		return []string{"read_file"}
	case "fetch":
		return []string{"web_fetch"}
	case "think":
		return []string{"todo_write"}
	case "other":
		return []string{"search_tool", "get_command_or_subagent_output"}
	default:
		return nil
	}
}

func nameIn(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// familyCompatible：update 带了族信号时，文本匹配不得跨族（bash 日志里
// 出现 read_file 的路径，不能把这条完成帧收口到那次 read 上）。
func familyCompatible(u *toolLineMeta, name string) bool {
	if names := familyNamesForOutput(u.outputType); len(names) > 0 {
		return nameIn(names, name)
	}
	if names := familyNamesForKind(u.kind); len(names) > 0 {
		return nameIn(names, name)
	}
	return true
}

// matchUpdateToCall 将一条匿名 tool_call_update 关联到开放的匿名 start。
// 每一档只在候选唯一时命中；多候选落到下一档，绝不 first-match。
func matchUpdateToCall(u *toolLineMeta, open []openCall) int {
	if len(open) == 0 {
		return -1
	}

	if hasFingerprint(u) {
		if i := uniqueOpen(open, func(c *openCall) bool { return fingerprintMatch(u, &c.meta) }); i >= 0 {
			return i
		}
	}
	if u.outputCmd != "" {
		if i := uniqueOpen(open, func(c *openCall) bool { return c.meta.command == u.outputCmd }); i >= 0 {
			return i
		}
	}
	if line, ok := readLineNum(u); ok {
		if i := uniqueOpen(open, func(c *openCall) bool {
			if c.meta.name != "read_file" {
				return false
			}
			if c.meta.hasOffset {
				return c.meta.offset == line
			}
			return line == 1
		}); i >= 0 {
			return i
		}
	}
	if names := familyNamesForOutput(u.outputType); len(names) > 0 {
		if i := uniqueOpen(open, func(c *openCall) bool { return nameIn(names, c.meta.name) }); i >= 0 {
			return i
		}
	}

	txt := u.title + "\n" + u.contentText
	if strings.TrimSpace(txt) != "" && txt != "\n" {
		inFamily := func(c *openCall) bool { return familyCompatible(u, c.meta.name) }
		if i := uniqueOpen(open, func(c *openCall) bool {
			return inFamily(c) && c.meta.taskID != "" && strings.Contains(txt, c.meta.taskID)
		}); i >= 0 {
			return i
		}
		if i := uniqueOpen(open, func(c *openCall) bool {
			return inFamily(c) && c.meta.path != "" && strings.Contains(txt, c.meta.path)
		}); i >= 0 {
			return i
		}
		if i := uniqueOpen(open, func(c *openCall) bool {
			return inFamily(c) && c.meta.url != "" && strings.Contains(txt, c.meta.url)
		}); i >= 0 {
			return i
		}
		if i := uniqueOpen(open, func(c *openCall) bool {
			if !inFamily(c) || c.meta.name == "" {
				return false
			}
			return strings.Contains(txt, fmt.Sprintf("Tool `%s` failed", c.meta.name))
		}); i >= 0 {
			return i
		}
	}

	if names := familyNamesForKind(u.kind); len(names) > 0 {
		if i := uniqueOpen(open, func(c *openCall) bool { return nameIn(names, c.meta.name) }); i >= 0 {
			return i
		}
	}

	if len(open) == 1 && isTerminalStatus(u.status) {
		if !hasFingerprint(u) || fingerprintMatch(u, &open[0].meta) {
			return 0
		}
	}
	return -1
}

func injectSynthToolCallID(obj map[string]any, synthID string) {
	if synthID == "" {
		return
	}
	params, ok := obj[kParams].(map[string]any)
	if !ok {
		return
	}
	if upd, ok := params[kUpdate].(map[string]any); ok {
		if cur, _ := upd["toolCallId"].(string); strings.TrimSpace(cur) == "" {
			upd["toolCallId"] = synthID
		}
	}
	if meta, ok := params[kMeta].(map[string]any); ok {
		if upParams, ok := meta["updateParams"].(map[string]any); ok {
			if cur, _ := upParams["toolCallId"].(string); strings.TrimSpace(cur) == "" {
				upParams["toolCallId"] = synthID
			}
		}
	}
}

func envelopeSynthMeta(obj map[string]any) (ts int64, eventID string) {
	params, _ := obj[kParams].(map[string]any)
	return paramsSynthMeta(params)
}

func paramsSynthMeta(params map[string]any) (ts int64, eventID string) {
	if params == nil {
		return 0, ""
	}
	meta, _ := params[kMeta].(map[string]any)
	if meta == nil {
		return 0, ""
	}
	if n, ok := asInt(meta["agentTimestampMs"]); ok {
		ts = n
	}
	if s, ok := meta["eventId"].(string); ok {
		eventID = strings.TrimSpace(s)
	}
	return ts, eventID
}

func hashSynthFallback(obj map[string]any) string {
	payload := any(obj)
	if params, ok := obj[kParams].(map[string]any); ok {
		payload = map[string]any{
			"update": params[kUpdate],
			"ts":     obj["timestamp"],
		}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "0"
	}
	h := fnv.New64a()
	_, _ = h.Write(b)
	return fmt.Sprintf("%x", h.Sum64())
}

// normalizeSyntheticToolCallsInSlice 透传回退路径就地修复一页信封。
// 有 agentTimestampMs 时与历史主路径同一套 synth:call:<ts>:<k>，跨页不撞
// 页内下标；否则用 eventId / 信封哈希，避免两页都叫 synth:call:0。
func normalizeSyntheticToolCallsInSlice(updates []any) {
	if len(updates) == 0 {
		return
	}
	var open []openCall
	tsCount := make(map[int64]int)
	seenHash := make(map[string]int)

	for _, raw := range updates {
		obj, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		params, ok := obj[kParams].(map[string]any)
		if !ok {
			continue
		}
		upd, ok := params[kUpdate].(map[string]any)
		if !ok {
			continue
		}
		su, _ := upd["sessionUpdate"].(string)
		if su == "turn_completed" || su == "response_completed" {
			open = nil
			continue
		}
		tm := parseToolLineMeta(upd)
		if tm == nil {
			continue
		}
		ts, eventID := envelopeSynthMeta(obj)

		if tm.sessionUpdate == "tool_call" {
			if tm.toolCallID != "" {
				continue
			}
			id := assignStartID(tsCount, ts, eventID, nil)
			if id == "" {
				base := "synth:call:h:" + hashSynthFallback(obj)
				n := seenHash[base]
				seenHash[base] = n + 1
				id = base
				if n > 0 {
					id = fmt.Sprintf("%s-%d", base, n)
				}
			}
			injectSynthToolCallID(obj, id)
			open = append(open, openCall{id: id, tsMs: ts, meta: *tm})
			continue
		}
		if tm.sessionUpdate != "tool_call_update" || tm.toolCallID != "" {
			continue
		}
		matchIdx := matchUpdateToCall(tm, open)
		if matchIdx < 0 {
			continue
		}
		injectSynthToolCallID(obj, open[matchIdx].id)
		if isTerminalStatus(tm.status) {
			open = append(open[:matchIdx], open[matchIdx+1:]...)
		}
	}
}

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
	if sessionUpdate == "turn_completed" || sessionUpdate == "response_completed" {
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
