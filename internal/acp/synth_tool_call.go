package acp

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────
// synth_tool_call.go — 缺失 toolCallId 的确定性合成与注入
//
// 契约背景：
// 部分 LLM（或兼容 OpenAI 格式的第三方网关）返回的工具调用中缺失 call_id，
// agent 原样写入 updates.jsonl 时 toolCallId 为空字符串 ""。这破坏了 FE
// 全量补全（契约 [E]）按 toolCallId 回填正文的基石，退化为脆弱的匿名猜谜，
// 在并发同族工具调用（如同时 read 多个文件或并发 web_fetch）时导致正文提取
// 歧义丢弃、坐标窗口兜底失效、乱序串台、去重键错位等一系列严重缺陷。
//
// 本模块在 host 历史归一化层提供确定性的合成 toolCallId 注入：
// 1. 在扫描会话 updates.jsonl 构建归一化视图时提取工具时序特征；
// 2. 遍历归一化序为缺失 ID 的 tool_call 分配全局稳定唯一的 "synth:call:<msgSeq>"；
// 3. 基于参数指纹、输出特征、报错文本、工具族与单候选收敛算法，精确关联后续
//    所有的 tool_call_update 并打上相同 ID；
// 4. 分页读取及 RPC 透传输出信封时就地注入，存量历史与新增会话均透明受益。
// ─────────────────────────────────────────────────────────────────────

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
	name          string // 原始工具名，如 run_terminal_command, read_file, web_fetch
	kind          string // execute, read, fetch, search, think, other
	status        string // pending, in_progress, completed, failed
	title         string

	// 参数指纹
	command   string
	path      string
	offset    int
	hasOffset bool
	url       string
	query     string
	taskID    string

	// 输出特征
	outputType    string // Bash, ReadFile, WebSearch, SearchTool, TaskOutput, TodosUpdated
	outputCmd     string
	contentPrefix string // ReadFile content 头部（用于匹配行号锚点）
	errorMsg      string
	contentText   string // content 提取的文本摘要
}

// parseToolLineMetaFromWire 从 wire payload 提取轻量工具元数据。
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

	if su == "tool_call" && tm.title != "" {
		tm.name = tm.title
	}
	if up.Meta != nil && up.Meta.Tool != nil {
		xai := *up.Meta.Tool
		if n, ok := xai["name"].(string); ok && n != "" {
			tm.name = n
		}
		if tm.kind == "" {
			if k, ok := xai["kind"].(string); ok && k != "" {
				tm.kind = strings.ToLower(strings.TrimSpace(k))
			}
		}
	}

	if len(up.RawInput) > 0 {
		var ri struct {
			Command    string `json:"command"`
			TargetFile string `json:"target_file"`
			Path       string `json:"path"`
			FilePath   string `json:"file_path"`
			Offset     *int   `json:"offset"`
			URL        string `json:"url"`
			Query      string `json:"query"`
			TaskID     any    `json:"task_id"`
			TaskIDs    any    `json:"task_ids"`
		}
		if json.Unmarshal(up.RawInput, &ri) == nil {
			if ri.Command != "" {
				tm.command = ri.Command
			}
			for _, p := range []string{ri.TargetFile, ri.Path, ri.FilePath} {
				if p != "" {
					tm.path = p
					break
				}
			}
			if ri.Offset != nil {
				tm.offset = *ri.Offset
				tm.hasOffset = true
			}
			if ri.URL != "" {
				tm.url = ri.URL
			}
			if ri.Query != "" {
				tm.query = ri.Query
			}
			if s, ok := ri.TaskID.(string); ok && s != "" {
				tm.taskID = s
			} else if arr, ok := ri.TaskIDs.([]any); ok && len(arr) > 0 {
				if s, ok := arr[0].(string); ok && s != "" {
					tm.taskID = s
				}
			}
		}
	}

	if len(up.RawOutput) > 0 {
		var ro struct {
			Type        string `json:"type"`
			Command     string `json:"command"`
			Message     string `json:"message"`
			FileContent *struct {
				Content string `json:"content"`
			} `json:"FileContent"`
		}
		if json.Unmarshal(up.RawOutput, &ro) == nil {
			if ro.Type != "" {
				tm.outputType = ro.Type
			}
			if ro.Command != "" {
				tm.outputCmd = ro.Command
			}
			if ro.Message != "" {
				tm.errorMsg = ro.Message
			}
			if ro.FileContent != nil && ro.FileContent.Content != "" {
				if len(ro.FileContent.Content) > 80 {
					tm.contentPrefix = ro.FileContent.Content[:80]
				} else {
					tm.contentPrefix = ro.FileContent.Content
				}
			}
		}
	}

	if len(up.Content) > 0 {
		var arr []any
		if json.Unmarshal(up.Content, &arr) == nil {
			parseContentText(arr, tm)
		}
	}

	return tm
}

// parseToolLineMeta 从信封的 map[string]any 提取轻量工具元数据（用于 RPC 透传切片处理）。
func parseToolLineMeta(upd map[string]any) *toolLineMeta {
	su, _ := upd["sessionUpdate"].(string)
	if su != "tool_call" && su != "tool_call_update" {
		return nil
	}
	tm := &toolLineMeta{
		sessionUpdate: su,
	}
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

	if su == "tool_call" && tm.title != "" {
		tm.name = tm.title
	}
	if meta, ok := upd["_meta"].(map[string]any); ok {
		if xai, ok := meta["x.ai/tool"].(map[string]any); ok {
			if n, ok := xai["name"].(string); ok && n != "" {
				tm.name = n
			}
			if tm.kind == "" {
				if k, ok := xai["kind"].(string); ok && k != "" {
					tm.kind = strings.ToLower(strings.TrimSpace(k))
				}
			}
		}
	}

	if ri, ok := upd["rawInput"].(map[string]any); ok {
		parseRawInputFingerprint(ri, tm)
	}
	if ro, ok := upd["rawOutput"].(map[string]any); ok {
		parseRawOutputFeatures(ro, tm)
	}
	if content, ok := upd["content"].([]any); ok {
		parseContentText(content, tm)
	}

	return tm
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
	if off, ok := ri["offset"].(float64); ok {
		tm.offset = int(off)
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
			if len(cnt) > 80 {
				tm.contentPrefix = cnt[:80]
			} else {
				tm.contentPrefix = cnt
			}
		}
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
}

// openCall 代表当前 Turn 内尚未收口的工具调用。
type openCall struct {
	msgSeq int
	id     string
	meta   *toolLineMeta
}

// resolveSyntheticToolCallIDs 遍历会话归一化时序，建立 msgSeq → 合成 toolCallId 映射。
func resolveSyntheticToolCallIDs(order []int, lines []updateLineMeta) map[int]string {
	synthIDs := make(map[int]string)
	var openCalls []openCall

	for seq, lineIdx := range order {
		m := &lines[lineIdx]
		if m.isTurnCompleted {
			openCalls = nil
			continue
		}
		if m.tool == nil {
			continue
		}
		tm := m.tool
		if tm.sessionUpdate == "tool_call" {
			synthID := tm.toolCallID
			if synthID == "" {
				synthID = fmt.Sprintf("synth:call:%d", seq)
				synthIDs[seq] = synthID
			}
			openCalls = append(openCalls, openCall{
				msgSeq: seq,
				id:     synthID,
				meta:   tm,
			})
		} else if tm.sessionUpdate == "tool_call_update" {
			if tm.toolCallID != "" {
				continue
			}
			matchIdx := matchUpdateToCall(tm, openCalls)
			if matchIdx >= 0 {
				matched := openCalls[matchIdx]
				synthIDs[seq] = matched.id
				if tm.status == "completed" || tm.status == "failed" {
					openCalls = append(openCalls[:matchIdx], openCalls[matchIdx+1:]...)
				}
			}
		}
	}
	return synthIDs
}

// matchUpdateToCall 将一条 tool_call_update 确定性关联到当前的开放调用。
func matchUpdateToCall(u *toolLineMeta, openCalls []openCall) int {
	if len(openCalls) == 0 {
		return -1
	}

	// 1. 参数指纹精准匹配
	if u.command != "" || u.path != "" || u.url != "" || u.query != "" || u.taskID != "" {
		for i, c := range openCalls {
			sig := c.meta
			if u.command != "" && sig.command == u.command {
				return i
			}
			if u.path != "" && sig.path == u.path {
				return i
			}
			if u.url != "" && sig.url == u.url {
				return i
			}
			if u.query != "" && sig.query == u.query {
				return i
			}
			if u.taskID != "" && sig.taskID == u.taskID {
				return i
			}
		}
	}

	// 2. 输出特征匹配
	if u.outputCmd != "" {
		for i, c := range openCalls {
			if c.meta.command == u.outputCmd {
				return i
			}
		}
	}
	if u.contentPrefix != "" {
		for i, c := range openCalls {
			if c.meta.name == "read_file" {
				if c.meta.hasOffset && strings.Contains(u.contentPrefix, fmt.Sprintf("%d→", c.meta.offset)) {
					return i
				}
				if !c.meta.hasOffset && strings.Contains(u.contentPrefix, "1→") {
					return i
				}
			}
		}
	}
	if u.outputType != "" {
		switch u.outputType {
		case "Bash":
			cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "run_terminal_command" })
			if len(cands) == 1 {
				return cands[0]
			}
		case "WebSearch":
			cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "web_search" })
			if len(cands) == 1 {
				return cands[0]
			}
		case "SearchTool":
			cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "search_tool" })
			if len(cands) == 1 {
				return cands[0]
			}
		case "TaskOutput":
			cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "get_command_or_subagent_output" })
			if len(cands) == 1 {
				return cands[0]
			}
		case "TodosUpdated", "Todo":
			cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "todo_write" })
			if len(cands) == 1 {
				return cands[0]
			}
		}
	}

	// 3. 文本内容 / 报错文本匹配
	txt := u.title + "\n" + u.contentText
	if txt != "\n" {
		for i, c := range openCalls {
			if c.meta.taskID != "" && strings.Contains(txt, c.meta.taskID) {
				return i
			}
			if c.meta.path != "" && strings.Contains(txt, c.meta.path) {
				return i
			}
			if c.meta.url != "" && strings.Contains(txt, c.meta.url) {
				return i
			}
			if c.meta.name != "" && strings.Contains(txt, fmt.Sprintf("Tool `%s` failed", c.meta.name)) {
				cands := filterOpenCalls(openCalls, func(o *openCall) bool { return o.meta.name == c.meta.name })
				if len(cands) == 1 {
					return cands[0]
				}
			}
		}
	}

	// 4. 工具族单候选归属
	switch u.kind {
	case "execute":
		cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "run_terminal_command" })
		if len(cands) == 1 {
			return cands[0]
		}
	case "read":
		cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "read_file" })
		if len(cands) == 1 {
			return cands[0]
		}
	case "fetch":
		cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "web_fetch" })
		if len(cands) == 1 {
			return cands[0]
		}
	case "think":
		cands := filterOpenCalls(openCalls, func(c *openCall) bool { return c.meta.name == "todo_write" })
		if len(cands) == 1 {
			return cands[0]
		}
	case "other":
		cands := filterOpenCalls(openCalls, func(c *openCall) bool {
			return c.meta.name == "search_tool" || c.meta.name == "get_command_or_subagent_output"
		})
		if len(cands) == 1 {
			return cands[0]
		}
	}

	// 5. 立即失败/无正文完成帧（归属于最近打开的调用）
	if (u.status == "failed" || u.status == "completed") && (u.errorMsg != "" || u.outputType == "") {
		return len(openCalls) - 1
	}

	// 6. 单候选兜底
	if len(openCalls) == 1 {
		return 0
	}

	return -1
}

func filterOpenCalls(openCalls []openCall, predicate func(*openCall) bool) []int {
	var indices []int
	for i := range openCalls {
		if predicate(&openCalls[i]) {
			indices = append(indices, i)
		}
	}
	return indices
}

// injectSynthToolCallID 将合成 ID 注入到信封对象的 update 及 params._meta 中（若原本为空）。
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

// normalizeSyntheticToolCallsInSlice 用于透传回退路径（无全局 msgSeq 视图时）就地修复一页信封中的空 toolCallId。
func normalizeSyntheticToolCallsInSlice(updates []any) {
	if len(updates) == 0 {
		return
	}
	var openCalls []openCall
	callIdx := 0

	for seq, raw := range updates {
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
		if su == "turn_completed" {
			openCalls = nil
			continue
		}
		tm := parseToolLineMeta(upd)
		if tm == nil {
			continue
		}

		if tm.sessionUpdate == "tool_call" {
			synthID := tm.toolCallID
			if synthID == "" {
				callIdx++
				synthID = fmt.Sprintf("synth:call:%d", seq)
				injectSynthToolCallID(obj, synthID)
			}
			openCalls = append(openCalls, openCall{
				msgSeq: seq,
				id:     synthID,
				meta:   tm,
			})
		} else if tm.sessionUpdate == "tool_call_update" {
			if tm.toolCallID != "" {
				continue
			}
			matchIdx := matchUpdateToCall(tm, openCalls)
			if matchIdx >= 0 {
				matched := openCalls[matchIdx]
				injectSynthToolCallID(obj, matched.id)
				if tm.status == "completed" || tm.status == "failed" {
					openCalls = append(openCalls[:matchIdx], openCalls[matchIdx+1:]...)
				}
			}
		}
	}
}

// liveToolResolver 用于在实时 SSE 广播流中为缺少 ID 的工具事件提供实时合成注入。
type liveToolResolver struct {
	counter   int
	openCalls []openCall
}

func (r *liveToolResolver) handleLive(sessionUpdate string, update map[string]any) {
	if sessionUpdate == "turn_completed" {
		r.openCalls = nil
		return
	}
	tm := parseToolLineMeta(update)
	if tm == nil {
		return
	}

	if sessionUpdate == "tool_call" {
		tid, _ := update["toolCallId"].(string)
		if strings.TrimSpace(tid) == "" {
			r.counter++
			synthID := fmt.Sprintf("synth:live:%d", r.counter)
			update["toolCallId"] = synthID
			tm.toolCallID = synthID
			r.openCalls = append(r.openCalls, openCall{
				msgSeq: r.counter,
				id:     synthID,
				meta:   tm,
			})
		}
	} else if sessionUpdate == "tool_call_update" {
		tid, _ := update["toolCallId"].(string)
		if strings.TrimSpace(tid) == "" && len(r.openCalls) > 0 {
			matchIdx := matchUpdateToCall(tm, r.openCalls)
			if matchIdx >= 0 {
				matched := r.openCalls[matchIdx]
				update["toolCallId"] = matched.id
				if tm.status == "completed" || tm.status == "failed" {
					r.openCalls = append(r.openCalls[:matchIdx], r.openCalls[matchIdx+1:]...)
				}
			}
		}
	}
}

