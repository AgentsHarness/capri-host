package acp

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
)

func familyNamesForKind(kind string) []string {
	switch kind {
	case "execute":
		return []string{"run_terminal_command"}
	case "read":
		return []string{"read_file"}
	case "edit", "write":
		return []string{"search_replace", "write"}
	case "search":
		return []string{"grep", "search_tool", "web_search"}
	case "fetch", "web_fetch":
		return []string{"web_fetch"}
	case "web_search":
		return []string{"web_search"}
	case "list":
		return []string{"list_dir"}
	case "think":
		return []string{"todo_write"}
	case "plan", "enter_plan", "exit_plan":
		return []string{"enter_plan_mode", "exit_plan_mode", "todo_write"}
	case "other":
		return []string{"search_tool", "get_command_or_subagent_output", "kill_command_or_subagent"}
	case "ask_user":
		return []string{"ask_user_question"}
	case "workflow":
		return []string{"workflow"}
	case "monitor":
		return []string{"monitor"}
	case "image_gen":
		return []string{"image_gen", "image_edit", "image_to_video", "reference_to_video"}
	case "use_tool":
		return []string{"use_tool"}
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
		if isTerminalStatus(u.status) {
			matching := filterOpen(open, func(c *openCall) bool { return nameIn(names, c.meta.name) })
			if len(matching) > 1 {
				hasIdentical := false
				for j := 1; j < len(matching); j++ {
					if sameStartIdentity(&open[matching[0]].meta, &open[matching[j]].meta) {
						hasIdentical = true
						break
					}
				}
				if !hasIdentical {
					return matching[0]
				}
			}
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
		if su == "turn_completed" {
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
