package server

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// lite_detail_test.go — POST /api/session-updates 的 detail 契约（[A][B]）
// 在 HTTP 层的形状：缺省 / full / 未知值与今天的响应逐字节相等（旧 FE 零风
// 险）；lite 只裁工具正文并回显 projected/omittedBytes；meta 档连 updates
// 键都不回（不是空数组）。

// liteDetailLine 构造一行存储信封（agent 落盘形状：params._meta 带
// agentTimestampMs ⇒ host 走本地分页）。
func liteDetailLine(t *testing.T, sid string, n int, ts int64, update map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"timestamp": ts / 1000,
		"method":    "session/update",
		"params": map[string]any{
			"sessionId": sid,
			"update":    update,
			"_meta": map[string]any{
				"agentTimestampMs": float64(ts),
				"eventId":          "e-" + itoa(int64(n)),
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return string(raw)
}

// liteDetailSession 落一个含大工具正文的会话，返回临时 grok home。
func liteDetailSession(t *testing.T) (home, sid string) {
	t.Helper()
	home = t.TempDir()
	sid = "sess-lite"
	lines := []string{
		liteDetailLine(t, sid, 0, 1000, map[string]any{
			"sessionUpdate": "user_message_chunk",
			"content":       map[string]any{"type": "text", "text": "看下这个文件"},
		}),
		liteDetailLine(t, sid, 1, 1100, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": strings.Repeat("说", 400)},
		}),
		liteDetailLine(t, sid, 2, 1200, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool-1",
			"status":        "completed",
			"kind":          "read",
			"title":         "Read main.go",
			"content": []any{map[string]any{
				"type":    "content",
				"content": map[string]any{"type": "text", "text": strings.Repeat("x", 5000)},
			}},
			"rawOutput": map[string]any{
				"exit_code": float64(0),
				"output":    strings.Repeat("o", 20000),
				"path":      "/ws/pkg/main.go",
			},
			"_meta": map[string]any{"promptIndex": float64(0)},
		}),
		liteDetailLine(t, sid, 3, 1300, map[string]any{
			"sessionUpdate": "turn_completed",
			"stopReason":    "end_turn",
		}),
	}
	writeUsageSession(t, home, "/ws", sid, strings.Join(lines, "\n"))
	return home, sid
}

func liteDetailBody(t *testing.T, home, sid, body string) []byte {
	t.Helper()
	s := usageServer(t, home)
	rec := postJSON(t, s, "/api/session-updates", body)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	return append([]byte(nil), rec.Body.Bytes()...)
}

// 缺省 / full / 未知值 → 今天的响应逐字节相等，且线上没有任何 lite 痕迹。
func TestSessionUpdatesDetailDefaultByteIdentical(t *testing.T) {
	home, sid := liteDetailSession(t)
	base := liteDetailBody(t, home, sid, `{"sessionId":"`+sid+`","cwd":"/ws"}`)
	for _, detail := range []string{"full", "LITE", "none", ""} {
		body := `{"sessionId":"` + sid + `","cwd":"/ws"}`
		if detail != "" {
			body = `{"sessionId":"` + sid + `","cwd":"/ws","detail":"` + detail + `"}`
		}
		got := liteDetailBody(t, home, sid, body)
		if !reflect.DeepEqual(got, base) {
			t.Errorf("detail=%q 的响应与缺省不逐字节相等（%d vs %d 字节）\n%s", detail, len(got), len(base), got)
		}
	}
	if strings.Contains(string(base), "projected") || strings.Contains(string(base), "omittedBytes") {
		t.Errorf("缺省响应回显了投影: %s", base)
	}
	if strings.Contains(string(base), `"lite"`) {
		t.Error("缺省响应里出现了 lite 标记")
	}
	if !strings.Contains(string(base), strings.Repeat("o", 20000)) {
		t.Error("缺省响应把工具正文裁了（应逐字节原样）")
	}
}

// lite：只裁工具正文，锚点与非工具信封不动，回显能力键。
func TestSessionUpdatesDetailLite(t *testing.T) {
	home, sid := liteDetailSession(t)
	full := liteDetailBody(t, home, sid, `{"sessionId":"`+sid+`","cwd":"/ws"}`)
	got := liteDetailBody(t, home, sid, `{"sessionId":"`+sid+`","cwd":"/ws","detail":"lite"}`)

	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("lite body 不是 JSON: %v (%s)", err, got)
	}
	if m["projected"] != "lite" {
		t.Errorf("projected = %v, want lite", m["projected"])
	}
	omitted, _ := m["omittedBytes"].(float64)
	if omitted <= 0 {
		t.Errorf("omittedBytes = %v, want 正数", m["omittedBytes"])
	}
	if len(got) >= len(full) {
		t.Errorf("lite 响应 %d 字节没比 full %d 小", len(got), len(full))
	}
	// 锚点语义不变（totalCount/hasMore/promptStarts 与 full 一致）。
	var fm map[string]any
	if err := json.Unmarshal(full, &fm); err != nil {
		t.Fatalf("full body 不是 JSON: %v", err)
	}
	for _, key := range []string{"ok", "sessionId", "totalCount", "hasMore", "promptStarts"} {
		if !reflect.DeepEqual(m[key], fm[key]) {
			t.Errorf("%v 被 lite 改了: %v, want %v", key, m[key], fm[key])
		}
	}
	updates, _ := m["updates"].([]any)
	if len(updates) != 4 {
		t.Fatalf("updates 条数 = %d, want 4（不许删信封行）", len(updates))
	}
	for i, raw := range updates {
		env := raw.(map[string]any)
		if env["msgSeq"] != float64(i) {
			t.Errorf("信封 %d msgSeq = %v, want %d", i, env["msgSeq"], i)
		}
	}
	// 工具信封：正文换摘要形状，params._meta 与保留字段原样。
	upd := updates[2].(map[string]any)["params"].(map[string]any)["update"].(map[string]any)
	ro := upd["rawOutput"].(map[string]any)
	if stub, ok := ro["output"].(map[string]any); !ok || stub["omitted"] != float64(20002) {
		t.Errorf("rawOutput.output 没换成 {omitted:20002}: %v", ro["output"])
	}
	if ro["path"] != "/ws/pkg/main.go" || ro["exit_code"] != float64(0) {
		t.Errorf("rawOutput 保留清单被改: %v", ro)
	}
	if upd["toolCallId"] != "tool-1" || upd["status"] != "completed" || upd["kind"] != "read" || upd["title"] != "Read main.go" {
		t.Errorf("工具信封身份字段被改: %v", upd)
	}
	blocks, _ := upd["content"].([]any)
	if len(blocks) != 1 {
		t.Fatalf("content 块数 = %d, want 1", len(blocks))
	}
	block := blocks[0].(map[string]any)
	if block["type"] != "content" {
		t.Errorf("content[0].type = %v, want 原块 type", block["type"])
	}
	if _, ok := block["omitted"].(float64); !ok {
		t.Errorf("content[0].omitted = %v, want 字节数", block["omitted"])
	}
	meta := upd["_meta"].(map[string]any)
	if meta["promptIndex"] != float64(0) {
		t.Errorf("update._meta.promptIndex 被改: %v", meta)
	}
	if stamp, ok := meta["lite"].(map[string]any); !ok || stamp["fields"] == nil {
		t.Errorf("工具信封没打 lite 标记: %v", meta)
	}
	// 非工具信封逐字节原样（用户/助手文本、回合终态）。
	for _, i := range []int{0, 1, 3} {
		fu := updates[i]
		gu := fm["updates"].([]any)[i]
		if !reflect.DeepEqual(mustMarshalLite(t, fu), mustMarshalLite(t, gu)) {
			t.Errorf("信封 %d（非工具）被 lite 改了", i)
		}
	}
}

// meta：不回 updates 键本身，锚点照常；omittedBytes 也按 [B] 回显。
func TestSessionUpdatesDetailMeta(t *testing.T) {
	home, sid := liteDetailSession(t)
	full := liteDetailBody(t, home, sid, `{"sessionId":"`+sid+`","cwd":"/ws"}`)
	got := liteDetailBody(t, home, sid, `{"sessionId":"`+sid+`","cwd":"/ws","detail":"meta"}`)

	if strings.Contains(string(got), `"updates"`) {
		t.Errorf("meta 档仍回了 updates 键: %s", got)
	}
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("meta body 不是 JSON: %v (%s)", err, got)
	}
	if _, ok := m["updates"]; ok {
		t.Error("meta 档的 updates 键没被省略（空数组也不行）")
	}
	if m["projected"] != "meta" {
		t.Errorf("projected = %v, want meta", m["projected"])
	}
	if _, ok := m["omittedBytes"]; !ok {
		t.Errorf("meta 档没回显 omittedBytes: %s", got)
	}
	var fm map[string]any
	if err := json.Unmarshal(full, &fm); err != nil {
		t.Fatalf("full body 不是 JSON: %v", err)
	}
	for _, key := range []string{"ok", "sessionId", "totalCount", "hasMore", "promptStarts"} {
		if !reflect.DeepEqual(m[key], fm[key]) {
			t.Errorf("meta 改了 %v: %v, want %v", key, m[key], fm[key])
		}
	}
}

func mustMarshalLite(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}
