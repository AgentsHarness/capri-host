package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/server"
)

// relay_server_chain_test.go — 生产装配路径的端到端锁定：Config.Local 接的是
// 真实的 server.New(...).Handler()（即 withCORS(withAuth(mux)) 全链），不是自制
// mux。回环 HTTP 路径（dualstream / e2e 覆盖）与进程内直调必须对同一请求给出
// 相同的状态码与响应体——hub 模式下浏览器看到的就是这条链的产出。
//
// 约束（本测试同时锁住的前提）：hub 只转发 /api/{path...}，而 /api/* 下没有
// 流式端点（唯一的 Flusher 消费者是 GET /events，由 hub 自身的环形缓冲供数）。
// relayResponseWriter 因此不实现 Flusher/Hijacker；若将来给 /api/* 加流式
// handler，中继路径会退化，届时必须先扩 relayResponseWriter。

// chainProbe 是一次请求的产出（状态码 + 响应体），两条路径各跑一遍再比对。
type chainProbe struct {
	status int
	body   string
}

func relayViaInProcess(t *testing.T, c *Client, method, path, body string) chainProbe {
	t.Helper()
	var raw json.RawMessage
	if body != "" {
		raw = json.RawMessage(body)
	}
	c.handleRelay(context.Background(), "req-"+path, "", method, path, raw)
	f := <-c.reqCh
	var frame struct {
		Status int             `json:"status"`
		Body   json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("in-process respond frame for %s %s is not valid JSON: %v (%s)", method, path, err, f.payload)
	}
	return chainProbe{frame.Status, string(frame.Body)}
}

func relayViaLoopback(t *testing.T, base, token, method, path, body string) chainProbe {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, base+path, rd)
	if err != nil {
		t.Fatalf("loopback request %s %s: %v", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("loopback %s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	rawBody, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("loopback read %s %s: %v", method, path, err)
	}
	return chainProbe{res.StatusCode, string(rawBody)}
}

// unwrapRelayBody 还原 respond 帧的 body 字段：JSON 对象/数组原样返回，
// 被包装成 JSON 字符串的原始文本解回字符串。
func unwrapRelayBody(t *testing.T, frameBody string) string {
	t.Helper()
	trimmed := strings.TrimSpace(frameBody)
	if strings.HasPrefix(trimmed, `"`) {
		var s string
		if err := json.Unmarshal([]byte(trimmed), &s); err != nil {
			t.Fatalf("respond body not an unwrappable JSON string: %v (%s)", err, frameBody)
		}
		return s
	}
	return frameBody
}

// normalizeBody 抹平两种传输各自的空白差异：writeJSON 结尾多一个换行，
// respond 帧里的 json.RawMessage 又被压缩成单行。比对语义而非字节空白。
func normalizeBody(s string) string {
	trimmed := strings.TrimSpace(s)
	var buf bytes.Buffer
	if json.Compact(&buf, []byte(trimmed)) == nil {
		return buf.String()
	}
	return trimmed
}

// TestRelayThroughRealServerChain compares the in-process relay against the
// loopback path across the auth-gated, agent-less-failing and 404 variants of
// the real handler chain, then proves withAuth is not bypassed in-process.
func TestRelayThroughRealServerChain(t *testing.T) {
	const token = "chain-token"

	bridge := acp.NewBridge(acp.GrokConfig{Bin: "/nonexistent/grok", HostID: "h1", HostName: "H1"})
	defer bridge.Shutdown()
	srv := server.New(config.Config{HostID: "h1", HostName: "H1", AccessToken: token}, bridge)

	c := NewClient(Config{URL: "http://hub", HostID: "h1", Token: "t", AccessToken: token, Local: srv.Handler()})
	c.reqCh = make(chan reqFrame, 16)

	lb := httptest.NewServer(srv.Handler())
	defer lb.Close()

	cases := []struct {
		method, path, body string
	}{
		{"GET", "/api/status", ""},
		{"GET", "/api/hosts", ""},
		{"POST", "/api/git/branches", `{}`},
		{"POST", "/api/fs/list", `{}`},
		{"POST", "/api/prompt", `{"blocks":[]}`},
		{"GET", "/api/not-a-route", ""},
		{"POST", "/api/not-a-route", `{}`},
	}
	for _, tc := range cases {
		inProc := relayViaInProcess(t, c, tc.method, tc.path, tc.body)
		loop := relayViaLoopback(t, lb.URL, token, tc.method, tc.path, tc.body)
		if inProc.status != loop.status {
			t.Errorf("%s %s status: in-process=%d loopback=%d", tc.method, tc.path, inProc.status, loop.status)
		}
		// respond 帧对不可嵌入 JSON 的响应体做 JSON 字符串包装（既有语义），
		// 比对前先还原成原始字节。
		if normalizeBody(unwrapRelayBody(t, inProc.body)) != normalizeBody(loop.body) {
			t.Errorf("%s %s body mismatch:\n in-process: %s\n  loopback: %s", tc.method, tc.path, inProc.body, loop.body)
		}
	}

	// withAuth 在进程内路径上同样生效：token 不匹配 → 401，而不是绕过鉴权。
	bad := NewClient(Config{URL: "http://hub", HostID: "h1", Token: "t", AccessToken: "wrong-token", Local: srv.Handler()})
	bad.reqCh = make(chan reqFrame, 4)
	got := relayViaInProcess(t, bad, "GET", "/api/status", "")
	if got.status != http.StatusUnauthorized {
		t.Errorf("in-process relay without a valid token: status=%d, want 401 (body=%s)", got.status, got.body)
	}
	// 对照：回环路径同样 401，两侧语义对齐。
	if got := relayViaLoopback(t, lb.URL, "wrong-token", "GET", "/api/status", ""); got.status != http.StatusUnauthorized {
		t.Errorf("loopback relay without a valid token: status=%d, want 401", got.status)
	}
}
