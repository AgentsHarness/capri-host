package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
)

// hello_busy_test.go — SSE hello 的 busy 语义（F2 审计项）：hello 只反映
// 被 announce 的会话（sessionId 指向的那个），而不是所有会话的 OR 聚合。
// 聚合版 busy 保留在 /api/status（Status.Busy）。

// sseRecorder 是并发安全的 SSE 响应捕获器：handleSSE 常驻不返回，测试在
// goroutine 里跑它、在首个 Flush（hello 帧写完）时被唤醒读 body。
type sseRecorder struct {
	mu      sync.Mutex
	hdr     http.Header
	body    bytes.Buffer
	flushed chan struct{}
}

func (r *sseRecorder) Header() http.Header { return r.hdr }
func (r *sseRecorder) WriteHeader(int)     {}
func (r *sseRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(p)
}
func (r *sseRecorder) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case r.flushed <- struct{}{}:
	default:
	}
}
func (r *sseRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

// readSSEHello 打开 /events，读回首个 hello 帧并停掉 SSE goroutine。
func readSSEHello(t *testing.T, s *Server) map[string]any {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
	rec := &sseRecorder{flushed: make(chan struct{}, 4), hdr: http.Header{}}
	go s.http.Handler.ServeHTTP(rec, req)

	select {
	case <-rec.flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE hello 帧从未写出")
	}
	line := strings.TrimPrefix(strings.SplitN(rec.String(), "\n", 2)[0], "data: ")
	var hello map[string]any
	if err := json.Unmarshal([]byte(line), &hello); err != nil {
		t.Fatalf("hello 帧不是 JSON: %v (%q)", err, line)
	}
	return hello
}

// waitBusy 轮询直到会话 sid 进入在飞状态（回合已挂起）。
func waitBusy(t *testing.T, b *acp.Bridge, sid string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := b.SessionStateOf(sid); st != nil && st.Busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s 从未进入 busy 状态", sid)
}

// 会话 A（活动、空闲）+ 会话 B（后台在飞）→ hello（announce A）的 busy
// 必须为 false：页面刷新时后台会话的回合不得把前台视图钉成永久 busy
// （B 的 done 事件会被客户端按 sessionId 过滤掉，聚合 busy 永远清不掉）。
// 同时固定 /api/status 的聚合 busy 语义不变。
func TestSSEHelloBusyReflectsAnnouncedSessionOnly(t *testing.T) {
	// 慢速 fake agent：session/prompt 延迟应答，把 B 的回合挂在在飞状态。
	t.Setenv(ACPHostFakeAgentPromptDelayMs, "1000")
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s) // A = sess-new（活动会话）

	// 会话 B 入 roster；焦点切回 A（A 空闲且活动）。
	rec := postJSON(t, s, "/api/session-load", `{"sessionId":"sess-B","cwd":"/ws2"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("load B status = %d, body=%s", rec.Code, rec.Body.String())
	}
	rec = postJSON(t, s, "/api/session-load", `{"sessionId":"sess-new","cwd":"/ws"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("load A status = %d, body=%s", rec.Code, rec.Body.String())
	}

	// B 开一个在飞回合（慢速 fake agent 挂住应答；A 仍是活动会话）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	promptDone := make(chan struct{})
	go func() {
		_, _, _ = b.PromptWithOpts(ctx, "sess-B",
			[]acp.ContentBlock{{"type": "text", "text": "hi"}}, acp.PromptOpts{})
		close(promptDone)
	}()
	waitBusy(t, b, "sess-B")

	// /api/status 的聚合 busy 保持 true（任一会话在飞）。
	req := httptest.NewRequest("GET", "/api/status", nil)
	statusRec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(statusRec, req)
	if busy, _ := decodeBody(t, statusRec)["busy"].(bool); !busy {
		t.Errorf("/api/status busy = false, want true — 聚合 busy 语义不应改变")
	}

	// hello：announce A（sessionId=sess-new），busy 必须为 false。
	hello := readSSEHello(t, s)
	if sid, _ := hello["sessionId"].(string); sid != "sess-new" {
		t.Fatalf("hello sessionId = %v, want sess-new", hello["sessionId"])
	}
	if busy, _ := hello["busy"].(bool); busy {
		t.Fatalf("hello.busy = true, want false — hello 必须只反映被 announce 的会话（A 空闲，B 在飞）")
	}

	// 等后台回合自然结束，收尾干净。
	select {
	case <-promptDone:
	case <-time.After(10 * time.Second):
		t.Fatal("后台回合未结束")
	}
}

// ── busy 会话的并发 prompt（FE 队列对齐 TUI 的 host 侧改动）───────
//
// 会话 busy 时第二个 /api/prompt 不再 409：照常转发，agent 自己排队
// （xai-grok-shell 的权威 pending_inputs）。两个请求都 200，且 wire 上
// 有两条 session/prompt（fake agent 顺序应答）。

func TestPromptWhileBusyForwardsAndBothSucceed(t *testing.T) {
	t.Setenv(ACPHostFakeAgentPromptDelayMs, "500")
	recordPath := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv(ACPHostFakeAgentRecordRequests, recordPath)
	s, b := newFakeAgentServer(t)
	createActiveSession(t, s)
	ch, unsub := b.Subscribe()
	defer unsub()

	// A：慢回合（delay 挂住在飞）；B 在 A 在飞时发出。
	recA := make(chan *httptest.ResponseRecorder, 1)
	go func() { recA <- postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"a"}]}`) }()
	waitBusy(t, b, "sess-new")

	// B 在 busy 会话上：必须 200（受理）并转发（agent 排队），而不是 409。
	recB := postJSON(t, s, "/api/prompt", `{"blocks":[{"type":"text","text":"b"}]}`)
	if recB.Code != http.StatusOK {
		t.Fatalf("busy-session prompt status = %d, body=%s — want 200 (forwarded to agent queue)",
			recB.Code, recB.Body.String())
	}
	if m := decodeBody(t, recB); m["ok"] != true {
		t.Fatalf("busy-session prompt body = %v, want ok:true", m)
	}

	ra := <-recA
	if ra.Code != http.StatusOK {
		t.Fatalf("first prompt status = %d, body=%s", ra.Code, ra.Body.String())
	}

	// 受理即返回后成功只能从 live 通道确认：两个回合都以 done 事件收口。
	// 先等收口再数 wire 记录——B 的 session/prompt 行要等 fake agent
	// 答完 A（delay 500ms）后才会被读到。
	for i := 0; i < 2; i++ {
		waitEvent(t, ch, func(ev acp.Event) bool { return ev["type"] == "done" })
	}

	// 两个 prompt 都转发到了 agent：wire 上有两条 session/prompt。
	lines := readRecordedRequests(t, recordPath)
	n := 0
	for _, m := range lines {
		if m["method"] == "session/prompt" {
			n++
		}
	}
	if n != 2 {
		t.Errorf("wire session/prompt count = %d, want 2: %v", n, lines)
	}
}

// 404 语义保留：显式指向不存在的会话仍然 404（busy 放宽只影响 busy，
// 不影响 404；无活动会话时的恢复/新建逻辑不动）。
func TestPromptUnknownSessionStill404(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	rec := postJSON(t, s, "/api/prompt",
		`{"sessionId":"sess-ghost","blocks":[{"type":"text","text":"hi"}]}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown-session prompt status = %d, body=%s, want 404",
			rec.Code, rec.Body.String())
	}
}
