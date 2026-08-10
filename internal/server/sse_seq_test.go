package server

// ── SSE 事件帧结构（双连接去重前提）──────────────────────────────────
//
// hub 模式下前端选中本机 host 时开双连接（本地 SSE 近路 + hub WS），
// 去重依赖本地 SSE 事件带 (hostId, seq)：
//   - seq：bridge.Broadcast 附加的全局序号（与 hub 中继推回的事件同源）；
//   - hostId：handleSSE 推送前复制事件并附加本机 hostId。
// 本测试锁住这个不变量。

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benin/acp-host/internal/acp"
)

// readSSEData reads lines until the next "data: " frame and returns its
// raw payload (the first one may be the hello frame).
func readSSEData(t *testing.T, br *bufio.Reader) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE line: %v", err)
		}
		if strings.HasPrefix(line, "data: ") {
			return []byte(strings.TrimSpace(strings.TrimPrefix(line, "data: ")))
		}
	}
	t.Fatal("timed out waiting for SSE data frame")
	return nil
}

// TestSSEEventCarriesHostIDAndSeq: 广播事件经 /events 推送时带 hostId
// 与全局 seq（前端 (hostId, seq) 双路去重的前提）。
func TestSSEEventCarriesHostIDAndSeq(t *testing.T) {
	s, b := newFakeAgentServer(t)
	ts := httptest.NewServer(s.http.Handler)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)

	// hello 帧（无 seq，预期）；随后广播一个事件并断言其结构。
	readSSEData(t, br)

	b.Broadcast(acp.Event{"type": "chunk", "text": "x", "sessionId": "s1"})

	var ev map[string]any
	if err := json.Unmarshal(readSSEData(t, br), &ev); err != nil {
		t.Fatalf("bad event frame: %v", err)
	}
	if ev["type"] != "chunk" {
		t.Fatalf("type = %v, want chunk", ev["type"])
	}
	hostID, _ := ev["hostId"].(string)
	if hostID != "h" {
		t.Errorf("hostId = %q, want %q (bridge HostID)", hostID, "h")
	}
	seq, ok := ev["seq"].(float64)
	if !ok || seq < 1 {
		t.Errorf("seq = %v (%T), want positive number (bridge global seq)", ev["seq"], ev["seq"])
	}
}

// TestSSEBroadcastAddsSeqInPlace: Broadcast 就地附加 seq，不改其它键
// （共享 map 引用，所有订阅者读到同一事件同一 seq）。
func TestSSEBroadcastAddsSeqInPlace(t *testing.T) {
	_, b := newFakeAgentServer(t)
	ev := acp.Event{"type": "chunk", "text": "keep"}
	evCh, unsub := b.Subscribe()
	defer unsub()
	b.Broadcast(ev)

	select {
	case got := <-evCh:
		if got["text"] != "keep" {
			t.Errorf("text = %v, want keep", got["text"])
		}
		if _, ok := got["seq"]; !ok {
			t.Error("broadcast event missing seq")
		}
		// Caller map is mutated in place (same reference).
		if _, ok := ev["seq"]; !ok {
			t.Error("Broadcast should set seq on the caller map in place")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast not delivered")
	}
}

// TestSSEStripsFullUpdate: handleSSE 推送前剥离 fullUpdate（与 hub
// stripFullUpdate 对齐），但保留 hostId + seq 供双路去重。
func TestSSEStripsFullUpdate(t *testing.T) {
	s, b := newFakeAgentServer(t)
	ts := httptest.NewServer(s.http.Handler)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)

	// hello 帧
	readSSEData(t, br)

	// 广播带大体积 fullUpdate 的事件；SSE 线上不得出现该键。
	b.Broadcast(acp.Event{
		"type":       "session_update",
		"sessionId":  "s1",
		"fullUpdate": map[string]any{"huge": "payload", "nested": []any{1, 2, 3}},
		"text":       "ok",
	})

	raw := readSSEData(t, br)
	if strings.Contains(string(raw), "fullUpdate") {
		t.Fatalf("SSE wire still contains fullUpdate: %s", raw)
	}
	var ev map[string]any
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("bad event frame: %v", err)
	}
	if _, ok := ev["fullUpdate"]; ok {
		t.Error("fullUpdate key present on SSE wire after strip")
	}
	if ev["type"] != "session_update" {
		t.Errorf("type = %v, want session_update", ev["type"])
	}
	if ev["text"] != "ok" {
		t.Errorf("text = %v, want ok", ev["text"])
	}
	hostID, _ := ev["hostId"].(string)
	if hostID != "h" {
		t.Errorf("hostId = %q, want %q", hostID, "h")
	}
	seq, ok := ev["seq"].(float64)
	if !ok || seq < 1 {
		t.Errorf("seq = %v (%T), want positive number", ev["seq"], ev["seq"])
	}
}
