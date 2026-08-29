package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// relay_inprocess_test.go — 进程内中继路径（Config.Local = server.Handler()
// 等价物）。回环 HTTP 路径由 dualstream / e2e 覆盖；这里锁定进程内直调的
// 三个关键语义：正常往返（含 auth 头注入）、handler panic 不击穿会话
// goroutine（500 响应帧）、超 16MB 响应截断为 502（不产生超限 respond 帧）。
func TestRelayInProcess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/echo", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"ok":false,"error":"auth header lost: ` + got + `"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)
		w.Write(buf)
	})
	mux.HandleFunc("GET /api/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	mux.HandleFunc("GET /api/huge", func(w http.ResponseWriter, r *http.Request) {
		chunk := strings.Repeat("x", 1<<20)
		for i := 0; i < 17; i++ {
			w.Write([]byte(chunk))
		}
	})

	c := NewClient(Config{URL: "http://hub", HostID: "h1", Token: "t", AccessToken: "tok", Local: mux})
	c.reqCh = make(chan reqFrame, 1)

	// 1) 正常往返：body 原样回传，auth 头注入生效。
	c.handleRelay(context.Background(), "r1", "", "POST", "/api/echo", json.RawMessage(`{"q":1}`))
	f := <-c.reqCh
	var frame struct {
		Status int             `json:"status"`
		Body   json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("respond frame not valid JSON: %v (%s)", err, f.payload)
	}
	if frame.Status != http.StatusOK || string(frame.Body) != `{"q":1}` {
		t.Fatalf("relay echo: status=%d body=%s", frame.Status, frame.Body)
	}
	if !f.final {
		t.Error("in-process relay respond must be the request's final frame")
	}

	// 2) handler panic → 500 响应帧（不能击穿 handleRelay 的 goroutine）。
	c.handleRelay(context.Background(), "r2", "", "GET", "/api/panic", nil)
	f = <-c.reqCh
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("panic respond frame not valid JSON: %v (%s)", err, f.payload)
	}
	if frame.Status != http.StatusInternalServerError {
		t.Fatalf("panic relay: status=%d want 500", frame.Status)
	}

	// 3) 超大响应（17MB > 16MB 上限）→ 502，不回传超限帧。
	c.handleRelay(context.Background(), "r3", "", "GET", "/api/huge", nil)
	f = <-c.reqCh
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("huge respond frame not valid JSON: %v (%s)", err, f.payload)
	}
	if frame.Status != http.StatusBadGateway {
		t.Fatalf("huge relay: status=%d want 502", frame.Status)
	}
}
