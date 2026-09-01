package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// relay_inprocess_test.go — 进程内中继路径（Config.Local = server.Handler()
// 等价物）。回环 HTTP 路径由 dualstream / e2e 覆盖；这里锁定进程内直调的
// 四个关键语义：正常往返（含 auth 头注入）、handler panic 不击穿会话
// goroutine（500 响应帧）、上限以内的历史页原样回传、超过
// maxRelayResponseBytes 的响应截断为 502（不产生超限 respond 帧）。
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
		mb, _ := strconv.Atoi(r.URL.Query().Get("mb"))
		chunk := strings.Repeat("x", 1<<20)
		for i := 0; i < mb; i++ {
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

	// 3) 上限以内的历史页原样回传。16–28MB 这一段曾经是 502：进程内中继
	//    才是生产真正走的路（Config.Local 直调），它曾单独留着 16MB 的字面量。
	c.handleRelay(context.Background(), "r3", "", "GET", "/api/huge?mb=20", nil)
	f = <-c.reqCh
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("20MB respond frame not valid JSON: %v", err)
	}
	if frame.Status != http.StatusOK {
		t.Fatalf("20MB relay: status=%d want 200 (detail=full 补全页会被这一枪打死)", frame.Status)
	}
	// 非 JSON 正文会被包成 JSON 字符串回传，取长度得先解一层。
	var echoed string
	if err := json.Unmarshal(frame.Body, &echoed); err != nil {
		t.Fatalf("20MB body not a JSON string: %v", err)
	}
	if len(echoed) != 20<<20 {
		t.Fatalf("20MB relay: body=%d bytes want %d", len(echoed), 20<<20)
	}

	// 4) 超过 maxRelayResponseBytes → 502，不回传超限帧。
	c.handleRelay(context.Background(), "r4", "", "GET", "/api/huge?mb=30", nil)
	f = <-c.reqCh
	if err := json.Unmarshal(f.payload, &frame); err != nil {
		t.Fatalf("huge respond frame not valid JSON: %v (%s)", err, f.payload)
	}
	if frame.Status != http.StatusBadGateway {
		t.Fatalf("huge relay: status=%d want 502", frame.Status)
	}
	if !bytes.Contains(frame.Body, []byte("过大")) {
		t.Fatalf("huge relay body = %s, want explicit oversize error", frame.Body)
	}
}
