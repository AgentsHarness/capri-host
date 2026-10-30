package hub

import (
	"bytes"
	"context"
	"encoding/base64"
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
// readRelayAnswer drains one request's respond frames. A body over
// relayChunkSize is delivered as several frames (see respondChunked), so
// chunk frames are base64-decoded and concatenated before the caller sees
// them; small answers still arrive as a single `body` frame.
func readRelayAnswer(t *testing.T, c *Client) (status int, body json.RawMessage, final bool) {
	t.Helper()
	var joined []byte
	for {
		f := <-c.reqCh
		var fr struct {
			Status   int             `json:"status"`
			Body     json.RawMessage `json:"body"`
			Chunk    string          `json:"chunk"`
			Encoding string          `json:"encoding"`
			More     bool            `json:"more"`
		}
		if err := json.Unmarshal(f.payload, &fr); err != nil {
			t.Fatalf("respond frame not valid JSON: %v (%s)", err, f.payload)
		}
		if fr.Chunk == "" && fr.Encoding == "" {
			return fr.Status, fr.Body, f.final
		}
		if fr.Encoding != "base64" {
			t.Fatalf("chunk frame encoding = %q, want base64", fr.Encoding)
		}
		dec, err := base64.StdEncoding.DecodeString(fr.Chunk)
		if err != nil {
			t.Fatalf("chunk frame is not base64: %v", err)
		}
		joined = append(joined, dec...)
		status = fr.Status
		if !fr.More {
			return status, json.RawMessage(joined), f.final
		}
	}
}

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
	// Room for a whole chunked answer: the 20MB case below is 20 frames of
	// relayChunkSize, and respond enqueues them all before returning.
	c.reqCh = make(chan reqFrame, 64)

	// 1) 正常往返：body 原样回传，auth 头注入生效。
	c.handleRelay(context.Background(), "r1", "", "POST", "/api/echo", json.RawMessage(`{"q":1}`))
	status, body, final := readRelayAnswer(t, c)
	if status != http.StatusOK || string(body) != `{"q":1}` {
		t.Fatalf("relay echo: status=%d body=%s", status, body)
	}
	if !final {
		t.Error("in-process relay respond must be the request's final frame")
	}

	// 2) handler panic → 500 响应帧（不能击穿 handleRelay 的 goroutine）。
	c.handleRelay(context.Background(), "r2", "", "GET", "/api/panic", nil)
	if status, _, _ = readRelayAnswer(t, c); status != http.StatusInternalServerError {
		t.Fatalf("panic relay: status=%d want 500", status)
	}

	// 3) 上限以内的历史页原样回传。16–28MB 这一段曾经是 502：进程内中继
	//    才是生产真正走的路（Config.Local 直调），它曾单独留着 16MB 的字面量。
	c.handleRelay(context.Background(), "r3", "", "GET", "/api/huge?mb=20", nil)
	status, body, _ = readRelayAnswer(t, c)
	if status != http.StatusOK {
		t.Fatalf("20MB relay: status=%d want 200 (detail=full 补全页会被这一枪打死)", status)
	}
	// 非 JSON 正文会被包成 JSON 字符串回传，取长度得先解一层。
	var echoed string
	if err := json.Unmarshal(body, &echoed); err != nil {
		t.Fatalf("20MB body not a JSON string: %v", err)
	}
	if len(echoed) != 20<<20 {
		t.Fatalf("20MB relay: body=%d bytes want %d", len(echoed), 20<<20)
	}

	// 4) 超过 maxRelayResponseBytes → 502，不回传超限帧。
	c.handleRelay(context.Background(), "r4", "", "GET", "/api/huge?mb=30", nil)
	status, body, _ = readRelayAnswer(t, c)
	if status != http.StatusBadGateway {
		t.Fatalf("huge relay: status=%d want 502", status)
	}
	if !bytes.Contains(body, []byte("过大")) {
		t.Fatalf("huge relay body = %s, want explicit oversize error", body)
	}
}
