package hub

import (
	"net/http"
	"testing"
)

// request 帧的 idemKey/idemScope 必须注入本地 HTTP 请求；没有这两个
// 字段的老帧保持原样（header 为空）。
func TestRelayForwardsIdempotencyHeaders(t *testing.T) {
	headers := make(chan [2]string, 2)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/echo", func(w http.ResponseWriter, r *http.Request) {
		headers <- [2]string{r.Header.Get(idemKeyHeader), r.Header.Get(idemScopeHeader)}
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	c := NewClient(Config{HostID: "h1", Token: "tok123", Local: mux})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	f := requestFrame(1, 0, "1", "POST", "/api/echo", `{}`)
	f.IdemKey = "6f1e0c1e-7f5a-4c2b-9d3e-2a1b0c9d8e7f"
	f.IdemScope = "user:u1"
	h.tr.push(t, f)
	if got := waitRespond(t, h.tr); got.RespondStatus() != 200 {
		t.Fatalf("keyed relay status = %d", got.RespondStatus())
	}
	got := <-headers
	if got[0] != f.IdemKey || got[1] != f.IdemScope {
		t.Fatalf("in-process headers = %q/%q", got[0], got[1])
	}

	h.tr.push(t, requestFrame(2, 0, "2", "POST", "/api/echo", `{}`))
	if got := waitRespond(t, h.tr); got.RespondStatus() != 200 {
		t.Fatalf("plain relay status = %d", got.RespondStatus())
	}
	if got := <-headers; got != [2]string{"", ""} {
		t.Fatalf("plain relay sent idempotency headers = %q/%q", got[0], got[1])
	}
}

// LocalBase 回退路径同样注入两个 header（外部装配方与旧测试走这条）。
func TestRelayHTTPFallbackForwardsIdempotencyHeaders(t *testing.T) {
	headers := make(chan [2]string, 1)
	url := startTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- [2]string{r.Header.Get(idemKeyHeader), r.Header.Get(idemScopeHeader)}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	c := NewClient(Config{HostID: "h1", Token: "tok123", LocalBase: url})
	h := newSessionHarness(t, c, newTestBridge())
	h.handshake(helloAckFrame(c.epoch, 0, 0, watchingAudience(1)))

	f := requestFrame(1, 0, "1", "POST", "/api/echo", `{}`)
	f.IdemKey = "key-http"
	f.IdemScope = "guest:t1"
	h.tr.push(t, f)
	if got := waitRespond(t, h.tr); got.RespondStatus() != 200 {
		t.Fatalf("fallback relay status = %d", got.RespondStatus())
	}
	got := <-headers
	if got[0] != f.IdemKey || got[1] != f.IdemScope {
		t.Fatalf("fallback headers = %q/%q", got[0], got[1])
	}
}
