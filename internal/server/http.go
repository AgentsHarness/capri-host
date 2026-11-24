package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	cfg     config.Config
	bridge  bridgeAPI
	handler http.Handler
	http    *http.Server
	// promptFn runs a turn when injected (tests only; signature unchanged).
	// When nil, handlePrompt uses bridge.PromptWithOpts — the default path —
	// so the optional messageId/meta fields ride the session/prompt params.
	// The handler runs it detached from the client connection so a browser
	// crash mid-turn does not cancel the turn (the grok agent keeps running).
	promptFn func(ctx context.Context, sessionID string, blocks []acp.ContentBlock) (string, error)

	// hubCtl is the live hub client, injected by main after New (see
	// SetHubController). nil in local mode and in every test that does not
	// care about hub state.
	hubMu  sync.Mutex
	hubCtl HubController

	// authLimiter throttles failed inbound token attempts by client IP.
	// Successful requests clear the bucket; this is not a request quota.
	authLimiter *authAttemptLimiter

	// idem 是带 Idempotency-Key 的 relay 写请求的 at-most-once 存储，
	// 跨 host 重启由 ~/.capri-host/relay-idempotency.jsonl 恢复。
	idem *idemStore

	// quitFn triggers the same orderly shutdown a SIGINT would, so a
	// supervisor process can stop this host without hard-killing it and
	// orphaning the grok child. nil in tests and in any embedder that does
	// not care (see SetQuitFunc).
	quitFn func()
}

func New(cfg config.Config, bridge bridgeAPI) *Server {
	s := &Server{
		cfg:         cfg,
		bridge:      bridge,
		authLimiter: newAuthAttemptLimiter(),
		idem:        newIdemStore(cfg.HostID, cfg.IdemFile),
	}
	s.handler = s.routes()
	s.http = &http.Server{
		Addr:              net.JoinHostPort(bindAddrOf(cfg), strconv.Itoa(cfg.Port)),
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// bindAddrOf resolves the listen address for a Config, treating an unset
// BindAddr (hand-built Config in tests) as the config default.
func bindAddrOf(cfg config.Config) string {
	if strings.TrimSpace(cfg.BindAddr) == "" {
		return config.DefaultBindAddr
	}
	return cfg.BindAddr
}

// Handler 暴露完整的本地 API 处理链（CORS + token 鉴权 + 路由）。hub
// 中继模式用它做进程内直调（见 hub.Config.Local），不再绕道本机
// TCP 回环——同一条鉴权管线，少一个端口依赖。
func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) ListenAndServe() error {
	addr := net.JoinHostPort(bindAddrOf(s.cfg), strconv.Itoa(s.cfg.Port))
	log.Printf("[Capri-host] listening on http://%s", addr)
	if !s.cfg.BindIsLoopback() {
		log.Printf("[Capri-host] BIND=%s 非回环：同网段设备可直接访问本机 API", s.cfg.BindAddr)
	}
	log.Printf("[Capri-host] grok bin=%s hostId=%s name=%q", s.cfg.GrokBin, s.cfg.HostID, s.cfg.HostName)
	log.Printf("[Capri-host] mode=agent-only (no client fs/terminal)")
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.idem.close()
	return s.http.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// 防御性：告知前置代理（nginx 等）不要缓冲流式响应——SSE 一旦被
	// 缓冲，回合事件会成批到达，前端实时性受损（有时还会撑爆代理缓冲）。
	w.Header().Set("X-Accel-Buffering", "no-buffering")

	snap := s.bridge.Snapshot()
	// hello 的 busy 只反映被 announce 的会话（snap.SessionID）本身，而非
	// 所有会话的 OR 聚合（聚合版保留在 Status.Busy，供 /api/status）：
	// 另一个会话在后台跑回合时页面刷新，前台视图不应被永久置 busy ——
	// 后台会话的 done 事件会被客户端按 sessionId 过滤掉，永远清不掉。
	// 与 handleSessionLoad 返回 per-session busy 的语义一致。
	helloBusy := false
	if st := s.bridge.SessionStateOf(snap.SessionID); st != nil {
		helloBusy = st.Busy
	}
	hello := map[string]any{
		"type":              "hello",
		"ready":             snap.Ready,
		"busy":              helloBusy,
		"sessionId":         snap.SessionID,
		"cwd":               snap.Cwd,
		"error":             snap.BootError,
		"agentInfo":         snap.AgentInfo,
		"modes":             snap.Modes,
		"configOptions":     snap.ConfigOptions,
		"models":            snap.Models,
		"pendingRequests":   snap.PendingRequests,
		"hostId":            snap.HostID,
		"hostName":          snap.HostName,
		"homeDir":           snap.HomeDir,
		"capabilities":      snap.Capabilities,
		"agentCapabilities": snap.AgentCapabilities,
		"roster":            snap.Roster,
		"agentStartedAt":    snap.AgentStartedAt,
		"permissionMode":    snap.PermissionMode,
	}
	data, _ := json.Marshal(hello)
	rc := http.NewResponseController(w)
	if !writeSSEFrame(w, flusher, rc, data) {
		return // client gone before the hello even landed
	}

	ch, unsub := s.bridge.Subscribe()
	defer unsub()

	// Per-frame write deadline (rc above). writeSSEFrame only detects a
	// client that ERRORS; a half-open client (dead laptop, dropped Wi-Fi,
	// no RST) never errors — the write just blocks on a full TCP window
	// forever, and with it this handler, its subscription and its
	// 512-event channel. Requests have no default write timeout here, so
	// the deadline must be set explicitly; each frame refreshes it.

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !writeSSEFrame(w, flusher, rc, nil) {
				return
			}
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// 复制后附加 hostId：本地 SSE 事件需带 hostId 供前端按
			// (hostId, seq) 与 hub 中继推回的同源事件去重（双连接时
			// 本地 SSE 与 /ws/fe 各来一份）。不改 bridge 共享 map ——
			// hub-client 与 SSE 订阅同一事件，改 map 会污染上传路径。
			// 与 hub 的 stripFullUpdate 对齐：去掉大体积 fullUpdate，
			// 避免撑爆 SSE 帧与 Subscribe 缓冲导致 Broadcast drop。
			out := make(map[string]any, len(ev)+1)
			for k, v := range ev {
				out[k] = v
			}
			delete(out, "fullUpdate")
			out["hostId"] = snap.HostID
			b, err := json.Marshal(out)
			if err != nil {
				continue
			}
			if !writeSSEFrame(w, flusher, rc, b) {
				return
			}
		}
	}
}

// sseWriteTimeout bounds a single SSE frame write. Long enough that a
// briefly stalled mobile link is not dropped mid-turn, short enough that a
// dead peer cannot pin the handler (and its bridge subscription) forever.
const sseWriteTimeout = 30 * time.Second

// writeSSEFrame writes one SSE frame and flushes. Returns false when the
// client went away (write failed, flush failed, or the write blocked past
// sseWriteTimeout), so the SSE handler can return and unsubscribe
// immediately instead of blocking forever on a dead reader (TCP window
// full / RST) — a stuck handler would leak the subscription and its
// buffered channel for as long as the socket lingers.
func writeSSEFrame(w http.ResponseWriter, flusher http.Flusher, rc *http.ResponseController, data []byte) bool {
	if rc != nil {
		// Not all ResponseWriters support deadlines (middleware wrappers,
		// httptest); ignore ErrNotSupported and write without one.
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	}
	var err error
	if data == nil {
		// Comment frame (ticker ping); `%s` of nil would print "<nil>".
		_, err = fmt.Fprintf(w, ": ping\n\n")
	} else {
		_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	}
	if err != nil {
		return false
	}
	flusher.Flush()
	return true
}
