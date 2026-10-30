package hub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-wire"
)

// This file is the relay plane: one browser request becomes one local HTTP
// call, and its answer rides `respond` frames. respond does not consume the
// session seq space (seq 0); its delivery proof is the host's cumulative ack,
// so the relay deadline owns a lost answer.

// relayTimeout bounds one relayed local call. It is shorter than the hub's own
// RelayTimeout so this side fails first and answers with a real error.
const relayTimeout = 40 * time.Minute

// relayChunkSize is the raw body bytes carried by one chunked `respond`
// frame. A 28MB answer as a single frame would hold the transport's write lock
// for tens of seconds, blocking every event and heartbeat behind it. At ~1MB
// per chunk (≈1.37MB base64) the writer releases the lock between frames.
const relayChunkSize = 1 << 20

// serveRelay answers one `request` frame. Duplicates on a same-epoch resume
// must not re-execute — a prompt is not idempotent — so the memo either waits
// for the in-flight run and resends its result, resends a cached answer, or
// refuses.
func (c *Client) serveRelay(s *session, f wire.Frame) {
	e, owner := c.memo.begin(f.ReqID)
	if owner {
		go func() {
			status, body := c.executeRelay(s, f.Method, f.Path, f.Body)
			c.memo.finish(e, status, body)
			c.deliverRelay(f.ReqID, status, body)
		}()
		return
	}
	go func() {
		select {
		case <-e.done:
		case <-s.stopCh:
			return
		case <-s.ctx.Done():
			return
		}
		if status, body, ok := c.memo.response(e); ok {
			c.deliverRelay(f.ReqID, status, body)
			return
		}
		c.deliverRelay(f.ReqID, http.StatusConflict, mustJSON(map[string]any{
			"ok": false, "error": "duplicate request",
		}))
	}()
}

// deliverRelay sends one finished answer on the session that is live NOW, not
// the one that received the request. The execution deliberately outlives its
// session, and the hub proved delivery of the request frame (it will not
// retransmit it), so answering the dead link would strand the browser for the
// whole relay deadline. With no live session the answer is dropped: respond is
// not in the session seq log and the relay deadline owns its loss (§5.4).
func (c *Client) deliverRelay(reqID string, status int, body json.RawMessage) bool {
	s := c.currentSession()
	if s == nil {
		log.Printf("[hub-client] 请求已执行但当前无会话，响应丢失 reqId=%s（由中继超时接管）", reqID)
		return false
	}
	s.respond(reqID, status, body)
	return true
}

// executeRelay runs one relayed request against the local HTTP API and returns
// what to answer with. The call is deliberately NOT bound to the session: a
// reconnect must not cancel an in-flight prompt (its duplicate would then have
// nothing to resend and the browser would wait out the relay deadline with a
// half-run turn behind it).
//
// Execution path: in-process via c.local (server.Handler()) when wired — 同一
// 条 withAuth/withCORS 管线，无本机端口依赖；否则回退 LocalBase 的 TCP 回环
// HTTP 直调（外部装配方与旧测试）。
func (c *Client) executeRelay(s *session, method, path string, body json.RawMessage) (int, json.RawMessage) {
	limit := s.maxRelay
	if limit <= 0 {
		limit = wire.DefaultLimits.MaxRelayBody
	}
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()

	if c.local != nil {
		return c.relayInProcess(ctx, method, path, body, limit)
	}

	var rd io.Reader
	if len(body) > 0 {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.LocalBase+path, rd)
	if err != nil {
		return 500, mustJSON(map[string]any{"ok": false, "error": "invalid relay request"})
	}
	req.Header.Set("Content-Type", "application/json")
	// LocalBase 的 /api/* 已配置入站 token（FE_TOKEN）时，中继请求必须
	// 带上它，否则 hub 模式下所有 API 会 401。
	if c.cfg.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.AccessToken)
	}
	res, err := c.httpc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 504, mustJSON(map[string]any{"ok": false, "error": "本地请求超时"})
		}
		return 502, mustJSON(map[string]any{"ok": false, "error": err.Error()})
	}
	defer res.Body.Close()
	// Read one byte past the cap so an oversized body is detected instead of
	// being silently truncated: a respond frame above the hub's limits would
	// kill the whole connection.
	rb, err := io.ReadAll(io.LimitReader(res.Body, int64(limit)+1))
	if err != nil {
		return 502, mustJSON(map[string]any{"ok": false, "error": err.Error()})
	}
	if len(rb) > limit {
		return 502, mustJSON(map[string]any{"ok": false, "error": fmt.Sprintf("本地响应过大（>%dMB），无法中继", limit>>20)})
	}
	return res.StatusCode, json.RawMessage(rb)
}

// relayResponseWriter 是进程内中继的响应记录器：缓存状态码与响应体
// （超过 limits.maxRelayBody 即截断并标记——respond 帧走 host↔hub 传输，
// 上限以上的帧会触发 hub 的读上限并杀掉整条连接，与回环路径同一门槛）。
// Flush 按 no-op 处理：中继应答是单帧全量回传，流式 flush 无处可去。
type relayResponseWriter struct {
	hdr       http.Header
	buf       bytes.Buffer
	code      int
	limit     int
	truncated bool
}

func (w *relayResponseWriter) Header() http.Header { return w.hdr }

func (w *relayResponseWriter) Write(p []byte) (int, error) {
	if w.truncated {
		return len(p), nil // 继续吞掉写入，让 handler 正常走完
	}
	if w.buf.Len()+len(p) > w.limit+1 {
		w.truncated = true
		return len(p), nil
	}
	return w.buf.Write(p)
}

func (w *relayResponseWriter) WriteHeader(code int) { w.code = code }

func (w *relayResponseWriter) Flush() {}

// relayInProcess 直接调用本 host 的 API 处理链并回帧。net/http 的服务器
// 通常会 recover handler panic；进程内直调没有这层防护，这里补齐——
// panic 必须变成 500 响应，而不是击穿 hub client 的会话 goroutine。
func (c *Client) relayInProcess(ctx context.Context, method, path string, body json.RawMessage, limit int) (int, json.RawMessage) {
	var rd io.Reader
	if len(body) > 0 {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://capri-local"+path, rd)
	if err != nil {
		return 500, mustJSON(map[string]any{"ok": false, "error": "invalid relay request"})
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.AccessToken)
	}
	rec := &relayResponseWriter{hdr: http.Header{}, limit: limit}
	func() {
		defer func() {
			if p := recover(); p != nil {
				log.Printf("[hub-client] 中继处理 panic（%s %s）: %v", method, path, p)
				rec.code = http.StatusInternalServerError
				rec.truncated = false
				rec.buf.Reset()
				rec.buf.Write(mustJSON(map[string]any{"ok": false, "error": "host 内部错误"}))
			}
		}()
		c.local.ServeHTTP(rec, req)
	}()
	if rec.truncated {
		return 502, mustJSON(map[string]any{"ok": false, "error": fmt.Sprintf("本地响应过大（>%dMB），无法中继", limit>>20)})
	}
	if rec.code == 0 {
		rec.code = http.StatusOK
	}
	return rec.code, json.RawMessage(rec.buf.Bytes())
}

// respond answers a relayed request with status and body.
func (s *session) respond(reqID string, status int, body json.RawMessage) {
	// A body that is not embeddable JSON (JSONL lines, plain text — json
	// compacts RawMessage and rejects concatenated values) must not silently
	// strand the hub's pending request: wrap it as a JSON string so the relay
	// still answers instead of hanging until the browser gives up.
	if !json.Valid(body) {
		body = mustJSON(string(body))
	}
	if len(body) > relayChunkSize {
		s.respondChunked(reqID, status, body)
		return
	}
	f := wire.Frame{
		V:     wire.ProtocolVersion,
		Type:  wire.TypeRespond,
		ReqID: reqID,
		Ack:   s.c.downApplied(),
	}
	f.SetRespondStatus(status)
	if len(body) > 0 {
		f.Body = body
	}
	payload, err := wire.Encode(f)
	if err != nil {
		log.Printf("[hub-client] 中转响应编码失败 reqId=%s: %v", reqID, err)
		return
	}
	// A relay request gets exactly one respond — its terminal frame, which
	// also closes the request's stream.
	s.sendRelay(relayFrame{reqID: reqID, payload: payload, final: true})
}

// respondChunked splits an oversized body into relayChunkSize chunks and
// queues one `respond` frame per chunk. Every frame repeats reqId/status and
// carries `chunk` (base64 of that slice) + `encoding:"base64"` + `more`; the
// hub appends the decoded bytes and only resolves the browser request on the
// `more:false` frame. final is true only there, so the QUIC per-request stream
// stays open across chunks and closes exactly once.
func (s *session) respondChunked(reqID string, status int, body json.RawMessage) {
	chunks := (len(body) + relayChunkSize - 1) / relayChunkSize
	for i := 0; i < chunks; i++ {
		start := i * relayChunkSize
		end := start + relayChunkSize
		if end > len(body) {
			end = len(body)
		}
		more := i < chunks-1
		f := wire.Frame{
			V:        wire.ProtocolVersion,
			Type:     wire.TypeRespond,
			ReqID:    reqID,
			Ack:      s.c.downApplied(),
			Chunk:    base64.StdEncoding.EncodeToString(body[start:end]),
			Encoding: "base64",
			More:     more,
		}
		f.SetRespondStatus(status)
		payload, err := wire.Encode(f)
		if err != nil {
			log.Printf("[hub-client] 分块响应编码失败 reqId=%s: %v", reqID, err)
			return
		}
		s.sendRelay(relayFrame{reqID: reqID, payload: payload, final: !more})
	}
}

// ── request dedupe ────────────────────────────────────────────────────

// Bounds on the seen-request table. A duplicate only matters across a
// same-epoch reconnect, which happens within seconds; the TTL keeps a hub that
// restarted and restarted its reqId counter from inheriting our memory of the
// previous incarnation.
const (
	memoIDs        = 256
	memoTTL        = 5 * time.Minute
	memoEntryBytes = 1 << 20
	memoTotalBytes = 8 << 20
)

// relayEntry is one seen request.
type relayEntry struct {
	done     chan struct{}
	finished bool
	status   int
	body     json.RawMessage
	cached   bool
	at       time.Time
}

// relayMemo remembers requests this process has started or answered, so a
// duplicate never runs a prompt twice.
type relayMemo struct {
	mu          sync.Mutex
	order       []string
	entries     map[string]*relayEntry
	cachedBytes int
}

func newRelayMemo() *relayMemo {
	return &relayMemo{entries: make(map[string]*relayEntry)}
}

// begin returns the entry for id and reports whether the caller owns the
// execution. A duplicate gets the existing entry and must wait for done.
func (m *relayMemo) begin(id string) (*relayEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[id]; ok {
		if !e.finished || time.Since(e.at) < memoTTL {
			return e, false
		}
		// A stale entry from a hub incarnation that restarted its counter:
		// forget it and treat this id as new.
		m.cachedBytes -= len(e.body)
		delete(m.entries, id)
	}
	e := &relayEntry{done: make(chan struct{}), at: time.Now()}
	m.entries[id] = e
	m.order = append(m.order, id)
	m.evictLocked()
	return e, true
}

// finish records the answer and releases any waiters.
func (m *relayMemo) finish(e *relayEntry, status int, body json.RawMessage) {
	m.mu.Lock()
	e.finished = true
	e.status = status
	e.at = time.Now()
	if len(body) <= memoEntryBytes && m.cachedBytes+len(body) <= memoTotalBytes {
		e.body = append(json.RawMessage(nil), body...)
		e.cached = true
		m.cachedBytes += len(e.body)
	}
	m.mu.Unlock()
	close(e.done)
}

// response returns the cached answer, if it is still in memory.
func (m *relayMemo) response(e *relayEntry) (int, json.RawMessage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !e.cached {
		return 0, nil, false
	}
	return e.status, e.body, true
}

// reset forgets every seen request (a new uplink epoch cannot receive their
// duplicates).
func (m *relayMemo) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[string]*relayEntry)
	m.order = nil
	m.cachedBytes = 0
}

// evictLocked keeps the table bounded, skipping requests that are still
// running: dropping one of those would let its duplicate execute again.
func (m *relayMemo) evictLocked() {
	for len(m.entries) > memoIDs {
		removed := false
		for i, id := range m.order {
			e, ok := m.entries[id]
			if !ok {
				m.order = append(m.order[:i], m.order[i+1:]...)
				removed = true
				break
			}
			if !e.finished {
				continue
			}
			m.cachedBytes -= len(e.body)
			delete(m.entries, id)
			m.order = append(m.order[:i], m.order[i+1:]...)
			removed = true
			break
		}
		if !removed {
			return
		}
	}
}
