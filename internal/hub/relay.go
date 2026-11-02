package hub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-hub/wire"
)

// This file is the relay plane: one browser request becomes one local HTTP
// call, and its answer rides `respond` frames. respond does not consume the
// session seq space (seq 0); its delivery proof is the hub's per-request ack
// table (RelayAcks), so an answer survives the link that first carried it.

// relayTimeout bounds one relayed local call. It is shorter than the hub's own
// RelayTimeout so this side fails first and answers with a real error.
const relayTimeout = 40 * time.Minute

// relayChunkSize is the raw body bytes carried by one chunked `respond`
// frame. A 28MB answer as a single frame would hold the transport's write lock
// for tens of seconds, blocking every event and heartbeat behind it. At 256KB
// per chunk the largest body is ~112 chunks (base64 ~341KB each), well inside
// the hub's 1500-frame resume window.
const relayChunkSize = 256 << 10

// serveRelay answers one `request` frame. Duplicates on a same-epoch resume
// must not re-execute — a prompt is not idempotent — so the memo admits one
// execution and every later delivery is a no-op: the answer already lives in
// the outbox and reaches the hub from there.
func (c *Client) serveRelay(s *session, f wire.Frame) {
	if c.memo.aborted(s.hubBoot, f.ReqID) {
		log.Printf("[hub-client] hub 已放弃该请求，不再执行 reqId=%s", f.ReqID)
		return
	}
	e, owner := c.memo.begin(s.hubBoot, f.ReqID)
	if !owner {
		return
	}
	go func() {
		// An abort can race begin: the hub discarded the request between the
		// frame arriving and the execution starting.
		if !c.memo.executable(e) {
			log.Printf("[hub-client] 请求在开始执行前已被放弃 reqId=%s", e.key.reqID)
			return
		}
		status, body := c.executeRelay(s, f.Method, f.Path, f.Body)
		c.answer(e, status, body)
	}()
}

// answer queues one completed answer in the durable outbox and wakes the
// writer. The answer is not tied to the session that received the request: the
// hub acked that request and will not replay it, so the outbox is its only
// route to the browser.
func (c *Client) answer(e *relayEntry, status int, body json.RawMessage) {
	if !c.memo.executable(e) {
		log.Printf("[hub-client] 请求执行完成但已被 hub 放弃，丢弃应答 reqId=%s", e.key.reqID)
		return
	}
	frames, final := encodeRelayAnswer(c.downApplied(), e.key.reqID, status, body)
	err := c.relayOutbox.add(e.key.reqID, e.key.hubBoot, frames, final)
	switch {
	case errors.Is(err, errRelayOutboxFull):
		log.Printf("[hub-client] 中继应答日志超出上限，reqId=%s 以 502 结束", e.key.reqID)
		payload := encodeRelayFailure(c.downApplied(), e.key.reqID, "中继应答积压，已放弃本次响应")
		if err := c.relayOutbox.addError(e.key.reqID, e.key.hubBoot, payload); err != nil {
			log.Printf("[hub-client] 502 应答也无法入队 reqId=%s: %v", e.key.reqID, err)
			c.memo.settle(e.key.hubBoot, e.key.reqID, false)
		}
	case err != nil:
		log.Printf("[hub-client] 中继应答无法入队 reqId=%s: %v", e.key.reqID, err)
		c.memo.settle(e.key.hubBoot, e.key.reqID, false)
	}
	c.signalRelay()
}

// signalRelay wakes the live session's writer after the answer log changed.
// With no session the answer simply waits for the next one.
func (c *Client) signalRelay() {
	if s := c.currentSession(); s != nil {
		s.w.pump()
	}
}

// applyRelayAcks reconciles the answer log with the hub's table and settles the
// memo tombstone of every answer it released. Done and Abort are also applied
// to requests with no queued answer: an abort can arrive before the request it
// suppresses (a retransmitted request after the hub answered 503).
func (c *Client) applyRelayAcks(hubBoot uint64, acks []wire.RelayAck, handshake bool) {
	for _, d := range c.relayOutbox.reconcile(acks, handshake) {
		c.memo.settle(hubBoot, d.reqID, d.aborted)
	}
	for _, a := range acks {
		if a.Done || a.Abort {
			c.memo.settle(hubBoot, a.ReqID, a.Abort)
		}
	}
}

// respond queues one answer without a memo entry (tests and callers outside a
// request frame).
func (s *session) respond(reqID string, status int, body json.RawMessage) {
	frames, final := encodeRelayAnswer(s.c.downApplied(), reqID, status, body)
	if err := s.c.relayOutbox.add(reqID, s.hubBoot, frames, final); err != nil {
		log.Printf("[hub-client] 中转响应编码/入队失败 reqId=%s: %v", reqID, err)
	}
	s.c.signalRelay()
}

// encodeRelayAnswer renders one answer as ready-to-send respond frames. A body
// that is not embeddable JSON (JSONL lines, plain text — json compacts
// RawMessage and rejects concatenated values) is wrapped as a JSON string so
// the hub's pending request is never stranded.
func encodeRelayAnswer(ack uint64, reqID string, status int, body json.RawMessage) ([][]byte, uint64) {
	if !json.Valid(body) {
		body = mustJSON(string(body))
	}
	if len(body) > relayChunkSize {
		return encodeRelayChunks(ack, reqID, status, body)
	}
	f := wire.Frame{
		V:        wire.ProtocolVersion,
		Type:     wire.TypeRespond,
		ReqID:    reqID,
		Ack:      ack,
		ChunkSeq: 1,
	}
	f.SetRespondStatus(status)
	if len(body) > 0 {
		f.Body = body
	}
	payload, err := wire.Encode(f)
	if err != nil {
		log.Printf("[hub-client] 中转响应编码失败 reqId=%s: %v", reqID, err)
		return nil, 0
	}
	return [][]byte{payload}, 1
}

// encodeRelayChunks splits an oversized body into relayChunkSize slices. Every
// frame repeats reqId/status and carries `chunk` (base64 of that slice) +
// `encoding:"base64"` + `more`. ChunkSeq is 1-based and contiguous, so the hub
// can acknowledge exactly what it applied.
func encodeRelayChunks(ack uint64, reqID string, status int, body json.RawMessage) ([][]byte, uint64) {
	n := (len(body) + relayChunkSize - 1) / relayChunkSize
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		start := i * relayChunkSize
		end := start + relayChunkSize
		if end > len(body) {
			end = len(body)
		}
		more := i < n-1
		f := wire.Frame{
			V:        wire.ProtocolVersion,
			Type:     wire.TypeRespond,
			ReqID:    reqID,
			Ack:      ack,
			Chunk:    base64.StdEncoding.EncodeToString(body[start:end]),
			Encoding: "base64",
			More:     more,
			ChunkSeq: uint64(i + 1),
		}
		f.SetRespondStatus(status)
		payload, err := wire.Encode(f)
		if err != nil {
			log.Printf("[hub-client] 分块响应编码失败 reqId=%s: %v", reqID, err)
			return out, uint64(len(out))
		}
		out = append(out, payload)
	}
	return out, uint64(len(out))
}

// encodeRelayFailure renders the small terminal answer used when the answer log
// is over budget: the request fails, the session does not.
func encodeRelayFailure(ack uint64, reqID, reason string) []byte {
	frames, _ := encodeRelayAnswer(ack, reqID, http.StatusBadGateway, mustJSON(map[string]any{
		"ok": false, "error": reason,
	}))
	if len(frames) == 0 {
		return nil
	}
	return frames[0]
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

// ── request dedupe ────────────────────────────────────────────────────

// Bounds on the seen-request table. A duplicate only matters across a
// same-epoch reconnect, which happens within seconds; the TTL bounds how long a
// finished reqId stays a tombstone. hubBoot scopes the key, so a restarted
// hub's reused reqId can never inherit (or suppress) the previous process's
// request.
const (
	memoIDs = 256
	memoTTL = 5 * time.Minute
)

// relayKey names one request: a reqId is only unique inside one hub process.
type relayKey struct {
	hubBoot uint64
	reqID   string
}

// relayEntry is one seen request. settled is false while the request is
// executing or its answer is still waiting for the hub's confirmation; only a
// settled entry may be evicted, because evicting a live one would let its
// duplicate execute again.
type relayEntry struct {
	key     relayKey
	settled bool
	aborted bool
	at      time.Time
}

// relayMemo remembers requests this process has started or resolved, so a
// duplicate never runs a prompt twice.
type relayMemo struct {
	mu      sync.Mutex
	order   []relayKey
	entries map[relayKey]*relayEntry
}

func newRelayMemo() *relayMemo {
	return &relayMemo{entries: make(map[relayKey]*relayEntry)}
}

// begin returns the entry for (hubBoot, reqID) and reports whether the caller
// owns the execution. An existing unsettled entry — running, or answered but
// not yet confirmed — is never taken over.
func (m *relayMemo) begin(hubBoot uint64, id string) (*relayEntry, bool) {
	key := relayKey{hubBoot: hubBoot, reqID: id}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		if !e.settled || time.Since(e.at) < memoTTL {
			return e, false
		}
		// A settled tombstone older than the TTL no longer suppresses a
		// repeat: the hub has long since given up on it.
		delete(m.entries, key)
	}
	e := &relayEntry{key: key, at: time.Now()}
	m.entries[key] = e
	m.order = append(m.order, key)
	m.evictLocked()
	return e, true
}

// executable reports whether e may still run: an abort that raced begin wins.
func (m *relayMemo) executable(e *relayEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !e.aborted
}

// aborted reports whether the hub discarded reqID.
func (m *relayMemo) aborted(hubBoot uint64, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[relayKey{hubBoot: hubBoot, reqID: id}]
	return e != nil && e.aborted
}

// settle records that the answer for (hubBoot, reqID) is resolved. An abort
// also suppresses a later execution of a request the hub already returned 503
// for; it creates the tombstone when the abort arrives before the request.
func (m *relayMemo) settle(hubBoot uint64, id string, aborted bool) {
	key := relayKey{hubBoot: hubBoot, reqID: id}
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key]
	if e == nil {
		if !aborted {
			return
		}
		e = &relayEntry{key: key}
		m.entries[key] = e
		m.order = append(m.order, key)
	}
	e.settled = true
	e.aborted = e.aborted || aborted
	e.at = time.Now()
	m.evictLocked()
}

// pruneHub drops every entry of another hub incarnation. A restarted hub has
// its own reqId namespace, so nothing keyed by the old boot can ever be
// answered or deduped there.
func (m *relayMemo) pruneHub(hubBoot uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.entries {
		if key.hubBoot != hubBoot {
			delete(m.entries, key)
		}
	}
	kept := m.order[:0]
	for _, key := range m.order {
		if key.hubBoot == hubBoot {
			kept = append(kept, key)
		}
	}
	m.order = kept
}

// evictLocked keeps the table bounded, skipping entries that are still live:
// dropping one of those would let its duplicate execute again.
func (m *relayMemo) evictLocked() {
	for len(m.entries) > memoIDs {
		removed := false
		for i, key := range m.order {
			e, ok := m.entries[key]
			if !ok {
				m.order = append(m.order[:i], m.order[i+1:]...)
				removed = true
				break
			}
			if !e.settled {
				continue
			}
			delete(m.entries, key)
			m.order = append(m.order[:i], m.order[i+1:]...)
			removed = true
			break
		}
		if !removed {
			return
		}
	}
}
