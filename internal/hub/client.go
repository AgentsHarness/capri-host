// Package hub implements the acp-host side of the hub relay: pairing with
// acp-hub (pairing code → token), forwarding local bridge events over
// WebSocket or QUIC, and serving relayed browser requests by executing
// them against this host's local HTTP API.
package hub

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
)

// Config configures the hub client.
type Config struct {
	URL       string // hub base URL, e.g. http://hub-host:8787
	HostID    string
	HostName  string
	PairCode  string // one-time pairing code; ignored when a token exists
	Token     string // existing token (HOST_TOKEN); takes precedence
	LocalBase string // this host's local HTTP base, e.g. http://127.0.0.1:8765
	StateFile string // token persistence path (default ~/.acp-host/hub.json)
	// QUICPort is the hub's QUIC UDP port for the host transport
	// (default 8788). QUIC is tried first, WebSocket falls back.
	QUICPort int
	// DisableQUIC forces WebSocket only (tests / debugging).
	DisableQUIC bool
	// QUICHost overrides the QUIC dial target (host[:port]) — handy when
	// the hub domain resolves through a proxy/fake-ip that drops UDP.
	// Default: the URL host + QUICPort.
	QUICHost string
}

// Client forwards events to the hub and executes relayed requests against
// the local HTTP API over a single outbound WebSocket or QUIC connection.
type Client struct {
	cfg   Config
	httpc *http.Client

	stateMu sync.Mutex
	token   string

	// forwardEvents is true only while at least one browser is subscribed
	// to the hub's /ws/fe stream. The hub pushes {type:"subscribers",
	// count:N} (and hello.subscribers) so idle hosts stop uploading
	// bridge traffic; host_status heartbeats always continue.
	fwdMu         sync.Mutex
	forwardEvents bool
	fwdKnown      bool // false until the hub tells us a count

	// sendCh carries pre-marshaled frames to the active write loop.
	// Created in Run; closed only when Run exits (not per reconnect).
	sendCh chan []byte

	// Reliability: every forwarded event keeps its bridge.Broadcast seq
	// (dual-path FE dedupes by (hostId,seq)) and is kept in a bounded
	// replay buffer. After a reconnect the hub tells us its last seen
	// seq (hello.seq); buffered events after that point are re-sent so a
	// disconnect does not lose transcript.
	// lastSentSeq is the max event seq successfully put on sendCh (live
	// or replay). Used to resume after a subscribers 0→1 transition
	// without re-sending what the hub already got.
	// nextSeq tracks the max data-plane seq this process has observed
	// (bridge-assigned or fallback). Compared against hello.seq to detect
	// host process restart: hub may still hold a high LastSeq from the
	// previous process while this process restarts at 1.
	seqMu       sync.Mutex
	nextSeq     uint64
	replay      []acp.Event // ring, newest at the end, capped
	lastSentSeq uint64
}

// replayCap bounds the host-side replay buffer (events).
const replayCap = 5000

// maxFrameBytes caps any single events frame. The hub's host WS read
// limit is 32MB; keeping frames well below it means an oversized frame
// (e.g. one giant session update) is dropped with a log instead of
// killing the connection — which would otherwise livelock the resume
// (hello.seq never advances, and the same oversized frame is retried on
// every reconnect).
const maxFrameBytes = 8 << 20

// replayFrameBudget bounds a single replay frame. A resume after a long
// disconnect can carry thousands of buffered events; packing them into
// one multi-MB frame would exceed the hub read limit and repeat the
// reconnect forever. Replay is chunked to this budget instead.
const replayFrameBudget = 1 << 20

// NewClient returns a hub client. LocalBase defaults to 127.0.0.1:8765.
func NewClient(cfg Config) *Client {
	if cfg.LocalBase == "" {
		cfg.LocalBase = "http://127.0.0.1:8765"
	}
	if cfg.QUICPort == 0 {
		cfg.QUICPort = 8788
	}
	return &Client{
		cfg: cfg,
		// Generous timeout: relayed prompts can run up to 30 minutes.
		httpc: &http.Client{Timeout: 50 * time.Minute},
	}
}

// Run connects the host to the hub: pairs when no token exists, forwards
// bridge events over WebSocket, and serves relayed requests. It reconnects
// with backoff and blocks until ctx is done. Pairing failures (hub down at
// startup) retry with backoff instead of exiting permanently.
func (c *Client) Run(ctx context.Context, bridge *acp.Bridge) {
	// ensureToken failure must not be terminal: the hub may be briefly
	// unreachable at boot (network flap). Retry with the same backoff the
	// session loop uses.
	pairBackoff := time.Second
	for {
		err := c.ensureToken(ctx)
		if err == nil {
			break
		}
		log.Printf("[hub-client] 配对失败: %v（%.0fs 后重试）", err, pairBackoff.Seconds())
		select {
		case <-ctx.Done():
			return
		case <-time.After(pairBackoff):
		}
		if pairBackoff < 30*time.Second {
			pairBackoff *= 2
		}
	}
	log.Printf("[hub-client] connected to hub %s as %s (%s)", c.cfg.URL, c.cfg.HostID, c.cfg.HostName)

	c.sendCh = make(chan []byte, 256)

	// Bridge events → hub (async queue + batch; no text merge — dual-path seq).
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)

	// Token rotation: if the hub rejects our token and a pair code is
	// available, re-pair once and keep going.
	repaired := false
	backoff := time.Second
	for ctx.Err() == nil {
		// QUIC first (loss-resilient, connection-migrating), WS fallback.
		err := error(nil)
		if c.cfg.DisableQUIC {
			err = c.wsSession(ctx, bridge)
		} else {
			err = c.quicSession(ctx, bridge)
			if err != nil && !ctxDone(ctx) {
				log.Printf("[hub-client] QUIC 连接失败: %v，回退 WebSocket", err)
				err = c.wsSession(ctx, bridge)
			}
		}
		if ctx.Err() != nil {
			break
		}
		// Pause forwarding until the next hello re-arms subscribers.
		c.fwdMu.Lock()
		c.fwdKnown = false
		c.forwardEvents = false
		c.fwdMu.Unlock()

		if isAuthErr(err) && c.cfg.PairCode != "" && !repaired {
			log.Printf("[hub-client] hub 拒绝了旧 token，重新配对…")
			c.clearState()
			if perr := c.ensureToken(ctx); perr == nil {
				repaired = true
				backoff = time.Second
				continue
			} else {
				log.Printf("[hub-client] 重新配对失败: %v", perr)
			}
		}
		log.Printf("[hub-client] hub 连接断开: %v（%.0fs 后重连）", err, backoff.Seconds())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func isAuthErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	if strings.Contains(s, "401") || strings.Contains(s, "StatusPolicyViolation") {
		return true
	}
	// Match auth-specific phrases only — bare "token" is too broad
	// (e.g. "token bucket", log lines mentioning HOST_TOKEN).
	low := strings.ToLower(s)
	return strings.Contains(low, "auth failed") || strings.Contains(low, "no token")
}

func ctxDone(ctx context.Context) bool {
	return ctx.Err() != nil
}

// ── pairing ───────────────────────────────────────────────────────────

func (c *Client) ensureToken(ctx context.Context) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.cfg.Token != "" {
		c.token = c.cfg.Token
		return nil
	}
	if st := c.loadStateLocked(); st != nil && st.URL == c.cfg.URL && st.Token != "" {
		c.token = st.Token
		return nil
	}
	if c.cfg.PairCode == "" {
		return errors.New("缺少配对信息：设置 HUB_PAIR_CODE（或 HOST_TOKEN），配对码在 hub 启动日志 / GET /api/pairing 查看")
	}
	token, err := c.pair(ctx, c.cfg.PairCode)
	if err != nil {
		return err
	}
	c.token = token
	c.saveStateLocked(stateFile{URL: c.cfg.URL, HostID: c.cfg.HostID, Token: token})
	log.Printf("[hub-client] 配对成功，token 已保存到 %s", c.stateFileLocked())
	return nil
}

func (c *Client) pair(ctx context.Context, code string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"code":     code,
		"hostId":   c.cfg.HostID,
		"hostName": c.cfg.HostName,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.URL+"/api/pair", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpc.Do(req)
	if err != nil {
		return "", fmt.Errorf("无法连接 hub %s: %w", c.cfg.URL, err)
	}
	defer res.Body.Close()
	var out struct {
		OK    bool   `json:"ok"`
		Token string `json:"token"`
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&out)
	if res.StatusCode != 200 || !out.OK || out.Token == "" {
		return "", fmt.Errorf("hub 拒绝配对: %s", out.Error)
	}
	return out.Token, nil
}

// stateFile persists {url, hostId, token} so restarts skip re-pairing.
type stateFile struct {
	URL    string `json:"url"`
	HostID string `json:"hostId"`
	Token  string `json:"token"`
}

func (c *Client) stateFileLocked() string {
	if c.cfg.StateFile != "" {
		return c.cfg.StateFile
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".acp-host", "hub.json")
}

func (c *Client) loadStateLocked() *stateFile {
	path := c.stateFileLocked()
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var st stateFile
	if json.Unmarshal(b, &st) != nil {
		return nil
	}
	return &st
}

func (c *Client) saveStateLocked(st stateFile) {
	path := c.stateFileLocked()
	if path == "" {
		return
	}
	b, _ := json.Marshal(st)
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, b, 0o600)
}

// clearState forgets the persisted token so the next Run re-pairs.
func (c *Client) clearState() {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.token = ""
	if path := c.stateFileLocked(); path != "" {
		_ = os.Remove(path)
	}
}

// ── event forwarding (bridge → hub) ───────────────────────────────────

// setBrowserSubscribers enables/disables bridge→hub event upload based on
// the hub's live browser /ws/fe subscriber count.
func (c *Client) setBrowserSubscribers(count int) {
	enable := count > 0
	c.fwdMu.Lock()
	prev := c.forwardEvents
	known := c.fwdKnown
	c.forwardEvents = enable
	c.fwdKnown = true
	c.fwdMu.Unlock()
	if !known || prev != enable {
		if enable {
			log.Printf("[hub-client] 浏览器在线 (subscribers=%d)，开始上报事件", count)
		} else {
			log.Printf("[hub-client] 无浏览器订阅，暂停事件上报（保留 host_status 心跳）")
		}
	}
}

func (c *Client) forwardingEnabled() bool {
	c.fwdMu.Lock()
	defer c.fwdMu.Unlock()
	// Until the hub announces a count, stay paused so a quiet hub does
	// not attract full event traffic by default.
	return c.fwdKnown && c.forwardEvents
}

// seqAndReplay appends evs to the replay ring, keeping each event's
// own seq when present (bridge.Broadcast 分配全局序号，本地 SSE 与中继
// 同源 —— 前端双路去重依赖两侧 seq 一致)；无 seq 的事件（旧桥/内部
// 事件）才由本客户端分配兜底。定位一律按事件实际 seq 比较。
func (c *Client) seqAndReplay(evs []acp.Event) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	for _, ev := range evs {
		if s := eventSeq(ev); s == 0 {
			c.nextSeq++
			ev["seq"] = c.nextSeq
		} else if s > c.nextSeq {
			c.nextSeq = s
		}
		c.replay = append(c.replay, ev)
		if len(c.replay) > replayCap {
			n := len(c.replay) - replayCap
			c.replay = c.replay[n:]
		}
	}
}

func (c *Client) forwardLoop(ctx context.Context, bridge *acp.Bridge) {
	ch, unsub := bridge.Subscribe()
	defer unsub()
	batch := make([]acp.Event, 0, 32)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		// No chunk/thought text merge on the hub path (mergeChunkEvents
		// was removed): dual-path FE dedupes by (hostId,seq); merging
		// would keep the first seq and concatenate text, so local SSE
		// "a","b","c" (seq 1,2,3) would collide with hub seq1 "abc".
		// Batching (50ms / 32) still packs multiple events into one
		// frame — that is not text merge. Seq holes from slow-consumer
		// drops are repaired by FE gap-pull, not by coalescing.
		evs := batch
		batch = make([]acp.Event, 0, 32)
		// Always enter replay (even while browser subscribers are
		// paused) so a later 0→1 resume can catch up.
		c.seqAndReplay(evs)
		if c.forwardingEnabled() {
			c.enqueueEvents(evs)
		}
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			// Clone first: bridge.Broadcast hands the same map to every
			// subscriber (local SSE + hub forwardLoop). Mutating the shared
			// Event (stripFullUpdate) races with handleSSE's json.Marshal
			// and can crash the process with concurrent map write.
			// Always buffer (paused or not) so replay stays complete.
			ev = cloneEvent(ev)
			stripFullUpdate(ev)
			batch = append(batch, ev)
			if len(batch) >= 32 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-heartbeat.C:
			flush()
			// Always heartbeat: keeps hub registry ready flag fresh even
			// with zero browser subscribers. Control frame, not data-plane seq.
			c.enqueueHostStatus(bridge)
		}
	}
}

// eventSeq extracts an event's seq (bridge uint64 / float64 after JSON
// round-trip; also int/int64/json.Number for robustness).
func eventSeq(ev acp.Event) uint64 {
	switch s := ev["seq"].(type) {
	case uint64:
		return s
	case float64:
		return uint64(s)
	case int:
		return uint64(s)
	case int64:
		return uint64(s)
	case json.Number:
		if n, err := s.Int64(); err == nil && n >= 0 {
			return uint64(n)
		}
	}
	return 0
}

// enqueueEvents pushes a type:"events" frame onto the async send queue.
// Non-blocking: if the writer is stuck or disconnected, drop the batch
// rather than stall the bridge subscriber (replay buffer still holds it).
func (c *Client) enqueueEvents(evs []acp.Event) {
	if len(evs) == 0 || c.sendCh == nil {
		return
	}
	payload := c.marshalEventsFrame(evs)
	if payload == nil {
		return
	}
	select {
	case c.sendCh <- payload:
		c.noteLastSentSeq(evs)
	default:
		log.Printf("[hub-client] 发送队列满，丢弃 %d 条事件（重放缓冲已保留）", len(evs))
	}
}

// enqueueEventsCritical is the critical variant of enqueueEvents: it
// blocks up to 5s for a send slot (enqueueFrame's critical discipline)
// instead of dropping, so replay frames survive send-queue pressure.
func (c *Client) enqueueEventsCritical(evs []acp.Event) {
	if len(evs) == 0 || c.sendCh == nil {
		return
	}
	payload := c.marshalEventsFrame(evs)
	if payload == nil {
		return
	}
	select {
	case c.sendCh <- payload:
		c.noteLastSentSeq(evs)
	case <-time.After(5 * time.Second):
		log.Printf("[hub-client] 关键事件帧入队超时（队列满），丢弃 %d 条事件（重放缓冲已保留）", len(evs))
	}
}

// noteLastSentSeq records the max event seq successfully put on sendCh.
func (c *Client) noteLastSentSeq(evs []acp.Event) {
	var max uint64
	for _, ev := range evs {
		if s := eventSeq(ev); s > max {
			max = s
		}
	}
	if max == 0 {
		return
	}
	c.seqMu.Lock()
	if max > c.lastSentSeq {
		c.lastSentSeq = max
	}
	c.seqMu.Unlock()
}

// marshalEventsFrame builds a type:"events" frame for evs, enforcing the
// maxFrameBytes cap (an oversized frame would exceed the hub's read limit
// and kill the connection, livelocking the resume — the replay buffer
// still holds the events for a later reconnect). Returns nil when the
// frame cannot be built or must be dropped.
func (c *Client) marshalEventsFrame(evs []acp.Event) []byte {
	first := eventSeq(evs[0])
	payload, err := json.Marshal(map[string]any{
		"v":        1,
		"type":     "events",
		"seqStart": uint64(first),
		"events":   evs,
	})
	if err != nil {
		return nil
	}
	if len(payload) > maxFrameBytes {
		log.Printf("[hub-client] 事件帧过大（%d KB），丢弃 %d 条事件（重放缓冲已保留）", len(payload)>>10, len(evs))
		return nil
	}
	return payload
}

// sendReplayAfter re-sends buffered events with seq > after, in order,
// so a reconnect does not lose transcript. The backlog is packed into
// frames of at most replayFrameBudget bytes (see enqueueReplay), so a
// large resume cannot build one multi-MB frame that would exceed the
// hub's read limit. Returns the seq of the last re-sent event (0 when
// nothing was sent).
func (c *Client) sendReplayAfter(after uint64) uint64 {
	c.seqMu.Lock()
	var idx int
	// 第一个事件实际 seq > after 的位置。一律按事件自带 seq 比较。
	for idx = 0; idx < len(c.replay); idx++ {
		if eventSeq(c.replay[idx]) > after {
			break
		}
	}
	if idx >= len(c.replay) {
		c.seqMu.Unlock()
		return 0
	}
	evs := append([]acp.Event(nil), c.replay[idx:]...)
	last := eventSeq(evs[len(evs)-1])
	// Release before enqueue so noteLastSentSeq can take seqMu.
	c.seqMu.Unlock()
	c.enqueueReplay(evs)
	return last
}

// enqueueReplay packs replay events into frames of at most
// replayFrameBudget bytes (marshaling each event once for sizing) and
// enqueues them in order via the CRITICAL path — replay is reconnect
// catch-up, so frames must not be dropped when the send queue happens to
// be full (enqueueEvents would silently lose them; the hub would then
// still be missing transcript after the resume). A single event larger
// than the budget is sent alone; marshalEventsFrame drops it (with a
// log) if it exceeds maxFrameBytes.
func (c *Client) enqueueReplay(evs []acp.Event) {
	raws := make([][]byte, len(evs))
	for i, ev := range evs {
		if raw, err := json.Marshal(ev); err == nil {
			raws[i] = raw
		}
	}
	var frame []acp.Event
	size := 0
	flush := func() {
		if len(frame) == 0 {
			return
		}
		c.enqueueEventsCritical(frame)
		frame = nil
		size = 0
	}
	for i, ev := range evs {
		if len(frame) > 0 && size+len(raws[i]) > replayFrameBudget {
			flush()
		}
		frame = append(frame, ev)
		size += len(raws[i])
	}
	flush()
}

// enqueueHostStatus sends a control frame (not an events frame) so it
// does not consume the data-plane seq space shared with bridge.Broadcast
// (dual-path FE dedupes by (hostId,seq)). Shape:
//
//	{"v":1,"type":"host_status","ready":true}
//
// No seq, not stored in the replay buffer. critical=false: under pressure
// the next heartbeat will refresh the hub registry flag.
func (c *Client) enqueueHostStatus(bridge *acp.Bridge) {
	snap := bridge.Snapshot()
	payload, err := json.Marshal(map[string]any{
		"v":     1,
		"type":  "host_status",
		"ready": snap.Ready,
	})
	if err != nil {
		return
	}
	c.enqueueFrame(payload, false)
}

// handleHelloSeq resumes after hub hello.seq, or resets the seq epoch when
// this process's max observed seq is below the hub watermark (typical
// after host process restart: bridge recounts from 1 while hub still
// holds the previous process's LastSeq).
//
// Without a reset, hello would push lastSentSeq to the alien high value
// (0→1 catch-up would no-op) and hub would skip fan-out for every new
// event with seq <= old LastSeq.
func (c *Client) handleHelloSeq(hubSeq uint64) {
	c.seqMu.Lock()
	localMax := c.nextSeq
	c.seqMu.Unlock()

	// Epoch mismatch: hub remembers a higher watermark than this process
	// has ever produced. Same-process reconnect always has nextSeq >=
	// hubSeq (we assigned those seqs before disconnect).
	if hubSeq > localMax {
		log.Printf("[hub-client] seq 世代重置: hub.seq=%d > localMax=%d（host 进程重启？），请求 hub 清零并补发本地缓冲", hubSeq, localMax)
		c.enqueueSeqReset()
		// Do NOT advance lastSentSeq from the alien hubSeq. Replay
		// everything this process has buffered (after 0).
		if last := c.sendReplayAfter(0); last > 0 {
			log.Printf("[hub-client] 世代重置后补发本地缓冲 seq 1..%d", last)
		}
		return
	}

	// Same process (or hub is behind): precise resume.
	if last := c.sendReplayAfter(hubSeq); last > 0 {
		log.Printf("[hub-client] 重连补发事件 seq %d..%d", hubSeq+1, last)
	}
	// Hub is caught up through hubSeq — raise watermark so a later 0→1
	// does not re-send what hub already has. Only safe when hubSeq is in
	// this process's seq space (hubSeq <= localMax).
	c.seqMu.Lock()
	if hubSeq > c.lastSentSeq {
		c.lastSentSeq = hubSeq
	}
	c.seqMu.Unlock()
}

// enqueueSeqReset asks the hub to clear per-host LastSeq + event buffer
// so a restarted host can uplink from seq 1 again. Critical: must land
// before any subsequent events frame on the same connection.
//
//	{"v":1,"type":"seq_reset"}
func (c *Client) enqueueSeqReset() {
	payload, err := json.Marshal(map[string]any{
		"v":    1,
		"type": "seq_reset",
	})
	if err != nil {
		return
	}
	c.enqueueFrame(payload, true)
}

// drainSendCh non-blocking drains the uplink queue. Called once per new
// transport session so stale frames (and a subsequent sendReplayAfter)
// cannot double-send after reconnect.
func (c *Client) drainSendCh() {
	if c.sendCh == nil {
		return
	}
	n := 0
	for {
		select {
		case <-c.sendCh:
			n++
		default:
			if n > 0 {
				log.Printf("[hub-client] 重连前清空发送队列 %d 帧", n)
			}
			return
		}
	}
}

// enqueueFrame queues a pre-built JSON frame. critical=true blocks briefly
// so request responds are less likely to be dropped under load.
func (c *Client) enqueueFrame(payload []byte, critical bool) {
	if c.sendCh == nil {
		return
	}
	if critical {
		select {
		case c.sendCh <- payload:
		case <-time.After(5 * time.Second):
			log.Printf("[hub-client] 关键帧入队超时，丢弃")
		}
		return
	}
	select {
	case c.sendCh <- payload:
	default:
		log.Printf("[hub-client] 发送队列满，丢弃帧")
	}
}

func cloneEvent(ev acp.Event) acp.Event {
	out := make(acp.Event, len(ev))
	for k, v := range ev {
		out[k] = v
	}
	return out
}

func stripFullUpdate(ev acp.Event) {
	delete(ev, "fullUpdate")
}

// ── sessions (WebSocket / QUIC share the frame loop) ─────────────────

func (c *Client) wsURL() string {
	u := strings.TrimRight(c.cfg.URL, "/")
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	case strings.HasPrefix(u, "wss://") || strings.HasPrefix(u, "ws://"):
		// already ws
	default:
		u = "ws://" + u
	}
	return u + "/ws/host"
}

func (c *Client) hostToken() (string, error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.token == "" {
		if c.cfg.Token != "" {
			return c.cfg.Token, nil
		}
		return "", errors.New("401: no token")
	}
	return c.token, nil
}

// wsSession dials the hub WebSocket and runs the shared frame loop.
func (c *Client) wsSession(ctx context.Context, bridge *acp.Bridge) error {
	tok, err := c.hostToken()
	if err != nil {
		return err
	}
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization": []string{"Bearer " + tok},
		},
	}
	conn, resp, err := websocket.Dial(dialCtx, c.wsURL(), opts)
	if err != nil {
		if resp != nil && resp.StatusCode == 401 {
			return fmt.Errorf("401 hub ws auth failed: %w", err)
		}
		return fmt.Errorf("hub ws dial: %w", err)
	}
	conn.SetReadLimit(16 << 20)
	defer conn.Close(websocket.StatusNormalClosure, "")

	log.Printf("[hub-client] 已连接 hub（ws）")
	return c.runSession(ctx, bridge,
		func(payload []byte) error {
			// Write timeout: prefer the session ctx so cancel aborts the
			// write. Cap at 30s while the session is alive. If ctx is
			// already done, WithTimeout(ctx, …) would fail immediately
			// with a useless error — fall back to Background + 2s so a
			// final write attempt can still surface a socket error and
			// exit the write loop quickly.
			parent := ctx
			to := 30 * time.Second
			if ctx.Err() != nil {
				parent = context.Background()
				to = 2 * time.Second
			}
			wctx, cancel := context.WithTimeout(parent, to)
			defer cancel()
			return conn.Write(wctx, websocket.MessageText, payload)
		},
		func() ([]byte, error) {
			_, data, err := conn.Read(ctx)
			return data, err
		},
	)
}

// quicSession dials the hub over QUIC (UDP): one bidirectional stream,
// 4-byte length-prefixed JSON frames, first frame carries the auth token.
func (c *Client) quicSession(ctx context.Context, bridge *acp.Bridge) error {
	tok, err := c.hostToken()
	if err != nil {
		return err
	}
	host, port := c.quicAddr()
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(dialCtx, net.JoinHostPort(host, port), &tls.Config{
		InsecureSkipVerify: true, // hub QUIC uses self-signed certs
	}, &quic.Config{KeepAlivePeriod: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("hub quic dial %s: %w", host, err)
	}
	defer conn.CloseWithError(0, "")

	sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
	stream, err := conn.OpenStreamSync(sctx)
	scancel()
	if err != nil {
		return fmt.Errorf("hub quic open stream: %w", err)
	}
	defer stream.Close()

	send := func(payload []byte) error {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err := stream.Write(lenBuf[:]); err != nil {
			return err
		}
		_, err := stream.Write(payload)
		return err
	}
	recv := func() ([]byte, error) {
		var lenBuf [4]byte
		if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
			return nil, err
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n > 32<<20 {
			return nil, fmt.Errorf("frame too large: %d", n)
		}
		buf := make([]byte, n)
		_, err := io.ReadFull(stream, buf)
		return buf, err
	}

	auth, _ := json.Marshal(map[string]any{"v": 1, "type": "auth", "token": tok})
	if err := send(auth); err != nil {
		return fmt.Errorf("hub quic auth send: %w", err)
	}
	log.Printf("[hub-client] 已连接 hub（quic %s:%s）", host, port)
	return c.runSession(ctx, bridge, send, recv)
}

// quicAddr resolves the hub QUIC endpoint: HUB_QUIC_HOST override wins,
// else the URL host + QUICPort.
func (c *Client) quicAddr() (host, port string) {
	if h := strings.TrimSpace(c.cfg.QUICHost); h != "" {
		if hh, pp, err := net.SplitHostPort(h); err == nil {
			return hh, pp
		}
		return h, strconv.Itoa(c.cfg.QUICPort)
	}
	u := strings.TrimRight(c.cfg.URL, "/")
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimPrefix(u, "wss://")
	u = strings.TrimPrefix(u, "ws://")
	if i := strings.IndexByte(u, '/'); i >= 0 {
		u = u[:i]
	}
	if h, p, err := net.SplitHostPort(u); err == nil {
		return h, p
	}
	return u, strconv.Itoa(c.cfg.QUICPort)
}

// runSession runs the shared frame loop over a generic transport.
func (c *Client) runSession(ctx context.Context, bridge *acp.Bridge, send func([]byte) error, recv func() ([]byte, error)) error {
	// Drop stale uplink frames left from a previous session before the
	// write loop starts, so they cannot interleave with hello-driven
	// replay and double-send.
	c.drainSendCh()
	c.enqueueHostStatus(bridge)

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	errCh := make(chan error, 2)
	go func() { errCh <- c.writeLoop(sessCtx, send) }()
	go func() { errCh <- c.readLoop(sessCtx, recv, bridge) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		sessCancel()
		if err == nil {
			return errors.New("hub 关闭了连接")
		}
		return err
	}
}

func (c *Client) writeLoop(ctx context.Context, send func([]byte) error) error {
	// Application ping so idle NATs do not drop the socket.
	ping := time.NewTicker(10 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ping.C:
			payload, _ := json.Marshal(map[string]any{"v": 1, "type": "ping", "ts": time.Now().Unix()})
			if err := send(payload); err != nil {
				return err
			}
		case payload, ok := <-c.sendCh:
			if !ok {
				return errors.New("send channel closed")
			}
			if err := send(payload); err != nil {
				return err
			}
		}
	}
}

func (c *Client) readLoop(ctx context.Context, recv func() ([]byte, error), bridge *acp.Bridge) error {
	for {
		data, err := recv()
		if err != nil {
			return err
		}
		var msg struct {
			Type        string          `json:"type"`
			ReqID       string          `json:"reqId"`
			Method      string          `json:"method"`
			Path        string          `json:"path"`
			Body        json.RawMessage `json:"body"`
			Count       *int            `json:"count"`       // type:"subscribers"
			Subscribers *int            `json:"subscribers"` // type:"hello"
			Seq         *uint64         `json:"seq"`         // type:"hello" — hub's last seen seq
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[hub-client] 忽略无法解析的中转消息: %v", err)
			continue
		}
		switch msg.Type {
		case "hello":
			if msg.Subscribers != nil {
				c.setBrowserSubscribers(*msg.Subscribers)
			}
			// Resume against hub's last-seen seq — or reset the epoch when
			// this process cannot produce seqs that high (host restart).
			if msg.Seq != nil {
				c.handleHelloSeq(*msg.Seq)
			}
		case "subscribers":
			n := 0
			if msg.Count != nil {
				n = *msg.Count
			}
			was := c.forwardingEnabled()
			c.setBrowserSubscribers(n)
			if !was && c.forwardingEnabled() {
				// Catch up events buffered while paused (they were
				// seqAndReplay'd but never enqueued).
				c.seqMu.Lock()
				after := c.lastSentSeq
				c.seqMu.Unlock()
				if last := c.sendReplayAfter(after); last > 0 {
					log.Printf("[hub-client] 订阅恢复补发事件 seq %d..%d", after+1, last)
				}
				c.enqueueHostStatus(bridge)
			}
		case "request":
			go c.handleRelay(ctx, msg.ReqID, msg.Method, msg.Path, msg.Body)
		case "ping":
			pong, _ := json.Marshal(map[string]any{"v": 1, "type": "pong"})
			c.enqueueFrame(pong, false)
		case "pong":
			// ignore
		}
	}
}

// handleRelay executes one relayed browser request against the local HTTP
// API and posts the answer back to the hub over the WebSocket.
func (c *Client) handleRelay(ctx context.Context, reqID, method, path string, body json.RawMessage) {
	ctx, cancel := context.WithTimeout(ctx, 50*time.Minute)
	defer cancel()

	var rd io.Reader
	if len(body) > 0 {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.LocalBase+path, rd)
	if err != nil {
		c.respond(reqID, 500, mustJSON(map[string]any{"ok": false, "error": "invalid relay request"}))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return // stream dropped or shutdown; the hub already failed the request
		}
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": err.Error()}))
		return
	}
	defer res.Body.Close()
	// Read one byte past the cap so an oversized body is detected instead
	// of being silently truncated. The respond frame rides the host↔hub
	// transport, where a 16MB+ frame would exceed the hub's WS read limit
	// and kill the whole connection.
	rb, err := io.ReadAll(io.LimitReader(res.Body, 16<<20+1))
	if err != nil {
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": err.Error()}))
		return
	}
	if len(rb) > 16<<20 {
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": "本地响应过大（>16MB），无法中继"}))
		return
	}
	c.respond(reqID, res.StatusCode, json.RawMessage(rb))
}

func (c *Client) respond(reqID string, status int, body json.RawMessage) {
	payload, err := json.Marshal(map[string]any{
		"v":      1,
		"type":   "respond",
		"reqId":  reqID,
		"status": status,
		"body":   body,
	})
	if err != nil {
		return
	}
	c.enqueueFrame(payload, true)
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
