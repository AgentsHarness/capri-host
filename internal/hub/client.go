// Package hub implements the capri-host side of the hub relay: pairing with
// capri-hub (pairing code → token), forwarding local bridge events over
// WebSocket or QUIC, and serving relayed browser requests by executing
// them against this host's local HTTP API.
package hub

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
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
	"sync/atomic"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
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
	// AccessToken is this host's inbound API token (FE_TOKEN). Relayed
	// requests execute against LocalBase, which now gates /api/* — the
	// client re-attaches the token so hub-mode traffic passes its own
	// gate. Empty when the host API is open.
	AccessToken string
	StateFile   string // token persistence path (default ~/.capri-host/hub.json)
	// QUICPort is the hub's QUIC UDP port for the host transport
	// (default 8788). QUIC is tried first, WebSocket falls back.
	QUICPort int
	// DisableQUIC forces WebSocket only (tests / debugging).
	DisableQUIC bool
	// QUICHost overrides the QUIC dial target (host[:port]) — handy when
	// the hub domain resolves through a proxy/fake-ip that drops UDP.
	// Default: the URL host + QUICPort.
	QUICHost string
	// QUICInsecure disables QUIC server-certificate verification
	// (HUB_QUIC_INSECURE=1). Only for a self-signed hub you control on a
	// trusted network — see quicTLSClientConfig for why the default
	// verifies whenever the hub URL is https. Prefer QUICPin, which keeps
	// verification but pins the exact certificate.
	QUICInsecure bool
	// QUICPin pins the hub's QUIC TLS certificate by the sha256 of its
	// SubjectPublicKeyInfo (HUB_QUIC_PIN, hex or base64 — see
	// parseSPKIPin). When set, the system CA path is replaced by an exact
	// SPKI fingerprint match: a self-signed hub cert can be verified
	// without trusting anyone who happens to answer on its UDP port.
	// Takes precedence over QUICInsecure and the https policy. A
	// malformed value fails the QUIC handshake (Run then falls back to
	// WebSocket).
	QUICPin string
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

	// reqCh carries relay respond frames (type:"respond") to the request
	// plane write loop. With the QUIC transport each in-flight request gets
	// a dedicated stream (T1) so a 16MB fs/read-file answer cannot
	// head-of-line block either the control stream or other requests'
	// responds; over WS everything multiplexes onto one connection. Created
	// in Run next to sendCh.
	reqCh chan reqFrame

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
	// hubAckSeq is the hub's data-plane watermark as last announced this
	// session (hello.seq, refreshed by every ping's piggy-backed "seq").
	// It anchors needCatchUp repairs more precisely than lastSentSeq:
	// lastSentSeq only proves ENQUEUE, hubAckSeq proves DELIVERY. Reset to
	// 0 per session (a restarted hub counts from zero) and never records
	// an ack naming a seq this process never produced (stale pre-reset
	// frame — see noteHubAck).
	seqMu       sync.Mutex
	nextSeq     uint64
	replay      []acp.Event // ring, newest at the end, capped
	lastSentSeq uint64
	hubAckSeq   uint64

	// enqueueMu serializes replay and live event enqueues so frames
	// reach the hub in strictly increasing seq order. Without it, a live
	// batch (higher seqs) could land between replay frames after a
	// reconnect: the hub's stale-seq gate (s <= last seen) would then
	// drop every replay event that follows — and those events are absent
	// from the hub's gap-pull buffer too, so the transcript is
	// permanently lost.
	enqueueMu sync.Mutex

	// subsGen is the highest subscriber-count generation applied (see
	// applySubscribers). Reset per session: a restarted hub counts from
	// low again.
	subsGen atomic.Uint64

	// lastRecvUnixNano is when the last downlink frame arrived, for the
	// session idle watchdog (see sessionIdleTimeout).
	lastRecvUnixNano atomic.Int64

	// needCatchUp is set when a live or replay events frame was dropped
	// before reaching the wire: the send queue was full, a replay held the
	// ordering lock (enqueueEvents TryLock missed), or a critical enqueue
	// timed out on a full queue. Those seqs may never reach the hub, so the
	// FE's gap-pull (which reads the hub's buffer) can never fill the hole
	// either — its ordered-delivery buffer then waits for a predecessor
	// that will never arrive. The heartbeat re-sends from the hub's ack
	// watermark to close the gap once the pressure passes.
	needCatchUp atomic.Bool

	// deflateOK is armed by the hub's hello echo ("deflate":true) after we
	// offered compression in the QUIC auth frame / WS handshake. Until then
	// (or with an old hub) every frame goes out as raw JSON — see
	// PROTOCOL.md for the wire format.
	deflateOK atomic.Bool

	// quicDial, when non-nil, replaces quic.DialAddr inside quicSession.
	// Test seam for the connection-migration test (T6): the real session
	// logic runs unmodified over a caller-controlled UDP socket that can
	// rebind mid-session. nil in production.
	quicDial func(ctx context.Context, addr string, tlsConf *tls.Config, cfg *quic.Config) (*quic.Conn, error)

	// QUIC negative cache (Run goroutine only — see pickTransport /
	// noteQuicOutcome): consecutive ESTABLISHMENT failures and the cooldown
	// deadline after which QUIC is skipped entirely for a while.
	quicFails         int
	quicCooldownUntil time.Time
}

// replayCap bounds the host-side replay buffer (events).
const replayCap = 5000

// replayHighWater is where seqAndReplay compacts back down to replayCap
// (amortized, see seqAndReplay).
const replayHighWater = 2 * replayCap

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

// healthySessionMin is how long a hub session must last to count as
// healthy and reset the reconnect backoff (see Run).
const healthySessionMin = 60 * time.Second

// sessionIdleTimeout fails a hub session that has received NOTHING for
// this long. The hub pings every 25s and answers our 10s pings, so silence
// this long means the link is dead even though the socket still looks
// open. Without it a half-open TCP connection (NAT rebind, silent
// middlebox drop, laptop sleep) wedges the host permanently: reads block
// forever, writes keep succeeding into the kernel buffer, no error ever
// surfaces, and the reconnect loop is never entered — the host looks
// online in the hub registry while uploading nothing. The hub has always
// had the mirror-image guard (hostReadTimeout); the host had none.
const sessionIdleTimeout = 75 * time.Second

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
	c.reqCh = make(chan reqFrame, 64)

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
		// The negative cache (pickTransport) skips QUIC while in cooldown
		// so a UDP-blocked network does not pay the dial timeout on every
		// reconnect.
		err := error(nil)
		sessionStart := time.Now()
		if c.pickTransport() == "ws" {
			err = c.wsSession(ctx, bridge)
		} else {
			established, qerr := c.quicSession(ctx, bridge)
			err = qerr
			if qerr != nil && !ctxDone(ctx) {
				c.noteQuicOutcome(established)
				log.Printf("[hub-client] QUIC 连接失败: %v，回退 WebSocket", qerr)
				err = c.wsSession(ctx, bridge)
			}
		}
		if ctx.Err() != nil {
			break
		}
		// A session that stayed up is evidence the hub is healthy: reset
		// the backoff. Without this, the delay only ever grows and pins at
		// 30s for the rest of the process — so a single blip after hours of
		// uptime costs 30 seconds of downtime, and every later blip does
		// too. Only sessions shorter than the threshold keep escalating
		// (that is the case backoff exists for: a hub that rejects or drops
		// us immediately).
		if time.Since(sessionStart) >= healthySessionMin {
			backoff = time.Second
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
	return filepath.Join(home, ".capri-host", "hub.json")
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
		if len(c.replay) > replayHighWater {
			// Compact into a FRESH array rather than resliceing: Go keeps
			// the whole backing array alive for as long as any slice of it
			// is, so `c.replay = c.replay[n:]` would pin every dropped
			// Event map (session updates can be large) until the next
			// reallocation — roughly doubling steady-state memory. Compact
			// at the high-water mark so the copy stays amortized O(1).
			trimmed := make([]acp.Event, replayCap, replayHighWater)
			copy(trimmed, c.replay[len(c.replay)-replayCap:])
			c.replay = trimmed
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
			c.repairDroppedEvents()
			// Always heartbeat: keeps hub registry ready flag fresh even
			// with zero browser subscribers. Control frame, not data-plane seq.
			c.enqueueHostStatus(bridge)
		}
	}
}

// repairDroppedEvents is the heartbeat's catch-up step: a frame dropped
// before the wire (queue full / replay held the lock / critical timeout)
// left a hole the FE cannot repair on its own — those events never reached
// the hub's gap-pull buffer. Re-send from the hub's ack watermark once the
// burst has passed.
func (c *Client) repairDroppedEvents() {
	if !c.needCatchUp.Swap(false) || !c.forwardingEnabled() {
		return
	}
	c.seqMu.Lock()
	// Anchor at the DELIVERY watermark when this session has one: it covers
	// anything not provably on the hub, including frames lost between
	// enqueue and the wire; re-sent in-flight frames are deduped by the
	// hub's stale-seq gate, and the re-sent window is bounded (queue depth
	// plus one ping interval of production). Before the first ack (old
	// hub, hello not yet processed) fall back to the enqueue watermark.
	after := c.lastSentSeq
	if c.hubAckSeq > 0 {
		after = c.hubAckSeq
	}
	c.seqMu.Unlock()
	if last := c.sendReplayAfter(after); last > 0 {
		log.Printf("[hub-client] 补发被丢弃的事件 seq %d..%d", after+1, last)
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
// The enqueueMu ordering lock keeps live frames strictly after any
// in-progress replay so the hub never sees a higher seq first (see
// enqueueReplay).
func (c *Client) enqueueEvents(evs []acp.Event) {
	// Try-lock, never block: a large replay can hold the ordering lock for
	// a long time (each frame briefly blocks on a full queue). Blocking
	// here would stall forwardLoop's flush — the bridge subscriber channel
	// then fills and Broadcast drops droppable events BEFORE they reach the
	// replay ring, where no repair path can recover them. The batch is
	// already in the ring (flush ran seqAndReplay first), so dropping here
	// is safe: needCatchUp's heartbeat repair re-sends it in order.
	if !c.enqueueMu.TryLock() {
		c.needCatchUp.Store(true)
		log.Printf("[hub-client] 重放占用发送通道，丢弃 %d 条事件（重放缓冲已保留，稍后补发）", len(evs))
		return
	}
	defer c.enqueueMu.Unlock()
	c.enqueueEventsLocked(evs, false)
}

// enqueueEventsCritical is the critical variant of enqueueEvents: it
// blocks up to 5s for a send slot (enqueueFrame's critical discipline)
// instead of dropping, so replay frames survive send-queue pressure.
func (c *Client) enqueueEventsCritical(evs []acp.Event) {
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	c.enqueueEventsLocked(evs, true)
}

// enqueueEventsLocked is the shared body of enqueueEvents /
// enqueueEventsCritical; the caller must hold enqueueMu.
func (c *Client) enqueueEventsLocked(evs []acp.Event, critical bool) {
	if len(evs) == 0 || c.sendCh == nil {
		return
	}
	payload := c.marshalEventsFrame(evs)
	if payload == nil {
		return
	}
	if critical {
		select {
		case c.sendCh <- payload:
			c.noteLastSentSeq(evs)
		case <-time.After(5 * time.Second):
			// lastSentSeq was NOT advanced, so a later repair re-sends this
			// batch; without the flag nothing retriggered it until the next
			// reconnect, leaving the hub missing transcript the whole
			// session.
			c.needCatchUp.Store(true)
			log.Printf("[hub-client] 关键事件帧入队超时（队列满），丢弃 %d 条事件（重放缓冲已保留，稍后补发）", len(evs))
		}
		return
	}
	select {
	case c.sendCh <- payload:
		c.noteLastSentSeq(evs)
	default:
		// Dropped: the hub will never see these seqs, and neither will the
		// FE's gap-pull (it reads the hub's buffer). Flag a catch-up so the
		// heartbeat re-sends from lastSentSeq instead of leaving a hole the
		// FE can only wait forever on.
		c.needCatchUp.Store(true)
		log.Printf("[hub-client] 发送队列满，丢弃 %d 条事件（重放缓冲已保留，稍后补发）", len(evs))
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

// noteHubAck records the hub's data-plane watermark from a ping's
// piggy-backed "seq" ack (same meaning as hello.seq). An ack naming a seq
// this process never produced is ignored: after a seq_reset the hub counts
// from zero again, and a ping already in flight when the reset landed can
// still carry the previous epoch's high watermark — anchoring a repair
// there would skip exactly the low seqs the hub now needs.
func (c *Client) noteHubAck(seq uint64) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if seq > c.nextSeq {
		return
	}
	if seq > c.hubAckSeq {
		c.hubAckSeq = seq
	}
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
	// Ordering lock first (enqueueMu → seqMu, matching enqueueEvents):
	// held across the whole replay so no live frame can land before the
	// replay frames — see enqueueReplay for why that would lose events.
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	return c.sendReplayAfterLocked(after)
}

// sendReplayAfterLocked is sendReplayAfter's body; the caller must hold
// enqueueMu. Callers that must emit a frame IMMEDIATELY BEFORE the replay
// (seq_reset — see handleHelloSeq) hold the lock across both so nothing
// can be interleaved between them.
func (c *Client) sendReplayAfterLocked(after uint64) uint64 {
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
	c.enqueueReplayLocked(evs)
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
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	c.enqueueReplayLocked(evs)
}

// enqueueReplayLocked packs replay events into frames and enqueues them
// in order; the caller must hold enqueueMu. The lock spans the whole
// marshal + flush so no live frame can land before the replay frames:
// the hub's stale-seq gate would otherwise drop the replay events that
// arrive after higher live seqs — and they are absent from the hub's
// gap-pull buffer too, so the transcript would be permanently lost.
// Live enqueues simply wait for the lock and then land after the
// replay, in strictly increasing seq order.
func (c *Client) enqueueReplayLocked(evs []acp.Event) {
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
		// Frames are sent via the CRITICAL path — replay is reconnect
		// catch-up, so frames must not be dropped when the send queue
		// happens to be full (a dropped replay frame would leave the hub
		// missing transcript after the resume).
		c.enqueueEventsLocked(frame, true)
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
		// enqueueMu must span BOTH the seq_reset frame and the replay it
		// authorizes. Taking it only inside sendReplayAfter would let a
		// concurrent live batch (forwardLoop) slip in between, producing
		// the uplink order [seq_reset][live seq 200…][replay seq 1…]: the
		// hub zeroes its watermark, jumps to 200 on the live frame, then
		// drops the entire replay as stale — and those events are gone
		// from its gap-pull buffer too (seq_reset cleared it), so the
		// transcript is permanently lost. This is exactly the hazard the
		// enqueueMu doc describes; the reset path just wasn't covered.
		c.enqueueMu.Lock()
		c.enqueueSeqResetLocked()
		last := c.sendReplayAfterLocked(0)
		c.enqueueMu.Unlock()
		// The hub's watermark is now 0 (seq_reset cleared it); so is our
		// view of it. A ping carrying the pre-reset high ack must not
		// re-anchor repairs past the replay (noteHubAck filters it).
		c.seqMu.Lock()
		c.hubAckSeq = 0
		c.seqMu.Unlock()
		// Do NOT advance lastSentSeq from the alien hubSeq. Replay
		// everything this process has buffered (after 0).
		if last > 0 {
			log.Printf("[hub-client] 世代重置后补发本地缓冲 seq 1..%d", last)
		}
		return
	}

	// Same process (or hub is behind): precise resume.
	if last := c.sendReplayAfter(hubSeq); last > 0 {
		log.Printf("[hub-client] 重连补发事件 seq %d..%d", hubSeq+1, last)
	}
	// hello.seq doubles as this session's first delivery ack.
	c.noteHubAck(hubSeq)
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
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	c.enqueueSeqResetLocked()
}

// enqueueSeqResetLocked is enqueueSeqReset's body; the caller must hold
// enqueueMu (handleHelloSeq keeps it across the reset + the replay).
func (c *Client) enqueueSeqResetLocked() {
	payload, err := json.Marshal(map[string]any{
		"v":    1,
		"type": "seq_reset",
	})
	if err != nil {
		return
	}
	c.enqueueFrame(payload, true)
}

// drainSendCh non-blocking drains the uplink queues. Called once per new
// transport session so stale frames (and a subsequent sendReplayAfter)
// cannot double-send after reconnect.
func (c *Client) drainSendCh() {
	n := 0
	drain := func(ch chan []byte) {
		for {
			select {
			case <-ch:
				n++
			default:
				return
			}
		}
	}
	drainReq := func(ch chan reqFrame) {
		for {
			select {
			case <-ch:
				n++
			default:
				return
			}
		}
	}
	if c.sendCh != nil {
		drain(c.sendCh)
	}
	if c.reqCh != nil {
		drainReq(c.reqCh)
	}
	if n > 0 {
		log.Printf("[hub-client] 重连前清空发送队列 %d 帧", n)
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

// wsSession dials the hub WebSocket and runs the shared frame loop. Over WS
// there is a single connection, so both planes multiplex onto it (the
// request-plane writer shares the connection under a write mutex).
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
			// Offer uplink compression; armed only when the hub's hello
			// echoes "deflate":true (see PROTOCOL.md).
			"X-Hub-Deflate": []string{"1"},
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

	// coder/websocket forbids concurrent writes; both planes share the
	// connection under this mutex.
	var wmu sync.Mutex
	write := func(payload []byte) error {
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
		if c.deflateOK.Load() {
			if zp, ok := deflatePayload(payload); ok {
				wmu.Lock()
				defer wmu.Unlock()
				return conn.Write(wctx, websocket.MessageBinary, zp)
			}
		}
		wmu.Lock()
		defer wmu.Unlock()
		return conn.Write(wctx, websocket.MessageText, payload)
	}
	return c.runSession(ctx, bridge, frameTransport{
		sendControl: write,
		// WS is a single connection: both planes multiplex onto it, so the
		// per-request routing is a no-op (reqId/final ignored).
		sendRequest: func(_ string, payload []byte, _ bool) error { return write(payload) },
		recvControl: func() ([]byte, error) {
			_, data, err := conn.Read(ctx)
			return data, err
		},
	})
}

// quicSession dials the hub over QUIC (UDP): a control/event stream, a
// shared request stream, and one dedicated stream per in-flight relay
// request — all carrying 4-byte length-prefixed JSON frames. It returns
// whether the session was ESTABLISHED (dial + streams + auth send all
// succeeded — runSession then owns the outcome) so the caller's negative
// cache can distinguish "UDP path broken" from "session ran and dropped".
//
// Stream A (control/event plane), opened first: auth, events, ping/pong,
// host_status, seq_reset, the hub's hello/subscribers downlink, and the
// relayed `request` downlink. Stream B (shared request plane) is opened and
// primed up front purely as the FALLBACK: whenever a per-request stream
// cannot be opened (peer stream limit, old hub) or the in-flight cap is
// hit, respond frames ride B — or A when even B failed — which is exactly
// the WS-equivalent single-stream behavior.
//
// Per-request streams (T1): the first respond for a relay request opens a
// dedicated bidirectional stream, primed with a no-op pong, and every
// respond for that request rides the same stream until its final frame
// closes it. Frames are self-describing (JSON "type" + "reqId"), so the hub
// accepts streams in arrival order and dispatches purely by frame type —
// no stream carries a request's identity. Keeping each request's answer on
// its own stream removes QUIC flow-control head-of-line blocking between a
// 16MB relay answer (which saturates its stream's window for seconds) and
// both the control frames and other requests' small responds. See
// PROTOCOL.md.
func (c *Client) quicSession(ctx context.Context, bridge *acp.Bridge) (bool, error) {
	tok, err := c.hostToken()
	if err != nil {
		return false, err
	}
	host, port := c.quicAddr()
	dialCtx, cancel := context.WithTimeout(ctx, quicDialTimeout)
	defer cancel()
	// KeepAlivePeriod 15s: with the control plane on its own stream (T1),
	// a lazier keepalive suffices and saves mobile battery. 0-RTT is NOT
	// enabled: quic-go's early data replays the transport's 0-RTT write
	// path wholesale — there is no per-frame (hello/ping only) granularity
	// — so a replayed auth/events frame could reach the hub twice. Raw
	// JSON fallback keeps the link correct without it.
	dial := quic.DialAddr
	if c.quicDial != nil {
		// Test seam (T6 connection migration): run the session over a
		// caller-controlled UDP socket.
		dial = c.quicDial
	}
	conn, err := dial(dialCtx, net.JoinHostPort(host, port), c.quicTLSClientConfig(host),
		&quic.Config{KeepAlivePeriod: quicKeepAlive})
	if err != nil {
		return false, fmt.Errorf("hub quic dial %s: %w", host, err)
	}
	defer conn.CloseWithError(0, "")

	openStream := func() (*quic.Stream, error) {
		sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
		defer scancel()
		s, err := conn.OpenStreamSync(sctx)
		if err != nil {
			return nil, fmt.Errorf("hub quic open stream: %w", err)
		}
		return s, nil
	}
	streamA, err := openStream()
	if err != nil {
		return false, err
	}
	defer streamA.Close()
	// Stream B failure is not fatal: an older hub may cap bidirectional
	// streams at 1. It is only the shared fallback for per-request streams;
	// without it responds share stream A (WS-equivalent behavior).
	streamB, err2 := openStream()
	if err2 != nil {
		log.Printf("[hub-client] 共享请求流创建失败（%v），中转响应退回控制流", err2)
		streamB = streamA
	} else {
		defer streamB.Close()
	}

	// Auth rides stream A, uncompressed (negotiation has not happened yet).
	// The "deflate":true field offers uplink compression; the hub arms it by
	// echoing "deflate":true in its hello (see PROTOCOL.md).
	auth, _ := json.Marshal(map[string]any{"v": 1, "type": "auth", "token": tok, "deflate": true})
	if err := c.quicSendFrame(streamA, auth); err != nil {
		return false, fmt.Errorf("hub quic auth send: %w", err)
	}
	// A QUIC stream only becomes visible to the peer once a frame references
	// it — OpenStreamSync alone transmits nothing, so the hub's AcceptStream
	// would block forever and the shared request plane could never attach.
	// Prime stream B with a no-op pong (ignored by the hub's type dispatch).
	if err := c.quicSendFrame(streamB, reqStreamPrime); err != nil {
		return false, fmt.Errorf("hub quic request-plane prime: %w", err)
	}
	log.Printf("[hub-client] 已连接 hub（quic %s:%s）", host, port)

	plane := &quicReqPlane{
		conn:    conn,
		ctx:     ctx,
		shared:  streamB,
		writeFn: c.quicSendFrame,
		streams: make(map[string]*quic.Stream),
	}
	defer plane.closeAll()

	tr := frameTransport{
		sendControl: func(payload []byte) error { return c.quicSendFrame(streamA, payload) },
		sendRequest: plane.send,
		recvControl: quicRecvFrame(streamA),
	}
	if streamB != streamA {
		tr.recvRequest = quicRecvFrame(streamB)
	}
	return true, c.runSession(ctx, bridge, tr)
}

// reqStreamPrime is the no-op activation frame sent on every freshly opened
// request-plane stream (shared stream B and each per-request stream):
// OpenStreamSync transmits nothing, so without a first frame the hub's
// AcceptStream would block forever and could never attach its read loop. A
// pong is harmless — the hub's type dispatch ignores it.
var reqStreamPrime = mustJSON(map[string]any{"v": 1, "type": "pong"})

// reqStreamMax caps how many per-request QUIC streams may be open at once.
// Past the cap (or when OpenStream fails) responds fall back to the shared
// request stream: correctness first, head-of-line separation best-effort.
// A var so tests can shrink it.
var reqStreamMax = 64

// reqOpenTimeout bounds a per-request OpenStreamSync: opening must never
// wedge the request write loop — the shared fallback is one error away.
const reqOpenTimeout = 5 * time.Second

// quicReqPlane routes respond frames onto per-request QUIC streams (T1):
// one dedicated bidirectional stream per in-flight relay request, opened on
// the request's first respond, primed with a no-op pong, and closed after
// its final respond. State is scoped to one QUIC session; a plane whose
// session died simply fails its next write.
type quicReqPlane struct {
	conn    *quic.Conn
	ctx     context.Context // session ctx: open cancellation propagates
	shared  *quic.Stream    // shared fallback (stream B, or A when B failed)
	writeFn func(*quic.Stream, []byte) error
	mu      sync.Mutex
	streams map[string]*quic.Stream // reqId → dedicated stream
}

// send writes one respond frame for reqID: it opens (and primes) the
// request's dedicated stream on first use, writes to it (or to the shared
// fallback), and closes the dedicated stream after the request's final
// frame. Any write error fails the session, like every other transport
// write.
func (p *quicReqPlane) send(reqID string, payload []byte, final bool) error {
	s := p.streamFor(reqID)
	if err := p.writeFn(s, payload); err != nil {
		return err
	}
	if final {
		p.finish(reqID)
	}
	return nil
}

// streamFor returns the dedicated stream for reqID, opening one when this
// is the request's first respond. Open failure or a full cap degrades to
// the shared stream — an old hub or a stream-limit exhaustion must not
// break relay answers.
func (p *quicReqPlane) streamFor(reqID string) *quic.Stream {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.streams[reqID]; ok {
		return s
	}
	if len(p.streams) < reqStreamMax {
		if s, err := p.openLocked(); err == nil {
			p.streams[reqID] = s
			return s
		} else {
			log.Printf("[hub-client] 每请求流创建失败（%v），该请求响应走共享请求流", err)
		}
	} else {
		log.Printf("[hub-client] 在途请求流已达上限 %d，该请求响应走共享请求流", reqStreamMax)
	}
	return p.shared
}

// openLocked opens and primes one dedicated request stream; the caller
// holds p.mu.
func (p *quicReqPlane) openLocked() (*quic.Stream, error) {
	octx, cancel := context.WithTimeout(p.ctx, reqOpenTimeout)
	defer cancel()
	s, err := p.conn.OpenStreamSync(octx)
	if err != nil {
		return nil, fmt.Errorf("hub quic open request stream: %w", err)
	}
	if err := p.writeFn(s, reqStreamPrime); err != nil {
		s.Close()
		return nil, fmt.Errorf("hub quic request-stream prime: %w", err)
	}
	return s, nil
}

// finish closes reqID's dedicated stream (its final respond was written);
// the hub's read loop sees EOF and releases its side. Requests riding the
// shared stream have no entry and no cleanup.
func (p *quicReqPlane) finish(reqID string) {
	p.mu.Lock()
	s := p.streams[reqID]
	delete(p.streams, reqID)
	p.mu.Unlock()
	if s != nil {
		s.Close()
	}
}

// closeAll tears down every still-open dedicated stream (session end).
func (p *quicReqPlane) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, s := range p.streams {
		s.Close()
		delete(p.streams, id)
	}
}

// quicSendFrame writes one length-prefixed (optionally deflate-flagged)
// frame to a QUIC stream.
func (c *Client) quicSendFrame(stream *quic.Stream, payload []byte) error {
	// 写超时与 WS 路径对齐（30s）：QUIC stream.Write 在流控/拥塞窗口
	// 耗尽（对端停止 ACK）时会无限期阻塞。若无 deadline，writeLoop
	// 会静默卡死且重连永不触发——readLoop 因 hub keepalive 仍能收包
	// 而"看似正常"，整条上行通道就悄悄死掉。每次发送前设 deadline，
	// 返回前清除，避免残留影响后续复用。
	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return fmt.Errorf("hub quic set write deadline: %w", err)
	}
	defer stream.SetWriteDeadline(time.Time{})
	flag := uint32(0)
	if c.deflateOK.Load() {
		if zp, ok := deflatePayload(payload); ok {
			payload = zp
			flag = deflateFlagQUIC
		}
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload))|flag)
	if _, err := stream.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := stream.Write(payload)
	return err
}

// quicRecvFrame reads one length-prefixed frame from a QUIC stream.
func quicRecvFrame(stream *quic.Stream) func() ([]byte, error) {
	return func() ([]byte, error) {
		var lenBuf [4]byte
		if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
			return nil, err
		}
		n := binary.BigEndian.Uint32(lenBuf[:]) &^ deflateFlagQUIC
		if n > 32<<20 {
			return nil, fmt.Errorf("frame too large: %d", n)
		}
		buf := make([]byte, n)
		_, err := io.ReadFull(stream, buf)
		return buf, err
	}
}

// quicDialTimeout bounds the QUIC handshake before falling back to
// WebSocket. 3s was too tight for a lossy mobile / intercontinental path
// (the handshake needs ~2 RTT, and quic-go retries a lost Initial after
// ~1s), so a perfectly usable QUIC path was being abandoned for the
// slower TCP fallback. WS fallback is still immediate on real failure.
const quicDialTimeout = 8 * time.Second

// QUIC negative cache: after quicFailMax consecutive ESTABLISHMENT
// failures (dial / handshake / stream open / auth send — a session that
// ran and later dropped proves the UDP path works and does NOT count),
// Run skips QUIC for quicFailCooldown and dials WebSocket directly.
// Without this, a network that blocks UDP outright (corporate firewall,
// fake-ip proxy) pays the full quicDialTimeout on EVERY reconnect before
// the fallback. The cooldown lapses so a network that starts allowing UDP
// is picked back up; failing quicFailMax more times re-arms it.
const (
	quicFailMax      = 3
	quicFailCooldown = 5 * time.Minute
)

// pickTransport chooses this reconnect attempt's transport: "quic" unless
// disabled or inside the negative-cache cooldown, else "ws". Run goroutine
// only (guards the quicFails / quicCooldownUntil fields).
func (c *Client) pickTransport() string {
	if c.cfg.DisableQUIC || time.Now().Before(c.quicCooldownUntil) {
		return "ws"
	}
	return "quic"
}

// noteQuicOutcome updates the negative cache after a QUIC attempt. An
// established session resets the failure count (the path works); an
// establishment failure counts up and arms the cooldown at quicFailMax.
// Run goroutine only.
func (c *Client) noteQuicOutcome(established bool) {
	if established {
		c.quicFails = 0
		return
	}
	c.quicFails++
	if c.quicFails == quicFailMax {
		c.quicCooldownUntil = time.Now().Add(quicFailCooldown)
		log.Printf("[hub-client] QUIC 连续 %d 次建立失败，%.0f 秒内直接使用 WebSocket（跳过每次重连的握手等待）",
			c.quicFails, quicFailCooldown.Seconds())
	}
}

// quicKeepAlive is the QUIC transport keepalive interval. 15s (was 10s):
// the hub pings every 25s and answers ours, and with the control plane on
// its own stream a slightly lazier keepalive no longer risks NAT expiry
// for the data plane — meanwhile mobile clients spend less radio awake
// time.
const quicKeepAlive = 15 * time.Second

// quicTLSClientConfig builds the TLS config for the QUIC host transport.
//
// The hub deliberately REFUSES to run QUIC with a self-signed certificate
// once FE_TOKEN is set (see its quicTLSConfig), i.e. it treats a real
// certificate as a production requirement. That requirement bought nothing
// while this client dialed with InsecureSkipVerify unconditionally: anyone
// able to answer on the hub's UDP port could impersonate it, receive this
// host's token, and then drive relayed requests — which execute agent
// operations (shell, file writes) on this machine. The WebSocket path has
// always verified TLS normally via https://, so QUIC was the odd one out.
//
// Policy, in order:
//
//  1. HUB_QUIC_PIN set: skip the system CA path and require the peer's
//     leaf certificate SPKI sha256 to match the pin EXACTLY (certificate
//     pinning — the verifiable form of a self-signed hub). A mismatch (or
//     a malformed pin) fails the handshake.
//  2. HUB_QUIC_INSECURE=1 or a plain-http hub URL (localhost / lab, no
//     certificate to trust anyway): skip verification.
//  3. Otherwise (https hub): verify against the hub's DNS name.
//
// A verification failure is not fatal — Run falls back to WebSocket, which
// carries the same frames over verified TLS.
func (c *Client) quicTLSClientConfig(dialHost string) *tls.Config {
	// Must match capri-hub quicTLSConfig ALPN ("capri-hub"); an empty
	// client ALPN yields CRYPTO_ERROR "tls: no application protocol".
	conf := &tls.Config{NextProtos: []string{"capri-hub"}}
	if c.cfg.QUICPin != "" {
		conf.InsecureSkipVerify = true // CA path replaced by the pin check
		pin, err := parseSPKIPin(c.cfg.QUICPin)
		if err != nil {
			// Fail closed: a malformed pin must not silently downgrade to
			// no verification. The error surfaces as a handshake failure
			// (→ WebSocket fallback) with the reason inline.
			conf.VerifyPeerCertificate = func(_ [][]byte, _ [][]*x509.Certificate) error {
				return fmt.Errorf("HUB_QUIC_PIN 无效: %w", err)
			}
			return conf
		}
		conf.VerifyPeerCertificate = pinSPKIVerify(pin)
		return conf
	}
	if c.cfg.QUICInsecure || !strings.HasPrefix(strings.TrimSpace(c.cfg.URL), "https://") {
		conf.InsecureSkipVerify = true
		return conf
	}
	// Verify against the hub's DNS name from HUB_URL, not the dial target:
	// HUB_QUIC_HOST may legitimately point at a raw IP to bypass a
	// proxy/fake-ip resolver, and the certificate still names the domain.
	conf.ServerName = c.urlHostname()
	if conf.ServerName == "" {
		conf.ServerName = dialHost
	}
	return conf
}

// parseSPKIPin decodes an HUB_QUIC_PIN value: the sha256 digest of the hub
// certificate's DER SubjectPublicKeyInfo, as 64 hex characters or base64
// (std/URL alphabet, padded or raw — 32 bytes after decoding). A
// "sha256/" or "sha256//" prefix (RFC 7469 pinning notation) is accepted.
func parseSPKIPin(v string) ([32]byte, error) {
	var out [32]byte
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "sha256//")
	s = strings.TrimPrefix(s, "sha256/")
	s = strings.TrimSpace(s)
	if len(s) == 64 {
		if b, err := hex.DecodeString(s); err == nil {
			copy(out[:], b)
			return out, nil
		}
	}
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil && len(b) == 32 {
			copy(out[:], b)
			return out, nil
		}
	}
	return out, fmt.Errorf("无法解析（应为证书 SPKI sha256 的 64 位 hex 或 base64）: %q", v)
}

// pinSPKIVerify returns a tls.VerifyPeerCertificate callback that requires
// the peer's LEAF certificate to have exactly the pinned SPKI: it hashes
// cert.RawSubjectPublicKeyInfo (DER) with sha256 and compares all 32 bytes.
// Certificate/key rotation invalidates the pin by design — that is the
// point of pinning.
func pinSPKIVerify(pin [32]byte) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("hub 未提供证书（SPKI pin 校验失败）")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("hub 证书解析失败（SPKI pin 校验）: %w", err)
		}
		sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
		if sum != pin {
			return fmt.Errorf("hub 证书 SPKI 指纹不匹配: 实际 sha256:%s ≠ pin sha256:%s（hub 换证书了？更新 HUB_QUIC_PIN）",
				hex.EncodeToString(sum[:]), hex.EncodeToString(pin[:]))
		}
		return nil
	}
}

// urlHostname returns the hostname part of the configured hub URL.
func (c *Client) urlHostname() string {
	u := strings.TrimRight(strings.TrimSpace(c.cfg.URL), "/")
	for _, p := range []string{"https://", "http://", "wss://", "ws://"} {
		u = strings.TrimPrefix(u, p)
	}
	if i := strings.IndexByte(u, '/'); i >= 0 {
		u = u[:i]
	}
	if h, _, err := net.SplitHostPort(u); err == nil {
		return h
	}
	return u
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

// frameTransport abstracts the two uplink/downlink planes of a session.
// Both transports (WS, QUIC) produce it; runSession wires the queues and
// read loops to it.
type frameTransport struct {
	// sendControl carries events / ping / pong / host_status / seq_reset.
	sendControl func([]byte) error
	// sendRequest carries type:"respond" frames (relay answers), routed by
	// reqId: over QUIC each in-flight request rides its own stream (T1);
	// over WS (or a QUIC hub that allows no extra streams) every frame
	// multiplexes onto the shared connection. final marks a request's last
	// frame so the transport can release its per-request stream.
	sendRequest func(reqID string, payload []byte, final bool) error
	// recvControl reads downlink frames from the control plane.
	recvControl func() ([]byte, error)
	// recvRequest, when non-nil, reads downlink frames from the request
	// plane's shared stream as well (frames are dispatched by type,
	// whichever stream they arrive on).
	recvRequest func() ([]byte, error)
}

// runSession runs the shared frame loop over a generic transport: one
// control-plane write loop (with the application ping), one request-plane
// write loop, and read loops for every downlink stream. Any loop failing
// fails the session.
func (c *Client) runSession(ctx context.Context, bridge *acp.Bridge, tr frameTransport) error {
	// Drop stale uplink frames left from a previous session before the
	// write loop starts, so they cannot interleave with hello-driven
	// replay and double-send.
	c.drainSendCh()
	// Compression is armed per session by the hub's hello echo; a stale
	// flag from a previous session must not leak into this one.
	c.deflateOK.Store(false)
	// New session ⇒ new hub process possibly, whose subscriber-count
	// generation counter restarts low. Clear our high-water mark or every
	// count from a restarted hub would look stale and be ignored.
	c.subsGen.Store(0)
	// The delivery ack is per-session too: a restarted hub counts from
	// zero, and the previous session's ack says nothing about what THIS
	// hub process has seen. hello.seq re-arms it.
	c.seqMu.Lock()
	c.hubAckSeq = 0
	c.seqMu.Unlock()
	c.markRecv()
	c.enqueueHostStatus(bridge)

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	errCh := make(chan error, 5)
	go func() { errCh <- c.writeLoop(sessCtx, tr.sendControl) }()
	// Request-plane loop always runs; over WS it multiplexes onto the same
	// connection under the transport's write mutex.
	go func() { errCh <- c.writeReqLoop(sessCtx, tr.sendRequest) }()
	go func() { errCh <- c.readLoop(sessCtx, tr.recvControl, bridge) }()
	if tr.recvRequest != nil {
		go func() { errCh <- c.readLoop(sessCtx, tr.recvRequest, bridge) }()
	}
	go func() { errCh <- c.idleWatchdog(sessCtx) }()

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

// markRecv records that a frame arrived (idle watchdog liveness).
func (c *Client) markRecv() {
	c.lastRecvUnixNano.Store(time.Now().UnixNano())
}

// idleWatchdog fails the session when no frame has arrived for
// sessionIdleTimeout, so the reconnect loop can take over. See
// sessionIdleTimeout for why the transport's own error reporting is not
// enough (half-open sockets never report anything).
func (c *Client) idleWatchdog(ctx context.Context) error {
	t := time.NewTicker(sessionIdleTimeout / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			last := c.lastRecvUnixNano.Load()
			if last == 0 {
				continue
			}
			if idle := time.Since(time.Unix(0, last)); idle >= sessionIdleTimeout {
				return fmt.Errorf("hub 静默 %.0fs（无任何下行帧），判定链路已死", idle.Seconds())
			}
		}
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

// writeReqLoop is the request-plane write loop: relay respond frames only,
// no ping (the control plane keeps the connection alive). Frames carry
// their reqId so the transport can route each request to its own stream
// and close it after the request's final frame (T1).
func (c *Client) writeReqLoop(ctx context.Context, send func(reqID string, payload []byte, final bool) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-c.reqCh:
			if !ok {
				return errors.New("request send channel closed")
			}
			if err := send(f.reqID, f.payload, f.final); err != nil {
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
		// Any downlink frame (ping included) proves the link is alive.
		c.markRecv()
		var msg struct {
			Type        string          `json:"type"`
			ReqID       string          `json:"reqId"`
			HostID      string          `json:"hostId"`
			Method      string          `json:"method"`
			Path        string          `json:"path"`
			Body        json.RawMessage `json:"body"`
			Count       *int            `json:"count"`       // type:"subscribers"
			Subscribers *int            `json:"subscribers"` // type:"hello" / "ping"
			SubsGen     *uint64         `json:"subsGen"`     // type:"ping" — ordering stamp
			Gen         *uint64         `json:"gen"`         // type:"subscribers" — ordering stamp
			Seq         *uint64         `json:"seq"`         // type:"hello" — hub's last seen seq
			Deflate     *bool           `json:"deflate"`     // type:"hello" — uplink compression ack
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[hub-client] 忽略无法解析的中转消息: %v", err)
			continue
		}
		switch msg.Type {
		case "hello":
			if msg.Subscribers != nil {
				c.applySubscribers(*msg.Subscribers, 0, bridge)
			}
			// Hub echoed our compression offer (we sent "deflate":true in
			// the QUIC auth frame / X-Hub-Deflate:1 on the WS dial):
			// arm uplink flate for events/respond frames.
			if msg.Deflate != nil && *msg.Deflate {
				if c.deflateOK.CompareAndSwap(false, true) {
					log.Printf("[hub-client] 上行压缩已启用（deflate）")
				}
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
			var gen uint64
			if msg.Gen != nil {
				gen = *msg.Gen
			}
			c.applySubscribers(n, gen, bridge)
		case "request":
			go c.handleRelay(ctx, msg.ReqID, msg.HostID, msg.Method, msg.Path, msg.Body)
		case "ping":
			// The hub re-asserts the authoritative subscriber count on its
			// ping, so a `subscribers` frame lost to a write error or a
			// superseded connection self-heals within one ping interval
			// instead of leaving us paused while a browser waits on a
			// frozen page.
			if msg.Subscribers != nil {
				var gen uint64
				if msg.SubsGen != nil {
					gen = *msg.SubsGen
				}
				c.applySubscribers(*msg.Subscribers, gen, bridge)
			}
			// Data-plane delivery ack piggy-backed on the liveness ping
			// (absent on old hubs): anchors needCatchUp repairs at the
			// hub's actual watermark instead of the enqueue watermark.
			if msg.Seq != nil {
				c.noteHubAck(*msg.Seq)
			}
			pong, _ := json.Marshal(map[string]any{"v": 1, "type": "pong"})
			c.enqueueFrame(pong, false)
		case "pong":
			// ignore
		}
	}
}

// applySubscribers applies a hub subscriber-count update, ignoring frames
// that lost a race with a newer one.
//
// The count is ABSOLUTE state that decides whether we upload bridge events
// at all, and the hub writes its notifications from one goroutine per host
// — so a browser refresh (unsubscribe → 0, resubscribe → 1, microseconds
// apart) can arrive as 1 then 0. Applying the stale 0 pauses our uplink
// while host_status heartbeats keep us "online": the user's freshly
// reloaded page looks connected and never updates. `gen` is the hub's
// monotonic stamp; gen==0 means a hub too old to send one (apply as
// before). On a 0→1 transition we also replay what was buffered while
// paused.
func (c *Client) applySubscribers(count int, gen uint64, bridge *acp.Bridge) {
	if gen > 0 {
		for {
			prev := c.subsGen.Load()
			if gen <= prev {
				return // superseded by a newer count already applied
			}
			if c.subsGen.CompareAndSwap(prev, gen) {
				break
			}
		}
	}
	was := c.forwardingEnabled()
	c.setBrowserSubscribers(count)
	if !was && c.forwardingEnabled() {
		// Catch up events buffered while paused (they were seqAndReplay'd
		// but never enqueued).
		c.seqMu.Lock()
		after := c.lastSentSeq
		c.seqMu.Unlock()
		if last := c.sendReplayAfter(after); last > 0 {
			log.Printf("[hub-client] 订阅恢复补发事件 seq %d..%d", after+1, last)
		}
		c.enqueueHostStatus(bridge)
	}
}

// handleRelay executes one relayed browser request against the local HTTP
// API and posts the answer back to the hub over the WebSocket. The hub
// routes by hostId and stamps every request frame with its target; a
// frame naming another host (hub bug / stale routing) is rejected here
// instead of executing locally. Empty hostId (pre-hostId hubs) is
// tolerated for rolling upgrades — the hub is the trust boundary either
// way, so the check is defense-in-depth, not auth.
func (c *Client) handleRelay(ctx context.Context, reqID, hostID, method, path string, body json.RawMessage) {
	if hostID != "" && hostID != c.cfg.HostID {
		log.Printf("[hub-client] 拒绝非本机中转请求: target=%s self=%s %s %s", hostID, c.cfg.HostID, method, path)
		c.respond(reqID, 404, mustJSON(map[string]any{"ok": false, "error": "host 未配对"}))
		return
	}
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
	// LocalBase 的 /api/* 已配置入站 token（FE_TOKEN）时，中继请求必须
	// 带上它，否则 hub 模式下所有 API 会 401。
	if c.cfg.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.AccessToken)
	}
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
	// A body that is not embeddable JSON (JSONL lines, plain text — json
	// compacts RawMessage and rejects concatenated values) must not
	// silently strand the hub's pending request: wrap it as a JSON string
	// so the relay still answers instead of hanging until the browser
	// gives up.
	if !json.Valid(body) {
		body = mustJSON(string(body))
	}
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
	// Request plane: over QUIC each request's answer rides its own stream
	// (T1) so a large relay answer (fs/read-file can reach 16MB) never
	// head-of-line blocks the control/event frames or other requests.
	// A relay request gets exactly one respond — its terminal frame, which
	// also closes the request's stream.
	c.enqueueReqFrame(reqFrame{reqID: reqID, payload: payload, final: true})
}

// reqFrame is one queued request-plane frame: the marshaled respond body,
// the reqId it answers, and whether it is the request's final frame (the
// transport closes the request's dedicated stream after it).
type reqFrame struct {
	reqID   string
	payload []byte
	final   bool
}

// enqueueReqFrame queues a respond frame onto the request plane queue.
// Critical: request answers must not be silently dropped under load (the
// browser is blocked on them), so block briefly instead of dropping.
func (c *Client) enqueueReqFrame(f reqFrame) {
	if c.reqCh == nil {
		c.enqueueFrame(f.payload, true)
		return
	}
	select {
	case c.reqCh <- f:
	case <-time.After(5 * time.Second):
		log.Printf("[hub-client] 中转响应帧入队超时（队列满），丢弃 reqId=%s 帧", f.reqID)
	}
}

// ── uplink compression (host → hub) ───────────────────────────────────

// minCompressSize is the payload floor below which a frame is sent raw:
// flate headers + small-input overhead usually make tiny frames BIGGER, and
// the hub→FE path uses the same threshold.
const minCompressSize = 256

// deflateFlagQUIC is OR'd into the 4-byte big-endian length prefix on the
// QUIC transport to mark a flate-compressed payload (bit 31; the effective
// frame cap stays far below 2^31 so the flag bit is always free).
const deflateFlagQUIC = 1 << 31

var flateWriterPool = sync.Pool{
	// NewWriter never fails for a valid level.
	New: func() any {
		w, _ := flate.NewWriter(nil, flate.DefaultCompression)
		return w
	},
}

var flateBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// deflatePayload flate-compresses payload (RFC 1951 raw deflate, matching
// the hub→FE writer). Returns (nil, false) when compression is not worth it
// (too small, or no gain) — the caller then sends the raw JSON.
func deflatePayload(payload []byte) ([]byte, bool) {
	if len(payload) < minCompressSize {
		return nil, false
	}
	buf := flateBufPool.Get().(*bytes.Buffer)
	w := flateWriterPool.Get().(*flate.Writer)
	defer func() {
		w.Reset(nil)
		flateWriterPool.Put(w)
	}()
	buf.Reset()
	defer flateBufPool.Put(buf)
	w.Reset(buf)
	if _, err := w.Write(payload); err != nil {
		return nil, false
	}
	if err := w.Close(); err != nil {
		return nil, false
	}
	if buf.Len() >= len(payload) {
		return nil, false
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, true
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
