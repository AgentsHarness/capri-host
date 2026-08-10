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

	// Reliability: every forwarded event gets a monotonic seq and is kept
	// in a bounded replay buffer. After a reconnect the hub tells us its
	// last seen seq (hello.seq); buffered events after that point are
	// re-sent so a disconnect does not lose transcript.
	seqMu    sync.Mutex
	nextSeq  uint64
	replay   []acp.Event // ring, newest at the end, capped
	replayOf uint64      // seq of replay[0]; replay[i] has seq replayOf+i+1

	inflightMu sync.Mutex
	inflight   map[string]context.CancelFunc // reqId → cancel of the local HTTP call
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
		httpc:    &http.Client{Timeout: 50 * time.Minute},
		inflight: make(map[string]context.CancelFunc),
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

	// Bridge events → hub (async queue + batch + chunk merge).
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
	return strings.Contains(s, "401") || strings.Contains(s, "StatusPolicyViolation") || strings.Contains(s, "token")
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

// seqAndReplay assigns the next seqs to evs (in order) and appends them
// to the replay ring. It is called for every batch that is actually sent
// (or attempted), so wire seqs and replay seqs are identical and
// contiguous — the hub counter then tracks the host's sequence exactly.
func (c *Client) seqAndReplay(evs []acp.Event) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	for _, ev := range evs {
		c.nextSeq++
		ev["seq"] = c.nextSeq
		c.replay = append(c.replay, ev)
		if len(c.replay) > replayCap {
			n := len(c.replay) - replayCap
			c.replay = c.replay[n:]
			c.replayOf += uint64(n)
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
		if !c.forwardingEnabled() {
			// Drop queued events while nobody is listening; browsers
			// rehydrate via /api/status and session-updates on connect.
			batch = batch[:0]
			return
		}
		// Merge FIRST, then assign seqs to the merged events: seqs on the
		// wire (and in the replay buffer) are contiguous — no holes from
		// merged-away events — so the hub counter never jumps, the FE does
		// not fire spurious gap-pulls, and a reconnect replay cannot
		// duplicate text that a merged event already carried.
		evs := mergeChunkEvents(batch)
		batch = make([]acp.Event, 0, 32)
		c.seqAndReplay(evs)
		c.enqueueEvents(evs)
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
			if !c.forwardingEnabled() {
				// Discard immediately — do not buffer while paused.
				continue
			}
			// Clone first: bridge.Broadcast hands the same map to every
			// subscriber (local SSE + hub forwardLoop). Mutating the shared
			// Event (stripFullUpdate / merge) races with handleSSE's
			// json.Marshal and can crash the process with concurrent map write.
			ev = cloneEvent(ev)
			// Strip bulky wire copies FE does not use on the live path.
			stripFullUpdate(ev)
			// Seqs are assigned in flush, after merge (see flush).
			batch = append(batch, ev)
			if len(batch) >= 32 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-heartbeat.C:
			flush()
			// Always heartbeat: keeps hub registry ready flag fresh even
			// with zero browser subscribers.
			c.enqueueHostStatus(bridge)
		}
	}
}

// eventSeq extracts the seq assigned by seqAndReplay (a uint64; float64
// after a JSON round trip, e.g. in tests).
func eventSeq(ev acp.Event) uint64 {
	switch s := ev["seq"].(type) {
	case uint64:
		return s
	case float64:
		return uint64(s)
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
	first := eventSeq(evs[0])
	payload, err := json.Marshal(map[string]any{
		"v":        1,
		"type":     "events",
		"seqStart": uint64(first),
		"events":   evs,
	})
	if err != nil {
		return
	}
	// Hard cap: an oversized frame would exceed the hub's read limit and
	// kill the connection (livelocking the resume). Drop it instead — the
	// replay buffer still holds the events for a later reconnect, and the
	// next hello may still deliver them if they fit.
	if len(payload) > maxFrameBytes {
		log.Printf("[hub-client] 事件帧过大（%d KB），丢弃 %d 条事件（重放缓冲已保留）", len(payload)>>10, len(evs))
		return
	}
	select {
	case c.sendCh <- payload:
	default:
		log.Printf("[hub-client] 发送队列满，丢弃 %d 条事件（重放缓冲已保留）", len(evs))
	}
}

// sendReplayAfter re-sends buffered events with seq > after, in order,
// so a reconnect does not lose transcript. The backlog is packed into
// frames of at most replayFrameBudget bytes (see enqueueReplay), so a
// large resume cannot build one multi-MB frame that would exceed the
// hub's read limit. Returns the seq of the last re-sent event (0 when
// nothing was sent).
func (c *Client) sendReplayAfter(after uint64) uint64 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	var idx int
	// replay[i] has seq replayOf+i+1, so the first event with seq > after
	// sits at index after-replayOf (>= when after < replayOf). The `>=`
	// here matters: `>` would skip one extra event and every reconnect
	// would lose the first event after the hub's last seen seq.
	for idx = 0; idx < len(c.replay); idx++ {
		if c.replayOf+uint64(idx) >= after {
			break
		}
	}
	if idx >= len(c.replay) {
		return 0
	}
	evs := append([]acp.Event(nil), c.replay[idx:]...)
	if len(evs) == 0 {
		return 0
	}
	last := eventSeq(evs[len(evs)-1])
	c.enqueueReplay(evs)
	return last
}

// enqueueReplay packs replay events into frames of at most
// replayFrameBudget bytes (marshaling each event once for sizing) and
// enqueues them in order. A single event larger than the budget is sent
// alone; enqueueEvents drops it (with a log) if it exceeds maxFrameBytes.
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
		c.enqueueEvents(frame)
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

func (c *Client) enqueueHostStatus(bridge *acp.Bridge) {
	snap := bridge.Snapshot()
	ev := acp.Event{"type": "host_status", "ready": snap.Ready}
	// Assign a real seq like any other event: a seq-less events frame
	// would make the hub renumber from 1 and reset its per-host counter,
	// triggering a full replay (and FE re-emission) on the next reconnect.
	c.seqAndReplay([]acp.Event{ev})
	c.enqueueEvents([]acp.Event{ev})
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

// mergeChunkEvents coalesces consecutive chunk/thought events for the same
// session into one larger text event to cut WS frame volume.
func mergeChunkEvents(evs []acp.Event) []acp.Event {
	if len(evs) <= 1 {
		return evs
	}
	out := make([]acp.Event, 0, len(evs))
	var cur acp.Event
	for _, ev := range evs {
		if cur == nil {
			cur = cloneEvent(ev)
			continue
		}
		if canMergeChunk(cur, ev) {
			ta, _ := cur["text"].(string)
			tb, _ := ev["text"].(string)
			cur["text"] = ta + tb
			// Prefer the latest messageId if present.
			if mid, ok := ev["messageId"]; ok {
				cur["messageId"] = mid
			}
			continue
		}
		out = append(out, cur)
		cur = cloneEvent(ev)
	}
	if cur != nil {
		out = append(out, cur)
	}
	return out
}

func canMergeChunk(a, b acp.Event) bool {
	ta, _ := a["type"].(string)
	tb, _ := b["type"].(string)
	if ta != tb || (ta != "chunk" && ta != "thought") {
		return false
	}
	sa, _ := a["sessionId"].(string)
	sb, _ := b["sessionId"].(string)
	if sa != sb {
		return false
	}
	// Different messageIds are separate streams — do not merge.
	ma, ha := a["messageId"]
	mb, hb := b["messageId"]
	if ha || hb {
		return ha && hb && fmt.Sprint(ma) == fmt.Sprint(mb)
	}
	return true
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
			wctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
			// Resume: re-send events the hub missed while we were offline.
			if msg.Seq != nil {
				if last := c.sendReplayAfter(*msg.Seq); last > 0 {
					log.Printf("[hub-client] 重连补发事件 seq %d..%d", *msg.Seq+1, last)
				}
			}
		case "subscribers":
			n := 0
			if msg.Count != nil {
				n = *msg.Count
			}
			was := c.forwardingEnabled()
			c.setBrowserSubscribers(n)
			if !was && c.forwardingEnabled() {
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
	c.inflightMu.Lock()
	c.inflight[reqID] = cancel
	c.inflightMu.Unlock()
	defer func() {
		c.inflightMu.Lock()
		delete(c.inflight, reqID)
		c.inflightMu.Unlock()
		cancel()
	}()

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
