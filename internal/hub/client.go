// Package hub implements the Capri-host side of the hub relay: pairing with
// capri-hub (pairing code → token), forwarding local bridge events over
// WebSocket or QUIC with the v3 session protocol, and serving relayed browser
// requests by executing them against this host's local HTTP API.
//
// This file is the connection shell: configuration, pairing, transport
// selection, reconnect policy and the audience/filter state that outlives a
// session. The protocol itself lives in session.go (frames, outbox,
// heartbeats), transport_ws.go / transport_quic.go (the wire) and relay.go
// (the request plane).
package hub

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
	"github.com/AgentsHarness/capri-hub/wire"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures the hub client.
type Config struct {
	URL      string // hub base URL, e.g. http://hub-host:8787
	HostID   string
	HostName string
	// Port is this host's local HTTP listen port (advertised to the hub
	// so the FE can probe 127.0.0.1:<port>). 0 = omit.
	Port      int
	PairCode  string // one-time pairing code; ignored when a token exists
	Token     string // existing token (HOST_TOKEN); takes precedence
	LocalBase string // this host's local HTTP base, e.g. http://127.0.0.1:8765
	// Local 是本 host 的进程内 API 处理链（server.Handler()）。设置后
	// 中继请求直接调用，不再绕道 LocalBase 的 TCP 回环——少一个端口/
	// 监听依赖，鉴权与超时语义不变（Authorization 头照常注入，走同一
	// 条 withAuth 管线）。为 nil 时回退 LocalBase HTTP 直调（兼容旧测试
	// 与外部装配方）。
	Local http.Handler
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
	// Proxy is the HTTP(S) proxy URL for this client's own outbound HTTP and
	// WebSocket dials. Empty = direct, regardless of the process environment:
	// the hub link must honour the "Hub 代理" switch even when the agent's
	// side still inherits the process proxy.
	Proxy string
	// NoProxy is the comma-separated bypass list paired with Proxy.
	NoProxy string
}

// Client forwards events to the hub and executes relayed requests against
// the local HTTP API over a single outbound WebSocket or QUIC connection.
type Client struct {
	cfg   Config
	httpc *http.Client

	// local 非空时中继请求进程内直调（server.Handler()）；nil 回退 LocalBase。
	local http.Handler

	stateMu sync.Mutex
	token   string

	// epoch identifies this process's uplink sequence space. A reconnect with
	// the same epoch resumes; a restart gets a new one and the hub resets both
	// directions. Generated once in NewClient.
	epoch uint64

	// out is the uplink log. It outlives every session: events the bridge
	// publishes while disconnected are logged and replayed after the next
	// handshake.
	out *outbox

	// memo dedupes relayed requests across a same-epoch resume, so a prompt is
	// never executed twice. Entries are scoped by the hub's boot id: a
	// restarted hub reuses reqIds and must not inherit these answers.
	memo *relayMemo
	// relayOutbox holds finished answers until the hub confirms every chunk.
	// It is per process, not per session, so an answer outlives the link that
	// produced it; hubBoot scopes it the same way the memo is scoped.
	relayOutbox *relayOutbox

	// relayStats is the debug-only local relay latency summary exposed
	// through GET /api/hub/state; per-request detail is in [relay] logs.
	relayStats relayStats

	// wake tells the session loop the uplink log grew; statusCh tells it the
	// host's registry fields changed and a heartbeat is due now.
	wake     chan struct{}
	statusCh chan struct{}

	// Audience: the hub's absolute viewer state for this host. audSet is the
	// display set plus one level of subagent children; the relation table is
	// what lets a watched parent keep its child's transcript.
	audMu          sync.Mutex
	aud            wire.Audience
	audKnown       bool
	audSet         map[string]bool
	viewChildren   map[string]string
	viewChildOrder []string

	// sess is the live session, nil while disconnected. The answer log's wake
	// signal and the overflow path reach it here.
	sessMu sync.Mutex
	sess   *session

	// downApplied is the highest down seq this host has applied, reported in
	// hello.resume.down.
	downSeq atomic.Uint64

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

	// QUIC flapping (Run goroutine only): sessions that ESTABLISHED and then
	// died young. A handshake proves the UDP path exists, so these are not
	// establishment failures — yet a path that hands out a session and then
	// loses it (egress IP rotating under the hub's view of the flow, CGNAT
	// rebinding, a lossy relay) is just as unusable.
	quicFlapFails int

	// ── control surface (see control.go) ──────────────────────────────
	connected   atomic.Bool
	transport   atomic.Value // string: TransportQUIC | TransportWS
	connectedAt atomic.Int64 // unix nano, 0 when not connected

	// ctlMu guards sessCancel and lastErr.
	ctlMu sync.Mutex
	// sessCancel cancels the CURRENT session's context (a child of Run's
	// ctx), which is what lets Pair drop a live connection without taking
	// the whole process down. nil while no session is running.
	sessCancel context.CancelFunc
	lastErr    string

	// forceRepair is set by requestRepair and consumed once by Run's
	// reconnect loop: it means "the token changed, reconnect now".
	forceRepair atomic.Bool
	// repairCh wakes Run out of a pairing retry or reconnect backoff sleep.
	// Created in NewClient, not Run, because Pair may be called before Run
	// starts. Buffered 1: the signal is a level, not a queue.
	repairCh chan struct{}

	// name is the current display name. Deliberately NOT cfg.HostName:
	// Rename mutates it while Run, State and pair are live, and cfg is read
	// without a lock all over this file, so writing it in place would be a
	// data race — the same reason Manager builds a new Client instead of
	// retargeting cfg.URL. atomic.Value because the readers are lock-free
	// today and should stay that way.
	name atomic.Value // string
}

const (
	// healthySessionMin is how long a hub session must last to count as
	// healthy and reset the reconnect backoff (see Run).
	healthySessionMin = 60 * time.Second
	// reconnectBackoffMax caps the reconnect delay so a recovered path is
	// retried quickly instead of waiting out an escalated timer.
	reconnectBackoffMax = 10 * time.Second
)

func (c *Client) hostName() string {
	if v, ok := c.name.Load().(string); ok {
		return v
	}
	return ""
}

func NewClient(cfg Config) *Client {
	if cfg.LocalBase == "" {
		cfg.LocalBase = "http://127.0.0.1:8765"
	}
	if cfg.QUICPort == 0 {
		cfg.QUICPort = 8788
	}
	epoch, err := wire.NewEpoch()
	if err != nil {
		// crypto/rand failing is not a state this process can serve the
		// protocol in, but a non-zero timestamp keeps the invariant "epoch is
		// non-zero and per-process" instead of panicking at startup.
		epoch = uint64(time.Now().UnixNano()) | 1
		log.Printf("[hub-client] 无法生成随机 epoch（%v），使用时间戳 %d", err, epoch)
	}
	c := &Client{
		cfg:   cfg,
		local: cfg.Local,
		// Generous timeout: relayed prompts can run up to 30 minutes.
		httpc: &http.Client{
			Timeout: 50 * time.Minute,
			Transport: &http.Transport{
				// 代理只按本客户端的配置决定，不读进程环境变量：
				// 「Hub 代理」关掉时必须真直连，哪怕 host 进程还带着
				// 从 launchd / 系统代理继承来的 HTTPS_PROXY。
				Proxy: proxyFunc(cfg.Proxy, cfg.NoProxy),
			},
		},
		// Created here rather than in Run: Pair is reachable from the tray
		// and the HTTP API before Run's first iteration, and a nil channel
		// would make requestRepair's non-blocking send silently vanish.
		repairCh: make(chan struct{}, 1),
		epoch:    epoch,
		out:      newOutbox(),
		memo:     newRelayMemo(),

		relayOutbox: newRelayOutbox(),
		wake:        make(chan struct{}, 1),
		statusCh:    make(chan struct{}, 1),
	}
	c.name.Store(cfg.HostName)
	return c
}

// proxyFunc turns the configured proxy URL into the transport's Proxy
// callback. Empty URL means a nil proxy func → direct dials (never the
// environment default), which is what makes the Hub switch authoritative.
func proxyFunc(rawURL, rawNoProxy string) func(*http.Request) (*url.URL, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		log.Printf("[hub-client] 代理地址无效，直连: %v", err)
		return nil
	}
	bypass := parseNoProxy(rawNoProxy)
	return func(req *http.Request) (*url.URL, error) {
		if bypassMatches(req.URL.Hostname(), bypass) {
			return nil, nil
		}
		return u, nil
	}
}

// parseNoProxy splits NO_PROXY into host patterns. A leading dot or "*."
// means "this domain and its subdomains"; "*" matches everything.
func parseNoProxy(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		p := strings.ToLower(strings.TrimSpace(part))
		if p == "" {
			continue
		}
		if i := strings.Index(p, ":"); i >= 0 && !strings.Contains(p, "]") {
			p = p[:i]
		}
		p = strings.TrimPrefix(p, ".")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// bypassMatches reports whether host is covered by the NO_PROXY patterns.
func bypassMatches(host string, patterns []string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, p := range patterns {
		if p == "*" {
			return true
		}
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

// ── connect / reconnect ───────────────────────────────────────────────

// Run connects the host to the hub: pairs when no token exists, forwards
// bridge events, and serves relayed requests. It reconnects with backoff and
// blocks until ctx is done. Pairing failures (hub down at startup) retry with
// backoff instead of exiting permanently.
func (c *Client) Run(ctx context.Context, bridge bridgeSource) {
	// ensureToken failure must not be terminal: the hub may be briefly
	// unreachable at boot (network flap). Retry with the same backoff the
	// session loop uses.
	pairBackoff := time.Second
	for {
		err := c.ensureToken(ctx)
		if err == nil {
			break
		}
		c.setLastErr(err)
		wait := backoffJitter(pairBackoff)
		log.Printf("[hub-client] 配对失败: %v（%.1fs 后重试）", err, wait.Seconds())
		select {
		case <-ctx.Done():
			return
		case <-c.repairCh:
			// Pair() persisted a token while we were waiting — retry at
			// once. Without this a tray pairing would sit idle for up to
			// reconnectBackoffMax behind a backoff timer that has nothing
			// left to wait for.
			c.forceRepair.Store(false)
			pairBackoff = reconnectDelay(pairBackoff, 0, true)
			continue
		case <-time.After(wait):
		}
		pairBackoff = nextBackoff(pairBackoff)
	}
	c.setLastErr(nil)
	log.Printf("[hub-client] connected to hub %s as %s (%s)", c.cfg.URL, c.cfg.HostID, c.hostName())

	// Bridge events → uplink log. The log outlives sessions, so this runs for
	// the life of Run rather than per connection.
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)

	// Token rotation: if the hub rejects our token and a pair code is
	// available, re-pair once and keep going.
	repaired := false
	backoff := time.Second
	for ctx.Err() == nil {
		// Each session runs on its OWN cancellable context, a child of
		// Run's. Both transports block for the whole life of a session, so
		// this is the only handle that can end one early — it is what lets
		// Pair swap the token on a connected host instead of requiring a
		// restart. Cancelling it never affects ctx, so the checks below
		// still distinguish "session ended" from "process shutting down".
		sessCtx, sessCancel := context.WithCancel(ctx)
		c.setSessCancel(sessCancel)

		start := time.Now()
		reason, err := c.attempt(sessCtx, bridge)
		c.setSessCancel(nil)
		sessCancel()

		if ctx.Err() != nil {
			break
		}
		// A session that stayed up is evidence the hub is healthy: reset
		// the backoff. Without this, the delay only ever grows and pins at
		// reconnectBackoffMax for the rest of the process.
		backoff = reconnectDelay(backoff, time.Since(start), false)
		// The audience belongs to the connection: until the next hello_ack
		// says otherwise, nobody is watching.
		c.forgetAudience()

		// A Pair/Unpair replaced the credential: reconnect immediately on
		// the new one. Checked before the auth-retry branch below because the
		// session very likely ended by our own cancel, whose error says
		// nothing about the hub.
		if c.forceRepair.Swap(false) {
			select {
			case <-c.repairCh: // consume the paired wakeup
			default:
			}
			log.Printf("[hub-client] 配对信息已更新，立即重连")
			repaired = false
			backoff = reconnectDelay(backoff, 0, true)
			continue
		}

		c.setLastErr(err)
		wait := backoffJitter(backoff)
		policy := reconnectPolicy(reason, err)
		if policy == wire.CloseSuperseded {
			// Another connection already holds this hostId. The credential
			// is FINE: re-pairing here would delete a working token and
			// spend a pairing code every time a second host process (or a
			// fast reconnect) overlaps the old socket.
			log.Printf("[hub-client] hub: 本连接已被同 hostId 的新连接取代，立即重连（不重新配对）")
			backoff = reconnectDelay(backoff, 0, true)
			continue
		}
		if policy == wire.CloseAuth && c.cfg.PairCode != "" && !repaired {
			log.Printf("[hub-client] hub 拒绝了旧 token，重新配对…")
			c.clearState()
			if perr := c.ensureToken(ctx); perr == nil {
				repaired = true
				backoff = reconnectDelay(backoff, 0, true)
				continue
			} else {
				log.Printf("[hub-client] 重新配对失败: %v", perr)
			}
		}
		switch policy {
		case wire.CloseLiveness:
			log.Printf("[hub-client] 链路静默超时，判定半开（%.1fs 后重连）", wait.Seconds())
		case wire.CloseShutdown:
			log.Printf("[hub-client] hub 正在关闭连接（%.1fs 后重连）", wait.Seconds())
		case wire.CloseProtocol:
			log.Printf("[hub-client] hub 拒绝了握手（协议版本不匹配），请把 host 与 hub 升到同一版本（%.1fs 后重试）", wait.Seconds())
		case wire.CloseOverflow:
			log.Printf("[hub-client] 上行日志超出上限，断开重连以便按水位重放（%.1fs 后重连）", wait.Seconds())
		}
		log.Printf("[hub-client] hub 连接断开: %v（%.1fs 后重连）", describe(err), wait.Seconds())
		select {
		case <-ctx.Done():
			return
		case <-c.repairCh:
			// Paired while we were backing off: skip the remaining wait
			// and do not escalate the delay — the next attempt uses a
			// credential we have never tried.
			c.forceRepair.Store(false)
			log.Printf("[hub-client] 配对信息已更新，提前重连")
			backoff = reconnectDelay(backoff, 0, true)
		case <-time.After(wait):
			backoff = nextBackoff(backoff)
		}
	}
}

// attempt runs one connection attempt: QUIC first (loss-resilient), WebSocket
// as the fallback for an establishment failure.
func (c *Client) attempt(ctx context.Context, bridge bridgeSource) (wire.CloseReason, error) {
	if c.pickTransport() == TransportWS {
		return c.wsSession(ctx, bridge)
	}
	start := time.Now()
	established, reason, err := c.quicSession(ctx, bridge)
	if established {
		c.noteQuicOutcome(true)
		c.noteQuicSessionEnd(true, time.Since(start))
		return reason, err
	}
	if ctxDone(ctx) {
		return reason, err
	}
	c.noteQuicOutcome(false)
	log.Printf("[hub-client] QUIC 连接失败: %v，回退 WebSocket", err)
	return c.wsSession(ctx, bridge)
}

// reconnectPolicy names the verdict the reconnect loop acts on. A local close
// reason wins; otherwise a peer close carried by the error; otherwise an auth
// failure detected from the handshake (a rejected token has no close frame).
func reconnectPolicy(reason wire.CloseReason, err error) wire.CloseReason {
	if reason.Valid() && reason != wire.CloseNormal {
		return reason
	}
	if r, ok := closeReasonOf(err); ok {
		return r
	}
	if isAuthErr(err) {
		return wire.CloseAuth
	}
	return wire.CloseNormal
}

func describe(err error) string {
	if err == nil {
		return "连接已关闭"
	}
	return err.Error()
}

// reconnectDelay returns the delay before the next attempt: a healthy
// session or a freshly supplied credential resets it to 1s, otherwise it
// escalates (see nextBackoff).
func reconnectDelay(prev, lasted time.Duration, repaired bool) time.Duration {
	if repaired || lasted >= healthySessionMin {
		return time.Second
	}
	return nextBackoff(prev)
}

// nextBackoff doubles d, clamped to reconnectBackoffMax.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > reconnectBackoffMax {
		return reconnectBackoffMax
	}
	return d
}

// backoffJitter spreads one wait over [0.8d, 1.2d]. Every host behind a hub
// saw the same outage, so without jitter a hub restart makes all of them
// redial on the same tick; the spread breaks up that herd. Jitter is applied
// to the WAIT only — nextBackoff still receives the unjittered sequence, so
// the escalation (and its tests) is unchanged, and the 1.2× overshoot is
// bounded by the cap being a scheduling floor rather than a hard limit.
//
// math/rand (auto-seeded since Go 1.20) is deliberate: this is scheduling
// noise, not something an attacker should not predict.
func backoffJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return time.Duration(float64(d) * (0.8 + rand.Float64()*0.4))
}

func isAuthErr(err error) bool {
	if err == nil {
		return false
	}
	// A WS close is decided by its frame, never by its text: the library's
	// Error() renders the status constant, which every policy close shares.
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		r, ok := wire.CloseReasonFromWS(int(ce.Code), ce.Reason)
		return ok && r == wire.CloseAuth
	}
	// Handshake rejection: the upgrade is an HTTP request, so the status
	// is there to be read instead of grepped out of the error text.
	var he *wsHandshakeError
	if errors.As(err, &he) {
		return he.status == http.StatusUnauthorized
	}
	// QUIC says it in the application error code, since there is no close
	// frame to carry a reason.
	var ae *quic.ApplicationError
	if errors.As(err, &ae) {
		return uint64(ae.ErrorCode) == wire.CloseAuth.QUICCode()
	}
	if errors.Is(err, errNoToken) || errors.Is(err, errHubAuthRejected) {
		return true
	}
	// Match auth-specific phrases only — bare "token" is too broad
	// (e.g. "token bucket", log lines mentioning HOST_TOKEN).
	low := strings.ToLower(err.Error())
	return strings.Contains(low, "auth failed") || strings.Contains(low, "no token")
}

// errNoToken: this process has no credential at all (neither HOST_TOKEN nor
// a paired state file). Re-pairing is the only fix, so it must classify as
// an auth failure rather than as a transport error.
var errNoToken = errors.New("401: no token")

// errHubAuthRejected: the hub refused this host's credential on a transport
// that has no WS close frame to say so (a QUIC auth close surfaced without an
// application error code).
var errHubAuthRejected = errors.New("hub 拒绝了本机的凭据")

// wsHandshakeError is a WebSocket handshake the hub answered with an HTTP
// status instead of 101. It keeps the response so the reconnect loop can
// classify by StatusCode; the wrapped error is only for the log.
type wsHandshakeError struct {
	status int
	err    error
}

func (e *wsHandshakeError) Error() string {
	return fmt.Sprintf("%d hub ws handshake failed: %v", e.status, e.err)
}

func (e *wsHandshakeError) Unwrap() error { return e.err }

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

// pairTimeout bounds a single pairing request. c.httpc.Timeout is 50
// minutes (relayed prompts run that long), so without a call-level deadline
// a hub whose hostname resolves to a blackhole keeps Run's pairing retry
// parked inside one request — no backoff, no error, no retry. Var (not
// const) so tests can shrink it.
var pairTimeout = 15 * time.Second

func (c *Client) pair(ctx context.Context, code string) (string, error) {
	pctx, cancel := context.WithTimeout(ctx, pairTimeout)
	defer cancel()
	payload := map[string]any{
		"code":     code,
		"hostId":   c.cfg.HostID,
		"hostName": c.hostName(),
	}
	if p := c.listenPort(); p > 0 {
		payload["port"] = p
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(pctx, "POST", c.cfg.URL+"/api/pair", bytes.NewReader(body))
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

// stateFile persists {url, hostId, token} so restarts skip re-pairing. An
// alias rather than a second declaration: the same file is read by the GUI
// supervisors, and hubstate is where that shared shape is defined.
type stateFile = hubstate.StoredToken

func (c *Client) stateFileLocked() string {
	if c.cfg.StateFile != "" {
		return c.cfg.StateFile
	}
	return defaultStateFile()
}

// defaultStateFile is where the pairing token lives when Config.StateFile is
// unset. It defers to config so the host, both GUI supervisors and this package
// cannot disagree about the path — and so CAPRI_HOME moves all of them at once.
func defaultStateFile() string {
	return config.HubStatePath()
}

func (c *Client) loadStateLocked() *stateFile {
	return hubstate.ReadToken(c.stateFileLocked())
}

func (c *Client) saveStateLocked(st stateFile) {
	_ = writeStateFile(c.stateFileLocked(), st)
}

// writeStateFile persists a pairing at 0600 — it holds a hub credential.
func writeStateFile(path string, st stateFile) error {
	if path == "" {
		return nil
	}
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
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

// tokenLocked is the credential hello must carry.
func (c *Client) tokenLocked() string {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.token != "" {
		return c.token
	}
	return c.cfg.Token
}

func (c *Client) hostToken() (string, error) {
	tok := c.tokenLocked()
	if tok == "" {
		return "", errNoToken
	}
	return tok, nil
}

// ── session wiring ────────────────────────────────────────────────────

func (c *Client) setSession(s *session) {
	c.sessMu.Lock()
	c.sess = s
	c.sessMu.Unlock()
}

func (c *Client) currentSession() *session {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	return c.sess
}

func (c *Client) setDownApplied(n uint64) {
	c.downSeq.Store(n)
}

func (c *Client) downApplied() uint64 {
	return c.downSeq.Load()
}

// signal wakes the session loop after the uplink log grew. The channel is a
// level (buffered 1): a wake that lands while the loop is already draining
// covers the batch that just arrived.
func (c *Client) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// notifyStatus asks the session loop to re-read the host status and send a
// heartbeat when it changed.
func (c *Client) notifyStatus() {
	select {
	case c.statusCh <- struct{}{}:
	default:
	}
}

// endSession closes the live session with reason. The hub replays after the
// reconnect, so this is the overflow path out of the uplink log. It reports
// whether a session was actually running.
func (c *Client) endSession(reason wire.CloseReason, err error) bool {
	s := c.currentSession()
	if s == nil {
		return false
	}
	s.stop(reason, err)
	return true
}
