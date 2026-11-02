// Package hub implements the Capri-host side of the hub relay: pairing with
// capri-hub (pairing code → token), forwarding local bridge events over
// WebSocket or QUIC with the v2 session protocol, and serving relayed browser
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
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"

	"github.com/AgentsHarness/capri-hub/wire"
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

// ── audience / uplink filter ──────────────────────────────────────────

// setAudience installs the hub's absolute viewer state.
func (c *Client) setAudience(a wire.Audience) {
	next := make(map[string]bool, len(a.Sessions))
	for _, sid := range a.Sessions {
		if sid != "" {
			next[sid] = true
		}
	}
	c.audMu.Lock()
	prevN, prevAll, prevW := len(c.audSet), c.aud.All, c.aud.Watchers
	c.aud = a
	c.audKnown = true
	c.audSet = next
	c.audMu.Unlock()
	if prevW != a.Watchers {
		if a.Watchers == 0 {
			log.Printf("[hub-client] 无浏览器订阅，暂停事件上报（继续发送心跳）")
		} else {
			log.Printf("[hub-client] 浏览器在线 (watchers=%d)，开始上报事件", a.Watchers)
		}
	}
	if prevN != len(next) || prevAll != a.All {
		if a.All {
			log.Printf("[hub-client] 订阅页面未声明会话，上行事件不过滤")
		} else {
			log.Printf("[hub-client] 上行只发送 %d 个被查看会话的正文（其余只留序号）", len(next))
		}
	}
}

// forgetAudience drops the viewer state when the link ends: until the next
// hello_ack says otherwise, nobody is watching.
func (c *Client) forgetAudience() {
	c.audMu.Lock()
	c.audKnown = false
	c.audSet = nil
	c.audMu.Unlock()
	c.out.setPaused(true)
}

func (c *Client) watchers() int {
	c.audMu.Lock()
	defer c.audMu.Unlock()
	return c.aud.Watchers
}

// viewingFiltered reports whether events of sessionID must be replaced by a
// placeholder on the wire. Filtering only starts after the hub has sent an
// audience; a host that guessed "nobody is watching" would silently stop the
// transcript.
//
// A session that spawns a subagent stays watched through its children: the
// child transcript feeds the parent page's mini scrollback, and the page
// never declares the child id.
func (c *Client) viewingFiltered(sessionID string) bool {
	c.audMu.Lock()
	defer c.audMu.Unlock()
	if !c.audKnown || c.aud.All {
		return false
	}
	if sessionID == "" || c.audSet[sessionID] {
		return false
	}
	if parent := c.viewChildren[sessionID]; parent != "" && c.audSet[parent] {
		return false
	}
	return true
}

// noteViewingChild records a subagent child session so its stream follows
// its parent's watch state (see viewingFiltered). One level, same rule as the
// hub's guest whitelist; the table is a bounded FIFO because a long-lived host
// spawns without limit. Evicting the oldest relation is safe: the child is
// merely trimmed until the hub re-supplies it.
func (c *Client) noteViewingChild(childSessionID, subagentID, parentSessionID string) {
	if childSessionID == "" || parentSessionID == "" || childSessionID == parentSessionID {
		return
	}
	c.audMu.Lock()
	defer c.audMu.Unlock()
	c.setViewChildLocked(childSessionID, parentSessionID)
	if subagentID != "" {
		c.setViewChildLocked(subagentID, parentSessionID)
	}
}

// setViewChildLocked inserts key without letting viewChildren exceed
// viewingChildCap, evicting oldest-first. Caller holds audMu.
func (c *Client) setViewChildLocked(key, parentSessionID string) {
	if c.viewChildren == nil {
		c.viewChildren = make(map[string]string, 16)
	}
	if _, ok := c.viewChildren[key]; ok {
		c.viewChildren[key] = parentSessionID
		return
	}
	for len(c.viewChildren) >= viewingChildCap {
		old := c.viewChildOrder[0]
		c.viewChildOrder = c.viewChildOrder[1:]
		delete(c.viewChildren, old)
	}
	c.viewChildren[key] = parentSessionID
	c.viewChildOrder = append(c.viewChildOrder, key)
}

// viewingChildCap bounds the parent/child table (FIFO eviction above).
const viewingChildCap = 4096

// bulkTranscriptTypes are the session-scoped types that carry a
// conversation's body; trimUnwatched replaces them for unwatched sessions.
// Control rails and terminals stay unfiltered so a page switching in finds
// the session consistent.
var bulkTranscriptTypes = map[string]bool{
	"chunk":                true,
	"thought":              true,
	"user_chunk":           true,
	"user_message":         true,
	"text":                 true,
	"image":                true,
	"tool_call":            true,
	"tool_call_update":     true,
	"plan":                 true,
	"session_notification": true, // 未建模 kind 的 generic 载体
	"ext_notification":     true, // x.ai/* 直通载体（可能带正文）
}

// trimUnwatched replaces the body of events belonging to sessions no browser
// page displays with a seq-only placeholder, in place, so per-host seq stays
// gapless and the FE's ordered delivery is untouched. The FE rebuilds an
// unwatched session from host history on switch.
//
// It runs before the event enters the uplink log, so what a later replay sends
// is the placeholder, not the payload nobody asked for.
func (c *Client) trimUnwatched(evs []acp.Event) {
	for _, ev := range evs {
		t, _ := ev["type"].(string)
		if t == "subagent_spawned" {
			// Register the relation even when the spawn itself is kept: the
			// child's later events must follow the parent's watch state.
			c.noteSpawnEvent(ev)
		}
		if !bulkTranscriptTypes[t] {
			continue
		}
		sid := eventSessionID(ev)
		if !c.viewingFiltered(sid) {
			continue
		}
		seq := ev["seq"]
		for k := range ev {
			delete(ev, k)
		}
		ev["type"] = "trimmed"
		ev["sessionId"] = sid
		if seq != nil {
			ev["seq"] = seq
		}
	}
}

// noteSpawnEvent records a subagent_spawned event's parent/child relation
// (see noteViewingChild).
func (c *Client) noteSpawnEvent(ev acp.Event) {
	parent, _ := ev["sessionId"].(string)
	up, _ := ev["update"].(map[string]any)
	if up == nil {
		return
	}
	child, _ := up["child_session_id"].(string)
	subID, _ := up["subagent_id"].(string)
	c.noteViewingChild(child, subID, parent)
}

// eventSessionID reads an event's session id, preferring the top-level
// `sessionId` (the withSid convention) and falling back to the params copy
// older generic carriers use.
func eventSessionID(ev acp.Event) string {
	if sid, _ := ev["sessionId"].(string); sid != "" {
		return sid
	}
	if p, ok := ev["params"].(map[string]any); ok {
		if sid, _ := p["sessionId"].(string); sid != "" {
			return sid
		}
	}
	return ""
}

// ── event forwarding (bridge → uplink log) ────────────────────────────

// forwardLoop batches bridge events into the uplink log. The log is per
// process, not per session, so a disconnect never loses transcript: the next
// handshake replays from the hub's watermark.
func (c *Client) forwardLoop(ctx context.Context, bridge bridgeSource) {
	ch, unsub := bridge.Subscribe()
	defer unsub()
	batch := make([]acp.Event, 0, 32)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		// No chunk/thought text merge on the hub path: dual-path FE dedupes
		// by (hostId,seq); merging would keep the first seq and concatenate
		// text, so local SSE "a","b","c" (seq 1,2,3) would collide with hub
		// seq1 "abc". Batching (50ms / 32) still packs multiple events into
		// one frame — that is not text merge.
		evs := batch
		batch = make([]acp.Event, 0, 32)
		// Trim BEFORE the log so a later replay sends the same placeholders
		// instead of resurrecting payloads for sessions nobody is watching.
		c.trimUnwatched(evs)
		var overflow error
		for _, ev := range evs {
			raw, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if err := c.out.appendEvent(raw); err != nil {
				// The critical tail no longer fits. Keep logging the rest of
				// the batch — abandoning it would drop a turn terminal that
				// lands behind the overflow point, and the next append would
				// replace its seq with a silent placeholder — then end the
				// session so the hub replays after the reconnect.
				if overflow == nil {
					overflow = err
				}
			}
		}
		if overflow != nil && c.endSession(wire.CloseOverflow, overflow) {
			log.Printf("[hub-client] %v", overflow)
		}
		c.signal()
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			// Clone first: bridge.Broadcast hands the same map to every
			// subscriber (local SSE + hub forwardLoop). Mutating the shared
			// Event (stripFullUpdate) races with handleSSE's json.Marshal
			// and can crash the process with concurrent map write.
			ev = cloneEvent(ev)
			stripFullUpdate(ev)
			// Host registry fields only change on these transitions —
			// mirror them to the hub right away (a heartbeat) instead of
			// waiting for the next keepalive. client_request_resolved is here
			// for the DECREASE: pendingCount only drops when a request is
			// answered or times out.
			switch t, _ := ev["type"].(string); t {
			case "busy", "sessions_changed", "client_request", "client_request_resolved", "error":
				c.notifyStatus()
			}
			batch = append(batch, ev)
			if len(batch) >= 32 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
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

// ── local HTTP surface ────────────────────────────────────────────────

// listenPort returns the local HTTP port advertised to the hub: Config.Port
// when set, else the port parsed from LocalBase (http://127.0.0.1:8765).
func (c *Client) listenPort() int {
	if c.cfg.Port > 0 && c.cfg.Port <= 65535 {
		return c.cfg.Port
	}
	if c.cfg.LocalBase == "" {
		return 0
	}
	u, err := url.Parse(c.cfg.LocalBase)
	if err != nil {
		return 0
	}
	p := u.Port()
	if p == "" {
		return 0
	}
	n, err := strconv.Atoi(p)
	if err != nil || n <= 0 || n > 65535 {
		return 0
	}
	return n
}

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

// ── QUIC transport selection ──────────────────────────────────────────

// QUIC negative cache: after quicFailMax consecutive ESTABLISHMENT
// failures (dial / handshake / stream open / auth send — a session that
// ran and later dropped proves the UDP path works and does NOT count),
// Run skips QUIC for quicFailCooldown and dials WebSocket directly.
const (
	quicFailMax      = 3
	quicFailCooldown = 5 * time.Minute
)

// QUIC flapping: a session counts as "young" when it died before
// quicFlapMin. Three in a row arm quicFailCooldown of WebSocket instead.
const (
	quicFlapMax = 3
	quicFlapMin = 2 * time.Minute
)

// pickTransport chooses this reconnect attempt's transport: "quic" unless
// disabled or inside the negative-cache cooldown, else "ws". Run goroutine
// only (guards the quicFails / quicCooldownUntil fields).
func (c *Client) pickTransport() string {
	if c.cfg.DisableQUIC || time.Now().Before(c.quicCooldownUntil) {
		return TransportWS
	}
	return TransportQUIC
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

// noteQuicSessionEnd records how long a session lasted, for the flapping
// heuristic above. Run goroutine only.
func (c *Client) noteQuicSessionEnd(established bool, lived time.Duration) {
	if !established {
		return // an establishment failure is the negative cache's business
	}
	if lived >= quicFlapMin {
		// A session that carried real traffic proves the path can hold one;
		// forget earlier young deaths.
		c.quicFlapFails = 0
		return
	}
	c.quicFlapFails++
	if c.quicFlapFails == quicFlapMax {
		c.quicCooldownUntil = time.Now().Add(quicFailCooldown)
		log.Printf("[hub-client] QUIC 连续 %d 次连上即断（每次不足 %.0f 秒），%.0f 秒内改用 WebSocket：握手能通过但会话保不住，通常是出口地址漂移或 UDP 通路丢包",
			c.quicFlapFails, quicFlapMin.Seconds(), quicFailCooldown.Seconds())
	}
}

// ── QUIC TLS ──────────────────────────────────────────────────────────

// quicTLSClientConfig builds the TLS config for the QUIC host transport.
//
// Policy, in order:
//
//  1. HUB_QUIC_PIN set: skip the system CA path and require the peer's
//     leaf certificate SPKI sha256 to match the pin EXACTLY.
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
			// no verification.
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

// inflatePayload reverses deflatePayload for a downlink frame.
func inflatePayload(payload []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(payload))
	defer r.Close()
	out, err := io.ReadAll(io.LimitReader(r, 32<<20))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// encodeFrameLen writes one QUIC length prefix, flagging a compressed body.
func frameLengthPrefix(n int, compressed bool) [4]byte {
	var buf [4]byte
	v := uint32(n)
	if compressed {
		v |= deflateFlagQUIC
	}
	binary.BigEndian.PutUint32(buf[:], v)
	return buf
}
