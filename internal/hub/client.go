// Package hub implements the Capri-host side of the hub relay: pairing with
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

	// forwardEvents is true only while at least one browser is watching
	// THIS host. The hub pushes {type:"subscribers", count:N} (and
	// hello.subscribers) where N counts the /ws/fe subscribers that receive
	// this host's stream (a browser filtered to another host, or with no
	// live browser at all, is not an audience here) so idle hosts stop
	// uploading bridge traffic; host_status heartbeats always continue.
	fwdMu         sync.Mutex
	forwardEvents bool
	fwdKnown      bool // false until the hub tells us a count

	// statusMu guards lastStatus — the host_status registry fields
	// (ready/busy/booting/pendingCount) most recently sent to the hub.
	// maybeNotifyHostStatus diffs against it so a thinking/pending/idle
	// transition reaches the hub immediately instead of on the next
	// heartbeat.
	statusMu   sync.Mutex
	lastStatus *hostStatusFields

	// sendCh carries pre-marshaled frames to the active write loop.
	// Created in Run; closed only when Run exits (not per reconnect).
	sendCh chan []byte

	// sendBudget bounds the BYTES queued on sendCh (including the frame the
	// writer is holding): the channel itself only bounds the frame count, and
	// one frame may be 8MB. Created in NewClient so it is never nil, even for
	// tests that never call Run.
	sendBudget *sendBudget

	// reqCh carries relay respond frames (type:"respond") to the request
	// plane write loop. With the QUIC transport each in-flight request gets
	// a dedicated stream (T1) so a relay answer up to the 28MB
	// maxRelayResponseBytes ceiling cannot head-of-line block either the
	// control stream or other requests' responds; over WS everything
	// multiplexes onto one connection. Created in Run next to sendCh.
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
	replay      []replayItem // ring, newest at the end, capped
	replayBytes int          // len(raw) sum of replay, guarded by seqMu
	lastSentSeq uint64
	hubAckSeq   uint64
	// pausedTrimmedThrough is the highest seq compaction actually dropped
	// while forwarding was paused (see compactReplayLocked). Once the ring's
	// oldest seq is above the hub's watermark the two can no longer be
	// bridged by a replay — the missing range is gone — so a resume must
	// start a new hub epoch with seq_reset instead (see
	// resumeAfterPausedTrimLocked). Zero means nothing was trimmed.
	pausedTrimmedThrough uint64

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
	// session idle watchdog (see defaultSessionIdleTimeout).
	lastRecvUnixNano atomic.Int64

	// idleTimeoutNanos is THIS session's downlink watchdog budget: the hub's
	// hello.idleTimeout, else the local default. An atomic rather than a
	// plain field because the watchdog goroutine reads it while readLoop
	// writes it when hello lands (the watchdog starts first — a session can
	// go silent before the hello is decoded). Reset to the default at the
	// top of every session, so a reconnect cannot inherit the previous
	// hub's (possibly longer) budget.
	idleTimeoutNanos atomic.Int64

	// needCatchUp is set when a live or replay events frame was dropped
	// before reaching the wire: the send queue was full, a replay held the
	// ordering lock (enqueueEvents TryLock missed), or a critical enqueue
	// timed out on a full queue. While it is set, live sends stop. A later
	// seq must not pass the hole: the hub's watermark would step over it
	// and the repair, which resends from that watermark, would be rejected
	// as stale. The repair runs under enqueueMu and clears the flag only
	// once it holds the lock.
	needCatchUp atomic.Bool

	// probe is hello.probe for the current session. The host_status
	// cadence follows it: 15s when the hub has a socket probe, 5s when
	// it does not (QUIC). Cleared when the session ends.
	probe atomic.Bool

	// replaySaturated remembers that this session was asked to end
	// because the unacked replay tail no longer fits. Reset when the
	// next session starts.
	replaySaturated atomic.Bool
	// replayFuseLogged keeps the disconnected-tail trim from logging
	// once per event.
	replayFuseLogged atomic.Bool
	// pausedCapLogged keeps the paused byte-gate's critical-event drop from
	// logging on every flush; the trim itself keeps running.
	pausedCapLogged atomic.Bool
	// ackedTrimLogged keeps the acked-prefix byte trim from logging on
	// every flush; those events are delivered, so the trim is routine.
	ackedTrimLogged atomic.Bool

	// ctrlCh carries frames that must not wait behind, or be dropped
	// for, the event queue: pong, state_ack, host_status. nil when a
	// test wires only sendCh; enqueueControl falls back then.
	ctrlCh chan []byte

	// ctrlDropped counts control frames dropped because ctrlCh stayed full
	// for ctrlEnqueueWait. They are not lost data (the hub retransmits an
	// unacked state_ack, the next ping brings another pong, host_status is
	// re-sent on the next beat), but the count is the evidence that the
	// writer was stalled. Never reset.
	ctrlDropped atomic.Uint64

	// dropMu guards the queue-pressure log throttle. When the uplink is
	// down or the writer is stalled, the drop paths fire thousands of
	// times (2.5k identical lines during the Oct 7 DNS outage), burying
	// everything else in the log. One line per window reports the first
	// drop; the rest are counted and summarised at window rollover.
	dropMu     sync.Mutex
	dropWindow time.Time
	dropEvents int
	dropFrames int
	dropLogged bool

	// deflateOK records whether this session compresses uplink frames. It is
	// off only for the handshake frames written before the hub's hello; the
	// hub always supports the format, so the flag is armed as soon as the
	// session starts.
	deflateOK atomic.Bool

	// viewMu guards the uplink session filter: viewAll/viewSeen/viewGen/
	// viewSIDs and the viewChildren relation table. See trimUnwatched and
	// PROTOCOL.md §6.4.
	viewMu   sync.Mutex
	viewAll  bool            // true = audience unknown, filter nothing
	viewSeen bool            // false until the hub says anything
	viewGen  uint64          // highest viewing-set generation applied
	viewSIDs map[string]bool // sessions some page is displaying
	// viewChildren maps a subagent child session (and its subagent id) to
	// its parent, so a watched parent implies a watched child without a hub
	// round trip. viewChildOrder is the insertion order for FIFO eviction.
	viewChildren   map[string]string
	viewChildOrder []string

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
	// rebinding, a lossy relay) is just as unusable, and the old code reset
	// the failure count on every connect, so it could never back off at all.
	// quicFlapFails counts young deaths since the last session that lived
	// quicFlapMin; at quicFlapMax it arms the same cooldown.
	quicFlapFails int

	// ── control surface (see control.go) ──────────────────────────────
	//
	// connected / transport / connectedAt are the link's observable state.
	// Nothing tracked them before: whether a session was up was implicit in
	// Run being parked inside a blocking wsSession/quicSession call, and the
	// chosen transport was discarded after the dial. They are set by
	// noteConnected at each transport's handshake and cleared by
	// noteDisconnected in runSession, which both transports funnel through.
	connected   atomic.Bool
	transport   atomic.Value // string: TransportQUIC | TransportWS
	connectedAt atomic.Int64 // unix nano, 0 when not connected

	// ctlMu guards sessCancel, sessCtx and lastErr.
	ctlMu sync.Mutex
	// sessCancel cancels the CURRENT session's context (a child of Run's
	// ctx), which is what lets Pair drop a live connection without taking
	// the whole process down. nil while no session is running.
	// sessCtx is that same context, so a control frame can wait for a
	// queue slot until the session ends instead of being dropped.
	sessCancel context.CancelFunc
	sessCtx    context.Context
	lastErr    string

	// forceRepair is set by requestRepair and consumed once by Run's
	// reconnect loop: it means "the token changed, reconnect now" and
	// distinguishes a deliberate re-pair from the transport failure the
	// loop's existing auth-retry branch handles.
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

// replayItem is one ring slot. seq is denormalized for watermark
// compares; raw is the marshaled Event so the map (and any large tool
// payload it referenced) can be GC'd once seqAndReplay returns. typ is the
// event type, kept so paused compaction can tell chunk-like payloads (which
// history can rebuild) from critical events (which it cannot).
type replayItem struct {
	seq uint64
	typ string
	raw json.RawMessage
}

// droppable reports whether this event may be trimmed while no browser is
// watching. Unknown/empty types are treated as critical: keeping an event is
// always safe, dropping one that has no history is not.
func (it replayItem) droppable() bool {
	return acp.IsDroppableEventType(it.typ)
}

// replayRingBytes is the byte weight of a ring slice (the marshaled event
// bytes, which are what the ring actually holds). Used to recompute
// c.replayBytes after a trim, so the counter cannot drift from the slice.
func replayRingBytes(items []replayItem) int {
	n := 0
	for _, it := range items {
		n += len(it.raw)
	}
	return n
}

// replayCap bounds the host-side replay buffer (events). 1000 critical+
// droppable events covers a hub blip; FE HTTP history covers anything
// older. Was 5000 — Event maps in the ring were the host's steady-state
// heap.
const replayCap = 1000

// replayHighWater is where seqAndReplay compacts back down to replayCap
// (amortized, see seqAndReplay). Only events the hub has already acked
// are eligible. Unacked events stay: dropping them is how a hole becomes
// permanent.
const replayHighWater = 2 * replayCap

// replayHardCap is how many unacked events the ring will hold before the
// session is torn down so a reconnect can replay them. Past this the
// link is not delivering, and continuing to append without a bound is
// how a long outage keeps every chunk.
const replayHardCap = 8 * replayCap

// pausedReplayCap is how much of the ring survives while the hub has told
// us nobody is watching this host (fwdKnown && !forwardEvents). With no
// subscriber nothing will ever be acked, so an unacked tail judged against
// replayHardCap would end the session every 8000 events — a reconnect loop
// that replays the whole ring into a hub whose fan-out drops all of it. The
// paused tail is only what a 0→1 resume needs; anything older is rebuilt
// from host history by the FE, which never promised per-event replay of
// events nobody was watching.
const pausedReplayCap = 128

// replayByteCap bounds the replay ring by memory as well as by event count:
// replayHardCap events of the 8MB frame ceiling would be 64GB, and even a
// normal run can pin hundreds of MB in a few giant tool results. A var so
// tests can shrink it. At 64MB the ring holds several minutes of ordinary
// transcript and a 28MB relay-sized event several times over.
var replayByteCap = 64 << 20

// maxFrameBytes caps any single events frame. The hub's host WS read
// limit is 32MB; keeping frames well below it means an oversized frame
// (e.g. one giant session update) is dropped with a log instead of
// killing the connection — which would otherwise livelock the resume
// (hello.seq never advances, and the same oversized frame is retried on
// every reconnect).
const maxFrameBytes = 8 << 20

// sendByteCap bounds the bytes queued on sendCh (queued frames plus the one
// the writer is holding). The channel bounds the frame COUNT (256); one frame
// can be maxFrameBytes, so without this the theoretical backlog is 2GB. A var
// so tests can shrink it — the budget snapshots it when NewClient creates the
// client.
var sendByteCap = 64 << 20

// sendCriticalWait is how long a critical frame (replay, a critical control
// frame) waits for a queue slot AND byte budget before giving up. A var so
// tests can shrink it.
var sendCriticalWait = 5 * time.Second

// sendBudget tracks the bytes reserved on sendCh. A frame reserves before it
// is queued and is released by whoever consumes it — writeLoop after the
// write (success or failure), drainSendCh when a stale queue is cleared — so
// used covers the payload's whole lifetime, not just its time in the channel.
type sendBudget struct {
	mu   sync.Mutex
	used int
	cap  int
	// freed is closed and replaced on every release; waiters select on it, so
	// a release between the capacity check and the wait cannot be missed.
	freed chan struct{}
}

func newSendBudget(capacity int) *sendBudget {
	return &sendBudget{cap: capacity, freed: make(chan struct{})}
}

// tryReserve claims n bytes without blocking.
func (b *sendBudget) tryReserve(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.cap-b.used {
		return false
	}
	b.used += n
	return true
}

// waitReserve waits until n bytes fit, d passes (d > 0), or ctx ends. It
// never reserves on failure.
func (b *sendBudget) waitReserve(ctx context.Context, n int, d time.Duration) bool {
	var timeout <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	for {
		b.mu.Lock()
		if n <= b.cap-b.used {
			b.used += n
			b.mu.Unlock()
			return true
		}
		freed := b.freed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-freed:
		case <-timeout:
			return false
		}
	}
}

// release frees n reserved bytes.
func (b *sendBudget) release(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0 // defensive; a double release must not create free space
	}
	close(b.freed)
	b.freed = make(chan struct{})
	b.mu.Unlock()
}

func (b *sendBudget) usedBytes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// maxRelayResponseBytes caps a relayed local-API response body. The respond
// frame carries it verbatim, so the ceiling is the transport's own frame
// limit: the hub's host-side WS read limit is 32MB and the QUIC length
// prefix is a signed 31-bit count (33,554,431 bytes). 28MB leaves room for
// the frame envelope. This bound is what a session-history page runs into —
// a single agentic turn can carry tens of megabytes of tool output — so it
// is deliberately generous; the FE's lite replay (§ acp detail projection) is
// what keeps the actual pages small.
const maxRelayResponseBytes = 28 << 20

// replayFrameBudget bounds a single replay frame. A resume after a long
// disconnect can carry thousands of buffered events; packing them into
// one multi-MB frame would exceed the hub read limit and repeat the
// reconnect forever. Replay is chunked to this budget instead.
const replayFrameBudget = 1 << 20

// healthySessionMin is how long a hub session must last to count as
// healthy and reset the reconnect backoff (see Run).
const healthySessionMin = 60 * time.Second

// wsDialTimeout bounds one WebSocket dial (DNS + TCP + TLS + upgrade). 6s
// covers the first three SYN retransmits (1+2+4) plus a TLS handshake on a
// high-RTT path; a genuinely slow path simply retries.
const wsDialTimeout = 6 * time.Second

// reconnectBackoffMax caps the reconnect delay so a recovered path is
// retried quickly instead of waiting out an escalated timer.
const reconnectBackoffMax = 10 * time.Second

// defaultSessionIdleTimeout fails a hub session that has received NOTHING
// (no downlink frame of any type) for this long. Without it a half-open TCP
// connection (NAT rebind, silent middlebox drop, laptop sleep) wedges the
// host permanently: reads block forever, writes keep succeeding into the
// kernel buffer, no error ever surfaces, and the reconnect loop is never
// entered — the host looks online in the hub registry while uploading
// nothing. The hub has always had the mirror-image guard (hostReadTimeout);
// the host had none.
//
// 20s leaves room around the hub's JSON ping (5s). That ping is the
// downlink beat; 20s spans four of them and tolerates three lost frames.
// The host does not send its own application ping: on QUIC the transport
// keepalive refreshes NAT, and on WebSocket the hub's ping is enough.
// Counting a host ping as peer traffic was also what kept the hub's
// socket probe from running.
//
// A hub states its own numbers in hello (idleTimeout / pingInterval); this
// constant is the value every session is reset to before hello is decoded,
// and the fallback if a hub advertises an out-of-range idleTimeout.
const defaultSessionIdleTimeout = 20 * time.Second

// Bounds on the liveness parameters a hub may advertise in hello. Values
// outside them are ignored and the local defaults stand: a typo in a hub
// config must not become a reconnect loop here (idleTimeout too small) or a
// session that takes minutes to notice death (too large).
const (
	minHelloPingSec = 1
	maxHelloPingSec = 60
	minHelloIdleSec = 5
	maxHelloIdleSec = 300
	// watchdogTicksPerTimeout splits the current budget into checks, so the
	// detection delay stays a fraction of whatever the hub advertises
	// instead of growing with it.
	watchdogTicksPerTimeout = 8
)

// heartbeatInterval is how often host_status is re-sent over QUIC, where no
// socket probe exists: the registry's 30s sweep reads only application
// uplinks, and this frame is the periodic one. 5s survives a GC pause or a
// lost packet yet stays well inside the hub's connection timeout.
//
// probeHeartbeatInterval is the cadence over WebSocket, where hello advertises
// `probe`: the hub has a protocol-level ping to detect a half-open socket
// sooner. That probe does NOT feed the registry — a control pong never
// reaches the hub's frame handler, so it refreshes nothing the sweep reads.
// The status frame therefore remains a registry beat on BOTH transports; the
// stretched cadence here buys idle uplink traffic, not liveness, and it must
// stay comfortably below the sweep (hub.HostSilenceTimeout, 30s) with room for
// one lost beat. 15s leaves a whole extra beat of slack; a cadence at or above
// the sweep would make a quiet-but-healthy host flap offline and back on every
// beat. (An earlier version of this comment claimed the hub refreshed LastSeen
// from the pong. It does not, and slowing this beat on that belief would flap
// idle hosts.)
//
// Vars (not consts) so tests can shrink them.
var (
	heartbeatInterval      = 5 * time.Second
	probeHeartbeatInterval = 15 * time.Second
)

// repairInterval is the gap-repair cadence, deliberately independent of the
// heartbeat: repair repays events that never reached the wire (the FE cannot
// gap-pull what the hub never buffered), so it must not wait for the slower
// WebSocket beat.
const repairInterval = 5 * time.Second

// heartbeatEvery is the host_status cadence for this session. hello.probe
// means the hub has a socket probe and host_status is only a registry
// re-assert, so the beat stretches to probeHeartbeatInterval. Without it
// (QUIC, or hello not yet read) the beat stays at heartbeatInterval,
// because that frame is what keeps LastSeen inside the sweep.
func (c *Client) heartbeatEvery() time.Duration {
	if c.probe.Load() {
		return probeHeartbeatInterval
	}
	return heartbeatInterval
}

// heartbeatTick paces the heartbeat CHECK, not the beat itself, so it only
// bounds how late a beat can be. Var (not const) so tests can shrink it
// along with the intervals.
var heartbeatTick = time.Second

// NewClient returns a hub client. LocalBase defaults to 127.0.0.1:8765.
// hostName is the display name currently in use. Safe from any goroutine,
// including while Rename is applying a new one.
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
		// Byte budget for sendCh; NewClient (not Run) so tests that enqueue
		// without a session still account for their frames.
		sendBudget: newSendBudget(sendByteCap),
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

// Run connects the host to the hub: pairs when no token exists, forwards
// bridge events over WebSocket, and serves relayed requests. It reconnects
// with backoff and blocks until ctx is done. Pairing failures (hub down at
// startup) retry with backoff instead of exiting permanently.
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

	c.sendCh = make(chan []byte, 256)
	c.reqCh = make(chan reqFrame, 64)
	c.ctrlCh = make(chan []byte, 32)

	// Bridge events → hub (async queue + batch; no text merge — dual-path seq).
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
		c.bindSession(sessCtx, sessCancel)

		// QUIC first (loss-resilient, connection-migrating), WS fallback.
		// The negative cache (pickTransport) skips QUIC while in cooldown
		// so a UDP-blocked network does not pay the dial timeout on every
		// reconnect.
		var err error
		sessionStart := time.Now()
		if c.pickTransport() == "ws" {
			err = c.wsSession(sessCtx, bridge)
		} else {
			established, qerr := c.quicSession(sessCtx, bridge)
			err = qerr
			// ctxDone(sessCtx), not ctx: a repair-driven cancel must not
			// be mistaken for a broken UDP path and burn a pointless
			// WebSocket dial on the way out.
			if qerr != nil && !ctxDone(sessCtx) {
				c.noteQuicOutcome(established)
				c.noteQuicSessionEnd(established, time.Since(sessionStart))
				log.Printf("[hub-client] QUIC 连接失败: %v，回退 WebSocket", qerr)
				err = c.wsSession(sessCtx, bridge)
			}
		}
		c.bindSession(nil, nil)
		sessCancel()

		if ctx.Err() != nil {
			break
		}
		// A session that stayed up is evidence the hub is healthy: reset
		// the backoff. Without this, the delay only ever grows and pins at
		// reconnectBackoffMax for the rest of the process — so a single
		// blip after hours of uptime costs the full cap, and every later
		// blip does too. Only sessions shorter than the threshold keep
		// escalating (that is the case backoff exists for: a hub that
		// rejects or drops us immediately).
		backoff = reconnectDelay(backoff, time.Since(sessionStart), false)
		// Pause forwarding until the next hello re-arms subscribers.
		c.fwdMu.Lock()
		c.fwdKnown = false
		c.forwardEvents = false
		c.fwdMu.Unlock()

		// A Pair/Unpair replaced the credential: reconnect immediately on
		// the new one. This is checked before the auth-retry branch below
		// because the session very likely ended by our own cancel, whose
		// error says nothing about the hub. Re-arming `repaired` matters
		// too — the auth one-shot was spent on the OLD token and the new
		// one deserves its own retry.
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
		// The wait is this attempt's jittered backoff; `backoff` stays the
		// unjittered sequence so escalation is unchanged.
		wait := backoffJitter(backoff)
		// A close frame says what happened; a rejected handshake or a
		// missing credential says the same thing without one. Both end up
		// as closeAuth, and everything unrecognised keeps the old path
		// (backoff, no re-pair).
		kind := classifyClose(err)
		if kind == closeOther && isAuthErr(err) {
			kind = closeAuth
		}
		switch kind {
		case closeSuperseded:
			// Another connection already holds this hostId. The credential
			// is FINE: re-pairing here would delete a working token and
			// spend a pairing code every time a second host process (or a
			// fast reconnect) overlaps the old socket, which is exactly
			// what the old status-string match did.
			log.Printf("[hub-client] hub: 本连接已被同 hostId 的新连接取代，立即重连（不重新配对）")
			backoff = reconnectDelay(backoff, 0, true)
			continue
		case closeAuth:
			if c.cfg.PairCode != "" && !repaired {
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
		case closeLiveness:
			// The hub buried the link because our pongs stopped arriving:
			// a half-open socket, whose only recovery is a redial. Same
			// backoff as any other drop, so a genuinely dead network still
			// escalates instead of hammering.
			log.Printf("[hub-client] hub 探测超时，判定链路半开（%.1fs 后重连）", wait.Seconds())
		case closeShutdown:
			// The hub is going down (deploy, restart). Nothing to repair;
			// the ordinary backoff would otherwise look like an outage.
			log.Printf("[hub-client] hub 正在关闭连接（%.1fs 后重连）", wait.Seconds())
		case closeProtocol:
			// The hub refused the handshake: one side is on the old
			// protocol. Redialing cannot fix that, so say it plainly.
			log.Printf("[hub-client] hub 拒绝了握手（协议版本不匹配），请把 host 与 hub 升到同一版本（%.1fs 后重试）", wait.Seconds())
		}
		log.Printf("[hub-client] hub 连接断开: %v（%.1fs 后重连）", err, wait.Seconds())
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
	// Error() renders the status constant ("status = StatusPolicyViolation
	// and reason = ..."), which every policy close shares — matching on it
	// made a superseded or half-dead link look like a rejected credential.
	// A hub that means "auth" says so in the reason (see classifyClose).
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return classifyClose(err) == closeAuth
	}
	// Handshake rejection: the upgrade is an HTTP request, so the status
	// is there to be read instead of grepped out of the error text.
	var he *wsHandshakeError
	if errors.As(err, &he) {
		return he.status == http.StatusUnauthorized
	}
	// QUIC says it in the application error code, since there is no close
	// frame to carry a reason (see quicCode*). Only the auth code counts:
	// every other verdict keeps the ordinary redial path.
	var ae *quic.ApplicationError
	if errors.As(err, &ae) {
		return ae.ErrorCode == quicCodeAuth
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

// errHubAuthRejected: the hub refused this host's credential on a
// transport that has no WS close frame to say so. QUIC sends a JSON
// {"type":"auth_error"} frame and drops the connection (see
// serveQUICConn); readLoop maps it onto this sentinel so the reconnect
// loop re-pairs instead of treating every attempt as an ordinary drop.
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

// closeKind is what a hub-initiated WS close MEANS for this client. The
// hub picks the pair deliberately (hostClose in acp-hub cmd/capri-hub), so
// the reason is the contract — the code alone cannot carry it (1000 is both
// an orderly shutdown and "a newer connection replaced yours"; 1008 is both
// "re-pair" and "your pings stopped").
type closeKind int

const (
	// closeOther: not a WS close we recognise — the ordinary error path
	// (dial failure, watchdog, local cancel), or a peer that closed without
	// a reason and therefore cannot be interpreted.
	closeOther closeKind = iota
	// closeAuth: the hub does not accept our credential. Only recovery is
	// a fresh pairing.
	closeAuth
	// closeSuperseded: another connection took over this hostId. Redial,
	// but do NOT re-pair — the credential is fine and clearing it would
	// throw away a working pairing on every duplicate host process.
	closeSuperseded
	// closeShutdown: the hub is going down (or the process serving us is).
	closeShutdown
	// closeLiveness: the hub's probe went unanswered long enough to bury
	// the link. The common shape of a half-open socket.
	closeLiveness
	// closeProtocol: the hub refused our handshake because it lacks a
	// capability this build must announce. Redialing cannot fix it — one
	// side has to be upgraded — so the loop backs off and says so.
	closeProtocol
)

// Close reasons acp-hub sends. Duplicated here on purpose: the two modules
// are separate binaries with no shared package, and the strings are wire
// contract, so they must be reviewed together when either side changes.
// A close without one of these reasons lands in closeOther.
const (
	closeReasonAuth       = "auth"
	closeReasonSuperseded = "superseded"
	closeReasonShutdown   = "shutdown"
	closeReasonLiveness   = "liveness probe timeout"
	closeReasonProtocol   = "protocol version"
)

// QUIC application error codes. QUIC has no close-reason frame, so the hub
// carries the same verdicts in CloseWithError's code; without this table a
// superseded QUIC session was indistinguishable from an ordinary drop, and
// the old process re-acquired the same hostId on a 10s cycle while the new
// one held it. Mirrors errCode* in acp-hub's cmd/capri-hub/main.go — the two
// lists are wire contract and must move together.
const (
	quicCodeSuperseded = 2
	quicCodeAuth       = 3
	quicCodeProtocol   = 4
	quicCodeLiveness   = 5
	quicCodeUplinkIdle = 6
	quicCodePingWrite  = 7
	quicCodeShutdown   = 8
	quicCodeUnreadable = 9
)

// classifyClose maps a hub close to what it means for the reconnect loop,
// over either transport. Anything it cannot vouch for is closeOther, which
// keeps the pre-existing behavior (backoff, no re-pair) — the safe default,
// because a re-pair throws away a credential the hub may still accept.
func classifyClose(err error) closeKind {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return classifyWSReason(ce.Reason)
	}
	var ae *quic.ApplicationError
	if errors.As(err, &ae) {
		switch ae.ErrorCode {
		case quicCodeSuperseded:
			return closeSuperseded
		case quicCodeAuth:
			return closeAuth
		case quicCodeProtocol:
			return closeProtocol
		case quicCodeLiveness:
			return closeLiveness
		case quicCodeShutdown:
			return closeShutdown
		}
		// Uplink-idle, ping-write and unreadable all end an otherwise
		// healthy session: they are ordinary drops whose only recovery is
		// the same backoff-and-redial the WebSocket path uses.
	}
	return closeOther
}

// classifyWSReason is the WebSocket half of the table. The reason is the
// contract; the status code alone cannot separate shutdown from superseded
// and 1008 already means three different things.
func classifyWSReason(reason string) closeKind {
	switch reason {
	case closeReasonAuth:
		return closeAuth
	case closeReasonSuperseded:
		return closeSuperseded
	case closeReasonShutdown:
		return closeShutdown
	case closeReasonLiveness:
		return closeLiveness
	case closeReasonProtocol:
		return closeProtocol
	}
	// Empty reason = a hub that predates this contract (or a peer that
	// closed without saying why). Its code is the only signal and it is
	// ambiguous, so it must NOT be read as auth — that was the bug.
	return closeOther
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

// ── event forwarding (bridge → hub) ───────────────────────────────────

// setBrowserSubscribers enables/disables bridge→hub event upload based on
// the hub's live browser subscriber count for THIS host.
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

// replayPaused reports that the hub has positively told us this host has no
// audience (fwdKnown && !forwardEvents). A fwdKnown=false client is NOT
// paused: the hub has not spoken yet, and the ring must keep its normal
// shape for a reconnect.
//
// Lock order: callers may hold seqMu (compactReplayLocked), so this must not
// take any lock that could be held while seqMu is taken. Nothing acquires
// fwdMu and then seqMu — setBrowserSubscribers releases fwdMu before
// applySubscribers touches the ring.
func (c *Client) replayPaused() bool {
	c.fwdMu.Lock()
	defer c.fwdMu.Unlock()
	return c.fwdKnown && !c.forwardEvents
}

// seqAndReplay appends evs to the replay ring, keeping each event's
// own seq when present (bridge.Broadcast 分配全局序号，本地 SSE 与中继
// 同源 —— 前端双路去重依赖两侧 seq 一致)；无 seq 的事件（旧桥/内部
// 事件）才由本客户端分配兜底。定位一律按事件实际 seq 比较。
func (c *Client) seqAndReplay(evs []acp.Event) {
	c.seqMu.Lock()
	for _, ev := range evs {
		s := eventSeq(ev)
		if s == 0 {
			c.nextSeq++
			ev["seq"] = c.nextSeq
			s = c.nextSeq
		} else if s > c.nextSeq {
			c.nextSeq = s
		}
		c.clampOversized(ev)
		raw, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		typ, _ := ev["type"].(string)
		c.replay = append(c.replay, replayItem{seq: s, typ: typ, raw: raw})
		c.replayBytes += len(raw)
	}
	saturated := c.compactReplayLocked()
	c.seqMu.Unlock()
	// Cancel outside the lock: failSession takes ctlMu, and the session
	// goroutines take seqMu.
	if saturated && c.connected.Load() && c.replaySaturated.CompareAndSwap(false, true) {
		c.failSession(fmt.Errorf("未确认事件超过回放环上限（%d），断开会话以便按水位重放", replayHardCap))
	}
}

// clampOversized replaces a single event that cannot fit in one frame
// with an explicit error placeholder that keeps its seq. Dropping it
// would open a hole the hub then steps over. The placeholder is what
// both the live send and a later replay transmit.
func (c *Client) clampOversized(ev acp.Event) {
	raw, err := json.Marshal(ev)
	if err != nil || len(raw)+64 <= maxFrameBytes {
		return
	}
	seq := ev["seq"]
	sid := ev["sessionId"]
	for k := range ev {
		delete(ev, k)
	}
	ev["type"] = "error"
	ev["seq"] = seq
	if sid != nil {
		ev["sessionId"] = sid
	}
	ev["message"] = "事件超过单帧上限，正文已丢弃"
	log.Printf("[hub-client] 单条事件 %d 字节超过单帧上限，改为错误占位 seq=%v", len(raw), seq)
}

// compactPausedReplayLocked trims the ring while forwarding is paused: the
// most recent pausedReplayCap droppable events plus EVERY non-droppable one
// (client_request, done, error, …). Critical events may exceed the cap on
// purpose — a permission prompt is not in host history, so a resume cannot
// rebuild it — and the lock/ack state of a request is what the user acts on.
//
// Dropping events makes the ring's start jump above the hub's delivery
// watermark; pausedTrimmedThrough records the highest seq that was really
// dropped so the resume can detect that a replay alone cannot bridge the
// gap and must reset the epoch instead. Caller holds seqMu.
func (c *Client) compactPausedReplayLocked() {
	// Count-based trim: keep the newest pausedReplayCap droppable events.
	droppable := 0
	for _, it := range c.replay {
		if it.droppable() {
			droppable++
		}
	}
	drop := 0
	if droppable > pausedReplayCap {
		drop = droppable - pausedReplayCap
	}
	var droppedThrough uint64
	if drop > 0 {
		kept := make([]replayItem, 0, len(c.replay)-drop)
		for _, it := range c.replay {
			if drop > 0 && it.droppable() {
				drop--
				if it.seq > droppedThrough {
					droppedThrough = it.seq
				}
				continue
			}
			kept = append(kept, it)
		}
		c.replay = kept
		c.replayBytes = replayRingBytes(c.replay)
	}

	// Byte gate. Critical events bypass the count cap on purpose (a
	// permission prompt has no history), but they must not grow the ring
	// without bound either. Drop from the oldest: first only droppable
	// events, then — if the ring is still over the cap — the oldest critical
	// ones too. A dropped prompt is worse than a dropped chunk, but both
	// beat running out of memory, and the trim marker sends the resume
	// through seq_reset.
	if c.replayBytes > replayByteCap {
		bytesLeft := c.replayBytes
		kept := make([]replayItem, 0, len(c.replay))
		for _, it := range c.replay {
			if bytesLeft > replayByteCap && it.droppable() {
				bytesLeft -= len(it.raw)
				if it.seq > droppedThrough {
					droppedThrough = it.seq
				}
				continue
			}
			kept = append(kept, it)
		}
		criticalDropped := 0
		if bytesLeft > replayByteCap {
			out := kept[:0]
			for _, it := range kept {
				if bytesLeft > replayByteCap {
					bytesLeft -= len(it.raw)
					if it.seq > droppedThrough {
						droppedThrough = it.seq
					}
					criticalDropped++
					continue
				}
				out = append(out, it)
			}
			kept = out
			if criticalDropped > 0 && c.pausedCapLogged.CompareAndSwap(false, true) {
				log.Printf("[hub-client] 无订阅期间关键事件超过回放字节上限（%dMB），已丢弃最旧的关键事件 %d 条；恢复客户端时会走 seq_reset 重放",
					replayByteCap>>20, criticalDropped)
			}
		}
		c.replay = kept
		c.replayBytes = bytesLeft
	}
	if droppedThrough > c.pausedTrimmedThrough {
		c.pausedTrimmedThrough = droppedThrough
	}
}

// resumeAfterPausedTrimLocked repairs a resume whose replay ring no longer
// reaches the hub's watermark. Replaying the surviving tail (say 1050 after
// ack 1000) would park it in the hub's out-of-order set forever: 1001..1049
// no longer exist, so the cumulative watermark could never move and every
// later event would pile up behind the hole. A seq_reset starts a new hub
// epoch from the oldest event we still hold — the hub's first-event baseline
// rule then anchors the watermark just below it.
//
// The caller must hold enqueueMu, and it must span BOTH the reset and the
// replay: a live frame landing in between would let the hub jump to a higher
// seq first and stale-drop the replayed events. Returns false when the reset
// could not be queued; the trim marker and needCatchUp stay set so a later
// repair retries instead of replaying into the hole.
func (c *Client) resumeAfterPausedTrimLocked() bool {
	if !c.enqueueSeqResetLocked() {
		c.needCatchUp.Store(true)
		c.noteEventDrop("seq_reset 未能入队，暂不补发", 0)
		return false
	}
	last := c.sendReplayAfterLocked(0)
	// The hub's watermark is 0 after the reset; ours must be too, or the
	// next ping (still carrying the pre-reset number) would re-anchor
	// repairs past the replayed events. lastSentSeq follows what actually
	// reached the queue, so a yielded replay resumes from the right point.
	c.seqMu.Lock()
	c.hubAckSeq = 0
	c.lastSentSeq = last
	c.pausedTrimmedThrough = 0
	c.seqMu.Unlock()
	if last > 0 {
		log.Printf("[hub-client] 暂停裁剪后重置世代，重放本地缓冲 seq 至 %d", last)
	}
	return true
}

// compactReplayLocked drops only events the hub has acked (seq <=
// hubAckSeq) once the ring passes replayHighWater. Unacked events are
// kept. It reports whether the unacked tail itself exceeds replayHardCap,
// which is the point the session should end rather than grow without a
// delivery ack. While forwarding is paused (a known-empty audience) the ring
// is trimmed to pausedReplayCap droppable events plus every critical one, and
// saturation is never reported: no subscriber means no ack is coming, so
// growth is not evidence of a broken link. Caller holds seqMu.
func (c *Client) compactReplayLocked() (saturated bool) {
	// No audience: nothing sent can be acked, so the unacked tail must not
	// be measured against replayHardCap (that ended the session every 8000
	// events for a host nobody was watching). Keep a small tail for the
	// 0→1 resume instead; older events are rebuilt from host history.
	if c.replayPaused() {
		c.compactPausedReplayLocked()
		return false
	}
	// Both caps are checked before the cheap early return: a handful of
	// 8MB events passes replayHighWater's count check long after it passes
	// the byte budget.
	if len(c.replay) <= replayHighWater && c.replayBytes <= replayByteCap {
		return false
	}
	ack := c.hubAckSeq
	cut := 0
	if ack > 0 {
		for cut < len(c.replay) && c.replay[cut].seq <= ack {
			cut++
		}
	}
	unacked := len(c.replay) - cut
	unackedBytes := replayRingBytes(c.replay[cut:])
	if unacked > replayHardCap || unackedBytes > replayByteCap {
		if c.connected.Load() {
			// End the session instead of dropping. hello replays the tail.
			return true
		}
		// No session to end. Keep the newest unacked events that fit both
		// caps; anything older is in the agent transcript. Unacked events
		// are never silently dropped while a live session could replay
		// them — this branch only runs disconnected.
		dropN := 0
		for cut+dropN < len(c.replay) &&
			(len(c.replay)-(cut+dropN) > replayHardCap || unackedBytes > replayByteCap) {
			unackedBytes -= len(c.replay[cut+dropN].raw)
			dropN++
		}
		trimmed := make([]replayItem, len(c.replay)-(cut+dropN))
		copy(trimmed, c.replay[cut+dropN:])
		c.replay = trimmed
		c.replayBytes = replayRingBytes(c.replay)
		if c.replayFuseLogged.CompareAndSwap(false, true) {
			log.Printf("[hub-client] 断线期间未确认回放超出上限（上限 %d 条 / %dMB），更早的事件不再进入回放环（历史记录仍在）",
				replayHardCap, replayByteCap>>20)
		}
		return false
	}
	// Keep every unacked event. Drop the oldest acked ones until the
	// ring is back at replayCap, but never past `cut`.
	target := replayCap
	if unacked > target {
		target = unacked
	}
	countStart := 0
	if len(c.replay) > target {
		countStart = len(c.replay) - target
		if countStart > cut {
			countStart = cut
		}
	}

	// Byte trim of the ACKED prefix. The count trim above cannot bound the
	// ring when everything is acked (cut == len(replay), unacked == 0): a
	// full replayCap of 8MB events is 8GB, and "recent" acked events are
	// still just history the FE can rebuild. Keep the NEWEST acked events
	// that fit beside the unacked tail; the unacked tail itself is already
	// under replayByteCap (checked above), and dropping acked events never
	// fails the session — they are delivered by definition.
	//
	// The walk stops at the first event that does not fit rather than
	// skipping it: the kept ring stays a contiguous suffix, so a replay or
	// gap-pull never sees an older event behind a newer missing one.
	byteStart := cut
	budget := replayByteCap - unackedBytes
	for i := cut - 1; i >= countStart; i-- {
		if budget < len(c.replay[i].raw) {
			break
		}
		budget -= len(c.replay[i].raw)
		byteStart = i
	}
	start := countStart
	if byteStart > start {
		start = byteStart
	}
	if start <= 0 {
		return false
	}
	n := len(c.replay) - start
	capN := replayHighWater
	if n > capN {
		capN = n
	}
	trimmed := make([]replayItem, n, capN)
	copy(trimmed, c.replay[start:])
	c.replay = trimmed
	c.replayBytes = replayRingBytes(c.replay)
	if start > countStart && c.ackedTrimLogged.CompareAndSwap(false, true) {
		log.Printf("[hub-client] 已确认事件占用超过回放字节上限（%dMB），丢弃最旧的已确认事件 %d 条（历史记录仍在）",
			replayByteCap>>20, start-countStart)
	}
	return false
}

func (c *Client) forwardLoop(ctx context.Context, bridge bridgeSource) {
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
		// Trim BEFORE the replay ring so a later resume replays the same
		// small placeholders instead of resurrecting payloads for sessions
		// nobody is watching (the FE rebuilds those from host history).
		c.trimUnwatched(evs)
		// Always enter replay (even while browser subscribers are
		// paused) so a later 0→1 resume can catch up.
		c.seqAndReplay(evs)
		if c.forwardingEnabled() {
			c.enqueueEvents(evs)
		}
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// The heartbeat cadence is per session (the transport decides it), so
	// the ticker only paces the check and lastBeat is compared against the
	// current interval. A ticker built once from either constant would pin
	// the wrong cadence on the other transport.
	heartbeat := time.NewTicker(heartbeatTick)
	defer heartbeat.Stop()
	lastBeat := time.Now()
	var lastRepair time.Time
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
			// Host registry fields only change on these transitions —
			// mirror them to the hub right away (control frame, works
			// even while event upload is paused) instead of waiting for
			// the next heartbeat. client_request_resolved is here for the
			// DECREASE: pendingCount only drops when a request is answered
			// or times out, and on a probing hub the next beat is
			// probeHeartbeatInterval away.
			switch t, _ := ev["type"].(string); t {
			case "busy", "sessions_changed", "client_request", "client_request_resolved", "error":
				c.maybeNotifyHostStatus(bridge)
			}
			batch = append(batch, ev)
			if len(batch) >= 32 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-heartbeat.C:
			now := time.Now()
			flush()
			// Neither of these is useful without a live transport: the
			// uplink queue survives reconnects (drained on the next
			// session), so frames queued while disconnected are thrown
			// away anyway — on Oct 7 a DNS outage turned every beat
			// into "发送队列满，丢弃帧" for two and a half hours.
			//
			// Both deadlines stay un-advanced here, so a reconnect repairs
			// and re-asserts immediately instead of after a full interval.
			if !c.connected.Load() {
				continue
			}
			// Gap repair keeps its own fast cadence: it repays events that
			// never reached the wire, and the FE cannot fill those holes
			// itself. Slowing it down with a probe-backed heartbeat would
			// trade liveness messaging for a longer visible stall.
			if now.Sub(lastRepair) >= repairInterval {
				lastRepair = now
				c.repairDroppedEvents()
			}
			if now.Sub(lastBeat) < c.heartbeatEvery() {
				continue
			}
			lastBeat = now
			// Always heartbeat while connected: keeps hub registry ready
			// flag fresh even with zero browser subscribers. Control
			// frame, not data-plane seq.
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
	if !c.needCatchUp.Load() || !c.forwardingEnabled() {
		return
	}
	// Only repair once the queue has actually drained. Replaying into a
	// still-full queue is self-defeating: the replay's critical enqueue
	// blocks up to 5s per frame, times out, sets needCatchUp again, and
	// the next pass repeats the whole batch. That loop is what turned
	// one oversized burst (2026-10-22 22:14) into thousands of drops.
	// Leaving the flag set keeps the repair pending; nothing is lost
	// because the events stay in the ring.
	if c.sendQueuePressure() {
		return
	}
	// Hold the ordering lock before clearing the flag. A live enqueue
	// that observed needCatchUp==false in the gap would send a higher
	// seq first, and the hub watermark would step over the hole.
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	if !c.needCatchUp.Load() || c.sendQueuePressure() {
		return
	}
	c.seqMu.Lock()
	// Anchor at the delivery watermark. Live sends do not pass a hole,
	// so this watermark cannot have stepped over the events we are
	// about to resend. Before the first ack, fall back to the enqueue
	// watermark: nothing past it was handed to the writer.
	after := c.lastSentSeq
	if c.hubAckSeq > 0 {
		after = c.hubAckSeq
	}
	trimmed := c.pausedTrimmedThrough
	c.seqMu.Unlock()
	if trimmed > after {
		// The ring starts above the watermark, so a replay cannot bridge
		// the gap; restart the hub epoch from the oldest event still held.
		// needCatchUp is re-armed by the helper if the reset cannot land.
		c.needCatchUp.Store(false)
		c.resumeAfterPausedTrimLocked()
		return
	}
	c.needCatchUp.Store(false)
	if last := c.sendReplayAfterLocked(after); last > 0 {
		log.Printf("[hub-client] 补发被丢弃的事件 seq %d..%d", after+1, last)
	}
}

// sendQueuePressure reports whether the uplink queue is too full to accept
// a repair replay: more than half of sendCh's slots — or half of its byte
// budget — are still occupied. The writer drains one frame per send, so
// half-full means the backlog is at least half a round of writes; replaying
// now would refill it. The byte half matters because a few large frames can
// fill the budget while the channel looks empty.
func (c *Client) sendQueuePressure() bool {
	if c.sendCh == nil {
		return false
	}
	if len(c.sendCh) >= cap(c.sendCh)/2 {
		return true
	}
	if c.sendBudget != nil {
		b := c.sendBudget
		b.mu.Lock()
		used, capacity := b.used, b.cap
		b.mu.Unlock()
		if capacity > 0 && used >= capacity/2 {
			return true
		}
	}
	return false
}

// reserveSendNow claims n bytes of the send budget without waiting. A nil
// budget (a Client not built by NewClient) means accounting is disabled.
func (c *Client) reserveSendNow(n int) bool {
	if c.sendBudget == nil {
		return true
	}
	return c.sendBudget.tryReserve(n)
}

// reserveSendWait claims n bytes, waiting at most d (d = 0 means until the
// session ends). Callers must not hold seqMu/enqueueMu longer than the
// existing critical timeout already allows.
func (c *Client) reserveSendWait(n int, d time.Duration) bool {
	if c.sendBudget == nil {
		return true
	}
	return c.sendBudget.waitReserve(c.sessionCtx(), n, d)
}

// releaseSend returns n reserved bytes to the budget.
func (c *Client) releaseSend(n int) {
	if c.sendBudget != nil {
		c.sendBudget.release(n)
	}
}

// sendBudgetUsed is the diagnostics read side of the send budget.
func (c *Client) sendBudgetUsed() int {
	if c.sendBudget == nil {
		return 0
	}
	return c.sendBudget.usedBytes()
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
// enqueueReplayLocked).
func (c *Client) enqueueEvents(evs []acp.Event) {
	// Try-lock, never block: a large replay can hold the ordering lock for
	// a long time (each frame briefly blocks on a full queue). Blocking
	// here would stall forwardLoop's flush — the bridge subscriber channel
	// then fills and Broadcast drops droppable events BEFORE they reach the
	// replay ring, where no repair path can recover them. The batch is
	// already in the ring (flush ran seqAndReplay first), so dropping here
	// is safe: the repair re-sends it in order, and live sends stay
	// parked until that repair holds this same lock.
	if !c.enqueueMu.TryLock() {
		c.needCatchUp.Store(true)
		c.noteEventDrop("重放占用发送通道", len(evs))
		return
	}
	defer c.enqueueMu.Unlock()
	if c.needCatchUp.Load() {
		// A hole is already open. These events are in the ring.
		// Sending them would step past it.
		return
	}
	c.enqueueEventsLocked(evs, false)
}

// enqueueEventsLocked is the shared body of enqueueEvents (critical=false,
// drops under pressure) and the replay path (critical=true, blocks up to 5s
// for a send slot — enqueueFrame's critical discipline); the caller must
// hold enqueueMu.
func (c *Client) enqueueEventsLocked(evs []acp.Event, critical bool) {
	if len(evs) == 0 || c.sendCh == nil {
		return
	}
	// Same budget the replay path uses. A 50ms/32-event batch of tool
	// output can exceed maxFrameBytes even when each event fits; sending
	// it as one frame used to drop the batch with no catch-up flag.
	var batch []acp.Event
	size := 0
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		sending := batch
		batch = nil
		size = 0
		payload := c.marshalEventsFrame(sending)
		if payload == nil {
			c.needCatchUp.Store(true)
			c.noteEventDrop("事件帧无法编码", len(sending))
			return false
		}
		if !c.enqueuePayload(payload, sending, critical) {
			return false
		}
		return true
	}
	for _, ev := range evs {
		raw, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		if len(batch) > 0 && size+len(raw) > replayFrameBudget {
			if !flush() {
				return
			}
		}
		batch = append(batch, ev)
		size += len(raw)
	}
	flush()
}

// enqueuePayload puts one events frame on sendCh. On failure it sets
// needCatchUp and does not advance lastSentSeq, so the caller must not
// send a later seq. critical waits up to sendCriticalWait for a slot and
// for byte budget; the live path never blocks.
func (c *Client) enqueuePayload(payload []byte, evs []acp.Event, critical bool) bool {
	n := len(payload)
	if critical {
		if !c.reserveSendWait(n, sendCriticalWait) {
			c.needCatchUp.Store(true)
			c.noteEventDrop("关键事件帧入队超时（发送预算满）", len(evs))
			return false
		}
		select {
		case c.sendCh <- payload:
			c.noteLastSentSeq(evs)
			return true
		case <-time.After(sendCriticalWait):
			c.releaseSend(n)
			c.needCatchUp.Store(true)
			c.noteEventDrop("关键事件帧入队超时（队列满）", len(evs))
			return false
		}
	}
	if !c.reserveSendNow(n) {
		c.needCatchUp.Store(true)
		c.noteEventDrop("发送预算不足", len(evs))
		return false
	}
	select {
	case c.sendCh <- payload:
		c.noteLastSentSeq(evs)
		return true
	default:
		c.releaseSend(n)
		c.needCatchUp.Store(true)
		c.noteEventDrop("发送队列满", len(evs))
		return false
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

// setHubAck installs hello's watermark as-is (it may be lower than the
// previous session's). seq above anything this process produced is
// ignored, same as noteHubAck.
func (c *Client) setHubAck(seq uint64) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if seq > c.nextSeq {
		return
	}
	c.hubAckSeq = seq
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
// frames of at most replayFrameBudget bytes (see enqueueReplayLocked), so a
// large resume cannot build one multi-MB frame that would exceed the
// hub's read limit. Returns the seq of the last re-sent event (0 when
// nothing was sent).
func (c *Client) sendReplayAfter(after uint64) uint64 {
	// Ordering lock first (enqueueMu → seqMu, matching enqueueEvents):
	// held across the whole replay so no live frame can land before the
	// replay frames — see enqueueReplayLocked for why that would lose events.
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
		if c.replay[idx].seq > after {
			break
		}
	}
	if idx >= len(c.replay) {
		c.seqMu.Unlock()
		return 0
	}
	items := append([]replayItem(nil), c.replay[idx:]...)
	// Release before enqueue so noteLastSentSeq can take seqMu.
	c.seqMu.Unlock()
	return c.enqueueReplayLocked(items)
}

// enqueueReplayLocked packs replay events into frames of at most
// replayFrameBudget bytes (marshaling each event once for sizing) and
// enqueues them in order via the CRITICAL path — replay is reconnect
// catch-up, so frames must not be dropped when the send queue happens to
// be full (enqueueEvents would silently lose them; the hub would then
// still be missing transcript after the resume). A single event larger
// than the budget is sent alone; marshalEventsFrame drops it (with a
// log) if it exceeds maxFrameBytes. The caller must hold enqueueMu, and
// the lock spans the whole marshal + flush so no live frame can land
// before the replay frames:
// the hub's stale-seq gate would otherwise drop the replay events that
// arrive after higher live seqs — and they are absent from the hub's
// gap-pull buffer too, so the transcript would be permanently lost.
// Live enqueues simply wait for the lock and then land after the
// replay, in strictly increasing seq order.
//
// It yields to queue pressure: after each frame actually lands on sendCh,
// sendQueuePressure() is re-checked, and when the queue is at least half
// full the remaining events are left for the repair path (needCatchUp) with
// lastSentSeq advanced past whatever did land. Pushing the rest would time
// out each critical enqueue (5s), re-arm needCatchUp, and replay the same
// batch again on the next beat — the 2026-10-22 amplification loop. Returns
// the seq of the last event actually queued (0 when none was).
func (c *Client) enqueueReplayLocked(items []replayItem) uint64 {
	var frame []replayItem
	size := 0
	var lastSent uint64
	var firstSent uint64
	flush := func() (queued, ok bool) {
		if len(frame) == 0 {
			return false, true
		}
		// Frames are sent via the CRITICAL path — replay is reconnect
		// catch-up, so frames must not be dropped when the send queue
		// happens to be full (a dropped replay frame would leave the hub
		// missing transcript after the resume).
		queued, ok = c.enqueueReplayFrameLocked(frame)
		if queued {
			if firstSent == 0 {
				firstSent = frame[0].seq
			}
			lastSent = frame[len(frame)-1].seq
		}
		frame = nil
		size = 0
		return queued, ok
	}
	for i := range items {
		it := items[i]
		if len(frame) > 0 && size+len(it.raw) > replayFrameBudget {
			queued, ok := flush()
			if !ok {
				return lastSent
			}
			if queued && i < len(items) && c.sendQueuePressure() {
				c.needCatchUp.Store(true)
				log.Printf("[hub-client] 补发中途让出：队列已过半满，已入队 seq %d..%d，剩余 %d 条留待修补通道", firstSent, lastSent, len(items)-i)
				return lastSent
			}
		}
		frame = append(frame, it)
		size += len(it.raw)
	}
	flush()
	return lastSent
}

// enqueueReplayFrameLocked writes one events frame from already-marshaled
// ring slots (no Event map round-trip). Caller holds enqueueMu. queued
// reports whether the frame reached sendCh; ok reports whether the caller
// may keep sending (false on a critical-enqueue timeout, where every further
// frame would eat another 5s).
func (c *Client) enqueueReplayFrameLocked(items []replayItem) (queued, ok bool) {
	if len(items) == 0 {
		return false, true
	}
	if c.sendCh == nil {
		return false, false
	}
	raws := make([]json.RawMessage, len(items))
	for i, it := range items {
		raws[i] = it.raw
	}
	payload := c.marshalEventsFrameRaw(items[0].seq, raws)
	if payload == nil {
		// Do not skip this seq and continue. A later seq would weld the
		// hole. clampOversized makes a single event fit; a nil here is
		// the repair stopping so the next pass can try again.
		c.needCatchUp.Store(true)
		c.noteEventDrop("事件帧过大无法发送", len(items))
		return false, false
	}
	if !c.reserveSendWait(len(payload), sendCriticalWait) {
		c.needCatchUp.Store(true)
		c.noteEventDrop("关键事件帧入队超时（发送预算满）", len(items))
		return false, false
	}
	select {
	case c.sendCh <- payload:
		c.noteLastSentSeqItems(items)
		return true, true
	case <-time.After(sendCriticalWait):
		c.releaseSend(len(payload))
		c.needCatchUp.Store(true)
		c.noteEventDrop("关键事件帧入队超时（队列满）", len(items))
		return false, false
	}
}

func (c *Client) noteLastSentSeqItems(items []replayItem) {
	var max uint64
	for _, it := range items {
		if it.seq > max {
			max = it.seq
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

// marshalEventsFrameRaw builds a type:"events" frame from pre-marshaled
// event objects. json.RawMessage is emitted verbatim so replay does not
// unmarshal into Event maps.
func (c *Client) marshalEventsFrameRaw(seqStart uint64, raws []json.RawMessage) []byte {
	payload, err := json.Marshal(struct {
		V        int               `json:"v"`
		Type     string            `json:"type"`
		SeqStart uint64            `json:"seqStart"`
		Events   []json.RawMessage `json:"events"`
	}{V: 1, Type: "events", SeqStart: seqStart, Events: raws})
	if err != nil {
		return nil
	}
	if len(payload) > maxFrameBytes {
		log.Printf("[hub-client] 事件帧过大（%d KB），丢弃 %d 条事件（重放缓冲已保留）", len(payload)>>10, len(raws))
		return nil
	}
	return payload
}

// enqueueHostStatus sends a control frame (not an events frame) so it
// does not consume the data-plane seq space shared with bridge.Broadcast
// (dual-path FE dedupes by (hostId,seq)). Shape:
//
//	{"v":1,"type":"host_status","ready":true,"busy":false,"booting":false,"pendingCount":0}
//
// No seq, not stored in the replay buffer. Records the sent fields so
// maybeNotifyHostStatus can diff — the heartbeat keeps sending
// unconditionally, while a status change mid-window is pushed via
// maybeNotifyHostStatus.
//
// The frame rides the control queue, which is not dropped when the event
// queue is full. lastStatus moves only after the frame is actually
// queued, so a miss does not suppress the next diff.
func (c *Client) enqueueHostStatus(bridge bridgeSource) {
	snap := bridge.Snapshot()
	fields := c.hostStatusFieldsOf(snap)
	frame := map[string]any{
		"v":            1,
		"type":         "host_status",
		"ready":        fields.Ready,
		"busy":         fields.Busy,
		"booting":      fields.Booting,
		"pendingCount": fields.PendingCount,
	}
	if fields.Port > 0 {
		frame["port"] = fields.Port
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return
	}
	if !c.enqueueControl(payload) {
		return
	}
	c.statusMu.Lock()
	c.lastStatus = &fields
	c.statusMu.Unlock()
}

// hostStatusFields is the subset of the bridge Status snapshot the hub
// registry mirrors (ready/busy/booting/pendingCount), plus this host's
// local HTTP listen port. pendingCount is the number of pending client
// requests (permissions / x.ai questions); busy means at least one
// session has a turn in flight; booting means the agent process has not
// finished booting.
type hostStatusFields struct {
	Ready        bool
	Busy         bool
	Booting      bool
	PendingCount int
	Port         int
}

func (c *Client) hostStatusFieldsOf(s acp.Status) hostStatusFields {
	return hostStatusFields{
		Ready:        s.Ready,
		Busy:         s.Busy,
		Booting:      s.Booting,
		PendingCount: len(s.PendingRequests),
		Port:         c.listenPort(),
	}
}

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

// maybeNotifyHostStatus immediately re-sends host_status when the bridge
// snapshot's registry fields changed since the last frame, so thinking /
// pending / idle transitions reach the hub without waiting for the 5s
// heartbeat. The diff is against the last SENT fields (the heartbeat's
// unconditional frame refreshes it too), so a quiet host sends nothing.
// Must run on state transitions only — the caller gates it on event
// types, not per event, to avoid Snapshot() churn on chunk streams.
func (c *Client) maybeNotifyHostStatus(bridge bridgeSource) {
	cur := c.hostStatusFieldsOf(bridge.Snapshot())
	c.statusMu.Lock()
	last := c.lastStatus
	c.statusMu.Unlock()
	if last == nil || cur != *last {
		c.enqueueHostStatus(bridge)
	}
}

// handleHelloSeq resumes after hub hello.seq, or resets the seq epoch when
// this process's max observed seq is below the hub watermark (typical
// after host process restart: bridge recounts from 1 while hub still
// holds the previous process's LastSeq).
//
// Without a reset, hello would push lastSentSeq to the alien high value
// (0→1 catch-up would no-op) and hub would skip fan-out for every new
// event with seq <= old LastSeq.
func (c *Client) handleHelloSeq(hubSeq uint64) error {
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
		paused := c.replayPaused()
		c.enqueueMu.Lock()
		ok := c.enqueueSeqResetLocked()
		var last uint64
		if ok && !paused {
			last = c.sendReplayAfterLocked(0)
		}
		c.enqueueMu.Unlock()
		if !ok {
			return fmt.Errorf("seq_reset 未能入队，中止本次会话以免在旧水位上补发")
		}
		if paused {
			// Nothing was replayed, so the enqueue watermark must not
			// outlive the epoch: the 0→1 resume replays from 0. The trim
			// marker is obsolete too — the reset already started the new
			// epoch at the oldest event still held.
			c.seqMu.Lock()
			c.lastSentSeq = 0
			c.pausedTrimmedThrough = 0
			c.seqMu.Unlock()
		}
		// The hub's watermark is now 0 (seq_reset cleared it); so is our
		// view of it. A ping carrying the pre-reset high ack must not
		// re-anchor repairs past the replay (noteHubAck filters it).
		c.seqMu.Lock()
		c.hubAckSeq = 0
		c.seqMu.Unlock()
		// Do NOT advance lastSentSeq from the alien hubSeq. Replay
		// everything this process has buffered (after 0) — unless nobody
		// is watching, in which case the reset alone is enough and the
		// ring waits for the 0→1 resume.
		if last > 0 {
			log.Printf("[hub-client] 世代重置后补发本地缓冲 seq 1..%d", last)
		}
		return nil
	}

	// Same process (or hub is behind): precise resume.
	//
	// Paused (hub says nobody is watching): do not push the ring into a
	// hub whose fan-out would drop every frame, and do not let the
	// unacked size end the session. Record the hub's delivery watermark
	// and anchor lastSentSeq at it, so the 0→1 resume (applySubscribers)
	// sends exactly the events after it — the ring keeps only that tail
	// while paused (pausedReplayCap). If paused compaction has trimmed past
	// hubSeq, lastSentSeq still sits at hubSeq, so the resume sees the trim
	// marker and takes the seq_reset path instead of replaying into a hole.
	if c.replayPaused() {
		c.setHubAck(hubSeq)
		c.seqMu.Lock()
		c.lastSentSeq = hubSeq
		c.seqMu.Unlock()
		return nil
	}

	// Replay what the ring still holds above the hub's watermark — unless a
	// paused trim already dropped events between the two. Sending 1050 to a
	// hub parked at 1000 would leave 1001..1049 missing forever, so that
	// case resets the epoch instead. enqueueMu spans both the reset and the
	// replay, so no live frame can land in between.
	c.enqueueMu.Lock()
	c.seqMu.Lock()
	trimmed := c.pausedTrimmedThrough
	c.seqMu.Unlock()
	if trimmed > hubSeq {
		reset := c.resumeAfterPausedTrimLocked()
		c.enqueueMu.Unlock()
		if !reset {
			// Nothing was sent over the old watermark; needCatchUp is set
			// so a later repair retries the reset.
			return nil
		}
		log.Printf("[hub-client] 暂停裁剪越过 hub 水位（through %d > %d），重连时重置世代并重放", trimmed, hubSeq)
		return nil
	}
	last := c.sendReplayAfterLocked(hubSeq)
	c.enqueueMu.Unlock()
	if last > 0 {
		log.Printf("[hub-client] 重连补发事件 seq %d..%d", hubSeq+1, last)
	}
	// hello.seq is this session's delivery ack, absolutely: a restarted
	// hub counts from zero and a monotonic update would keep the previous
	// session's high watermark.
	c.setHubAck(hubSeq)
	// Hub is caught up through hubSeq — raise the enqueue watermark so a
	// later 0→1 does not re-send what the hub already has. Only safe when
	// hubSeq is in this process's seq space (the caller already checked).
	c.seqMu.Lock()
	if hubSeq > c.lastSentSeq {
		c.lastSentSeq = hubSeq
	}
	c.seqMu.Unlock()
	return nil
}

// enqueueSeqResetLocked asks the hub to clear per-host LastSeq + event
// buffer so a restarted host can uplink from seq 1 again. Critical: must
// land before any subsequent events frame on the same connection. The
// caller must hold enqueueMu (handleHelloSeq keeps it across the reset +
// the replay).
//
//	{"v":1,"type":"seq_reset"}
func (c *Client) enqueueSeqResetLocked() bool {
	payload, err := json.Marshal(map[string]any{
		"v":    1,
		"type": "seq_reset",
	})
	if err != nil {
		return false
	}
	return c.enqueueFrameWait(payload)
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
		// Released frame by frame: a stale queue still holds its budget, and
		// forgetting it would leak space for the life of the process.
	drainSend:
		for {
			select {
			case payload := <-c.sendCh:
				c.releaseSend(len(payload))
				n++
			default:
				break drainSend
			}
		}
	}
	if c.ctrlCh != nil {
		drain(c.ctrlCh)
	}
	if c.reqCh != nil {
		drainReq(c.reqCh)
	}
	if n > 0 {
		log.Printf("[hub-client] 重连前清空发送队列 %d 帧", n)
	}
}

// enqueueFrame queues a pre-built JSON frame on sendCh. critical waits
// up to sendCriticalWait for a slot and byte budget; otherwise the frame is
// dropped when either is exhausted. Reports whether the frame was queued.
func (c *Client) enqueueFrame(payload []byte, critical bool) bool {
	if c.sendCh == nil {
		return false
	}
	n := len(payload)
	if critical {
		if !c.reserveSendWait(n, sendCriticalWait) {
			c.noteFrameDrop("关键帧入队超时（发送预算满）", 0)
			return false
		}
		select {
		case c.sendCh <- payload:
			return true
		case <-time.After(sendCriticalWait):
			c.releaseSend(n)
			c.noteFrameDrop("关键帧入队超时（队列满）", 0)
			return false
		}
	}
	if !c.reserveSendNow(n) {
		c.noteFrameDrop("发送预算不足", 0)
		return false
	}
	select {
	case c.sendCh <- payload:
		return true
	default:
		c.releaseSend(n)
		c.noteFrameDrop("发送队列满", 0)
		return false
	}
}

// enqueueFrameWait blocks until the frame is queued or the session ends.
// seq_reset uses it: a dropped reset followed by a replay lets the hub
// keep the old watermark and stale-drop the whole replay. It waits for send
// budget too — the reset must never be silently dropped for a full queue.
func (c *Client) enqueueFrameWait(payload []byte) bool {
	if c.sendCh == nil {
		return false
	}
	n := len(payload)
	if !c.reserveSendWait(n, 0) {
		c.noteFrameDrop("会话结束，帧未入队", 0)
		return false
	}
	select {
	case c.sendCh <- payload:
		return true
	case <-c.sessionCtx().Done():
		c.releaseSend(n)
		c.noteFrameDrop("会话结束，帧未入队", 0)
		return false
	}
}

// ctrlEnqueueWait bounds how long readLoop may block handing a control frame
// to the writer. A writer stuck on one large frame (a 28MB respond on the WS
// transport) fills ctrlCh; blocking readLoop there stops it draining the
// socket, so relay requests and pings queue up behind a pong nobody is
// waiting for. Dropping under that pressure is safe: state_ack is
// retransmitted by the hub until acked, a lost pong is replaced by the next
// ping's, and host_status is re-sent on the next beat.
const ctrlEnqueueWait = 100 * time.Millisecond

// enqueueControl queues a small frame that must not be dropped because
// the event queue is full: pong, state_ack, host_status. When ctrlCh is
// unwired (tests), it falls back to a critical send on sendCh.
func (c *Client) enqueueControl(payload []byte) bool {
	if c.ctrlCh != nil {
		select {
		case c.ctrlCh <- payload:
			return true
		case <-c.sessionCtx().Done():
			return false
		case <-time.After(ctrlEnqueueWait):
			// Bounded wait, then drop: never park readLoop behind the
			// writer. Counted and logged (throttled) so a stalled writer
			// cannot silently swallow every ack.
			c.ctrlDropped.Add(1)
			c.noteFrameDrop("控制帧队列满", 0)
			return false
		}
	}
	return c.enqueueFrame(payload, true)
}

// dropLogWindow is how often the queue-pressure log may emit. One line per
// window keeps the first drop visible and rolls the rest into a count.
const dropLogWindow = 30 * time.Second

// noteFrameDrop records a dropped control frame (host_status, pong, a
// critical timeout) under queue pressure.
func (c *Client) noteFrameDrop(reason string, evs int) {
	c.noteDrop(1, evs, reason)
}

// noteEventDrop records a dropped batch of evs events (one frame's worth)
// under queue pressure.
func (c *Client) noteEventDrop(reason string, evs int) {
	c.noteDrop(0, evs, reason)
}

// noteDrop counts a suppressed uplink frame/event batch and logs at most
// one line per dropLogWindow, plus a summary of everything suppressed
// since the previous window. Without it a disconnected or stalled uplink —
// where every 50ms flush misses the queue — writes thousands of identical
// lines and hides the rest of the log (Oct 7: 2.5k lines over a two-hour
// DNS outage). frames counts control frames; evs counts events in a
// dropped events frame.
func (c *Client) noteDrop(frames, evs int, reason string) {
	c.dropMu.Lock()
	now := time.Now()
	var summary string
	if c.dropWindow.IsZero() {
		c.dropWindow = now
	} else if now.Sub(c.dropWindow) >= dropLogWindow {
		if c.dropFrames > 0 || c.dropEvents > 0 {
			summary = fmt.Sprintf("[hub-client] 上行队列压力持续：过去 %.0fs 共丢弃 %d 帧、%d 条事件（重放缓冲已保留，稍后补发）",
				dropLogWindow.Seconds(), c.dropFrames, c.dropEvents)
		}
		c.dropWindow = now
		c.dropFrames, c.dropEvents = 0, 0
		c.dropLogged = false
	}
	c.dropFrames += frames
	c.dropEvents += evs
	logNow := !c.dropLogged
	if logNow {
		c.dropLogged = true
	}
	c.dropMu.Unlock()

	if summary != "" {
		log.Print(summary)
	}
	if !logNow {
		return
	}
	switch {
	case evs > 0:
		log.Printf("[hub-client] %s，丢弃 %d 条事件（重放缓冲已保留，稍后补发）", reason, evs)
	case frames > 0:
		log.Printf("[hub-client] %s，丢弃帧", reason)
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
		return "", errNoToken
	}
	return c.token, nil
}

// wsSession dials the hub WebSocket and runs the shared frame loop. Over WS
// there is a single connection, so both planes multiplex onto it (the
// request-plane writer shares the connection under a write mutex).
func (c *Client) wsSession(ctx context.Context, bridge bridgeSource) error {
	tok, err := c.hostToken()
	if err != nil {
		return err
	}
	dialCtx, cancel := context.WithTimeout(ctx, wsDialTimeout)
	defer cancel()
	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization": []string{"Bearer " + tok},
			// Uplink compression is part of the protocol; the hello echoes
			// the same flag so both ends agree on the frame format.
			"X-Hub-Deflate": []string{"1"},
			// Confirmed state delivery is mandatory too: the hub refuses a
			// handshake without it (close reason "protocol version").
			"X-Hub-State-Ack": []string{"1"},
		},
		// 与 c.httpc 同一份代理判定，但独立实例：库会把
		// HTTPClient.Timeout 当成拨号超时，复用 50s 分钟的 httpc 会让
		// wsDialTimeout 失效。
		HTTPClient: &http.Client{Transport: c.httpc.Transport},
	}
	conn, resp, err := websocket.Dial(dialCtx, c.wsURL(), opts)
	if err != nil {
		// Classify the handshake by its HTTP status, not by the text the
		// library happens to include: a 401 is a rejected credential (the
		// reconnect loop re-pairs), anything else is a transport problem.
		if resp != nil {
			return &wsHandshakeError{status: resp.StatusCode, err: err}
		}
		return fmt.Errorf("hub ws dial: %w", err)
	}
	// 16MB read limit protects the DOWNLINK: a relayed request frame
	// carries a body the hub caps at 5MB plus the envelope, so 16MB is all
	// headroom. It is a different constraint from the 28MB uplink
	// maxRelayResponseBytes ceiling (hub-side read limit 32MB, QUIC length
	// prefix 31-bit) — do not tie the two numbers together.
	conn.SetReadLimit(16 << 20)
	defer conn.Close(websocket.StatusNormalClosure, "")

	log.Printf("[hub-client] 已连接 hub（ws）")
	c.noteConnected(TransportWS)

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
// cannot be opened (peer stream limit) or the in-flight cap is hit, respond
// frames ride B — or A when even B failed — which is exactly the
// WS-equivalent single-stream behavior.
//
// Per-request streams (T1): the first respond for a relay request opens a
// dedicated bidirectional stream, primed with a no-op pong, and every
// respond for that request rides the same stream until its final frame
// closes it. Frames are self-describing (JSON "type" + "reqId"), so the hub
// accepts streams in arrival order and dispatches purely by frame type —
// no stream carries a request's identity. Keeping each request's answer on
// its own stream removes QUIC flow-control head-of-line blocking between a
// relay answer (up to the 28MB maxRelayResponseBytes ceiling, which
// saturates its stream's window for seconds) and both the control frames
// and other requests' small responds. See PROTOCOL.md.
func (c *Client) quicSession(ctx context.Context, bridge bridgeSource) (bool, error) {
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
	// Stream B failure is not fatal: the hub may cap bidirectional streams
	// at 1. It is only the shared fallback for per-request streams; without
	// it responds share stream A (WS-equivalent behavior).
	streamB, err2 := openStream()
	if err2 != nil {
		log.Printf("[hub-client] 共享请求流创建失败（%v），中转响应退回控制流", err2)
		streamB = streamA
	} else {
		defer streamB.Close()
	}

	// Auth rides stream A, uncompressed (compression starts after the
	// hello). "deflate" and "stateAck" are part of the handshake contract:
	// the hub echoes the first and refuses a frame without the second.
	auth, _ := json.Marshal(map[string]any{
		"v": 1, "type": "auth", "token": tok, "deflate": true, "stateAck": true,
	})
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
	c.noteConnected(TransportQUIC)

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
// the shared stream — a stream-limit exhaustion must not break relay
// answers.
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

// QUIC flapping: a session counts as "young" when it died before
// quicFlapMin. Three in a row (well inside one reconnect cycle — the hub
// marks a silent uplink offline at 30s and drops the socket at 45s, so a
// rotation path dies well inside two minutes) arm quicFailCooldown of
// WebSocket instead.
const (
	quicFlapMax = 3
	quicFlapMin = 2 * time.Minute
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

// quicKeepAlive is the QUIC transport keepalive. It is what refreshes a
// client-side NAT binding. The hub's JSON ping is the downlink beat;
// this host does not send an application ping of its own.
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
func (c *Client) runSession(ctx context.Context, bridge bridgeSource, tr frameTransport) error {
	// Both transports call noteConnected at their handshake and funnel here,
	// so this is the one place that can reliably say the session is over.
	defer c.noteDisconnected()
	// Drop stale uplink frames left from a previous session before the
	// write loop starts, so they cannot interleave with hello-driven
	// replay and double-send.
	c.drainSendCh()
	// Compression is armed for the whole session; the flag is cleared at the
	// top of every session so no state leaks from a previous hub.
	c.deflateOK.Store(false)
	// The downlink watchdog budget is per session too: reset to the local
	// default before the read loop starts until hello advertises the hub's
	// own idleTimeout.
	c.idleTimeoutNanos.Store(0)
	// New session ⇒ new hub process possibly, whose subscriber-count
	// generation counter restarts low. Clear our high-water mark or every
	// count from a restarted hub would look stale and be ignored.
	c.subsGen.Store(0)
	// The viewing set is per-session state as well: a restarted hub starts
	// with no set, and a stale set from the previous session would keep
	// trimming sessions nobody is watching.
	c.viewMu.Lock()
	c.viewSeen = false
	c.viewAll = false
	c.viewGen = 0
	c.viewSIDs = nil
	c.viewMu.Unlock()
	// hubAckSeq is left as the previous session left it until hello
	// installs the new watermark (setHubAck). Zeroing it here would make
	// every buffered event look unacked for the whole dial, and the ring
	// would refuse to drop events the previous hub had already confirmed.
	c.replaySaturated.Store(false)
	c.probe.Store(false)
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

// idleWatchdog fails the session when no frame has arrived for the current
// session's idle budget, so the reconnect loop can take over. See
// defaultSessionIdleTimeout for why the transport's own error reporting is
// not enough (half-open sockets never report anything).
//
// The budget is re-read on every tick: hello arrives after this goroutine
// starts and may replace the default with the hub's own idleTimeout. The
// ticker only paces the CHECK, so a shorter budget applies at the next tick
// rather than a full old interval later.
func (c *Client) idleWatchdog(ctx context.Context) error {
	timeout := c.idleTimeout()
	t := time.NewTicker(watchdogTick(timeout))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if cur := c.idleTimeout(); cur != timeout {
				timeout = cur
				t.Reset(watchdogTick(timeout))
			}
			last := c.lastRecvUnixNano.Load()
			if last == 0 {
				continue
			}
			if idle := time.Since(time.Unix(0, last)); idle >= timeout {
				return fmt.Errorf("hub 静默 %.0fs（无任何下行帧，阈值 %.0fs），判定链路已死", idle.Seconds(), timeout.Seconds())
			}
		}
	}
}

// watchdogTick is the check interval for a given budget, floored so a tiny
// budget (tests) cannot produce a zero-duration ticker.
func watchdogTick(timeout time.Duration) time.Duration {
	if d := timeout / watchdogTicksPerTimeout; d > 0 {
		return d
	}
	return time.Millisecond
}

// idleTimeout is the current session's downlink watchdog budget.
func (c *Client) idleTimeout() time.Duration {
	if d := time.Duration(c.idleTimeoutNanos.Load()); d > 0 {
		return d
	}
	return defaultSessionIdleTimeout
}

// applyHubLiveness adopts the liveness parameters a hub advertised in its
// hello, so the two ends agree on when the link is dead instead of each
// hard-coding its own assumption (the MQTT Server Keep Alive idea). Both
// fields are mandatory in hello; a value out of range is a hub bug and
// keeps the local default rather than letting a typo become a reconnect
// loop here (idleTimeout too small) or a link that takes minutes to notice
// death (too large).
//
// pingInterval is accepted but only logged: it describes the hub's own
// cadence, and the watchdog needs only the resulting idleTimeout.
func (c *Client) applyHubLiveness(pingInterval, idleTimeout int) {
	if pingInterval < minHelloPingSec || pingInterval > maxHelloPingSec {
		log.Printf("[hub-client] 忽略越界的 hello.pingInterval=%d（允许 %d..%d）", pingInterval, minHelloPingSec, maxHelloPingSec)
	}
	if idleTimeout < minHelloIdleSec || idleTimeout > maxHelloIdleSec {
		log.Printf("[hub-client] 忽略越界的 hello.idleTimeout=%d（允许 %d..%d），沿用本地 %.0fs", idleTimeout, minHelloIdleSec, maxHelloIdleSec, c.idleTimeout().Seconds())
		return
	}
	c.idleTimeoutNanos.Store(int64(time.Duration(idleTimeout) * time.Second))
}

func (c *Client) writeLoop(ctx context.Context, send func([]byte) error) error {
	// Control frames (pong, state_ack, host_status) go out ahead of
	// events. A nil ctrlCh is never selected, so tests that only wire
	// sendCh keep a single queue.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload := <-c.ctrlCh:
			if err := send(payload); err != nil {
				return err
			}
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload := <-c.ctrlCh:
			if err := send(payload); err != nil {
				return err
			}
		case payload, ok := <-c.sendCh:
			if !ok {
				return errors.New("send channel closed")
			}
			// Release on both outcomes: the budget covers the payload until
			// the write attempt finishes, success or failure.
			err := send(payload)
			c.releaseSend(len(payload))
			if err != nil {
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

func (c *Client) readLoop(ctx context.Context, recv func() ([]byte, error), bridge bridgeSource) error {
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
			SubsGen     *uint64         `json:"subsGen"`     // type:"hello" — subscriber-count generation stamp (acked immediately)
			Gen         *uint64         `json:"gen"`         // type:"subscribers" / "viewing" — ordering stamp
			Seq         *uint64         `json:"seq"`         // type:"hello" / "ping" — hub watermark
			Deflate     *bool           `json:"deflate"`     // type:"hello" — uplink compression ack
			// PingInterval / IdleTimeout: type:"hello" only — the hub states
			// the time scale it judges silence on, in seconds, so the two
			// ends agree instead of each hard-coding it (MQTT Server Keep
			// Alive). See applyHubLiveness.
			PingInterval *int `json:"pingInterval"`
			IdleTimeout  *int `json:"idleTimeout"`
			// Probe is hello.probe. Present and true means this hub has a
			// socket probe, so host_status may slow down. Absent (QUIC)
			// keeps the fast beat.
			Probe *bool `json:"probe"`
			// Viewing is the hub's answer to "which sessions do its browser
			// pages display" — see viewingFiltered. Absent means no page has
			// declared anything yet, which leaves the host unfiltered
			// (viewSeen=false).
			Viewing *viewingView `json:"viewing"`
			// The standalone "viewing" frame carries the same set flat at the
			// top level; hello nests it under "viewing" instead.
			All      *bool    `json:"all"`
			Sessions []string `json:"sessions"`
			// Error is type:"auth_error" — the hub's reason for refusing
			// our credential over QUIC (WS expresses this as a close reason).
			Error string `json:"error"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[hub-client] 忽略无法解析的中转消息: %v", err)
			continue
		}
		switch msg.Type {
		case "hello":
			// hello is the version handshake: every field this build acts on
			// is mandatory. A hub that omits one predates the current
			// protocol, and the two ends must not run mismatched — fail the
			// session so the operator upgrades the hub instead of debugging
			// subtle behaviour differences.
			if msg.Seq == nil || msg.Subscribers == nil || msg.Deflate == nil || !*msg.Deflate ||
				msg.PingInterval == nil || msg.IdleTimeout == nil {
				return fmt.Errorf("hub 版本过旧：hello 缺少当前协议字段（seq/subscribers/deflate/pingInterval/idleTimeout），请升级 hub")
			}
			// hello.seq drives the whole resume: handleHelloSeq replays from
			// the hub's delivery watermark and also runs the 0→1 catch-up
			// when the subscriber count goes from zero to one.
			//
			// hello.subsGen stamps that count. Applying and acking it here
			// saves the hub's 1s retransmit of the same count as a standalone
			// `subscribers` frame on every connection; the retransmit remains
			// as the repair path when this ack is lost.
			subsGen := uint64(0)
			if msg.SubsGen != nil {
				subsGen = *msg.SubsGen
			}
			if c.applySubscribers(*msg.Subscribers, subsGen, bridge, false) && subsGen > 0 {
				c.sendStateAck("subscribers", subsGen)
			}
			if msg.Viewing != nil {
				c.applyViewing(*msg.Viewing)
			}
			c.deflateOK.Store(true)
			log.Printf("[hub-client] 上行压缩已启用（deflate）")
			c.applyHubLiveness(*msg.PingInterval, *msg.IdleTimeout)
			c.probe.Store(msg.Probe != nil && *msg.Probe)
			if err := c.handleHelloSeq(*msg.Seq); err != nil {
				return err
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
			if c.applySubscribers(n, gen, bridge, true) && gen > 0 {
				c.sendStateAck("subscribers", gen)
			}
		case "viewing":
			// The hub sends this set flat at the top level.
			v := viewingView{Sessions: msg.Sessions}
			if msg.All != nil {
				v.All = *msg.All
			}
			if msg.Gen != nil {
				v.Gen = *msg.Gen
			}
			if c.applyViewing(v) && v.Gen > 0 {
				c.sendStateAck("viewing", v.Gen)
			}
		case "request":
			go c.handleRelay(ctx, msg.ReqID, msg.HostID, msg.Method, msg.Path, msg.Body)
		case "auth_error":
			// QUIC-only: the hub writes this frame and closes when the
			// token is missing or invalid. Without it the session looks
			// like an ordinary drop — backoff redial forever, never a
			// re-pair. classifyClose stays WS-only (there is no close
			// frame here); the sentinel is what isAuthErr recognises.
			reason := msg.Error
			if reason == "" {
				reason = "hub 未说明原因"
			}
			return fmt.Errorf("%w: %s", errHubAuthRejected, reason)
		case "ping":
			// Data-plane delivery ack piggy-backed on the liveness ping:
			// anchors needCatchUp repairs at the hub's actual watermark
			// instead of the enqueue watermark. Absolute state (subscribers
			// / viewing) rides its own confirmed frames, never this one.
			if msg.Seq != nil {
				c.noteHubAck(*msg.Seq)
				// An ack that just landed is the cheapest possible moment
				// to repair: the hole is still open and the repair does not
				// have to wait out the next repairInterval. It runs on this
				// goroutine because readLoop must keep draining the socket;
				// repairDroppedEvents takes enqueueMu itself and is a no-op
				// unless a hole is actually open.
				if c.needCatchUp.Load() {
					go c.repairDroppedEvents()
				}
			}
			pong, _ := json.Marshal(map[string]any{"v": 1, "type": "pong"})
			c.enqueueControl(pong)
		case "pong":
			// ignore
		}
	}
}

// applySubscribers applies a hub subscriber-count update, ignoring frames
// that lost a race with a newer one. It reports whether the frame was
// accepted (applied, or already applied at this gen) — the caller acks on
// true, and a duplicate retransmit of an already-applied gen still acks so
// the hub's timer can stop.
//
// The count is ABSOLUTE state that decides whether we upload bridge events
// at all, and the hub writes its notifications from one goroutine per host
// — so a browser refresh (unsubscribe → 0, resubscribe → 1, microseconds
// apart) can arrive as 1 then 0. Applying the stale 0 pauses our uplink
// while host_status heartbeats keep us "online": the user's freshly
// reloaded page looks connected and never updates. `gen` is the hub's
// monotonic stamp (always present: hello's subscriber count is applied
// without one only because it precedes the first generation).
//
// replayOnResume controls whether a 0→1 transition also replays what was
// buffered while paused and re-asserts host_status. The `hello` frame is
// the one caller that passes false: handleHelloSeq runs immediately after
// it and replays from hello.seq, an interval that fully contains the 0→1
// catch-up (hubSeq <= lastSentSeq), so doing both would queue the same
// events twice — up to 2×replayCap events into a 256-slot queue, whose
// critical enqueues then time out and re-arm needCatchUp. hello also
// follows runSession's own host_status, so the extra status is redundant.
func (c *Client) applySubscribers(count int, gen uint64, bridge bridgeSource, replayOnResume bool) bool {
	if gen > 0 {
		for {
			prev := c.subsGen.Load()
			if gen < prev {
				return false // superseded by a newer count already applied
			}
			if gen == prev {
				return true // duplicate delivery of the newest state
			}
			if c.subsGen.CompareAndSwap(prev, gen) {
				break
			}
		}
	}
	was := c.forwardingEnabled()
	c.setBrowserSubscribers(count)
	if replayOnResume && !was && c.forwardingEnabled() {
		// Catch up events buffered while paused (they were seqAndReplay'd
		// but never enqueued). The ordering lock spans the decision AND the
		// replay: a live batch taking it first would let the hub see a
		// higher seq before the reset/replay and stale-drop the replay.
		c.enqueueMu.Lock()
		c.seqMu.Lock()
		after := c.lastSentSeq
		trimmed := c.pausedTrimmedThrough
		c.seqMu.Unlock()
		if trimmed > after {
			// Paused compaction dropped events the hub never received, so
			// the ring now starts above the hub's watermark: replaying
			// alone would leave a permanent hole (the hub parks the tail in
			// its out-of-order set and the watermark never moves). Reset
			// the epoch and replay what we still hold.
			if c.resumeAfterPausedTrimLocked() {
				log.Printf("[hub-client] 暂停裁剪越过 hub 水位（through %d > %d），已重置世代并重放", trimmed, after)
			}
		} else if last := c.sendReplayAfterLocked(after); last > 0 {
			log.Printf("[hub-client] 订阅恢复补发事件 seq %d..%d", after+1, last)
		}
		c.enqueueMu.Unlock()
		c.enqueueHostStatus(bridge)
	}
	return true
}

// viewingView is the hub's viewing set for this host: which sessions its
// browser pages are displaying (see Hub.ViewingStateFor on the hub side).
//
// All means the hub cannot narrow the audience — at least one page that
// receives this host's events has not declared a session yet (still
// loading). The host then filters nothing.
type viewingView struct {
	All      bool     `json:"all"`
	Sessions []string `json:"sessions"`
	// Gen is the hub's monotonic stamp. Viewing sets are absolute state
	// written from one goroutine per host, so a switch (old set → new set,
	// microseconds apart) can arrive out of order; without the stamp the
	// stale one wins and the host trims exactly the session the user just
	// opened. 0 means the frame carried no stamp; it is applied without the
	// ordering gate.
	Gen uint64 `json:"gen"`
}

// applyViewing replaces the uplink session filter. Absolute state: every
// frame carries the whole set, so a host that missed one is corrected by
// the hub's retransmit rather than staying stuck on a partial set.
//
// It reports whether the frame was accepted, so the caller can acknowledge
// it. A duplicate of an already-applied gen reports true as well: the hub
// only stops retransmitting once it sees the ack, and silence would make a
// lost ack look like a lost frame.
func (c *Client) applyViewing(v viewingView) bool {
	next := make(map[string]bool, len(v.Sessions))
	for _, sid := range v.Sessions {
		if sid != "" {
			next[sid] = true
		}
	}
	c.viewMu.Lock()
	if v.Gen > 0 && v.Gen < c.viewGen {
		c.viewMu.Unlock()
		return false // superseded by a newer set already applied
	}
	if v.Gen > 0 && v.Gen == c.viewGen && c.viewSeen {
		c.viewMu.Unlock()
		return true // duplicate of the set already in force
	}
	if v.Gen > c.viewGen {
		c.viewGen = v.Gen
	}
	prevAll, prevSeen, prevN := c.viewAll, c.viewSeen, len(c.viewSIDs)
	c.viewAll = v.All
	c.viewSeen = true
	c.viewSIDs = next
	c.viewMu.Unlock()
	if !prevSeen || prevAll != v.All || prevN != len(next) {
		if v.All {
			log.Printf("[hub-client] 订阅页面未声明会话，上行事件不过滤")
		} else {
			log.Printf("[hub-client] 上行只发送 %d 个被查看会话的正文（其余只留序号）", len(next))
		}
	}
	return true
}

// sendStateAck confirms that an absolute-state frame of `kind` at
// generation gen has been applied, which is what lets the hub stop
// retransmitting it. A gen of 0 means the frame carried no stamp, so there
// is nothing to confirm.
//
// The ack rides the control queue, not the event queue. A lost ack is
// recovered by the hub's backoff retransmit; dropping it because events
// filled sendCh is what made that retransmit run forever.
func (c *Client) sendStateAck(kind string, gen uint64) {
	if gen == 0 {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"v": 1, "type": "state_ack", "kind": kind, "gen": gen,
	})
	if err != nil {
		return
	}
	c.enqueueControl(payload)
}

// viewingFiltered reports whether events of sessionID must be replaced by
// a placeholder on the wire. Filtering only starts after the hub has sent a
// viewing set; a host that guessed "nobody is watching" would silently stop
// the transcript.
//
// A session that spawns a subagent stays watched through its children: the
// child transcript feeds the parent page's mini scrollback, and the page
// never declares the child id.
func (c *Client) viewingFiltered(sessionID string) bool {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	if !c.viewSeen || c.viewAll {
		return false
	}
	if sessionID == "" || c.viewSIDs[sessionID] {
		return false
	}
	if parent := c.viewChildren[sessionID]; parent != "" && c.viewSIDs[parent] {
		return false
	}
	return true
}

// noteViewingChild records a subagent child session so its stream follows
// its parent's watch state (see viewingFiltered). One level, same rule as
// the hub's guest whitelist; the table is a bounded FIFO because a
// long-lived host spawns without limit. Evicting the oldest relation is
// safe: the child is merely trimmed until the hub re-supplies it.
func (c *Client) noteViewingChild(childSessionID, subagentID, parentSessionID string) {
	if childSessionID == "" || parentSessionID == "" || childSessionID == parentSessionID {
		return
	}
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	c.setViewChildLocked(childSessionID, parentSessionID)
	if subagentID != "" {
		c.setViewChildLocked(subagentID, parentSessionID)
	}
}

// setViewChildLocked inserts key without letting viewChildren exceed
// viewingChildCap, evicting oldest-first. Caller holds viewMu.
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
// PROTOCOL.md §6.4 is the authoritative list; control rails and terminals
// stay unfiltered so a page switching in finds the session consistent.
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

// handleRelay executes one relayed browser request against the local HTTP
// API and posts the answer back to the hub over the WebSocket. The hub
// routes by hostId and stamps every request frame with its target; a
// frame naming another host (hub bug / stale routing) is rejected here
// instead of executing locally. Empty hostId (pre-hostId hubs) is
// tolerated for rolling upgrades — the hub is the trust boundary either
// way, so the check is defense-in-depth, not auth.
//
// Execution path: in-process via c.local (server.Handler()) when wired —
// 同一条 withAuth/withCORS 管线，无本机端口依赖；否则回退 LocalBase 的
// TCP 回环 HTTP 直调（外部装配方与旧测试）。
func (c *Client) handleRelay(ctx context.Context, reqID, hostID, method, path string, body json.RawMessage) {
	if hostID != "" && hostID != c.cfg.HostID {
		log.Printf("[hub-client] 拒绝非本机中转请求: target=%s self=%s %s %s", hostID, c.cfg.HostID, method, path)
		c.respond(reqID, 404, mustJSON(map[string]any{"ok": false, "error": "host 未配对"}))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 50*time.Minute)
	defer cancel()

	if c.local != nil {
		c.relayInProcess(ctx, reqID, method, path, body)
		return
	}

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
	// transport, where an over-limit frame would exceed the hub's read
	// limit and kill the whole connection (see maxRelayResponseBytes).
	rb, err := io.ReadAll(io.LimitReader(res.Body, maxRelayResponseBytes+1))
	if err != nil {
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": err.Error()}))
		return
	}
	if len(rb) > maxRelayResponseBytes {
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": fmt.Sprintf("本地响应过大（>%dMB），无法中继", maxRelayResponseBytes>>20)}))
		return
	}
	c.respond(reqID, res.StatusCode, json.RawMessage(rb))
}

// relayResponseWriter 是进程内中继的响应记录器：缓存状态码与响应体
// （超过 maxRelayResponseBytes 即截断并标记——respond 帧走 host↔hub 传输，
// 上限以上的帧会触发 hub 的 WS 读上限并杀掉整条连接，与回环路径同一门槛）。
// Flush 按 no-op 处理：中继应答是单帧全量回传，流式 flush 无处可去。
type relayResponseWriter struct {
	hdr       http.Header
	buf       bytes.Buffer
	code      int
	truncated bool
}

func (w *relayResponseWriter) Header() http.Header { return w.hdr }

func (w *relayResponseWriter) Write(p []byte) (int, error) {
	if w.truncated {
		return len(p), nil // 继续吞掉写入，让 handler 正常走完
	}
	if w.buf.Len()+len(p) > maxRelayResponseBytes+1 {
		w.truncated = true
		return len(p), nil
	}
	return w.buf.Write(p)
}

func (w *relayResponseWriter) WriteHeader(code int) { w.code = code }

func (w *relayResponseWriter) Flush() {}

// relayInProcess 直接调用本 host 的 API 处理链并回帧。net/http 的服务器
// 通常会 recover handler panic；进程内直调没有这层防护，这里补齐——
// panic 必须变成 500 响应帧，而不是击穿 hub client 的会话 goroutine。
func (c *Client) relayInProcess(ctx context.Context, reqID, method, path string, body json.RawMessage) {
	var rd io.Reader
	if len(body) > 0 {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://capri-local"+path, rd)
	if err != nil {
		c.respond(reqID, 500, mustJSON(map[string]any{"ok": false, "error": "invalid relay request"}))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.AccessToken)
	}
	rec := &relayResponseWriter{hdr: http.Header{}}
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
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": fmt.Sprintf("本地响应过大（>%dMB），无法中继", maxRelayResponseBytes>>20)}))
		return
	}
	if rec.code == 0 {
		rec.code = http.StatusOK
	}
	c.respond(reqID, rec.code, json.RawMessage(rec.buf.Bytes()))
}

// relayChunkSize is the raw body bytes carried by one chunked `respond`
// frame. A 28MB answer used to go out as a single frame whose write holds the
// transport's write lock for tens of seconds on a slow link, blocking every
// event, ping and host_status behind it. At ~1MB per chunk (≈1.37MB base64)
// the writer releases the lock between frames, so the link stays responsive
// and each frame stays far below both transports' 32MB read limit.
//
// Responses at or below this size keep the original single-frame shape; the
// hub assembles a chunked body back into the exact same bytes before handing
// it to the browser, so the FE never sees the split.
const relayChunkSize = 1 << 20

// respond answers a relayed request with status and body.
func (c *Client) respond(reqID string, status int, body json.RawMessage) {
	// A body that is not embeddable JSON (JSONL lines, plain text — json
	// compacts RawMessage and rejects concatenated values) must not
	// silently strand the hub's pending request: wrap it as a JSON string
	// so the relay still answers instead of hanging until the browser
	// gives up.
	if !json.Valid(body) {
		body = mustJSON(string(body))
	}
	if len(body) > relayChunkSize {
		c.respondChunked(reqID, status, body)
		return
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
	// (T1) so a large relay answer (up to the 28MB maxRelayResponseBytes
	// ceiling) never head-of-line blocks the control/event frames or other
	// requests.
	// A relay request gets exactly one respond — its terminal frame, which
	// also closes the request's stream.
	c.enqueueReqFrame(reqFrame{reqID: reqID, payload: payload, final: true})
}

// respondChunked splits an oversized body into relayChunkSize chunks and
// queues one `respond` frame per chunk. Every frame repeats reqId/status and
// carries `chunk` (base64 of that slice) + `encoding:"base64"` + `more`; the
// hub appends the decoded bytes and only resolves the browser request on the
// `more:false` frame. `final` is true only on that last frame, so the QUIC
// per-request stream stays open across chunks and closes exactly once.
//
// The chunking is not visible to the FE: the hub reassembles the exact
// original body bytes before answering the relayed HTTP request.
func (c *Client) respondChunked(reqID string, status int, body json.RawMessage) {
	chunks := (len(body) + relayChunkSize - 1) / relayChunkSize
	for i := 0; i < chunks; i++ {
		start := i * relayChunkSize
		end := start + relayChunkSize
		if end > len(body) {
			end = len(body)
		}
		more := i < chunks-1
		payload, err := json.Marshal(map[string]any{
			"v":        1,
			"type":     "respond",
			"reqId":    reqID,
			"status":   status,
			"chunk":    base64.StdEncoding.EncodeToString(body[start:end]),
			"encoding": "base64",
			"more":     more,
		})
		if err != nil {
			// Cannot happen for a string/number map; a partially queued
			// answer would leave the hub pending until its timeout, which
			// is the same outcome as any other write failure.
			log.Printf("[hub-client] 分块响应编码失败 reqId=%s: %v", reqID, err)
			return
		}
		c.enqueueReqFrame(reqFrame{reqID: reqID, payload: payload, final: !more})
	}
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
// A request has exactly one respond. Dropping it leaves the browser
// waiting out RelayTimeout while heartbeats keep the host looking alive,
// so this waits until the frame is queued or the session ends.
func (c *Client) enqueueReqFrame(f reqFrame) {
	if c.reqCh == nil {
		if !c.enqueueFrameWait(f.payload) {
			log.Printf("[hub-client] 中转响应随会话结束未发出 reqId=%s", f.reqID)
		}
		return
	}
	select {
	case c.reqCh <- f:
	case <-c.sessionCtx().Done():
		log.Printf("[hub-client] 中转响应随会话结束未发出 reqId=%s", f.reqID)
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
