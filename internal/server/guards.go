package server

import (
	"github.com/AgentsHarness/capri-host/internal/config"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── Host and browser-origin guards ────────────────────────────────────

// hostOnly strips an optional port and IPv6 brackets from a Host header.
func hostOnly(hostport string) string {
	hostport = strings.TrimSpace(hostport)
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(hostport, "[]")
}

// isLoopbackHost reports whether host names this machine's loopback
// interface: localhost, 127.0.0.0/8, or ::1.
func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// isAllowedHost reports whether a request's Host header names an address this
// host is deliberately serving. It is the DNS-rebinding line of defence: a
// page at evil.example that resolves to 127.0.0.1 keeps sending
// `Host: evil.example:8765`, so rejecting anything outside loopback and the
// configured bind address stops the page before Origin (which is also
// evil.example) is even considered. A literal IP cannot be supplied by DNS
// rebinding; it is allowed only when the listener itself is non-loopback.
func (s *Server) isAllowedHost(hostport string) bool {
	host := hostOnly(hostport)
	// HTTP/1.0 requests may omit Host. Browsers always send it, and local
	// non-browser clients are already inside the local-trust boundary.
	if host == "" || isLoopbackHost(host) {
		return true
	}
	if s.cfg.BindIsLoopback() {
		return false
	}
	bind := strings.Trim(strings.TrimSpace(s.cfg.BindAddr), "[]")
	if bind == "" {
		bind = config.DefaultBindAddr
	}
	if ip := net.ParseIP(host); ip != nil {
		if bind == "0.0.0.0" || bind == "::" {
			return true
		}
		if bip := net.ParseIP(bind); bip != nil {
			return bip.Equal(ip)
		}
		return false
	}
	// BIND may itself be a hostname; the browser will then put that same
	// name in Host.
	return strings.EqualFold(host, bind)
}

// withHostGuard rejects requests whose Host header is not a local address or
// the configured bind address. See isAllowedHost for the DNS-rebinding
// rationale.
func (s *Server) withHostGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isAllowedHost(r.Host) {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"ok": false, "error": "不受信任的 Host",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isLocalOrigin reports whether the request's Origin header (when present)
// names the host's own UI: loopback on any port, or the request's own Host
// when that Host itself passed the Host allowlist (same-origin calls through
// a LAN address). A missing Origin — curl, server-side callers, same-origin
// browser requests — is allowed: browsers only attach Origin on cross-origin
// or non-GET requests, and a hostile page cannot forge it to a local value.
// `Origin: null` (sandboxed iframes) has no host and is rejected.
//
// Origin.Host == r.Host alone is deliberately not enough: after DNS
// rebinding both are the attacker's domain. isAllowedHost is what makes the
// equality trustworthy.
func (s *Server) isLocalOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if isLoopbackHost(u.Hostname()) {
		return true
	}
	// Compare hostnames, not full host:port, so the same UI reached through
	// a LAN address on a different port (Vite dev on 192.168.x.y:5173 hitting
	// the host on :8765) stays local. The Host allowlist is what keeps this
	// from trusting a rebinding domain.
	return strings.EqualFold(u.Hostname(), hostOnly(r.Host)) && s.isAllowedHost(r.Host)
}

// originAllowed reports whether r's Origin may reach the host API: the local
// UI (including Vite dev on another loopback port), or the live hub URL's
// origin (deployed FE on the hub talking straight to this host's 127.0.0.1
// port). Requests without Origin come from non-browser callers and keep the
// local-trust behaviour.
func (s *Server) originAllowed(r *http.Request) bool {
	if r.Header.Get("Origin") == "" {
		// Browsers sometimes omit Origin on a form POST. Sec-Fetch-Site
		// still marks it cross-site; reject that. curl and other
		// non-browser callers send neither header and keep local trust.
		return !strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site")
	}
	return s.isLocalOrigin(r) || s.isTrustedHubOrigin(r.Header.Get("Origin"))
}

// isTrustedHubOrigin is true when origin matches the live hub URL's
// scheme+host (path ignored). Empty hub URL → nothing trusted beyond local
// origins. Uses hubSnapshot so a runtime pairing is trusted without a restart
// (s.cfg.HubURL is the startup copy and would stay empty until then).
func (s *Server) isTrustedHubOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	hubURL := s.hubSnapshot().HubURL
	if hubURL == "" {
		return false
	}
	ou, err := url.Parse(origin)
	if err != nil || ou.Scheme == "" || ou.Host == "" {
		return false
	}
	hu, err := url.Parse(hubURL)
	if err != nil || hu.Scheme == "" || hu.Host == "" {
		return false
	}
	return sameURLOrigin(ou, hu)
}

// sameURLOrigin compares scheme + hostname + effective port. Comparing raw
// Host strings would reject `https://hub.example` against a HubURL written as
// `https://hub.example:443`.
func sameURLOrigin(a, b *url.URL) bool {
	if !strings.EqualFold(a.Scheme, b.Scheme) ||
		!strings.EqualFold(a.Hostname(), b.Hostname()) {
		return false
	}
	return effectiveURLPort(a) == effectiveURLPort(b)
}

func effectiveURLPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	}
	return ""
}

// withCORS applies the same allowlist to every browser-facing API and live
// endpoint, sensitive or not. `Access-Control-Allow-Origin: *` is never sent:
// /api/prompt, /api/fs/write-file and the rest can drive the agent, so a
// random page must not be able to call them merely because it can reach
// 127.0.0.1. Untrusted Origins are rejected outright (not merely left without
// ACAO), which also blocks "simple" cross-origin POSTs that skip preflight.
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIOrLivePath(r.URL.Path) {
			if !s.originAllowed(r) {
				writeJSON(w, http.StatusForbidden, map[string]any{
					"ok": false, "error": "cross-origin request denied",
				})
				return
			}
			w.Header().Add("Vary", "Origin")
			if origin := r.Header.Get("Origin"); origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			// Authorization: the FE attaches Bearer on every API call; the
			// preflight must admit it or cross-origin direct host calls (hub
			// mode 双连接) would be blocked by the browser.
			// Idempotency-Key：直连模式下浏览器直接对本 host 发写请求时
			// 也要带幂等键，预检必须放行。X-Capri-Request-ID 只在调试直连
			// 时由调用方手动带，放行它不改变正常浏览器路径。
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key, X-Capri-Request-ID")
			// Chrome Private Network Access: only a trusted origin may learn
			// that this private-network host is reachable.
			if r.Header.Get("Access-Control-Request-Private-Network") == "true" {
				w.Header().Set("Access-Control-Allow-Private-Network", "true")
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── inbound access-token gate ─────────────────────────────────────────

// authRequiredForPath reports whether a path requires the configured
// access token. Exemptions (by design, the FE's boot probes):
//   - `GET /`            — SPA static assets; browsers cannot attach custom
//     headers to script/style loads, and the page itself is public.
//   - `GET /api/hosts`   — detectMode()/probeAccess() must reach it before
//     any token exists. It carries no conversation data; the response
//     declares `authRequired` so the FE knows to show the gate. Its 401
//     semantics belong to the HUB (the FE uses 401 to tell "this is a
//     hub, not a host") — a host must never 401 here.
//
// Everything else under /api/* and the /events SSE stream is gated —
// including GET /api/probe, which exists precisely to answer "does the key
// this browser holds also open this host" with its 200/401.
func authRequiredForPath(path string) bool {
	if strings.HasPrefix(path, "/api/") {
		return path != "/api/hosts"
	}
	return path == "/events"
}

// authAttemptLimiter throttles failed inbound token attempts per client IP.
// A successful request clears the bucket; only wrong credentials consume it,
// so normal UI polling and EventSource reconnects are unaffected.
type authAttemptLimiter struct {
	mu     sync.Mutex
	failed map[string][]time.Time
	block  map[string]time.Time
}

const (
	authFailureLimit  = 10
	authFailureWindow = time.Minute
	authBlockDuration = time.Minute
)

func newAuthAttemptLimiter() *authAttemptLimiter {
	return &authAttemptLimiter{failed: make(map[string][]time.Time), block: make(map[string]time.Time)}
}

func (l *authAttemptLimiter) allow(ip string) bool {
	if l == nil || ip == "" {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if until, ok := l.block[ip]; ok {
		if now.Before(until) {
			return false
		}
		delete(l.block, ip)
		delete(l.failed, ip)
	}
	l.pruneLocked(now.Add(-authFailureWindow))
	return true
}

func (l *authAttemptLimiter) fail(ip string) {
	if l == nil || ip == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-authFailureWindow)
	hs := l.failed[ip]
	i := 0
	for i < len(hs) && hs[i].Before(cutoff) {
		i++
	}
	hs = append(hs[i:], now)
	l.failed[ip] = hs
	if len(hs) >= authFailureLimit {
		l.block[ip] = now.Add(authBlockDuration)
	}
}

func (l *authAttemptLimiter) reset(ip string) {
	if l == nil || ip == "" {
		return
	}
	l.mu.Lock()
	delete(l.failed, ip)
	delete(l.block, ip)
	l.mu.Unlock()
}

func (l *authAttemptLimiter) pruneLocked(cutoff time.Time) {
	for ip, hs := range l.failed {
		i := 0
		for i < len(hs) && hs[i].Before(cutoff) {
			i++
		}
		if i == len(hs) {
			delete(l.failed, ip)
		} else {
			l.failed[ip] = hs[i:]
		}
	}
}

func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// withAuth gates /api/* (except /api/hosts) and /events with the host's
// access token when one is configured; empty token leaves the gate open
// (local trusted default). Accepted transports:
//   - Authorization: Bearer <token>   (the FE's apiFetch always sends it)
//   - ?token=<token>                  (EventSource cannot set headers)
func withAuth(next http.Handler, token string, limiter *authAttemptLimiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || !authRequiredForPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		ip := remoteIP(r)
		if !limiter.allow(ip) {
			w.Header().Set("Retry-After", strconv.Itoa(int(authBlockDuration/time.Second)))
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"ok": false, "error": "认证尝试过于频繁，请稍后再试",
			})
			return
		}
		bearer := bearerToken(r.Header.Get("Authorization"))
		queryToken := strings.TrimSpace(r.URL.Query().Get("token"))
		if tokenEqual(bearer, token) || tokenEqual(queryToken, token) {
			limiter.reset(ip)
			next.ServeHTTP(w, r)
			return
		}
		if bearer != "" || queryToken != "" {
			limiter.fail(ip)
		}
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"ok": false, "error": "需要有效的访问 token",
		})
	})
}

// bearerToken extracts the token from an Authorization header
// ("Bearer <token>", case-insensitive prefix, trimmed).
func bearerToken(auth string) string {
	auth = strings.TrimSpace(auth)
	const prefix = "Bearer "
	if len(auth) >= len(prefix) && strings.EqualFold(auth[:len(prefix)], prefix) {
		return strings.TrimSpace(auth[len(prefix):])
	}
	return ""
}

// tokenEqual compares two tokens in constant time so the shared secret
// cannot be fingerprinted via timing (same scheme as capri-hub).
func tokenEqual(a, b string) bool {
	if a == "" || b == "" || len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
