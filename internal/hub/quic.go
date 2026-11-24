package hub

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

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
