package hub

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
)

// TestParseSPKIPin covers every accepted HUB_QUIC_PIN spelling (hex, base64
// variants, RFC 7469 prefixes) and the malformed ones that must fail.
func TestParseSPKIPin(t *testing.T) {
	sum := sha256.Sum256([]byte("spki-pin-test-vector"))
	hexPin := hex.EncodeToString(sum[:])
	b64Pin := base64.StdEncoding.EncodeToString(sum[:])
	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "hex", in: hexPin},
		{name: "hex uppercase", in: strings.ToUpper(hexPin)},
		{name: "hex with sha256/ prefix", in: "sha256/" + hexPin},
		{name: "base64 padded", in: b64Pin},
		{name: "base64 with sha256// prefix", in: "sha256//" + b64Pin},
		{name: "base64 raw url", in: base64.RawURLEncoding.EncodeToString(sum[:])},
		{name: "base64 url padded", in: base64.URLEncoding.EncodeToString(sum[:])},
		{name: "surrounding whitespace", in: "  " + hexPin + "\n"},
		{name: "empty", in: "", wantErr: true},
		{name: "garbage", in: "!!", wantErr: true},
		{name: "hex too short", in: hexPin[:62], wantErr: true},
		{name: "base64 wrong byte count", in: base64.StdEncoding.EncodeToString([]byte("short")), wantErr: true},
		{name: "prefix alone", in: "sha256//", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSPKIPin(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parse(%q) = %x, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse(%q): %v", tc.in, err)
			}
			if got != sum {
				t.Fatalf("parse(%q) = %x, want %x", tc.in, got, sum)
			}
		})
	}
}

// TestQUICTLSPinPolicy pins the config-level behavior of HUB_QUIC_PIN: it
// replaces (not supplements) the CA path whenever set, wins over
// HUB_QUIC_INSECURE and the https policy, and a malformed value fails
// closed instead of silently disabling verification.
func TestQUICTLSPinPolicy(t *testing.T) {
	pin := hex.EncodeToString(make([]byte, 32))

	t.Run("pin replaces CA verification", func(t *testing.T) {
		c := NewClient(Config{URL: "https://hub.example.com", QUICPin: pin})
		conf := c.quicTLSClientConfig("203.0.113.10")
		if !conf.InsecureSkipVerify {
			t.Error("InsecureSkipVerify = false; the pin path must skip the system CA chain")
		}
		if conf.VerifyPeerCertificate == nil {
			t.Fatal("VerifyPeerCertificate = nil; the pin must be enforced in its place")
		}
		if conf.ServerName != "" {
			t.Errorf("ServerName = %q; hostname verification is meaningless under a pin", conf.ServerName)
		}
		if len(conf.NextProtos) != 1 || conf.NextProtos[0] != "capri-hub" {
			t.Errorf("NextProtos = %v, want [capri-hub]", conf.NextProtos)
		}
	})
	t.Run("pin wins over insecure and http policy", func(t *testing.T) {
		for _, url := range []string{"http://10.0.0.5:8787", "https://hub.example.com"} {
			c := NewClient(Config{URL: url, QUICInsecure: true, QUICPin: pin})
			conf := c.quicTLSClientConfig("h")
			if conf.VerifyPeerCertificate == nil {
				t.Errorf("URL %s: pin not enforced when QUICInsecure also set", url)
			}
		}
	})
	t.Run("no pin keeps the CA policy", func(t *testing.T) {
		c := NewClient(Config{URL: "https://hub.example.com"})
		conf := c.quicTLSClientConfig("203.0.113.10")
		if conf.VerifyPeerCertificate != nil {
			t.Error("VerifyPeerCertificate set without a pin")
		}
		if conf.InsecureSkipVerify {
			t.Error("InsecureSkipVerify set without a pin over https")
		}
	})
	t.Run("malformed pin fails closed", func(t *testing.T) {
		c := NewClient(Config{URL: "https://hub.example.com", QUICPin: "definitely-not-a-fingerprint"})
		conf := c.quicTLSClientConfig("hub.example.com")
		if conf.VerifyPeerCertificate == nil {
			t.Fatal("malformed pin produced no verifier — verification would be silently disabled")
		}
		if err := conf.VerifyPeerCertificate(nil, nil); err == nil {
			t.Fatal("malformed pin verifier accepted a handshake; must always fail")
		}
	})
}

// TestQUICTLSPinHandshake drives the pin through a real QUIC handshake:
// the matching fingerprint connects, any other fails the handshake (and
// only the handshake — Run falls back to WebSocket).
func TestQUICTLSPinHandshake(t *testing.T) {
	th := newTestQUICHub(t)
	th.serve(nil)
	pin := th.spkiPin()
	local := httptest.NewServer(nil)
	defer local.Close()

	// Matching pin: the session must establish and reach hello.
	c := NewClient(Config{
		URL:       fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:    "h1",
		Token:     "tok123",
		LocalBase: local.URL,
		QUICPin:   hex.EncodeToString(pin[:]),
	})
	c.sendCh = make(chan []byte, 64)
	c.reqCh = make(chan reqFrame, 16)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)
	done := make(chan error, 1)
	go func() { done <- c.quicSession(ctx, bridge) }()
	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("pinned session never reached hello (forwarding not enabled)")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// Wrong pin: the handshake itself must fail.
	bad := pin
	bad[0] ^= 0xFF
	c2 := NewClient(Config{
		URL:       fmt.Sprintf("https://127.0.0.1:%d", th.port()),
		HostID:    "h1",
		Token:     "tok123",
		LocalBase: local.URL,
		QUICPin:   hex.EncodeToString(bad[:]),
	})
	c2.sendCh = make(chan []byte, 64)
	c2.reqCh = make(chan reqFrame, 16)
	bctx, bcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer bcancel()
	err := c2.quicSession(bctx, bridge)
	if err == nil {
		t.Fatal("session established against a mismatched SPKI pin")
	}
	if !strings.Contains(err.Error(), "SPKI") {
		t.Errorf("error does not mention the pin mismatch: %v", err)
	}
}

// TestPinSPKIVerifyUnits covers the verifier callback against real
// certificate bytes: exact match passes; a different key's SPKI fails.
func TestPinSPKIVerifyUnits(t *testing.T) {
	th := newTestQUICHub(t)
	pin := th.spkiPin()
	verify := pinSPKIVerify(pin)

	if err := verify([][]byte{th.certDER}, nil); err != nil {
		t.Fatalf("matching SPKI rejected: %v", err)
	}
	// The hub listener below has a DIFFERENT key → different SPKI.
	other := newTestQUICHub(t)
	err := verify([][]byte{other.certDER}, nil)
	if err == nil {
		t.Fatal("different certificate SPKI accepted")
	}
	if !strings.Contains(err.Error(), "SPKI") {
		t.Errorf("mismatch error lacks SPKI context: %v", err)
	}
	// No certificate at all: rejected.
	if err := verify(nil, nil); err == nil {
		t.Fatal("empty certificate chain accepted")
	}
}
