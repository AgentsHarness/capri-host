package hub

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ── SPKI pinning ──────────────────────────────────────────────────────

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
		{name: "surrounding whitespace", in: "  " + hexPin + "\n"},
		{name: "empty", in: "", wantErr: true},
		{name: "garbage", in: "!!", wantErr: true},
		{name: "hex too short", in: hexPin[:62], wantErr: true},
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

// TestQUICTLSPinHandshake: the matching fingerprint connects, a different one
// fails the handshake itself.
func TestQUICTLSPinHandshake(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1))
	hub.serve()
	pin := hub.spkiPin()
	local := httptest.NewServer(nil)
	defer local.Close()

	c := NewClient(Config{
		URL: hub.url(), HostID: "h1", Token: "tok123", LocalBase: local.URL,
		QUICPin: hex.EncodeToString(pin[:]),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := c.quicSession(ctx, newTestBridge())
		done <- err
	}()
	hub.control(t)
	cancel()
	<-done

	bad := pin
	bad[0] ^= 0xFF
	c2 := NewClient(Config{
		URL: hub.url(), HostID: "h1", Token: "tok123", LocalBase: local.URL,
		QUICPin: hex.EncodeToString(bad[:]),
	})
	bctx, bcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer bcancel()
	if _, _, err := c2.quicSession(bctx, newTestBridge()); err == nil {
		t.Fatal("session established against a mismatched SPKI pin")
	} else if !strings.Contains(err.Error(), "SPKI") {
		t.Errorf("error does not mention the pin mismatch: %v", err)
	}
}

func TestPinSPKIVerifyUnits(t *testing.T) {
	hub := newQUICTestHub(t, watchingAudience(1))
	pin := hub.spkiPin()
	verify := pinSPKIVerify(pin)

	if err := verify([][]byte{hub.certDER}, nil); err != nil {
		t.Fatalf("matching SPKI rejected: %v", err)
	}
	other := newQUICTestHub(t, watchingAudience(1))
	if err := verify([][]byte{other.certDER}, nil); err == nil {
		t.Fatal("different certificate SPKI accepted")
	}
	if err := verify(nil, nil); err == nil {
		t.Fatal("empty certificate chain accepted")
	}
}
