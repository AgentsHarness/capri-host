package hub

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/quic-go/quic-go"
)

// TestQUICDualStreamPlaneSeparation verifies T1: the host client opens two
// bidirectional streams — control/event plane (stream A: auth, events) and
// request plane (stream B: relay respond) — and the hub dispatches purely
// by frame type on whichever stream it accepted.
func TestQUICDualStreamPlaneSeparation(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-hub"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	qtls := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos:   []string{"capri-hub"},
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", qtls, &quic.Config{})
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	defer ln.Close()
	quicPort := ln.Addr().(*net.UDPAddr).Port

	eventsCh := make(chan map[string]any, 16)
	respondCh := make(chan map[string]any, 4)
	serverErr := make(chan error, 1)

	streamOf := func(conn *quic.Conn, s *quic.Stream) int { return int(s.StreamID()) }
	_ = streamOf

	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		readFrame := func(s *quic.Stream) ([]byte, error) {
			var lb [4]byte
			if _, err := io.ReadFull(s, lb[:]); err != nil {
				return nil, err
			}
			buf := make([]byte, binary.BigEndian.Uint32(lb[:]))
			_, err := io.ReadFull(s, buf)
			return buf, err
		}
		writeFrame := func(s *quic.Stream, v any) error {
			b, _ := json.Marshal(v)
			var lb [4]byte
			binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
			if _, err := s.Write(lb[:]); err != nil {
				return err
			}
			_, err := s.Write(b)
			return err
		}

		sa, err := conn.AcceptStream(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		// Control plane is the first stream (smaller stream ID).
		if sa.StreamID() != 0 {
			serverErr <- fmt.Errorf("first accepted stream = %d, want 0", sa.StreamID())
			return
		}
		data, err := readFrame(sa)
		if err != nil {
			serverErr <- err
			return
		}
		var auth struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(data, &auth) != nil || auth.Type != "auth" {
			serverErr <- fmt.Errorf("bad auth: %s", data)
			return
		}
		if err := writeFrame(sa, map[string]any{"v": 1, "type": "hello", "service": "hub", "subscribers": 1, "seq": 0}); err != nil {
			serverErr <- err
			return
		}
		// Relay request goes down the request plane (stream B).
		sb, err := conn.AcceptStream(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		if sb.StreamID() == sa.StreamID() {
			serverErr <- fmt.Errorf("request plane shares stream %d with control plane", sa.StreamID())
			return
		}
		if err := writeFrame(sb, map[string]any{
			"v": 1, "type": "request", "reqId": "ds1",
			"method": "GET", "path": "/api/state",
		}); err != nil {
			serverErr <- err
			return
		}
		// Serve both streams concurrently; dispatch by frame type.
		done := make(chan struct{}, 2)
		serve := func(s *quic.Stream, sid int) {
			defer func() { done <- struct{}{} }()
			for {
				data, err := readFrame(s)
				if err != nil {
					return
				}
				var frame struct {
					Type   string           `json:"type"`
					Events []map[string]any `json:"events"`
					ReqID  string           `json:"reqId"`
					Status int              `json:"status"`
				}
				if json.Unmarshal(data, &frame) != nil {
					continue
				}
				switch frame.Type {
				case "events":
					for _, ev := range frame.Events {
						ev["__stream"] = sid
						eventsCh <- ev
					}
				case "respond":
					respondCh <- map[string]any{"reqId": frame.ReqID, "status": frame.Status, "__stream": sid}
				}
			}
		}
		go serve(sa, int(sa.StreamID()))
		go serve(sb, int(sb.StreamID()))
		<-done
	}()

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, 200, map[string]any{"ok": true})
	}))
	defer local.Close()

	c := NewClient(Config{
		URL:          fmt.Sprintf("https://127.0.0.1:%d", quicPort),
		HostID:       "h1",
		Token:        "tok123",
		LocalBase:    local.URL,
		QUICInsecure: true,
	})
	c.sendCh = make(chan []byte, 64)
	c.reqCh = make(chan []byte, 16)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "h1", HostName: "H1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)
	done := make(chan error, 1)
	go func() { done <- c.quicSession(ctx, bridge) }()

	// Wait for forwarding enabled (hello.subscribers=1 on stream A).
	deadline := time.After(5 * time.Second)
	for !c.forwardingEnabled() {
		select {
		case <-deadline:
			t.Fatal("forwarding never enabled over QUIC")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// The respond for the relay request must arrive on the request plane —
	// a DIFFERENT stream than the one carrying auth (stream A).
	select {
	case resp := <-respondCh:
		if resp["__stream"] == 0 {
			t.Errorf("respond arrived on control stream 0; want separate request plane")
		}
		if resp["reqId"] != "ds1" {
			t.Errorf("respond reqId = %v", resp["reqId"])
		}
	case <-time.After(5 * time.Second):
		select {
		case err := <-serverErr:
			t.Fatalf("no respond (server err: %v)", err)
		default:
			t.Fatal("no respond observed")
		}
	}

	// Events still ride the control plane (stream A).
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "plane a", "sessionId": "s1"})
	select {
	case ev := <-eventsCh:
		if ev["__stream"] != 0 {
			t.Errorf("events arrived on stream %v; want control plane 0", ev["__stream"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no events observed")
	}
}
