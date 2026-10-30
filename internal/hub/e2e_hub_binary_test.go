//go:build e2e

// Cross-repo end-to-end test: the REAL capri-hub binary (built from the
// repo at CAPRI_E2E_HUB) against the REAL host hub client in this
// package. Verifies the opt-net2 contract in production wiring: QUIC
// multi-stream sessions, per-request streams, uplink flate, SPKI pin,
// FE WS compressed fan-out, and control-plane liveness during a large
// relay response.
//
// Run: CAPRI_E2E_HUB=/path/to/acp-hub go test -tags e2e ./internal/hub -run TestE2EHubBinary -v
//
// The v2 session layer is exercised end to end here: hello/hello_ack over
// QUIC with an SPKI pin, per-event acking (the host's uplink log drains only
// once the hub acks), a compressed uplink, and a large relay answer on its own
// request stream while the control plane keeps carrying events.
package hub

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/coder/websocket"
)

const (
	hubPort  = 18787
	quicPort = 18788
	// The WS e2e runs after the QUIC one (or alone); separate ports keep a
	// lingering listener from the previous test out of the way.
	wsHubPort  = 18797
	wsQUICPort = 18798
)

// genCert writes a self-signed ECDSA cert/key pair and returns the
// sha256 SPKI pin (hex), mirroring the openssl command in DEPLOY.md.
func genCert(t *testing.T, dir string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "e2e-hub"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "key.pem"), keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("hub not reachable at %s", url)
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return m
}

// feConn is a minimal browser: compressed (/ws/fe?c=1) WS subscriber.
type feConn struct {
	conn   *websocket.Conn
	events chan map[string]any
}

func dialFE(t *testing.T, ctx context.Context, url string) *feConn {
	t.Helper()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("fe dial: %v", err)
	}
	fe := &feConn{conn: c, events: make(chan map[string]any, 256)}
	go func() {
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			if typ == websocket.MessageBinary {
				rc := flate.NewReader(bytes.NewReader(data))
				data, err = io.ReadAll(rc)
				rc.Close()
				if err != nil {
					return
				}
			}
			var f struct {
				Type   string            `json:"type"`
				Events []json.RawMessage `json:"events"`
			}
			if json.Unmarshal(data, &f) != nil {
				continue
			}
			for _, raw := range f.Events {
				var ev map[string]any
				if json.Unmarshal(raw, &ev) == nil {
					select {
					case fe.events <- ev:
					default:
					}
				}
			}
		}
	}()
	return fe
}

func (fe *feConn) waitEvent(t *testing.T, substr string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-fe.events:
			if b, _ := json.Marshal(ev); strings.Contains(string(b), substr) {
				return ev
			}
		case <-deadline:
			t.Fatalf("no event matching %q within %v", substr, timeout)
			return nil
		}
	}
}

func TestE2EHubBinaryHostClientQUIC(t *testing.T) {
	hubRepo := os.Getenv("CAPRI_E2E_HUB")
	if hubRepo == "" {
		t.Skip("CAPRI_E2E_HUB not set — cross-repo e2e disabled")
	}
	var dir string
	if os.Getenv("CAPRI_E2E_KEEP") != "" {
		dir, _ = os.MkdirTemp("/tmp", "capri-e2e-*")
		t.Logf("artifacts kept at %s", dir)
	} else {
		dir = t.TempDir()
	}
	pin := genCert(t, dir)

	// Build and start the REAL hub binary from the acp-hub-opt worktree.
	hubBin := filepath.Join(dir, "capri-hub")
	build := exec.Command("go", "build", "-o", hubBin, "github.com/AgentsHarness/capri-hub/cmd/capri-hub")
	build.Dir = hubRepo
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hub: %v\n%s", err, out)
	}
	logFile, _ := os.Create(filepath.Join(dir, "hub.log"))
	hubProc := exec.Command(hubBin)
	hubProc.Env = append(os.Environ(),
		fmt.Sprintf("PORT=%d", hubPort),
		fmt.Sprintf("QUIC_PORT=%d", quicPort),
		"QUIC_CERT="+filepath.Join(dir, "cert.pem"),
		"QUIC_KEY="+filepath.Join(dir, "key.pem"),
	)
	hubProc.Stdout = logFile
	hubProc.Stderr = logFile
	if err := hubProc.Start(); err != nil {
		t.Fatalf("start hub: %v", err)
	}
	defer func() {
		hubProc.Process.Kill()
		hubProc.Wait()
	}()
	waitHTTP(t, fmt.Sprintf("http://127.0.0.1:%d/health", hubPort), 10*time.Second)

	base := fmt.Sprintf("http://127.0.0.1:%d", hubPort)

	// The host's local API: a big (~5MB, VALID JSON) relay target that
	// answers SLOWLY so the relay is in flight while we stream events on
	// the control plane.
	bigBody := `{"rows":[` + strings.Repeat(`"0123456789abcdefghijklmnopqrstuvwxyz",`, 140000) + `"end"]}`
	relayed := make(chan struct{})
	var relayHit sync.Once
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/plain" {
			io.WriteString(w, "line one\nline two\n") // JSONL-ish, invalid JSON
			return
		}
		time.Sleep(1500 * time.Millisecond) // hold the relay open
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, bigBody)
		relayHit.Do(func() { close(relayed) })
	}))
	defer local.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fe := dialFE(t, ctx, fmt.Sprintf("ws://127.0.0.1:%d/ws/fe?c=1", hubPort))
	defer fe.conn.Close(websocket.StatusNormalClosure, "")

	// Pair + run the REAL host client over QUIC, pinning the self-signed cert.
	code, _ := getJSON(t, base+"/api/pairing")["code"].(string)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "e2e", HostName: "E2E"})
	client := NewClient(Config{
		URL:       base,
		HostID:    "e2e",
		HostName:  "E2E",
		PairCode:  code,
		LocalBase: local.URL,
		QUICPort:  quicPort,
		QUICHost:  "127.0.0.1",
		QUICPin:   pin,
		StateFile: filepath.Join(dir, "host-state.json"),
	})
	go client.Run(ctx, bridge)

	// Host must come online over QUIC.
	deadline := time.Now().Add(10 * time.Second)
	online := false
	for time.Now().Before(deadline) {
		m := getJSON(t, base+"/api/hosts")
		if m["defaultHostId"] == "e2e" {
			online = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !online {
		hubLog, _ := os.ReadFile(filepath.Join(dir, "hub.log"))
		t.Fatalf("host never came online; hub log:\n%s", hubLog)
	}

	// 1. Uplink events reach the FE over the compressed /ws/fe path.
	// forwardLoop subscribes to the bridge asynchronously (client_test
	// sleeps 80ms for the same reason), so probe until the round-trip
	// works instead of racing a single Broadcast.
	helloOK := false
	deadline2 := time.Now().Add(10 * time.Second)
	for !helloOK && time.Now().Before(deadline2) {
		bridge.Broadcast(acp.Event{"type": "chunk", "text": "e2e hello from host"})
		select {
		case ev := <-fe.events:
			if b, _ := json.Marshal(ev); strings.Contains(string(b), "e2e hello") {
				helloOK = true
			}
		case <-time.After(400 * time.Millisecond):
		}
	}
	if !helloOK {
		hubLog, _ := os.ReadFile(filepath.Join(dir, "hub.log"))
		t.Fatalf("uplink events never reached the FE; hub log:\n%s", hubLog)
	}

	// 2. Relay round-trip: a 4MB response through the hub. Fire it, then
	// IMMEDIATELY stream more events — with per-request streams the control
	// plane must not stall behind the big respond.
	relayDone := make(chan struct{})
	var relayErr error
	go func() {
		defer close(relayDone)
		resp, err := http.Get(base + "/api/big?host=e2e")
		if err != nil {
			relayErr = err
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			relayErr = err
			return
		}
		if len(body) != len(bigBody) || body[len(body)-1] != bigBody[len(bigBody)-1] {
			relayErr = fmt.Errorf("relay body = %d bytes, want %d", len(body), len(bigBody))
		}
	}()

	// While the relay is in flight, events must keep flowing.
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "during-relay event"})
	gotDuring := fe.waitEvent(t, "during-relay", 3*time.Second)
	if gotDuring == nil {
		t.Fatal("control plane stalled behind the big relay respond")
	}

	select {
	case <-relayDone:
	case <-time.After(30 * time.Second):
		localHit := false
		select {
		case <-relayed:
			localHit = true
		default:
		}
		hubLog, _ := os.ReadFile(filepath.Join(dir, "hub.log"))
		t.Fatalf("relay never completed (local API hit: %v); hub log:\n%s", localHit, hubLog)
	}
	if relayErr != nil {
		t.Fatalf("relay: %v", relayErr)
	}
	select {
	case <-relayed:
	default:
		t.Fatal("local API was never hit through the hub relay")
	}

	// 3. Non-JSON local bodies must not strand the relay (respond wraps
	// them as a JSON string instead of silently dropping the respond).
	resp2, err := http.Get(base + "/api/plain?host=e2e")
	if err != nil {
		t.Fatalf("plain relay: %v", err)
	}
	plain, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	// The host wraps non-JSON bodies as a JSON string so the relay still
	// answers (see respond); the browser sees the quoted form.
	if string(plain) != "\"line one\\nline two\\n\"" {
		t.Fatalf("plain relay body = %q", plain)
	}

	// 4. Post-relay liveness: more events after the big respond.
	bridge.Broadcast(acp.Event{"type": "chunk", "text": "after-relay event"})
	fe.waitEvent(t, "after-relay", 5*time.Second)

	// 5. The session must have run over QUIC, not the WebSocket fallback.
	if tr := client.State().Transport; tr != TransportQUIC {
		t.Fatalf("host session transport = %q, want quic", tr)
	}

	t.Log("E2E OK: QUIC+pin online, compressed uplink events, 4MB relay on request stream with live control plane")
}

// TestE2EHubBinaryHostClientWS is the WebSocket half of the cross-repo happy
// path: the REAL hub binary, a browser /ws/fe subscriber, and the REAL host
// client forced onto WebSocket. It asserts the v2 handshake surfaces the host
// as online, the audience gate lets uplink events through, and a relayed
// request is answered — the same contract the QUIC test pins.
func TestE2EHubBinaryHostClientWS(t *testing.T) {
	hubRepo := os.Getenv("CAPRI_E2E_HUB")
	if hubRepo == "" {
		t.Skip("CAPRI_E2E_HUB not set — cross-repo e2e disabled")
	}
	dir := t.TempDir()

	hubBin := filepath.Join(dir, "capri-hub")
	build := exec.Command("go", "build", "-o", hubBin, "github.com/AgentsHarness/capri-hub/cmd/capri-hub")
	build.Dir = hubRepo
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hub: %v\n%s", err, out)
	}
	logFile, _ := os.Create(filepath.Join(dir, "hub.log"))
	hubProc := exec.Command(hubBin)
	hubProc.Env = append(os.Environ(),
		fmt.Sprintf("PORT=%d", wsHubPort),
		fmt.Sprintf("QUIC_PORT=%d", wsQUICPort),
	)
	hubProc.Stdout = logFile
	hubProc.Stderr = logFile
	if err := hubProc.Start(); err != nil {
		t.Fatalf("start hub: %v", err)
	}
	defer func() {
		hubProc.Process.Kill()
		hubProc.Wait()
	}()
	waitHTTP(t, fmt.Sprintf("http://127.0.0.1:%d/health", wsHubPort), 10*time.Second)
	base := fmt.Sprintf("http://127.0.0.1:%d", wsHubPort)

	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true,"via":"ws-e2e","path":"`+r.URL.Path+`"}`)
	}))
	defer local.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fe := dialFE(t, ctx, fmt.Sprintf("ws://127.0.0.1:%d/ws/fe?c=1&host=e2e", wsHubPort))
	defer fe.conn.Close(websocket.StatusNormalClosure, "")

	code, _ := getJSON(t, base+"/api/pairing")["code"].(string)
	bridge := acp.NewBridge(acp.GrokConfig{Bin: "grok", HostID: "e2e", HostName: "E2E"})
	client := NewClient(Config{
		URL:         base,
		HostID:      "e2e",
		HostName:    "E2E",
		PairCode:    code,
		LocalBase:   local.URL,
		DisableQUIC: true,
		StateFile:   filepath.Join(dir, "host-state.json"),
	})
	go client.Run(ctx, bridge)

	// 1. Handshake + registry: the host is listed online under its hostId.
	deadline := time.Now().Add(10 * time.Second)
	online := false
	for time.Now().Before(deadline) {
		m := getJSON(t, base+"/api/hosts")
		hosts, _ := m["hosts"].([]any)
		for _, raw := range hosts {
			h, _ := raw.(map[string]any)
			if h["hostId"] == "e2e" && h["online"] == true {
				online = true
			}
		}
		if online && m["defaultHostId"] == "e2e" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !online {
		hubLog, _ := os.ReadFile(filepath.Join(dir, "hub.log"))
		t.Fatalf("host never came online over WS; hub log:\n%s", hubLog)
	}

	// 2. Audience: the browser subscriber exists, so the host's uplink is not
	// paused; its events reach this FE over the compressed path.
	if m := getJSON(t, base+"/api/subscribers"); m["count"].(float64) < 1 {
		t.Fatalf("hub reported no browser subscriber: %+v", m)
	}
	helloOK := false
	deadline2 := time.Now().Add(10 * time.Second)
	for !helloOK && time.Now().Before(deadline2) {
		bridge.Broadcast(acp.Event{"type": "chunk", "text": "ws e2e hello from host"})
		select {
		case ev := <-fe.events:
			if b, _ := json.Marshal(ev); strings.Contains(string(b), "ws e2e hello") {
				helloOK = true
			}
		case <-time.After(400 * time.Millisecond):
		}
	}
	if !helloOK {
		hubLog, _ := os.ReadFile(filepath.Join(dir, "hub.log"))
		t.Fatalf("uplink events never reached the FE over WS; hub log:\n%s", hubLog)
	}

	// 3. Relay request/response over the multiplexed WS link.
	resp, err := http.Get(base + "/api/status?host=e2e")
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var relayed map[string]any
	if err := json.Unmarshal(body, &relayed); err != nil {
		t.Fatalf("relay body %q: %v", body, err)
	}
	if relayed["via"] != "ws-e2e" || relayed["path"] != "/api/status" {
		t.Fatalf("relay did not execute against the local API: %s", body)
	}

	// 4. The session really ran over WebSocket.
	if tr := client.State().Transport; tr != TransportWS {
		t.Fatalf("host session transport = %q, want ws", tr)
	}

	t.Log("E2E OK: WS handshake online, audience-gated uplink events, relayed request/response")
}
