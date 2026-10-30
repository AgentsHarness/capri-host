package hub

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestReadLoopAuthErrorIsAuthFailure: the hub's QUIC plane reports a
// rejected credential as a JSON auth_error frame (there is no close frame to
// carry a reason). readLoop must turn that into an auth failure so the
// reconnect loop re-pairs instead of backing off forever — while leaving
// classifyClose (WS close frames only) untouched.
func TestReadLoopAuthErrorIsAuthFailure(t *testing.T) {
	c := NewClient(Config{URL: "http://x", HostID: "h1", Token: "tok", DisableQUIC: true})
	c.sendCh = make(chan []byte, 4)

	frames := []string{`{"v":1,"type":"auth_error","error":"token 无效"}`}
	var sent int
	recv := func() ([]byte, error) {
		if sent >= len(frames) {
			return nil, io.EOF
		}
		sent++
		return []byte(frames[sent-1]), nil
	}
	err := c.readLoop(context.Background(), recv, newStatusBridge())
	if err == nil {
		t.Fatal("readLoop must return an error on auth_error")
	}
	if !isAuthErr(err) {
		t.Fatalf("isAuthErr(%v) = false, want true", err)
	}
	if !strings.Contains(err.Error(), "token 无效") {
		t.Errorf("error %q must carry the hub's reason", err)
	}
	if got := classifyClose(err); got != closeOther {
		t.Errorf("classifyClose(auth_error) = %d, want closeOther (it is not a WS close)", got)
	}
}

// TestRunRepairsAfterAuthErrorFrame: end to end through Run's reconnect
// loop. A session rejected with auth_error must take the closeAuth branch —
// drop the persisted credential, pair again, and redial with the NEW token
// — instead of redialing the rejected one forever.
func TestRunRepairsAfterAuthErrorFrame(t *testing.T) {
	var mu sync.Mutex
	pairCalls := 0
	authHdrs := make(chan string, 8)
	closeConn := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pair", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		pairCalls++
		mu.Unlock()
		writeTestJSON(w, 200, map[string]any{"ok": true, "token": "tok-new"})
	})
	mux.HandleFunc("GET /ws/host", func(w http.ResponseWriter, r *http.Request) {
		select {
		case authHdrs <- r.Header.Get("Authorization"):
		default:
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if r.Header.Get("Authorization") != "Bearer tok-new" {
			// What serveQUICConn sends on a bad token; readLoop is shared
			// by both transports, so the frame exercises the same path.
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"v":1,"type":"auth_error","error":"token 无效"}`))
			return
		}
		<-closeConn
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer close(closeConn)

	// Seed the stale credential exactly where a real rotation leaves it.
	statePath := filepath.Join(t.TempDir(), "hub.json")
	if err := writeStateFile(statePath, stateFile{URL: ts.URL, HostID: "h1", Token: "tok-old"}); err != nil {
		t.Fatalf("seed state file: %v", err)
	}
	c := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", PairCode: "ABC123", StateFile: statePath, DisableQUIC: true})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, newStatusBridge())

	sawNew := false
	deadline := time.After(5 * time.Second)
	for !sawNew {
		select {
		case hdr := <-authHdrs:
			if hdr == "Bearer tok-new" {
				sawNew = true
			}
		case <-deadline:
			t.Fatal("host never redialed with the re-paired token after auth_error")
		}
	}
	mu.Lock()
	gotPair := pairCalls
	mu.Unlock()
	if gotPair != 1 {
		t.Errorf("pair calls = %d, want 1 (auth_error must trigger exactly one re-pair)", gotPair)
	}
}
