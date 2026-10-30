package hub

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPairHonorsCallTimeout: pair must not inherit c.httpc's 50-minute
// timeout (sized for relayed prompts). A hub hostname that resolves to a
// blackhole would otherwise park Run's pairing retry inside a single
// request, with no backoff and no error, forever.
func TestPairHonorsCallTimeout(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer ts.Close()
	defer close(release)

	prev := pairTimeout
	pairTimeout = 100 * time.Millisecond
	defer func() { pairTimeout = prev }()

	stateFile := filepath.Join(t.TempDir(), "hub.json")
	c := NewClient(Config{URL: ts.URL, HostID: "h1", HostName: "H1", PairCode: "ABC123", StateFile: stateFile, DisableQUIC: true})

	start := time.Now()
	err := c.ensureToken(context.Background())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ensureToken must fail when the pair request exceeds the call timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pair error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("pair took %v, want it bounded by the 100ms test timeout", elapsed)
	}
	if c.token != "" {
		t.Errorf("token = %q, want empty on a failed pair", c.token)
	}
	if _, statErr := os.Stat(stateFile); !os.IsNotExist(statErr) {
		t.Errorf("state file written by a failed pair: %v", statErr)
	}
}
