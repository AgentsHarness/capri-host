//go:build windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
)

// This file is the tray's entire view of the host: an HTTP client.
//
// Nothing here reaches into the host's packages, and nothing in the host
// knows the tray exists. The two contracts that do cross the boundary are
// imported rather than re-declared, because both are pure data with no
// platform or transport dependencies:
//
//   - config.File — the on-disk settings shape, which Capri.app, the host and
//     the user's text editor all share.
//   - hubstate.State — the link snapshot the host serves and every client
//     renders.
//
// Re-declaring either here would put the two sides one silent drift apart.

// baseURL is where this host listens. The tray talks to the loopback address
// on purpose: a host bound to 0.0.0.0 is still reachable locally, and going
// through the LAN address would depend on the machine's own routing.
func baseURL(port int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

type apiClient struct {
	base  string
	token string
	http  *http.Client
}

func newAPIClient(port int, token string) *apiClient {
	return &apiClient{
		base:  baseURL(port),
		token: token,
		// No Client.Timeout: each caller sets a context deadline. Pairing
		// and rename must be allowed to outlive the host-side bounds
		// (20s / 15s); a 10s client timeout used to fail the UI while the
		// host still succeeded in the background.
		http: &http.Client{},
	}
}

// envelope is the shape every control endpoint answers with. Version is
// /api/probe's; it rides along here so one decoder covers the lot.
type envelope struct {
	OK      bool            `json:"ok"`
	Error   string          `json:"error"`
	Version string          `json:"version"`
	Hub     *hubstate.State `json:"hub"`
}

func (a *apiClient) do(ctx context.Context, method, path string, body any) (envelope, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return envelope{}, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, rdr)
	if err != nil {
		return envelope{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// The host's gate is open when FE_TOKEN is unset, so an empty token here
	// is a working configuration rather than a missing one.
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}

	res, err := a.http.Do(req)
	if err != nil {
		return envelope{}, err
	}
	defer res.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if err != nil {
		return envelope{}, err
	}
	var env envelope
	_ = json.Unmarshal(raw, &env)
	if res.StatusCode != http.StatusOK {
		if env.Error != "" {
			return env, errors.New(env.Error)
		}
		return env, fmt.Errorf("Capri-host 返回 HTTP %d", res.StatusCode)
	}
	return env, nil
}

func (a *apiClient) hubState(ctx context.Context) (hubstate.State, error) {
	env, err := a.do(ctx, "GET", "/api/hub/state", nil)
	if err != nil {
		return hubstate.State{}, err
	}
	if env.Hub == nil {
		return hubstate.State{}, errors.New("Capri-host 未返回 hub 状态")
	}
	return *env.Hub, nil
}

// hostVersion reads the host's own build stamp, so the info panel reports the
// version actually listening rather than the tray's — they are two binaries
// and can legitimately be one upgrade apart.
func (a *apiClient) hostVersion(ctx context.Context) string {
	env, err := a.do(ctx, "GET", "/api/probe", nil)
	if err != nil {
		return ""
	}
	return env.Version
}

// alive reports whether something that speaks Capri-host is listening. It uses
// /api/hosts because that is the one endpoint the host never gates: the probe
// has to work before any token exists.
func (a *apiClient) alive(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", a.base+"/api/hosts", nil)
	if err != nil {
		return false
	}
	res, err := a.http.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode == http.StatusOK
}

// loadSettings reads the config file the tray and the host share. A missing
// file is normal — it is what a fresh install looks like — and yields the
// compiled defaults.
func loadSettings() (config.File, error) {
	return config.LoadFile()
}

// hostBinary locates the engine beside the running GUI.
//
// The layout it expects and the search order are in paths.go, which is
// platform-independent so the contract can be tested anywhere.
func hostBinary() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("定位当前程序失败: %w", err)
	}
	return resolveEngine(os.Getenv("CAPRI_HOST_BIN"), filepath.Dir(self), os.Stat)
}

// logf writes to the tray's own log. The tray is a GUI-subsystem binary with
// no stderr, so this is the only place its own diagnostics can go.
func logf(format string, args ...any) {
	if trayLog == nil {
		return
	}
	fmt.Fprintf(trayLog, time.Now().Format("2006-01-02 15:04:05 ")+format+"\n", args...)
}
