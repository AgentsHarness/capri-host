// Package hub implements the acp-host side of the hub relay: pairing with
// acp-hub (pairing code → token), forwarding local bridge events to the
// hub, and serving relayed browser requests over the hub's SSE stream by
// executing them against this host's local HTTP API.
package hub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/benin/acp-host/internal/acp"
)

// Config configures the hub client.
type Config struct {
	URL       string // hub base URL, e.g. http://hub-host:8787
	HostID    string
	HostName  string
	PairCode  string // one-time pairing code; ignored when a token exists
	Token     string // existing token (HOST_TOKEN); takes precedence
	LocalBase string // this host's local HTTP base, e.g. http://127.0.0.1:8765
	StateFile string // token persistence path (default ~/.acp-host/hub.json)
}

// Client forwards events to the hub and executes relayed requests against
// the local HTTP API.
type Client struct {
	cfg   Config
	httpc *http.Client

	stateMu sync.Mutex
	token   string

	inflightMu sync.Mutex
	inflight   map[string]context.CancelFunc // reqId → cancel of the local HTTP call
}

// NewClient returns a hub client. LocalBase defaults to 127.0.0.1:8765.
func NewClient(cfg Config) *Client {
	if cfg.LocalBase == "" {
		cfg.LocalBase = "http://127.0.0.1:8765"
	}
	return &Client{
		cfg: cfg,
		// Generous timeout: relayed prompts can run up to 30 minutes.
		httpc:    &http.Client{Timeout: 50 * time.Minute},
		inflight: make(map[string]context.CancelFunc),
	}
}

// Run connects the host to the hub: pairs when no token exists, forwards
// bridge events, and serves relayed requests. It reconnects with backoff
// and blocks until ctx is done.
func (c *Client) Run(ctx context.Context, bridge *acp.Bridge) {
	if err := c.ensureToken(ctx); err != nil {
		log.Printf("[hub-client] 配对失败: %v", err)
		return
	}
	log.Printf("[hub-client] connected to hub %s as %s (%s)", c.cfg.URL, c.cfg.HostID, c.cfg.HostName)

	// Bridge events → hub (batched), with a periodic host_status heartbeat.
	fwdCtx, fwdCancel := context.WithCancel(ctx)
	defer fwdCancel()
	go c.forwardLoop(fwdCtx, bridge)

	// Token rotation: if the hub rejects our token and a pair code is
	// available, re-pair once and keep going.
	repaired := false
	backoff := time.Second
	for ctx.Err() == nil {
		err := c.streamLoop(ctx, bridge)
		if ctx.Err() != nil {
			break
		}
		if isAuthErr(err) && c.cfg.PairCode != "" && !repaired {
			log.Printf("[hub-client] hub 拒绝了旧 token，重新配对…")
			c.clearState()
			if perr := c.ensureToken(ctx); perr == nil {
				repaired = true
				backoff = time.Second
				continue
			} else {
				log.Printf("[hub-client] 重新配对失败: %v", perr)
			}
		}
		log.Printf("[hub-client] hub 连接断开: %v（%.0fs 后重连）", err, backoff.Seconds())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func isAuthErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "401")
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

func (c *Client) pair(ctx context.Context, code string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"code":     code,
		"hostId":   c.cfg.HostID,
		"hostName": c.cfg.HostName,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.URL+"/api/pair", bytes.NewReader(body))
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

// stateFile persists {url, hostId, token} so restarts skip re-pairing.
type stateFile struct {
	URL    string `json:"url"`
	HostID string `json:"hostId"`
	Token  string `json:"token"`
}

func (c *Client) stateFileLocked() string {
	if c.cfg.StateFile != "" {
		return c.cfg.StateFile
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".acp-host", "hub.json")
}

func (c *Client) loadStateLocked() *stateFile {
	path := c.stateFileLocked()
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var st stateFile
	if json.Unmarshal(b, &st) != nil {
		return nil
	}
	return &st
}

func (c *Client) saveStateLocked(st stateFile) {
	path := c.stateFileLocked()
	if path == "" {
		return
	}
	b, _ := json.Marshal(st)
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, b, 0o600)
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

func (c *Client) forwardLoop(ctx context.Context, bridge *acp.Bridge) {
	ch, unsub := bridge.Subscribe()
	defer unsub()
	batch := make([]acp.Event, 0, 16)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		evs := batch
		batch = make([]acp.Event, 0, 16)
		if err := c.postEvents(ctx, evs); err != nil {
			log.Printf("[hub-client] 事件上报失败: %v", err)
		}
	}
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			batch = append(batch, ev)
			if len(batch) >= 16 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-heartbeat.C:
			flush()
			c.postHostStatus(ctx, bridge)
		}
	}
}

// postHostStatus keeps the hub registry's ready flag fresh.
func (c *Client) postHostStatus(ctx context.Context, bridge *acp.Bridge) {
	snap := bridge.Snapshot()
	_ = c.postEvents(ctx, []acp.Event{{"type": "host_status", "ready": snap.Ready}})
}

func (c *Client) postEvents(ctx context.Context, evs []acp.Event) error {
	body, _ := json.Marshal(map[string]any{"hostId": c.cfg.HostID, "events": evs})
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.URL+"/api/hub/events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.authorize(req)
	res, err := c.httpc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	if res.StatusCode != 200 {
		return fmt.Errorf("hub events 返回 %d", res.StatusCode)
	}
	return nil
}

// ── relayed request serving (hub → host) ──────────────────────────────

// streamLoop holds the outbound SSE connection to the hub. The hub pushes
// relayed browser requests (type:"request") which are executed against the
// local HTTP API; answers go back via POST /api/hub/respond.
func (c *Client) streamLoop(ctx context.Context, bridge *acp.Bridge) error {
	u := c.cfg.URL + "/api/hub/stream?host=" + url.QueryEscape(c.cfg.HostID)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	c.authorize(req)
	res, err := c.httpc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("hub stream 返回 %d: %s", res.StatusCode, strings.TrimSpace(string(b)))
	}
	log.Printf("[hub-client] 已连接 hub（stream）")
	c.postHostStatus(ctx, bridge)

	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var msg struct {
			Type   string          `json:"type"`
			ReqID  string          `json:"reqId"`
			Method string          `json:"method"`
			Path   string          `json:"path"`
			Body   json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			log.Printf("[hub-client] 忽略无法解析的中转消息: %v", err)
			continue
		}
		switch msg.Type {
		case "hello":
			// Stream confirmed by the hub.
		case "request":
			go c.handleRelay(ctx, msg.ReqID, msg.Method, msg.Path, msg.Body)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("hub 关闭了 stream")
}

// handleRelay executes one relayed browser request against the local HTTP
// API and posts the answer back to the hub.
func (c *Client) handleRelay(ctx context.Context, reqID, method, path string, body json.RawMessage) {
	ctx, cancel := context.WithTimeout(ctx, 50*time.Minute)
	c.inflightMu.Lock()
	c.inflight[reqID] = cancel
	c.inflightMu.Unlock()
	defer func() {
		c.inflightMu.Lock()
		delete(c.inflight, reqID)
		c.inflightMu.Unlock()
		cancel()
	}()

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
	res, err := c.httpc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return // stream dropped or shutdown; the hub already failed the request
		}
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": err.Error()}))
		return
	}
	defer res.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		c.respond(reqID, 502, mustJSON(map[string]any{"ok": false, "error": err.Error()}))
		return
	}
	c.respond(reqID, res.StatusCode, json.RawMessage(rb))
}

func (c *Client) respond(reqID string, status int, body json.RawMessage) {
	payload, _ := json.Marshal(map[string]any{
		"hostId": c.cfg.HostID,
		"reqId":  reqID,
		"status": status,
		"body":   body,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.URL+"/api/hub/respond", bytes.NewReader(payload))
	if err != nil {
		return
	}
	c.authorize(req)
	res, err := c.httpc.Do(req)
	if err != nil {
		log.Printf("[hub-client] 应答 %s 失败: %v", reqID, err)
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	res.Body.Close()
}

func (c *Client) authorize(req *http.Request) {
	c.stateMu.Lock()
	tok := c.token
	c.stateMu.Unlock()
	req.Header.Set("Authorization", "Bearer "+tok)
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
