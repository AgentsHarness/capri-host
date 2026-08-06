package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	promptTimeout    = 30 * time.Minute
	bootTimeout      = 2 * time.Minute
	approvalTimeout  = 15 * time.Minute
	protocolVersion  = 1
)

// Bridge owns one grok agent stdio process and one ACP session.
// It does NOT implement fs/* or terminal/* — agent runs tools itself.
type Bridge struct {
	cfg GrokConfig

	mu       sync.Mutex
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	cancelRd context.CancelFunc

	ready     bool
	booting   bool
	busy      bool
	bootError string
	sessionID string
	cwd       string
	agentInfo map[string]any
	modes     any
	configOpts any
	textBuf   string

	nextAgentID atomic.Int64
	pending     sync.Map // id(float64/int) -> chan rpcResult

	nextClientReqID atomic.Int64
	clientReqs      sync.Map // requestId string -> *clientRequest

	subscribersMu sync.Mutex
	subscribers   map[chan Event]struct{}

	hostID   string
	hostName string
}

type GrokConfig struct {
	Bin      string
	HostID   string
	HostName string
}

type rpcResult struct {
	result map[string]any
	err    error
}

type clientRequest struct {
	AgentID any
	Method  string
	Params  map[string]any
	done    chan struct{} // closed when resolved/rejected
	cancel  bool
	// isPermission: session/request_permission replies with the nested
	// {outcome:{outcome:"selected"|"cancelled"}} shape; x.ai/* requests
	// reply with the raw result object the browser supplied.
	isPermission bool
	outcome      map[string]any // session/request_permission result
	result       map[string]any // generic x.ai/* request result
	errMsg       string
}

func NewBridge(cfg GrokConfig) *Bridge {
	b := &Bridge{
		cfg:         cfg,
		hostID:      cfg.HostID,
		hostName:    cfg.HostName,
		cwd:         mustCwd(),
		subscribers: make(map[chan Event]struct{}),
	}
	b.nextAgentID.Store(1)
	b.nextClientReqID.Store(1)
	return b
}

func mustCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// Subscribe returns a buffered event channel; call unsubscribe to remove.
func (b *Bridge) Subscribe() (ch chan Event, unsubscribe func()) {
	ch = make(chan Event, 64)
	b.subscribersMu.Lock()
	b.subscribers[ch] = struct{}{}
	b.subscribersMu.Unlock()
	return ch, func() {
		b.subscribersMu.Lock()
		delete(b.subscribers, ch)
		b.subscribersMu.Unlock()
		// drain
		for {
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}

func (b *Bridge) Broadcast(ev Event) {
	if ev == nil {
		return
	}
	b.subscribersMu.Lock()
	defer b.subscribersMu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// slow consumer: drop
		}
	}
}

func (b *Bridge) Snapshot() Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	var pending []PendingReq
	b.clientReqs.Range(func(key, value any) bool {
		cr := value.(*clientRequest)
		pending = append(pending, PendingReq{
			RequestID: key.(string),
			Method:    cr.Method,
			Params:    cr.Params,
		})
		return true
	})
	return Status{
		Ready:           b.ready,
		Busy:            b.busy,
		Booting:         b.booting,
		SessionID:       b.sessionID,
		Cwd:             b.cwd,
		HostID:          b.hostID,
		HostName:        b.hostName,
		AgentInfo:       b.agentInfo,
		Modes:           b.modes,
		ConfigOptions:   b.configOpts,
		BootError:       b.bootError,
		Text:            b.textBuf,
		PendingRequests: pending,
		Capabilities:    DefaultClientCaps(),
	}
}

// Boot starts grok if needed and creates a session.
func (b *Bridge) Boot(ctx context.Context, sc SessionConfig) error {
	b.mu.Lock()
	if b.ready || b.booting {
		b.mu.Unlock()
		return nil
	}
	b.booting = true
	b.bootError = ""
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.booting = false
		b.mu.Unlock()
	}()

	if err := b.ensureProcess(); err != nil {
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	caps := DefaultClientCaps()
	initRes, err := b.request(ctx, "initialize", map[string]any{
		"protocolVersion": protocolVersion,
		"clientCapabilities": map[string]any{
			"fs": map[string]any{
				"readTextFile":  caps.FS.ReadTextFile,
				"writeTextFile": caps.FS.WriteTextFile,
			},
			"terminal": caps.Terminal,
			// x.ai extension capabilities (aligned with the Grok Build TUI):
			// bash output streams incrementally without ANSI color (we render
			// it as plain text), git head changes are notified, hunk tracking
			// stays agent-side.
			"meta": map[string]any{
				"x.ai/incrementalBashOutput": true,
				"x.ai/bashOutputNoColor":     true,
				"x.ai/gitHeadChanged":        true,
				"x.ai/hunkTracker":           map[string]any{"mode": "agent_only"},
			},
		},
		"clientInfo": map[string]any{
			"name":    "acp-host",
			"title":   "ACP Host",
			"version": "0.1.0",
		},
	}, bootTimeout)
	if err != nil {
		// A live-but-wedged process can fail initialize repeatedly; kill it so
		// the next Boot spawns a fresh agent instead of reusing it.
		b.killProcess()
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	b.mu.Lock()
	b.agentInfo = initRes
	b.mu.Unlock()

	if err := b.authenticate(ctx, initRes); err != nil {
		b.killProcess()
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	cwd := sc.Cwd
	if cwd == "" {
		cwd = mustCwd()
	}
	addDirs := sc.AdditionalDirectories
	if addDirs == nil {
		addDirs = []string{}
	}
	mcp := sc.MCPServers
	if mcp == nil {
		mcp = []map[string]any{}
	}

	sessRes, err := b.request(ctx, "session/new", map[string]any{
		"cwd":                   cwd,
		"additionalDirectories": addDirs,
		"mcpServers":            mcp,
	}, bootTimeout)
	if err != nil {
		b.killProcess()
		b.setBootError(err.Error())
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		return err
	}

	sid, _ := sessRes["sessionId"].(string)
	b.mu.Lock()
	b.sessionID = sid
	b.cwd = cwd
	b.modes = sessRes["modes"]
	b.configOpts = sessRes["configOptions"]
	b.textBuf = ""
	b.ready = true
	b.bootError = ""
	modes := b.modes
	configOpts := b.configOpts
	agentInfo := b.agentInfo
	b.mu.Unlock()

	b.Broadcast(Event{
		"type":          "ready",
		"sessionId":     sid,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"hostId":        b.hostID,
		"hostName":      b.hostName,
	})
	return nil
}

func (b *Bridge) authenticate(ctx context.Context, init map[string]any) error {
	methodsRaw, _ := init["authMethods"].([]any)
	methods := map[string]bool{}
	for _, m := range methodsRaw {
		mm, _ := m.(map[string]any)
		if id, _ := mm["id"].(string); id != "" {
			methods[id] = true
		}
	}
	var methodID string
	if os.Getenv("XAI_API_KEY") != "" && methods["xai.api_key"] {
		methodID = "xai.api_key"
	} else if methods["cached_token"] {
		methodID = "cached_token"
	}
	if methodID == "" {
		return errors.New("没有可用的认证方式，请先运行 `grok login`，或设置环境变量 XAI_API_KEY")
	}
	_, err := b.request(ctx, "authenticate", map[string]any{
		"methodId": methodID,
		"_meta":    map[string]any{"headless": true},
	}, bootTimeout)
	return err
}

func (b *Bridge) setBootError(msg string) {
	b.mu.Lock()
	b.bootError = msg
	b.ready = false
	b.mu.Unlock()
}

func (b *Bridge) ensureProcess() error {
	b.mu.Lock()
	if b.cmd != nil && b.cmd.Process != nil {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	cmd := exec.Command(b.cfg.Bin, "agent", "stdio")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("无法启动 %s: %w（请确认 grok CLI 已安装，或设置 GROK_BIN）", b.cfg.Bin, err)
	}

	rdCtx, cancelRd := context.WithCancel(context.Background())
	b.mu.Lock()
	b.cmd = cmd
	b.stdin = stdin
	b.cancelRd = cancelRd
	b.mu.Unlock()

	go b.readStdout(rdCtx, stdout)
	go b.readStderr(stderr)
	go b.waitProcess(cmd)

	log.Printf("[acp-host] spawned %s agent stdio pid=%d", b.cfg.Bin, cmd.Process.Pid)
	return nil
}

func (b *Bridge) waitProcess(cmd *exec.Cmd) {
	err := cmd.Wait()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
	b.mu.Lock()
	same := b.cmd == cmd
	if same {
		b.cmd = nil
		b.stdin = nil
		b.ready = false
		b.sessionID = ""
		if b.cancelRd != nil {
			b.cancelRd()
			b.cancelRd = nil
		}
	}
	b.mu.Unlock()
	if !same {
		return
	}
	b.failAllPending(fmt.Errorf("grok 进程已退出 (code=%d)", code))
	b.Broadcast(Event{
		"type": "status",
		"text": fmt.Sprintf("grok 进程已退出 (code=%d)，下一条消息时自动重启", code),
	})
}

func (b *Bridge) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		t := sc.Text()
		if t != "" {
			b.Broadcast(Event{"type": "log", "text": t})
		}
	}
}

func (b *Bridge) readStdout(ctx context.Context, r io.Reader) {
	sc := bufio.NewScanner(r)
	// 64MB max token: x.ai/session/updates can return a very large single-line
	// JSON array for big sessions; a too-small buffer would silently kill the
	// read loop (and wedge the whole bridge).
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !sc.Scan() {
			return
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}
		b.onAgentMessage(msg)
	}
}

func (b *Bridge) onAgentMessage(msg map[string]any) {
	if method, _ := msg["method"].(string); method == "session/update" {
		params, _ := msg["params"].(map[string]any)
		b.handleSessionUpdate(params)
		return
	}

	// x.ai/* extension notifications (no id). Also accept the wrapped
	// leader form {"method":"_x.ai/foo","params":{"method":"x.ai/foo",...}}.
	if method, _ := msg["method"].(string); strings.HasPrefix(method, "x.ai/") || strings.HasPrefix(method, "_x.ai/") {
		params, _ := msg["params"].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}
		realMethod, realParams := unwrapExtMethod(method, params)
		if msg["id"] == nil {
			b.handleXaiNotification(realMethod, realParams)
			return
		}
		// Agent → client extension request: forward for browser interaction.
		b.forwardXaiRequest(msg["id"], realMethod, realParams)
		return
	}

	id := msg["id"]
	if id == nil {
		return
	}

	// Response to our request?
	if ch, ok := b.pending.LoadAndDelete(idKey(id)); ok {
		c := ch.(chan rpcResult)
		if errObj, has := msg["error"]; has && errObj != nil {
			em := "agent error"
			if m, ok := errObj.(map[string]any); ok {
				if s, ok := m["message"].(string); ok {
					em = s
				}
			}
			c <- rpcResult{err: errors.New(em)}
		} else {
			res, _ := msg["result"].(map[string]any)
			if res == nil {
				res = map[string]any{}
			}
			c <- rpcResult{result: res}
		}
		return
	}

	// Agent → client request
	if method, _ := msg["method"].(string); method != "" {
		params, _ := msg["params"].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}
		b.handleAgentRequest(id, method, params)
	}
}

func idKey(id any) string {
	switch v := id.(type) {
	case float64:
		return fmt.Sprintf("%v", v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprintf("%v", id)
	}
}

func (b *Bridge) handleSessionUpdate(params map[string]any) {
	update, _ := params["update"].(map[string]any)
	if update == nil {
		return
	}
	kind, _ := update["sessionUpdate"].(string)
	switch kind {
	case "agent_message_chunk":
		if text := contentText(update["content"]); text != "" {
			b.mu.Lock()
			b.textBuf += text
			b.mu.Unlock()
			ev := Event{"type": "chunk", "text": text}
			if mid, ok := update["messageId"]; ok {
				ev["messageId"] = mid
			}
			b.Broadcast(ev)
		}
	case "user_message_chunk":
		if text := contentText(update["content"]); text != "" {
			b.Broadcast(Event{"type": "user_chunk", "text": text})
		}
	case "agent_thought_chunk":
		// content is typically { "type":"text", "text":"..." }; accept a few shapes
		if text := contentText(update["content"]); text != "" {
			b.Broadcast(Event{"type": "thought", "text": text})
		}
	case "tool_call":
		b.Broadcast(Event{"type": "tool_call", "toolCall": update})
	case "tool_call_update":
		b.Broadcast(Event{"type": "tool_call_update", "toolCallUpdate": update})
	case "plan":
		b.Broadcast(Event{"type": "plan", "entries": update["entries"]})
	case "usage_update":
		b.Broadcast(Event{
			"type": "usage",
			"used": update["used"],
			"size": update["size"],
			"cost": update["cost"],
		})
	case "current_mode_update":
		b.mu.Lock()
		if ms := update["modeState"]; ms != nil {
			b.modes = ms
		}
		modes := b.modes
		b.mu.Unlock()
		b.Broadcast(Event{"type": "modes_update", "modes": modes})
	case "config_option_update":
		b.mu.Lock()
		if co := update["configOptions"]; co != nil {
			b.configOpts = co
		}
		co := b.configOpts
		b.mu.Unlock()
		b.Broadcast(Event{"type": "config_options_update", "configOptions": co})
	case "available_commands_update":
		b.Broadcast(Event{"type": "commands_update", "commands": update["commands"]})
	case "session_info_update":
		b.Broadcast(Event{
			"type":      "session_info",
			"title":     update["title"],
			"updatedAt": update["updatedAt"],
		})
	default:
		// Some builds deliver x.ai-style lifecycle updates over the standard
		// session/update carrier; route them through the same
		// session_notification event so the frontend handles them identically
		// to the x.ai/session_notification channel.
		switch kind {
		case "subagent_spawned", "subagent_finished",
			"task_backgrounded", "task_completed", "monitor_event",
			"response_started", "reasoning_completed", "response_completed",
			"auto_compact_started", "auto_compact_completed",
			"auto_compact_failed", "auto_compact_cancelled",
			"auto_continue_completed", "image_compressed",
			"session_recap", "session_recap_unavailable":
			b.Broadcast(Event{
				"type":   "session_notification",
				"method": "session/update",
				"params": map[string]any{"update": update},
			})
		default:
			b.Broadcast(Event{"type": "unknown_update", "update": update})
		}
	}
}

// unwrapExtMethod normalizes the two x.ai wire forms:
//   - direct:  {"method":"x.ai/foo", "params":{...}}          -> x.ai/foo
//   - wrapped: {"method":"_x.ai/foo","params":{"method":"x.ai/foo","params":{...}}} -> x.ai/foo
func unwrapExtMethod(method string, params map[string]any) (string, map[string]any) {
	if !strings.HasPrefix(method, "_x.ai/") {
		return method, params
	}
	if inner, ok := params["method"].(string); ok && strings.HasPrefix(inner, "x.ai/") {
		if innerParams, ok := params["params"].(map[string]any); ok {
			return inner, innerParams
		}
		return inner, map[string]any{}
	}
	return strings.TrimPrefix(method, "_"), params
}

// handleXaiNotification forwards x.ai/* extension notifications to browsers
// as typed SSE events. Unknown methods fall back to a generic
// ext_notification event so nothing is silently dropped.
func (b *Bridge) handleXaiNotification(method string, params map[string]any) {
	switch method {
	case "x.ai/session_notification", "x.ai/session/update":
		b.Broadcast(Event{"type": "session_notification", "method": method, "params": params})
	case "x.ai/task_backgrounded":
		b.Broadcast(Event{"type": "task_backgrounded", "params": params})
	case "x.ai/task_completed":
		b.Broadcast(Event{"type": "task_completed", "params": params})
	case "x.ai/monitor_event":
		b.Broadcast(Event{"type": "monitor_event", "params": params})
	case "x.ai/git_head_changed":
		b.Broadcast(Event{"type": "git_head_changed", "params": params})
	case "x.ai/yolo_mode_changed":
		b.Broadcast(Event{"type": "yolo_mode_changed", "params": params})
	case "x.ai/mcp/server_status":
		b.Broadcast(Event{"type": "mcp_server_status", "params": params})
	case "x.ai/mcp/tools_changed", "x.ai/mcp_initialized":
		b.Broadcast(Event{"type": "mcp_tools_changed", "params": params})
	case "x.ai/mcp/servers_updated":
		b.Broadcast(Event{"type": "mcp_servers_updated", "params": params})
	case "x.ai/sessions/changed":
		b.Broadcast(Event{"type": "sessions_changed", "params": params})
	case "x.ai/models/update":
		b.Broadcast(Event{"type": "models_update", "params": params})
	case "x.ai/announcements/update":
		b.Broadcast(Event{"type": "announcements_update", "params": params})
	case "x.ai/scheduled_task_fired":
		b.Broadcast(Event{"type": "scheduled_task_fired", "params": params})
	case "x.ai/scheduled_task_inject_prompt":
		b.Broadcast(Event{"type": "scheduled_task_inject_prompt", "params": params})
	case "x.ai/session/prompt_complete":
		b.Broadcast(Event{"type": "prompt_complete", "params": params})
	default:
		b.Broadcast(Event{"type": "ext_notification", "method": method, "params": params})
	}
}

// handleAgentRequest — minimal client: only session/request_permission is interactive.
// fs/* and terminal/* are rejected (capabilities declared false; agent should not call them).
func (b *Bridge) handleAgentRequest(id any, method string, params map[string]any) {
	switch method {
	case "session/request_permission":
		b.forwardPermission(id, method, params)
	case "fs/read_text_file", "fs/write_text_file",
		"terminal/create", "terminal/output", "terminal/wait_for_exit",
		"terminal/kill", "terminal/release":
		b.respondError(id, fmt.Sprintf("客户端未提供 %s（纯 Agent 执行模式）", method), -32601)
	default:
		b.respondError(id, fmt.Sprintf("客户端不支持方法 %s", method), -32601)
	}
}

func (b *Bridge) forwardPermission(id any, method string, params map[string]any) {
	reqID := fmt.Sprintf("acp_cr_%d", b.nextClientReqID.Add(1))
	cr := &clientRequest{
		AgentID:      id,
		Method:       method,
		Params:       params,
		done:         make(chan struct{}),
		isPermission: true,
	}
	b.clientReqs.Store(reqID, cr)
	b.Broadcast(Event{
		"type":      "client_request",
		"requestId": reqID,
		"method":    method,
		"params":    params,
	})

	go b.waitClientResolution(reqID, cr)
}

// forwardXaiRequest forwards an agent → client extension request
// (x.ai/ask_user_question, x.ai/exit_plan_mode, x.ai/mcp/sdk_call, …) to
// the browser as a generic client_request; the browser's response is passed
// back verbatim as the JSON-RPC result.
func (b *Bridge) forwardXaiRequest(id any, method string, params map[string]any) {
	reqID := fmt.Sprintf("acp_cr_%d", b.nextClientReqID.Add(1))
	cr := &clientRequest{
		AgentID: id,
		Method:  method,
		Params:  params,
		done:    make(chan struct{}),
	}
	b.clientReqs.Store(reqID, cr)
	b.Broadcast(Event{
		"type":      "client_request",
		"requestId": reqID,
		"method":    method,
		"params":    params,
	})

	go b.waitClientResolution(reqID, cr)
}

// waitClientResolution blocks until the browser resolves the request,
// the approval timeout elapses, or the agent connection dies, then writes
// the JSON-RPC response back to the agent.
func (b *Bridge) waitClientResolution(reqID string, cr *clientRequest) {
	timer := time.NewTimer(approvalTimeout)
	defer timer.Stop()
	select {
	case <-cr.done:
		b.clientReqs.Delete(reqID)
		id := cr.AgentID
		if cr.cancel {
			if cr.isPermission {
				b.respond(id, map[string]any{
					"outcome": map[string]any{"outcome": "cancelled"},
				})
			} else {
				b.respondError(id, "已取消", -32800)
			}
			return
		}
		if cr.errMsg != "" {
			b.respondError(id, cr.errMsg, -32001)
			return
		}
		if cr.isPermission {
			b.respond(id, cr.outcome)
			return
		}
		b.respond(id, cr.result)
	case <-timer.C:
		b.clientReqs.Delete(reqID)
		if cr.isPermission {
			b.respond(cr.AgentID, map[string]any{
				"outcome": map[string]any{"outcome": "cancelled"},
			})
		} else {
			b.respondError(cr.AgentID, "审批超时", -32002)
		}
	}
}

// RespondPermission resolves a pending session/request_permission.
func (b *Bridge) RespondPermission(requestID, optionID string, cancelled bool) error {
	v, ok := b.clientReqs.LoadAndDelete(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	if cancelled {
		cr.cancel = true
	} else {
		cr.outcome = map[string]any{
			"outcome": map[string]any{
				"outcome":  "selected",
				"optionId": optionID,
			},
		}
	}
	close(cr.done)
	return nil
}

// RespondClientRequest resolves a forwarded x.ai/* request with either a
// raw result object (passed through as the JSON-RPC result) or an error
// message.
func (b *Bridge) RespondClientRequest(requestID string, result map[string]any, errMsg string) error {
	v, ok := b.clientReqs.LoadAndDelete(requestID)
	if !ok {
		return errors.New("审批请求不存在或已过期")
	}
	cr := v.(*clientRequest)
	if errMsg != "" {
		cr.errMsg = errMsg
	} else {
		cr.result = result
	}
	close(cr.done)
	return nil
}

func (b *Bridge) write(msg map[string]any) error {
	b.mu.Lock()
	stdin := b.stdin
	b.mu.Unlock()
	if stdin == nil {
		return errors.New("grok 进程未运行")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = stdin.Write(data)
	return err
}

func (b *Bridge) respond(id any, result map[string]any) {
	_ = b.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func (b *Bridge) respondError(id any, message string, code int) {
	_ = b.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func (b *Bridge) request(ctx context.Context, method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	id := b.nextAgentID.Add(1)
	key := idKey(id)
	ch := make(chan rpcResult, 1)
	// Agent replies with JSON numbers (float64); idKey normalizes both sides.
	b.pending.Store(key, ch)

	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	if err := b.write(msg); err != nil {
		b.pending.Delete(key)
		return nil, err
	}

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-tctx.Done():
		b.pending.Delete(key)
		return nil, fmt.Errorf("%s 超时", method)
	case res := <-ch:
		return res.result, res.err
	}
}

func (b *Bridge) failAllPending(err error) {
	b.pending.Range(func(key, value any) bool {
		b.pending.Delete(key)
		ch := value.(chan rpcResult)
		select {
		case ch <- rpcResult{err: err}:
		default:
		}
		return true
	})
}

// Prompt sends session/prompt and blocks until the turn finishes.
func (b *Bridge) Prompt(ctx context.Context, blocks []ContentBlock) (stopReason string, err error) {
	b.mu.Lock()
	if b.busy {
		b.mu.Unlock()
		return "", &HTTPError{Code: 409, Msg: "上一条消息还在处理中"}
	}
	b.busy = true
	b.mu.Unlock()
	b.Broadcast(Event{"type": "busy"})

	defer func() {
		b.mu.Lock()
		b.busy = false
		b.mu.Unlock()
	}()

	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return "", err
	}

	b.mu.Lock()
	sid := b.sessionID
	b.mu.Unlock()

	// convert blocks
	prompt := make([]any, 0, len(blocks))
	for _, bl := range blocks {
		prompt = append(prompt, map[string]any(bl))
	}

	res, err := b.request(ctx, "session/prompt", map[string]any{
		"sessionId": sid,
		"prompt":    prompt,
	}, promptTimeout)
	if err != nil {
		b.Broadcast(Event{"type": "error", "message": err.Error()})
		// Self-heal: a turn-end failure usually means the agent process is
		// wedged (it streams output but errors when resolving the turn).
		// Kill it and immediately rebuild the session so the UI recovers to
		// "ready" without waiting for the next user message. The failed turn
		// itself is not retried — the user resends it.
		rebootCtx, cancel := context.WithTimeout(context.Background(), bootTimeout)
		defer cancel()
		if rebootErr := b.NewSession(rebootCtx, SessionConfig{}); rebootErr != nil {
			log.Printf("[acp-host] auto-recovery failed: %v", rebootErr)
		} else {
			b.Broadcast(Event{"type": "status", "text": "检测到异常，会话已自动重建，请重发消息"})
		}
		return "", err
	}
	sr, _ := res["stopReason"].(string)
	if sr == "" {
		sr = "unknown"
	}
	b.Broadcast(Event{"type": "done", "stopReason": sr})
	return sr, nil
}

// Cancel sends session/cancel and cancels pending permissions.
func (b *Bridge) Cancel() {
	b.mu.Lock()
	sid := b.sessionID
	b.mu.Unlock()
	if sid != "" {
		_ = b.write(map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/cancel",
			"params":  map[string]any{"sessionId": sid},
		})
	}
	b.clientReqs.Range(func(key, value any) bool {
		b.clientReqs.Delete(key)
		cr := value.(*clientRequest)
		cr.cancel = true
		select {
		case <-cr.done:
		default:
			close(cr.done)
		}
		return true
	})
	b.Broadcast(Event{"type": "cancelled"})
}

// NewSession kills process and boots a new session.
func (b *Bridge) NewSession(ctx context.Context, sc SessionConfig) error {
	b.failAllPending(errors.New("会话已重建"))
	b.clientReqs.Range(func(key, value any) bool {
		b.clientReqs.Delete(key)
		cr := value.(*clientRequest)
		cr.cancel = true
		select {
		case <-cr.done:
		default:
			close(cr.done)
		}
		return true
	})

	b.killProcess()

	// small delay for process cleanup
	time.Sleep(100 * time.Millisecond)
	return b.Boot(ctx, sc)
}

// killProcess kills the grok agent process (if any) and resets all session
// state, so the next Boot starts from a fresh process. Used when a turn or a
// boot step fails on a possibly wedged agent (a live-but-broken process that
// streams output but errors at turn end / never answers RPCs).
func (b *Bridge) killProcess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	b.cmd = nil
	b.stdin = nil
	b.ready = false
	b.sessionID = ""
	b.busy = false
	b.textBuf = ""
	b.modes = nil
	b.configOpts = nil
	if b.cancelRd != nil {
		b.cancelRd()
		b.cancelRd = nil
	}
}

// SetMode calls session/set_mode.
func (b *Bridge) SetMode(ctx context.Context, modeID string) (map[string]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	b.mu.Lock()
	sid := b.sessionID
	b.mu.Unlock()
	return b.request(ctx, "session/set_mode", map[string]any{
		"sessionId": sid,
		"modeId":    modeID,
	}, 30*time.Second)
}

// ListSessions calls session/list if supported.
func (b *Bridge) ListSessions(ctx context.Context) ([]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	res, err := b.request(ctx, "session/list", map[string]any{}, 30*time.Second)
	if err != nil {
		return nil, err
	}
	sessions, _ := res["sessions"].([]any)
	return sessions, nil
}

// LoadSession switches the active session to a historical one (session/load),
// so subsequent prompts and live updates belong to that session.
func (b *Bridge) LoadSession(ctx context.Context, sessionID, cwd string) error {
	b.mu.Lock()
	if b.busy {
		b.mu.Unlock()
		return &HTTPError{Code: 409, Msg: "上一条消息还在处理中"}
	}
	b.mu.Unlock()

	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return err
	}
	if _, err := b.request(ctx, "session/load", map[string]any{
		"sessionId":  sessionID,
		"cwd":        cwd,
		"mcpServers": []any{},
	}, bootTimeout); err != nil {
		return err
	}

	b.mu.Lock()
	b.sessionID = sessionID
	b.cwd = cwd
	b.textBuf = ""
	b.ready = true
	b.bootError = ""
	agentInfo := b.agentInfo
	modes := b.modes
	configOpts := b.configOpts
	b.mu.Unlock()

	b.Broadcast(Event{
		"type":          "ready",
		"sessionId":     sessionID,
		"agentInfo":     agentInfo,
		"modes":         modes,
		"configOptions": configOpts,
		"hostId":        b.hostID,
		"hostName":      b.hostName,
	})
	return nil
}

// ── session history (x.ai/session/updates) ───────────────────────

// UpdatesPage is one page of a session's stored updates (message history).
// Each element of Updates is the full JSONL storage envelope
// {timestamp, method, params}, as returned by the x.ai/session/updates
// extension.
type UpdatesPage struct {
	Updates    []any `json:"updates"`
	TotalCount int   `json:"totalCount"`
	HasMore    bool  `json:"hasMore"`
}

// SessionUpdates fetches a session's stored updates (message history) via
// the ACP extension method x.ai/session/updates. Each element of the result
// is the full storage envelope {timestamp, method, params}.
func (b *Bridge) SessionUpdates(ctx context.Context, sessionID, cwd string, offset *int64, limit *int) (UpdatesPage, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return UpdatesPage{}, err
	}
	params := map[string]any{"sessionId": sessionID, "cwd": cwd}
	if offset != nil {
		params["offset"] = *offset
	}
	if limit != nil {
		params["limit"] = *limit
	}
	// ACP wire convention: extension methods are sent with an underscore
	// prefix ("_" + method). The agent's decoder strips it and routes the
	// remainder to the extension handler; sending "x.ai/session/updates"
	// bare yields -32601 method_not_found.
	res, err := b.request(ctx, "_x.ai/session/updates", params, 2*time.Minute)
	if err != nil {
		return UpdatesPage{}, err
	}
	updates, _ := res["updates"].([]any)
	total, _ := res["totalCount"].(float64)
	hasMore, _ := res["hasMore"].(bool)
	page := UpdatesPage{Updates: updates, TotalCount: int(total), HasMore: hasMore}
	log.Printf("[acp-host] session updates via _x.ai/session/updates ok (total=%d)", page.TotalCount)
	return page, nil
}

// ── x.ai client → agent extension methods ──────────────────────────
//
// Extension requests use the "_" prefix wire convention (same as
// _x.ai/session/updates above); the agent's decoder strips it and routes
// the remainder to the extension handler.

// ForkSession calls x.ai/session/fork: forks the current session into a new
// one (optionally a git worktree). Params follow the TUI's fork payload:
// {sourceSessionId, sourceCwd, newCwd, sessionKind:"fork", newSessionId?}.
func (b *Bridge) ForkSession(ctx context.Context, params map[string]any) (map[string]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	b.mu.Lock()
	sid := b.sessionID
	cwd := b.cwd
	b.mu.Unlock()
	p := map[string]any{
		"sourceSessionId": sid,
		"sourceCwd":       cwd,
		"newCwd":          cwd,
		"sessionKind":     "fork",
	}
	for k, v := range params {
		p[k] = v
	}
	return b.request(ctx, "_x.ai/session/fork", p, 60*time.Second)
}

// RenameSession calls x.ai/session/rename: {sessionId, title}.
func (b *Bridge) RenameSession(ctx context.Context, title string) (map[string]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	b.mu.Lock()
	sid := b.sessionID
	b.mu.Unlock()
	return b.request(ctx, "_x.ai/session/rename", map[string]any{
		"sessionId": sid,
		"title":     title,
	}, 30*time.Second)
}

// Recap fires x.ai/recap (fire-and-forget "where was I" summary; the recap
// arrives later as a SessionRecap session/update). Returns the ack.
func (b *Bridge) Recap(ctx context.Context, auto bool) (map[string]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	b.mu.Lock()
	sid := b.sessionID
	b.mu.Unlock()
	return b.request(ctx, "_x.ai/recap", map[string]any{
		"sessionId": sid,
		"auto":      auto,
	}, 30*time.Second)
}

// SubagentCancel calls x.ai/subagent/cancel: {subagentId}.
func (b *Bridge) SubagentCancel(ctx context.Context, subagentID string) (map[string]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	return b.request(ctx, "_x.ai/subagent/cancel", map[string]any{
		"subagentId": subagentID,
	}, 30*time.Second)
}

// TaskKill calls x.ai/task/kill: {sessionId, taskId}.
func (b *Bridge) TaskKill(ctx context.Context, taskID string) (map[string]any, error) {
	if err := b.Boot(ctx, SessionConfig{}); err != nil {
		return nil, err
	}
	b.mu.Lock()
	sid := b.sessionID
	b.mu.Unlock()
	return b.request(ctx, "_x.ai/task/kill", map[string]any{
		"sessionId": sid,
		"taskId":    taskID,
	}, 30*time.Second)
}

// Shutdown kills grok.
func (b *Bridge) Shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	if b.cancelRd != nil {
		b.cancelRd()
	}
}

// HTTPError carries an HTTP status.
type HTTPError struct {
	Code int
	Msg  string
}

func (e *HTTPError) Error() string { return e.Msg }

// contentText extracts text from an ACP ContentBlock-like value.
func contentText(v any) string {
	switch c := v.(type) {
	case string:
		return c
	case map[string]any:
		if t, ok := c["text"].(string); ok {
			return t
		}
		// nested content
		if inner, ok := c["content"]; ok {
			return contentText(inner)
		}
	case []any:
		var b strings.Builder
		for _, item := range c {
			b.WriteString(contentText(item))
		}
		return b.String()
	}
	return ""
}
