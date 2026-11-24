package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AgentsHarness/capri-host/internal/procattr"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Boot ensures the agent process is up (initialize + authenticate).
// It deliberately does NOT create a session — opening a client must not
// auto-create a new conversation. Sessions are created on demand: the
// first prompt (Prompt), or explicitly via NewSession / session/new.
func (b *Bridge) Boot(ctx context.Context) error {
	return b.ensureBooted(ctx)
}

// ensureBooted starts the grok agent process (if needed) and completes
// initialize + authenticate. Does not create any session. Concurrent
// callers wait for an in-flight boot instead of racing it (a racer would
// hit a nil stdin, misread it as a wedged agent, and kill a healthy
// process).
func (b *Bridge) ensureBooted(ctx context.Context) error {
	b.mu.Lock()
	// bootOK: the process is up and initialize + authenticate already
	// succeeded (just no session active yet) — nothing left to boot.
	if b.ready || b.bootOK {
		b.mu.Unlock()
		return nil
	}
	if b.booting {
		// The waiter's own ctx may be cancelled first (e.g. the client
		// disconnected), but that is NOT a boot failure: the boot is
		// bounded by bootTimeout and may still succeed. Reporting ctx.Err()
		// here would be a false failure. Keep waiting for
		// the in-flight boot (bootDone always closes, even on boot error)
		// and return its real outcome.
		//
		// A finished boot has three outcomes, and a waiter must not
		// misread any of them:
		//   - ready: a session became active — done.
		//   - bootError: the boot failed — report its real error.
		//   - bootOK: the boot succeeded but no session is active yet
		//     (ensureBooted never creates a session; ready only flips once
		//     createSession / LoadSession / session/resume succeeds). This
		//     is NOT a boot failure — return nil so the caller proceeds to
		//     establish its own session on the booted process.
		// A waiter that wakes from a bootDone a NEWER boot has already
		// replaced (booting=true with a different channel) must chain onto
		// that newer boot instead of inventing a failure: its outcome
		// fields are reset (bootOK=false, bootError=""), so judging them
		// would report "agent 启动失败" against a healthy process. In
		// production a finished boot flips booting=false and closes its
		// bootDone in one critical section (the boot role's defer), so
		// booting=true with the SAME channel means the channel was closed
		// without a live boot behind it — treat it as a finished boot and
		// judge its outcome.
		done := b.bootDone
		b.mu.Unlock()
		<-done
		b.mu.Lock()
		ready := b.ready
		errMsg := b.bootError
		ok := b.bootOK
		booting := b.booting
		sameDone := b.bootDone == done
		b.mu.Unlock()
		if booting && !sameDone {
			// The boot we waited on ended and a newer one is in flight —
			// chain onto it (each recursion waits on a strictly newer
			// bootDone, so this cannot loop on the same boot).
			return b.ensureBooted(ctx)
		}
		if ready || ok {
			return nil
		}
		if errMsg != "" {
			return errors.New(errMsg)
		}
		return errors.New("agent 启动失败")
	}
	b.booting = true
	b.bootError = ""
	b.bootOK = false
	b.bootDone = make(chan struct{})
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.booting = false
		close(b.bootDone)
		b.mu.Unlock()
	}()

	if err := b.ensureProcess(); err != nil {
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
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
			// stays agent-side. codeNavigation / folderTrust / fs_notify are
			// env-opt-in (absent = off, same as the TUI).
			kMeta: initCapabilitiesMeta(),
		},
		"clientInfo": map[string]any{
			"name":    "capri-host",
			kTitle:    "ACP Host",
			"version": EffectiveVersion(),
		},
		// `_meta` mirrors the Grok Build TUI's build_initialize_meta
		// (xai-grok-pager/src/acp/mod.rs): clientType defaults to the
		// pager's PAGER_CLIENT_TYPE and clientVersion to its build-time
		// version; the remaining seeds are env-driven and omitted when the
		// env var is absent (matching the TUI's default omit).
		kMeta: initMetaSeeds(),
	}, bootTimeout)
	if err != nil {
		// 假设 agent 可靠：初始化失败直接报错，不杀进程、不自愈。
		// 进程要不要重启由用户通过重启接口决定。
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
		return err
	}

	b.mu.Lock()
	b.agentInfo = initRes
	b.initAgentCapabilities = initRes["agentCapabilities"]
	b.mu.Unlock()

	// protocolVersion is informational for the host: a mismatch (the agent
	// speaking a different protocol) is only logged, never fatal — the wire
	// surface we use is small and stable. (JSON numbers decode as float64.)
	if pv, ok := initRes["protocolVersion"]; ok {
		if n, isNum := asInt(pv); !isNum || n != protocolVersion {
			log.Printf("[Capri-host] agent protocolVersion = %v, host expects %d (continuing)", pv, protocolVersion)
		}
	}

	if err := b.authenticate(ctx, initRes); err != nil {
		// 同 initialize：只报错，不杀进程。
		b.setBootError(err.Error())
		b.Broadcast(Event{kType: kError, "message": err.Error()})
		return err
	}

	// No session is auto-created at boot anymore (opening a client must
	// not spawn a new conversation), so announce the bare-ready state —
	// a client that connected while we were booting would otherwise stay
	// on "启动中…" forever. A session-level ready (with sessionId) follows
	// from createSession / LoadSession when one becomes active.
	b.mu.Lock()
	noSession := len(b.sessions) == 0
	agentInfo := b.agentInfo
	authMeta := b.authMeta
	hostID := b.hostID
	hostName := b.hostName
	b.bootOK = true
	b.mu.Unlock()
	if noSession {
		ev := Event{
			kType:       "ready",
			"agentInfo": agentInfo,
			"hostId":    hostID,
			"hostName":  hostName,
		}
		// authenticate 响应 `_meta`（AuthMeta）与 agentInfo 并列透传，
		// 仅非空才带（absent key ≠ off）。
		if authMeta != nil {
			ev["authMeta"] = authMeta
		}
		b.Broadcast(ev)
	}
	return nil
}

// Version is stamped at build time via
// go build -ldflags "-X github.com/AgentsHarness/capri-host/internal/acp.Version=<tag>".
// Plain `go build` / `go run` leaves it "dev"; EffectiveVersion reports a
// git-derived dev-<shortsha> label for those builds. It feeds both the
// initialize `clientInfo.version` and the `_meta` clientVersion, so the
// released binary's version always equals its git tag.
var Version = "dev"

// initClientType mirrors the Grok Build TUI's initialize `_meta`
// (xai-grok-pager/src/client_identity.rs): PAGER_CLIENT_TYPE is the literal
// "grok-pager"; clientVersion rides acp.Version (build-time injected).
const initClientType = "grok-pager"

// initMetaSeeds builds the initialize request `_meta` (the TUI's
// build_initialize_meta analog): clientType/clientVersion always present,
// the remaining seeds only when their env var is set (absent env = key
// omitted, matching the TUI's default). Verified against the agent's reads:
// clientIdentifier (acp_agent.rs:134), clientSource (mod.rs:2212),
// systemPromptOverride (mod.rs:1119), rules (mod.rs:1099), mcpApps
// (acp_agent.rs:167), bufferingSettings (acp_agent.rs:176), startupHints
// (agent_ops.rs:4108).
func initMetaSeeds() map[string]any {
	meta := map[string]any{
		"clientType":    initClientType,
		"clientVersion": EffectiveVersion(),
	}
	addStringEnv(meta, "clientIdentifier", "ACP_INIT_CLIENT_IDENTIFIER")
	addStringEnv(meta, "clientSource", "ACP_INIT_CLIENT_SOURCE")
	addStringEnv(meta, "systemPromptOverride", "ACP_INIT_SYSTEM_PROMPT_OVERRIDE")
	addStringEnv(meta, "rules", "ACP_INIT_RULES")
	if boolEnv("ACP_INIT_MCP_APPS") {
		meta["mcpApps"] = true
	}
	if v := jsonEnv("ACP_INIT_BUFFERING_SETTINGS"); v != nil {
		// BufferingSettings is camelCase on the wire (update_chunk_merge.rs).
		meta["bufferingSettings"] = v
	}
	if v := jsonEnv("ACP_INIT_STARTUP_HINTS"); v != nil {
		// StartupHints is camelCase (session/acp_types.rs).
		meta["startupHints"] = v
	}
	return meta
}

// initCapabilitiesMeta builds the clientCapabilities.meta object: the five
// always-on TUI capabilities plus env-opt-in codeNavigation / folderTrust /
// fs_notify (absent = off, matching the TUI where those keys are absent).
//
// x.ai/userMessageEcho 是「用户 prompt 的实时回显」开关（grok-shell
// session/user_echo.rs，TUI 恒带该键）：不带它，agent 把 user_message_chunk
// ——正文和附图各一块——只落盘、不活推（session/acp_session_impl/updates.rs
// 的 suppress_live_user_echo），于是发出的图只在回放/历史里出现，实时
// transcript 没有。这里对齐 TUI。
func initCapabilitiesMeta() map[string]any {
	meta := map[string]any{
		"x.ai/incrementalBashOutput": true,
		"x.ai/bashOutputNoColor":     true,
		"x.ai/gitHeadChanged":        true,
		"x.ai/hunkTracker":           map[string]any{"mode": "agent_only"},
		"x.ai/userMessageEcho":       true,
		// The web client draws a status row (context %, cost, branch), so it
		// advertises the same capability the TUI does. Without it the agent
		// skips the payload entirely (grok 1.0.89 statusLine gating) and the
		// FE's session_status chips stay empty.
		"x.ai/statusLine": true,
	}
	if boolEnv("ACP_CAP_CODE_NAVIGATION") {
		// Agent reads meta["x.ai/codeNavigation"]["enabled"] (code_nav.rs:9).
		meta["x.ai/codeNavigation"] = map[string]any{"enabled": true}
	}
	if boolEnv("ACP_CAP_FOLDER_TRUST_INTERACTIVE") {
		// Agent reads meta["x.ai/folderTrust"]["interactive"]
		// (folder_trust_prompt.rs:75).
		meta["x.ai/folderTrust"] = map[string]any{"interactive": true}
	}
	if boolEnv("ACP_CAP_FS_NOTIFY") {
		// Agent reads meta["x.ai/fs_notify"] as a bool (agent_ops.rs:4044).
		meta["x.ai/fs_notify"] = true
	}
	return meta
}

// addStringEnv adds key → env value to m when the env var is non-empty.
func addStringEnv(m map[string]any, key, env string) {
	if v := os.Getenv(env); v != "" {
		m[key] = v
	}
}

// boolEnv reports whether an env var is set to a truthy value
// (1/true/yes/on; case-insensitive). Unset or anything else = false.
func boolEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// jsonEnv parses an env var as a JSON object; nil when unset or invalid.
func jsonEnv(name string) map[string]any {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}
	var v map[string]any
	if json.Unmarshal([]byte(raw), &v) != nil {
		return nil
	}
	return v
}

// clientUserMessageEchoMeta 是**会话级**的「用户 prompt 实时回显」开关
// （grok-shell session/user_echo.rs 的 CLIENT_USER_MESSAGE_ECHO_META）。agent
// 在 session/new | session/load | session/resume 的 `_meta` 上读它；没有它，
// prompt 的 user_message_chunk（正文一块 + 每张附图一块）只落盘不活推——
// 表现就是「发出去的图实时看不见，回放里全都有」。
//
// 实机验证（grok 1.0.30，直连 `grok agent stdio` 探测）：initialize 的
// clientCapabilities.meta["x.ai/userMessageEcho"] 单独**不足以**打开回显，
// 会话 meta 才有效（两者都带最稳）。TUI 也是这条路径——leader 把 client
// 能力翻译成会话 meta 注入（xai-grok-shell/src/leader/server.rs）。
const clientUserMessageEchoMeta = "clientUserMessageEcho"

// withUserMessageEcho returns meta with the echo switch forced on. A copy:
// the caller's map may be reused by the HTTP layer (request body / seeds).
func withUserMessageEcho(meta map[string]any) map[string]any {
	out := make(map[string]any, len(meta)+1)
	for k, v := range meta {
		out[k] = v
	}
	out[clientUserMessageEchoMeta] = true
	return out
}

// intEnv parses an env var as an integer (ok=false when unset/invalid).
func intEnv(name string) (int64, bool) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (b *Bridge) setBootError(msg string) {
	b.mu.Lock()
	b.bootError = msg
	b.ready = false
	b.bootOK = false
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
	if len(b.cfg.SpawnEnv) > 0 {
		// 追加在 os.Environ 之后：同名变量后者生效，这是「Agent 代理」
		// 关闭时把继承来的代理变量覆盖成空的做法。
		cmd.Env = append(os.Environ(), b.cfg.SpawnEnv...)
	}
	// The agent talks JSON-RPC over the pipes attached below, so it never
	// needs a console window. Suppressing it is what keeps a double-clicked
	// GUI host from popping a terminal (see internal/procattr).
	procattr.HideConsole(cmd)
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
	b.agentStartedAt = time.Now().UnixMilli()
	// Fresh agent process: its in-memory permission mode is the default
	// ask (the agent never persists it), so the host's mirror resets too.
	b.permMode = "ask"
	b.mu.Unlock()

	go b.readStdout(rdCtx, stdout)
	go b.readStderr(stderr)
	go b.waitProcess(cmd)

	log.Printf("[Capri-host] spawned %s agent stdio pid=%d", b.cfg.Bin, cmd.Process.Pid)
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
	b.mu.Unlock()
	if !same {
		// 进程已被外部清理取代（host 不再主动 killProcess，此分支仅
		// 防御）：补一条实际死亡时间戳，时间线才完整。
		if cmd.Process != nil {
			log.Printf("[Capri-host] agent process %d reaped (superseded, code=%d)", cmd.Process.Pid, code)
		}
		return
	}
	lastID, _ := b.resetProcessRoster("process-exit", cmd)
	log.Printf("[Capri-host] grok process exited (code=%d), lastSession=%s", code, lastID)
	b.Broadcast(Event{
		kType:    "status",
		kText:    "连接HOST异常，请检查后重试",
		"action": "restart-agent", // 进程已死：唯一恢复动作是用户重启
	})
}

func (b *Bridge) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		t := sc.Text()
		if t != "" {
			b.Broadcast(Event{kType: "log", kText: t})
		}
	}
}

func (b *Bridge) readStdout(ctx context.Context, r io.Reader) {
	// 不用 bufio.Scanner：其单行上限（曾为 64MB）在
	// x.ai/session/updates 返回超大单行 JSON 时会把整条 stdout 通道
	// 打成永久损坏（Scanner 超长后不可恢复，所有在飞 RPC 全部失败、
	// 前端要求重启 agent）。改用 Reader 手动按行切分，只保留一个
	// 512MB 的防御上限防内存失控；JSON-RPC 行本来就整体驻留内存。
	br := bufio.NewReaderSize(r, 64*1024)
	const maxLineBytes = 512 << 20 // 512MB 单行防御上限
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line, err := br.ReadBytes('\n')
		if len(line) > maxLineBytes {
			log.Printf("[Capri-host] stdout 单行超过 %dMB 上限（%d 字节），agent 输出通道已损坏", maxLineBytes>>20, len(line))
			b.failAllPending(fmt.Errorf("agent 输出通道已损坏: 单行 %d 字节超上限", len(line)))
			b.Broadcast(Event{
				kType:    "status",
				kText:    "agent 输出通道异常，请重启 agent",
				"action": "restart-agent",
			})
			return
		}
		if err != nil {
			// EOF 且该行有内容：处理完最后一行再退出（Scanner 对无
			// 尾随换行的最后一行同样会返回）。
			if len(line) > 0 && err == io.EOF {
				b.handleStdoutLineContext(ctx, line)
			}
			if err != io.EOF {
				log.Printf("[Capri-host] stdout 扫描错误: %v — agent 输出通道已损坏", err)
				b.failAllPending(fmt.Errorf("agent 输出通道已损坏: %v", err))
				b.Broadcast(Event{
					kType:    "status",
					kText:    "agent 输出通道异常，请重启 agent",
					"action": "restart-agent",
				})
			}
			return
		}
		b.handleStdoutLineContext(ctx, line)
	}
}

func (b *Bridge) handleStdoutLine(line []byte) {
	b.handleStdoutLineContext(context.Background(), line)
}

func (b *Bridge) handleStdoutLineContext(ctx context.Context, line []byte) {
	b.agentMessageMu.Lock()
	defer b.agentMessageMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		return
	}
	b.onAgentMessage(msg)
}
