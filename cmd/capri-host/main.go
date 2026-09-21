package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/server"
)

// version rides acp.Version, which release builds stamp via
// -ldflags "-X .../internal/acp.Version=<tag>"; local builds show "dev".
var version = acp.Version

// This binary is deliberately UI-free: it is a console program that logs to
// stderr and serves an HTTP API. Every visible affordance — the tray icon, the
// dialogs, the browser tab — belongs to a supervisor process (the Windows
// tray, Capri.app) that spawns this one, captures its output and drives the
// control endpoints in internal/server/http_hub.go.
//
// Keeping it that way is what lets the same binary run under launchd, systemd,
// a terminal, or a container without growing a platform-specific UI layer it
// cannot use.

func main() {
	log.Printf("[Capri-host] version %s", version)
	cfg := config.Load()

	// 代理要在任何出网客户端之前落地：hub 中继、agent 探测都读环境变量。
	// 配置留空则改用 macOS 系统网络设置；系统也没开才是直连。
	cfg = cfg.WithSystemProxy()
	cfg.ApplyProxyEnv()
	if desc := cfg.ProxyDescription(); desc != "" {
		log.Printf("[Capri-host] proxy %s", desc)
	}
	// 非回环监听必须配入站钥匙：withAuth 在 FE_TOKEN 为空时是刻意开放的
	// （本机可信），那句话只在回环 socket 上成立。
	if err := config.CheckBindPolicy(cfg); err != nil {
		log.Fatalf("[Capri-host] %v", err)
	}

	// Adopt a per-machine identity BEFORE anything that can pair. The hub
	// keys its host table by host id, so a first pairing against the compiled
	// default "local" would mint a token bound to "local", and every
	// unconfigured host on the same hub would displace the previous one.
	// Deriving the id from the OS hostname is stable across restarts, so a
	// token minted now still matches after a reboot. Nothing is written to
	// disk here — persistHubChoice stamps it on the first successful pairing.
	if cfg.HostID == config.DefaultHostID && cfg.HostName == config.DefaultHostName {
		if id, name, ok := config.MachineHostIdentity(); ok {
			cfg.HostID, cfg.HostName = id, name
			log.Printf("[Capri-host] 采用本机标识 host_id=%s host_name=%q", id, name)
		}
	}

	bridge := acp.NewBridge(acp.GrokConfig{
		Bin:      cfg.GrokBin,
		HostID:   cfg.HostID,
		HostName: cfg.HostName,
		// Pinned to config.Dir() rather than left to the bridge's own
		// default so the whole app directory relocates as a unit under
		// CAPRI_HOME. The default resolves to the same ~/.capri-host path,
		// so existing installs see no change — and a supervisor that reads
		// this file agrees with the host about where it lives.
		LastSessionFile: filepath.Join(config.Dir(), "last-session.json"),
		ResidentCap:     cfg.ResidentCap,
		UsageLedgerOn:   !cfg.UsageLedgerDisabled(),
	})
	srv := server.New(cfg, bridge)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The hub manager is built unconditionally, even with no HUB_URL. That is
	// what lets a supervisor pair a host that has never been configured: a
	// user types a hub address into the tray dialog, the manager retargets a
	// client at it, and nothing has to be hand-edited or restarted.
	hubMgr := hub.NewManager(hub.Config{
		URL:         cfg.HubURL,
		HostID:      cfg.HostID,
		HostName:    cfg.HostName,
		Port:        cfg.Port,
		PairCode:    cfg.HubPairCode,
		Token:       cfg.HostToken,
		LocalBase:   fmt.Sprintf("http://127.0.0.1:%d", cfg.Port),
		AccessToken: cfg.AccessToken,
		// 进程内直调本 host 的 API 处理链（同一条 withAuth 管线），中继
		// 不再绕道本机 TCP 回环。LocalBase 仅作回退保留。
		Local: srv.Handler(),
		// Pinned for the same reason as LastSessionFile: Capri.app and the
		// Windows tray both read hub.json to answer "is this host paired",
		// so all three derive it from one place.
		StateFile: config.HubStatePath(),
		// Optional: bypass proxy/fake-ip DNS for the QUIC transport
		// (e.g. HUB_QUIC_HOST=203.0.113.10).
		QUICHost: os.Getenv("HUB_QUIC_HOST"),
		// Escape hatch for a self-signed hub on a trusted network.
		// Leave unset in production: the QUIC transport otherwise
		// verifies the hub certificate whenever HUB_URL is https
		// (a failure just falls back to WebSocket over verified TLS).
		QUICInsecure: os.Getenv("HUB_QUIC_INSECURE") == "1",
		// Pin the hub's QUIC certificate SPKI (HUB_QUIC_PIN, sha256
		// hex/base64 — see docs/DEPLOY.md). Replaces CA verification
		// with an exact fingerprint match; the safe way to run QUIC
		// against a self-signed hub cert you control.
		QUICPin: cfg.HubQUICPin,
	}, persistHubChoice(cfg))
	// Keep the reference: dropping it is what made the pairing code a
	// startup-only input, with nothing able to report whether the relay was up.
	srv.SetHubController(hubMgr)
	go hubMgr.Run(ctx, bridge)

	// 让监管进程（Windows 托盘、Capri.app 之类）能请求一次有序退出：
	// Windows 不给 console 子进程送 SIGTERM，硬杀会把它名下的 grok 子进程
	// 留成孤儿，而走 HTTP 就复用 main 这一条已有的退出路径。
	srv.SetQuitFunc(stop)

	// Eager boot in background (non-fatal if grok missing until first prompt).
	// Boot only warms the agent process — it does NOT create a session, so
	// capri-fe opening does not auto-start a new conversation. The first
	// prompt restores the last known session when one exists; only a machine
	// with no last-session pointer creates a new chat on demand.
	go func() {
		bootCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := bridge.Boot(bootCtx); err != nil {
			log.Printf("[Capri-host] initial boot: %v", err)
		}
	}()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	// grok 在 stdio 客户端不断开时不会自己 idle-unload；host 按 resident
	// 上限把空闲会话 session/close 掉，名册仍留给 FE 列表/再 load。
	bridge.StartIdleUnload(ctx)

	// 用量台账：在 agent 的 30 天会话清理删掉源文件之前把回合用量抄一份到
	// ~/.capri-host/，让 /usage 的历史回溯不再受清理期限限制。启动先跑一轮
	// 对账（首次把现存会话全部入账），之后周期增量同步。
	bridge.StartUsageLedgerSync(ctx)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		// A dead listener is the one failure a supervisor cannot see any
		// other way. Staying alive here would leave the tray reporting
		// "正在启动…" against a process that will never listen, so say what
		// happened and exit non-zero instead.
		log.Printf("[Capri-host] 服务退出: %v", err)
		stop()
		bridge.Shutdown()
		os.Exit(1)
	}

	log.Printf("[Capri-host] shutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	bridge.Shutdown()
}

// persistHubChoice returns the callback the hub manager invokes after pairing
// against a new address, so the choice survives a restart.
//
// It also stamps the adopted machine identity into config.json the first time
// those keys are empty: the id was derived in main and used for this pairing,
// but nothing else writes it down. Already-set keys are left alone — Rename
// writes host_name first, and overwriting it with the startup snapshot would
// undo a "本机代号" that happened before this pairing.
func persistHubChoice(cfg config.Config) hub.PairPersist {
	return func(hubURL string) error {
		err := config.UpdateFile(func(f *config.File) {
			f.HubURL = hubURL
			if strings.TrimSpace(f.HostID) == "" && cfg.HostID != config.DefaultHostID {
				f.HostID = cfg.HostID
			}
			if strings.TrimSpace(f.HostName) == "" && cfg.HostName != config.DefaultHostName {
				f.HostName = cfg.HostName
			}
		})
		if err != nil {
			return err
		}
		log.Printf("[Capri-host] hub 地址已写入 %s", config.Path())
		return nil
	}
}
