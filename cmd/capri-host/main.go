package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/applog"
	"github.com/AgentsHarness/capri-host/internal/autostart"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/server"
	"github.com/AgentsHarness/capri-host/internal/singleton"
	"github.com/AgentsHarness/capri-host/internal/tray"
)

// version rides acp.Version, which release builds stamp via
// -ldflags "-X .../internal/acp.Version=<tag>"; local builds show "dev".
var version = acp.Version

// mutexName is the single-instance key. Scoped to the logon session, not the
// machine — see internal/singleton.
const mutexName = "capri-host-single-instance"

func main() {
	// Settings must be migrated before they are read: the old PowerShell
	// launcher held the only copy of HUB_URL and FE_TOKEN, so a first run of
	// the single exe would otherwise come up in local mode and look broken.
	migrated, migrateErr := config.MigrateLegacyEnv()

	cfg := config.Load()

	// Logging comes second, but before anything that can fail in an
	// interesting way. A Windows GUI binary has no stderr, so until this runs
	// every log line in the process is written into a void.
	logFile, logErr := applog.Setup(applog.Options{
		Dir:        config.LogDir(),
		Name:       "host.log",
		AlsoStderr: true,
	})
	if logFile != nil {
		defer logFile.Close()
	}

	log.Printf("[capri-host] version %s", version)
	if logErr != nil {
		log.Printf("[capri-host] 日志文件不可用: %v", logErr)
	}
	if migrateErr != nil {
		log.Printf("[capri-host] 迁移 env.ps1 失败: %v", migrateErr)
	} else if migrated != "" {
		log.Printf("[capri-host] 已从 env.ps1 生成配置文件 %s", migrated)
	}
	if cfg.ConfigError != nil {
		// A malformed settings file is reported loudly and then ignored:
		// refusing to boot would strand a user whose only UI is a tray icon
		// that never appears.
		log.Printf("[capri-host] 配置文件有误，已忽略: %v", cfg.ConfigError)
		tray.Alert("Capri Host 配置有误", fmt.Sprintf("%v\n\n本次启动已使用默认设置。", cfg.ConfigError))
	} else if cfg.ConfigSource != "" {
		log.Printf("[capri-host] 已读取配置 %s", cfg.ConfigSource)
	}

	localURL := fmt.Sprintf("http://localhost:%d/", cfg.Port)

	// One host per session. Two copies would race for the port and the loser
	// would die on bind with nowhere to print why, so the second launch
	// behaves the way a user expects a double-click to behave: it surfaces the
	// instance that is already running.
	release, sole, err := singleton.Acquire(mutexName)
	if err != nil {
		log.Printf("[capri-host] 单实例检查失败（继续启动）: %v", err)
	} else if !sole {
		log.Printf("[capri-host] 已有实例在运行，打开 %s 后退出", localURL)
		tray.OpenURL(localURL)
		return
	}
	if release != nil {
		defer release()
	}

	// The launcher used to put grok's directory on PATH before exec'ing us.
	config.EnsureGrokOnPath(cfg.GrokBin)

	bridge := acp.NewBridge(acp.GrokConfig{
		Bin:      cfg.GrokBin,
		HostID:   cfg.HostID,
		HostName: cfg.HostName,
		// Pinned to AppDir rather than left to its own default so the whole
		// app directory relocates as a unit under CAPRI_HOST_DIR. The
		// default AppDir is ~/.capri-host, which is exactly where this file
		// already lived — existing installs see no change.
		LastSessionFile: filepath.Join(config.AppDir(), "last-session.json"),
	})
	srv := server.New(cfg, bridge)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Hub relay mode: pair with capri-hub and serve requests through it.
	// The browser talks to the hub; this host keeps working locally too.
	var hubClient *hub.Client
	if cfg.HubURL != "" {
		hubClient = hub.NewClient(hub.Config{
			URL:         cfg.HubURL,
			HostID:      cfg.HostID,
			HostName:    cfg.HostName,
			PairCode:    cfg.HubPairCode,
			Token:       cfg.HostToken,
			LocalBase:   fmt.Sprintf("http://127.0.0.1:%d", cfg.Port),
			AccessToken: cfg.AccessToken,
			// Same reason as LastSessionFile above: keep every piece of
			// per-install state under one relocatable directory. The
			// default resolves to ~/.capri-host/hub.json, which is the
			// path the client would have chosen on its own.
			StateFile: filepath.Join(config.AppDir(), "hub.json"),
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
		})
		// The reference is kept this time. Dropping it is what made the
		// pairing code a startup-only input and left nothing able to report
		// whether the relay was actually up.
		srv.SetHubController(hubClient)
		go hubClient.Run(ctx, bridge)
	}

	// Eager boot in background (non-fatal if grok missing until first prompt).
	// Boot only warms the agent process — it does NOT create a session, so
	// capri-fe opening does not auto-start a new conversation. The first
	// prompt restores the last known session when one exists; only a machine
	// with no last-session pointer creates a new chat on demand.
	go func() {
		bootCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := bridge.Boot(bootCtx); err != nil {
			log.Printf("[capri-host] initial boot: %v", err)
		}
	}()

	serveErr := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		log.Printf("[capri-host] server stopped: %v", err)
		serveErr <- err
	}()

	// A bind failure is the one startup error a tray user must be told about:
	// nothing else in the UI would ever appear, and the log is not somewhere
	// they are looking.
	if !waitForListen(ctx, cfg.Port, 10*time.Second) {
		select {
		case err := <-serveErr:
			log.Printf("[capri-host] 端口 %d 无法监听: %v", cfg.Port, err)
			tray.Alert("Capri Host 启动失败",
				fmt.Sprintf("端口 %d 无法监听：\n\n%v\n\n日志：%s", cfg.Port, err, logPath(logFile)))
			return
		default:
			log.Printf("[capri-host] 端口 %d 尚未就绪，继续启动", cfg.Port)
		}
	}

	// Opened on a double-click, not at logon. Autostart exists so the host is
	// already running when you sit down; throwing a browser tab at every
	// sign-in would make the feature something you turn off again.
	switch {
	case autostart.LaunchedAtLogon():
		log.Printf("[capri-host] 开机自启启动，跳过打开浏览器")
	case cfg.OpenBrowser:
		log.Printf("[capri-host] 打开 %s", localURL)
		tray.OpenURL(localURL)
	}

	if cfg.EnableTray && tray.Supported() {
		deps := tray.Deps{
			Version:    version,
			Port:       cfg.Port,
			HostID:     cfg.HostID,
			HostName:   cfg.HostName,
			HubURL:     cfg.HubURL,
			LogPath:    logPath(logFile),
			ConfigPath: config.ConfigPath(),
			Quit:       stop,
		}
		if hubClient != nil {
			deps.HubState = hubClient.State
			deps.Pair = hubClient.Pair
		}
		// A signal (or any other cancel) must also take the tray down, or
		// Run would keep blocking main after the rest has shut down.
		go func() {
			<-ctx.Done()
			tray.Stop()
		}()
		// Blocks on the main goroutine: systray owns a window on Windows and
		// its message pump belongs to the thread that created it.
		tray.Run(deps)
		stop() // tray quit — cancel the hub client and the boot context
	} else {
		<-ctx.Done()
	}

	log.Printf("[capri-host] shutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	bridge.Shutdown()
}

// waitForListen reports whether the port accepts connections within timeout.
// A TCP dial is used rather than an HTTP probe because the API may require a
// token, and "listening" is the only thing being asked.
func waitForListen(ctx context.Context, port int, timeout time.Duration) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

func logPath(lf *applog.File) string {
	if lf == nil {
		return ""
	}
	return lf.Path()
}
