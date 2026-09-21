//go:build windows

package main

import (
	"path/filepath"

	"fyne.io/systray"
	"github.com/ncruces/zenity"

	"github.com/AgentsHarness/capri-host/internal/config"
)

// version is stamped at build time via -ldflags "-X main.version=<tag>", the
// same tag the host is built with. Local builds show "dev".
var version = "dev"

// trayLog is the tray's own log file. The tray is a GUI-subsystem binary with
// no stderr, so this is the only place its diagnostics can go. The host's
// output goes to a separate file — two processes, two streams, one directory.
var trayLog *rotatingFile

// main is a supervisor, not the host.
//
// It owns the visible surface (tray icon, dialogs, browser tab) and the host's
// lifetime, and talks to the host over loopback only. None of this is compiled
// into Capri-host, which is why the same host binary also runs under a
// terminal, launchd, systemd or a container.
func main() {
	logDir := filepath.Join(config.Dir(), "logs")

	var err error
	trayLog, err = openRotating(filepath.Join(logDir, "Capri.log"), 0, 0)
	if err != nil {
		// No log to report to. Carry on with logging disabled rather than
		// refuse to start: the tray is the only UI this machine has.
		trayLog = nil
	}
	defer func() {
		if trayLog != nil {
			_ = trayLog.Close()
		}
	}()
	logf("Capri %s 启动", version)

	// One tray per logon session. A second double-click should surface the
	// instance already running rather than start a rival supervisor that
	// fights over the same host process and port.
	release, sole, err := acquireSingleton()
	if err != nil {
		logf("单实例检查失败（继续启动）: %v", err)
	} else if !sole {
		logf("已有托盘在运行，打开本机页面后退出")
		s, _ := loadSettings()
		openURL(localURL(s.ListenPort()))
		return
	}
	if release != nil {
		defer release()
	}

	bin, err := hostBinary()
	if err != nil {
		logf("%v", err)
		_ = zenity.Error(err.Error()+"\n\n请重新解压完整的 Capri 目录后重试。",
			zenity.Title("Capri 无法启动"))
		return
	}

	// The host's stdio sink. It is opened once and shared across restarts so
	// the log accumulates instead of resetting on every restart.
	hostLog, err := openRotating(filepath.Join(logDir, "Capri-host.log"), 0, 0)
	if err != nil {
		logf("无法打开 host 日志: %v", err)
		_ = zenity.Error("无法打开日志文件：\n\n"+err.Error(),
			zenity.Title("Capri 无法启动"))
		return
	}
	defer func() { _ = hostLog.Close() }()

	app := newTrayApp(version, trayLog, newHostProc(bin, hostLog))
	// Blocks on the main goroutine: systray owns a window and its message pump
	// belongs to the thread that created it.
	systray.Run(app.onReady, app.onExit)
	logf("托盘已退出")
}
