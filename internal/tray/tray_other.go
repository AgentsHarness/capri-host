//go:build !windows

package tray

import (
	"log"
	"os/exec"
	"runtime"
)

// Supported is false off Windows: the tray exists to give a double-clicked
// Windows binary a UI, and on other platforms the host is run from a shell or a
// service manager where a tray icon has no home.
func Supported() bool { return false }

// Run returns immediately so main can fall back to waiting on its context.
func Run(Deps) {}

// Stop is a no-op.
func Stop() {}

// Alert logs instead of showing a dialog: off Windows the process has a working
// stderr, so the log is already where the operator is looking.
func Alert(title, msg string) {
	log.Printf("[%s] %s", title, msg)
}

// OpenURL hands the URL to the desktop's opener when there is one.
func OpenURL(u string) {
	if u == "" {
		return
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	default:
		cmd = exec.Command("xdg-open", u)
	}
	if err := cmd.Start(); err != nil {
		log.Printf("[tray] 打开 %s 失败: %v", u, err)
	}
}
