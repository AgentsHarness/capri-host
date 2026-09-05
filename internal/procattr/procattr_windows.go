//go:build windows

// Package procattr suppresses the console window Windows would otherwise
// create for a child process.
//
// This matters only because the host is now linked with -H=windowsgui. A
// console-subsystem binary owns a console that its children inherit, so before
// the single-exe change every child was silently adopted into the launcher's
// hidden console. A GUI-subsystem process has no console at all, so Windows
// allocates a fresh one for each console child — and the default terminal
// makes it visible. Without this, double-clicking the host pops a terminal for
// the grok agent and flashes another for every `git` call.
package procattr

import (
	"os/exec"
	"syscall"
)

// createNoWindow is CREATE_NO_WINDOW. The child still gets a console (so its
// stdio handles behave normally and the pipes acp attaches keep working), but
// that console has no window.
const createNoWindow = 0x08000000

// HideConsole marks cmd so starting it creates no console window. Safe to call
// on a command that already has a SysProcAttr — the flag is merged, not
// assigned over the top.
func HideConsole(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= createNoWindow
}
