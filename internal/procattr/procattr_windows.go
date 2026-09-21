//go:build windows

// Package procattr suppresses the console window Windows would otherwise
// create for a child process.
//
// It exists because of a Windows quirk that bites any process without a
// console: a console-subsystem parent owns a console its children inherit and
// appear nowhere, but a parent that has NO console forces Windows to allocate a
// fresh one for every console child — and the default terminal makes that one
// visible.
//
// Two processes here are in that position. Capri.exe is a GUI-subsystem
// binary, so it spawns the host with this flag. Capri-host may itself be
// started detached (a service manager, a supervisor that handed it no console),
// so it applies the same flag to the grok, git and shell children it runs.
// Without it, double-clicking the tray flashes a terminal for the host and
// another for every `git` call.
//
// The child still gets a console — so its stdio handles behave normally and the
// pipes acp attaches keep working — it simply has no window.
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
