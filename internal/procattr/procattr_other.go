//go:build !windows

// Package procattr suppresses the console window Windows would otherwise
// create for a child process. On other platforms there is nothing to suppress,
// so every function here is a no-op — callers need no build tags.
package procattr

import "os/exec"

// HideConsole does nothing off Windows.
func HideConsole(*exec.Cmd) {}
