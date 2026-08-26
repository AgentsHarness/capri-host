//go:build windows

package autostart

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

// runKey is the per-user logon-run list. HKCU is deliberate: the machine-wide
// equivalent under HKLM would require elevation and would try to start the host
// for every account on the box, including ones with no grok installed.
//
// A var rather than a const so tests can point at a scratch key instead of
// writing into the real logon-run list of whoever runs them.
var runKey = `Software\Microsoft\Windows\CurrentVersion\Run`

// valueName is this program's entry. Stable across versions so an upgrade
// updates the registration instead of leaving a second, stale one behind.
var valueName = "CapriHost"

// Supported reports whether autostart can be toggled on this platform.
func Supported() bool { return true }

// command is the exact string written to the Run key.
//
// The exe path is quoted because Windows splits an unquoted Run value on
// spaces, and the default install path (under a user profile) very often
// contains one.
func command() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("定位当前程序失败: %w", err)
	}
	// Resolve symlinks so a link that is later deleted does not leave a
	// registration pointing at nothing.
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	if abs, err := filepath.Abs(exe); err == nil {
		exe = abs
	}
	return `"` + exe + `" ` + Flag, nil
}

// Get reads the current registration.
func Get() (Status, error) {
	want, wantErr := command()

	k, err := registry.OpenKey(registry.CURRENT_USER, runKey, registry.QUERY_VALUE)
	if err != nil {
		if errors.Is(err, registry.ErrNotExist) {
			return Status{}, nil
		}
		return Status{}, fmt.Errorf("读取自启项失败: %w", err)
	}
	defer k.Close()

	got, _, err := k.GetStringValue(valueName)
	if err != nil {
		if errors.Is(err, registry.ErrNotExist) {
			return Status{}, nil
		}
		return Status{}, fmt.Errorf("读取自启项失败: %w", err)
	}

	st := Status{Enabled: true, Command: got}
	// Compare the executable path only. Comparing whole command lines would
	// report every flag change as stale, and case-insensitively because
	// Windows paths are.
	if wantErr == nil {
		st.Stale = !strings.EqualFold(exeOf(got), exeOf(want))
	}
	return st, nil
}

// Enabled is Get without the detail, for callers that only render a checkbox.
func Enabled() bool {
	st, err := Get()
	return err == nil && st.Enabled
}

// Enable registers the current executable, overwriting any previous entry.
func Enable() error {
	cmd, err := command()
	if err != nil {
		return err
	}
	k, _, err := registry.CreateKey(registry.CURRENT_USER, runKey, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("打开自启注册表项失败: %w", err)
	}
	defer k.Close()
	if err := k.SetStringValue(valueName, cmd); err != nil {
		return fmt.Errorf("写入自启项失败: %w", err)
	}
	return nil
}

// Disable removes the registration. Removing one that is not there succeeds:
// the caller asked for a state, not for an event.
func Disable() error {
	k, err := registry.OpenKey(registry.CURRENT_USER, runKey, registry.SET_VALUE)
	if err != nil {
		if errors.Is(err, registry.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("打开自启注册表项失败: %w", err)
	}
	defer k.Close()
	if err := k.DeleteValue(valueName); err != nil &&
		!errors.Is(err, registry.ErrNotExist) &&
		!errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return fmt.Errorf("删除自启项失败: %w", err)
	}
	return nil
}

// Set applies want.
func Set(want bool) error {
	if want {
		return Enable()
	}
	return Disable()
}

// Sync repoints a stale registration at the running executable and reports
// whether it changed anything.
//
// Called at startup so moving or upgrading the exe does not quietly break
// autostart: the registration would still be there, the checkbox would still
// look on, and nothing would start at the next logon.
func Sync() (bool, error) {
	st, err := Get()
	if err != nil || !st.Enabled || !st.Stale {
		return false, err
	}
	if err := Enable(); err != nil {
		return false, err
	}
	return true, nil
}

// exeOf pulls the program path out of a Run value, whether or not it is quoted.
func exeOf(cmdline string) string {
	s := strings.TrimSpace(cmdline)
	if s == "" {
		return ""
	}
	if s[0] == '"' {
		if end := strings.IndexByte(s[1:], '"'); end >= 0 {
			return s[1 : 1+end]
		}
		return s[1:]
	}
	if sp := strings.IndexByte(s, ' '); sp >= 0 {
		return s[:sp]
	}
	return s
}
