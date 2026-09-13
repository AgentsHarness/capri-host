//go:build !windows

package autostart

import "errors"

// errUnsupported is returned rather than silently doing nothing: a caller that
// asked to enable autostart needs to know it did not happen.
var errUnsupported = errors.New("此平台不支持开机自启")

// Supported reports whether autostart can be toggled on this platform.
//
// False off Windows. Login-item registration elsewhere means a launchd plist or
// a systemd user unit — both are the platform's own job to manage, and neither
// belongs behind a tray checkbox that only the Windows build has.
func Supported() bool { return false }

// Get reports no registration.
func Get() (Status, error) { return Status{}, nil }

// Enabled is always false.
func Enabled() bool { return false }

// Enable fails loudly.
func Enable() error { return errUnsupported }

// Disable succeeds: there is nothing registered to remove.
func Disable() error { return nil }

// Set applies want, failing only when asked to enable.
func Set(want bool) error {
	if want {
		return errUnsupported
	}
	return nil
}

// Sync has nothing to repoint.
func Sync() (bool, error) { return false, nil }
