//go:build !windows

// Package power holds the machine awake on request. Only Windows has a real
// implementation; elsewhere the type exists so callers need no build tags of
// their own, and Supported reports false so a UI can hide the control rather
// than offer one that does nothing.
package power

import "sync"

type Inhibitor struct {
	mu      sync.Mutex
	enabled bool
}

func New(string) *Inhibitor { return &Inhibitor{} }

// Supported is false: keeping a POSIX machine awake means talking to whatever
// the desktop session happens to run (systemd-inhibit, IOKit, an XDG portal),
// which is out of scope for a Windows tray.
func Supported() bool { return false }

func (i *Inhibitor) Enable() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.enabled = true
	return nil
}

func (i *Inhibitor) Disable() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.enabled = false
	return nil
}

func (i *Inhibitor) Enabled() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.enabled
}

func (i *Inhibitor) Toggle() (bool, error) {
	if i.Enabled() {
		return false, i.Disable()
	}
	return true, i.Enable()
}

func (i *Inhibitor) Close() error { return i.Disable() }
