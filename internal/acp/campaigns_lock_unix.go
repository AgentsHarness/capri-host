//go:build !windows

package acp

import (
	"os"
	"syscall"
)

// lockCampaignsState takes an advisory exclusive lock on <state>.lock
// (the agent uses the same lock file for its own dismiss writer).
// Best-effort: any failure returns a no-op release, and the caller
// proceeds without the lock.
func lockCampaignsState(path string) (release func()) {
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return func() {}
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		_ = lock.Close()
		return func() {}
	}
	return func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}
}
