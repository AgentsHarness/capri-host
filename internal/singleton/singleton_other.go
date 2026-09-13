//go:build !windows

// Package singleton stops a second copy of the process from starting.
//
// The guard exists for the Windows double-click path: two instances would race
// for the same TCP port and the loser would die with a bind error the user never
// sees, because a GUI-subsystem binary has nowhere to print it. Elsewhere the
// host is started from a shell that reports the bind failure plainly, so this is
// a no-op rather than an unnecessary lock file.
package singleton

// Acquire always succeeds on non-Windows platforms.
func Acquire(string) (release func(), ok bool, err error) {
	return func() {}, true, nil
}
