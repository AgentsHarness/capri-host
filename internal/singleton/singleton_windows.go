//go:build windows

package singleton

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// Acquire takes a named mutex for this logon session.
//
// Named "Local\\…" rather than "Global\\…" on purpose: the guard is meant to
// stop one user double-clicking the exe twice, not to stop a second user on the
// same machine from running their own host — their state lives in a different
// profile. A Global name would also need privileges this process should not
// require.
//
// Returns ok=false with a nil error when another instance already holds it,
// which is an ordinary outcome rather than a failure.
func Acquire(name string) (release func(), ok bool, err error) {
	n, err := windows.UTF16PtrFromString(`Local\` + name)
	if err != nil {
		return nil, false, err
	}
	// CreateMutex returns a valid handle in BOTH cases; ERROR_ALREADY_EXISTS
	// is how it reports that we are the second caller, not that it failed.
	h, err := windows.CreateMutex(nil, false, n)
	if err != nil && !errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		return nil, false, fmt.Errorf("CreateMutex 失败: %w", err)
	}
	if errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		if h != 0 {
			_ = windows.CloseHandle(h)
		}
		return nil, false, nil
	}
	return func() { _ = windows.CloseHandle(h) }, true, nil
}
