// Package autostart registers the host to start when the user signs in.
//
// It is off by default and only ever writes to the current user's own
// registration, so enabling it never needs administrator rights and never
// affects another account on the machine.
package autostart

import "os"

// Flag is appended to the registered command line so the process can tell a
// logon launch from a double-click.
//
// This exists because the two launches want different behaviour: a
// double-click should open the web UI, and a logon launch should not — nobody
// wants a browser tab thrown at them every time they sign in. Without a marker
// the process cannot distinguish the two, and "start with Windows" would come
// with an unwanted tab as a side effect.
const Flag = "--autostart"

// Status describes the current registration.
type Status struct {
	// Enabled is true when a registration for this program exists.
	Enabled bool
	// Command is what the registration will run. Empty when not registered.
	Command string
	// Stale is true when a registration exists but points at a different
	// executable — the usual cause is that the exe was moved or replaced
	// since it was enabled, which would make the registration silently do
	// nothing at the next logon.
	Stale bool
}

// LaunchedAtLogon reports whether this process was started by the registration
// rather than by a person.
func LaunchedAtLogon() bool {
	for _, a := range os.Args[1:] {
		if a == Flag || a == "-autostart" || a == "/autostart" {
			return true
		}
	}
	return false
}
