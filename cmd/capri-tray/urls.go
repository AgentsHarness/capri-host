// This file builds and pre-checks the addresses a person types or clicks.
//
// It carries no build tag on purpose: none of it touches Windows, and leaving
// it in the platform-tagged set would mean the address rules only ever got
// exercised inside a Windows binary somebody had to run by hand. Plain string
// work belongs where `go test ./...` can reach it on any machine.

package main

import (
	"fmt"

	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

// defaultPort mirrors the host's compiled default. It is what the tray assumes
// before it has managed to read a settings file.
const defaultPort = 8765

// localURL is the address to open on this machine. "localhost" rather than
// 127.0.0.1 so the browser treats it as a trustworthy origin.
func localURL(port int) string {
	return fmt.Sprintf("http://localhost:%d/", port)
}

// lanURL is the address another device on the same network uses — the one you
// type into a phone. Empty when this machine has no usable LAN address, which
// the menu shows by disabling the item rather than opening a URL that cannot
// work.
func lanURL(ni netinfo.Info, port int) string {
	ip := netinfo.PreferredIP(ni)
	if ip == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:%d/", ip, port)
}
