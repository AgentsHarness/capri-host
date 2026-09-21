package config

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"regexp"
	"strings"
)

// DefaultHostID and DefaultHostName are the compiled-in identity used when
// nothing else supplies one. Exported so a caller can tell "the user never
// chose an identity" from "the user chose this one" — the hub keys its host
// table by id, so leaving every unconfigured host on the same default makes
// two of them displace each other on the same hub.
const (
	DefaultHostID   = "local"
	DefaultHostName = "Local Host"
)

// nonAlnum collapses anything that is not a letter or digit, so a machine name
// with spaces or CJK still yields a usable host id.
var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// MachineHostIdentity derives a per-machine host id and display name from the
// OS hostname.
//
// This matters because the hub keys its host table by host id: two people who
// both run an unconfigured host would arrive as the compiled default and
// silently displace each other on the hub. ok is false only when the OS gives
// us no hostname at all, in which case the caller should keep its own default.
//
// The derivation is stable across restarts, so a token minted against a
// derived id still matches after a reboot without anything being written to
// disk first.
func MachineHostIdentity() (id, name string, ok bool) {
	h, err := os.Hostname()
	if err != nil {
		return "", "", false
	}
	name = strings.TrimSpace(h)
	if name == "" {
		return "", "", false
	}
	// Strip a DNS suffix: "pc.lan" and "pc" are the same machine.
	if i := strings.IndexByte(name, '.'); i > 0 {
		name = name[:i]
	}

	id = strings.Trim(nonAlnum.ReplaceAllString(strings.ToLower(name), "-"), "-")
	if id == "" {
		// A purely non-ASCII hostname (common on Chinese Windows) leaves
		// nothing to slug. Hash it instead of picking at random, so the id is
		// stable across restarts — a host whose id changed every launch would
		// accumulate dead entries in the hub's host table.
		sum := sha256.Sum256([]byte(name))
		id = "host-" + hex.EncodeToString(sum[:4])
	}
	if len(id) > 48 {
		id = strings.Trim(id[:48], "-")
	}
	return id, name, true
}
