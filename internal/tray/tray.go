// Package tray runs the host's system tray menu in-process.
//
// The previous incarnation of this feature was a PowerShell script that
// launched the exe, supervised it, and taskkill'd it plus its grok child on
// exit. Folding it into the binary removes the supervision problem rather than
// solving it: there is one process, so there is nothing to lose track of, and
// "quit" is an ordinary shutdown instead of a kill.
package tray

import (
	"context"
	"fmt"
	"strings"

	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

// Deps is everything the menu needs from the rest of the program. Function
// fields rather than concrete types keep the tray independent of how the host
// is wired, and let the non-Windows build ignore all of it.
type Deps struct {
	Version    string
	Port       int
	HostID     string
	HostName   string
	LogPath    string
	ConfigPath string

	// HubState returns the live hub link. Never nil in practice: the host
	// always builds a hub manager, so that a machine with no hub configured
	// still has something for the pairing menu entry to talk to.
	//
	// The hub address deliberately is NOT a field here. It can change while
	// the process runs — that is the point of the pairing dialog — so reading
	// it from the snapshot is the only way the menu cannot go stale.
	HubState func() hub.State
	// PairWith points the host at hubURL (empty = keep the current one) and
	// exchanges code for a token, without a restart.
	PairWith func(ctx context.Context, hubURL, code string) error
	// Rename changes this host's display name (hub registry, bridge,
	// config.toml) without a restart. The tray dialog calls it with the name
	// the user typed.
	Rename func(ctx context.Context, newName string) error
	// Quit begins an orderly shutdown of the whole host.
	Quit func()
}

// HubURL is the currently configured hub, empty in local mode.
func (d Deps) HubURL() string {
	if d.HubState == nil {
		return ""
	}
	return d.HubState().HubURL
}

// LiveHostName is the display name to show in the tooltip / info dialog. It
// prefers the live hub state (which reflects a rename the tray itself just
// performed, since the manager updates cfg.HostName synchronously) and falls
// back to the compiled-in field before the hub manager is wired.
func (d Deps) LiveHostName() string {
	if d.HubState != nil {
		if n := d.HubState().HostName; n != "" {
			return n
		}
	}
	return d.HostName
}

// LocalURL is the address to open on this machine.
func (d Deps) LocalURL() string {
	return fmt.Sprintf("http://localhost:%d/", d.Port)
}

// LANURL is the address another device on the same network uses — the one you
// type into a phone. Empty when this machine has no usable LAN address, which
// the menu shows by disabling the item rather than opening a URL that cannot
// work.
func (d Deps) LANURL(ni netinfo.Info) string {
	ip := lanIP(ni)
	if ip == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:%d/", ip, d.Port)
}

// lanIP picks the address to hand another device: the default route's source
// address when it belongs to a real interface, else the first candidate.
//
// Preferring the default route's address is what fixes the case the old
// PowerShell tray got wrong — on a machine running a proxy, adapter metrics put
// a virtual NIC first and it would advertise an address nothing could reach.
func lanIP(ni netinfo.Info) string {
	for _, ifc := range ni.Ifaces {
		if ifc.IP == ni.Outbound {
			return ifc.IP
		}
	}
	if len(ni.Ifaces) > 0 {
		return ni.Ifaces[0].IP
	}
	return ni.Outbound
}

// statusText is the one-line link summary shown in the tooltip and menu.
//
// The distinction between "未配对" and "连接中…" is the whole reason the hub
// client grew a State method: previously nothing could tell them apart, so a
// host that had never been paired looked identical to one whose hub was simply
// down, and the only remedy offered for either was to restart the process.
func statusText(st hub.State) string {
	switch {
	case !st.Configured:
		return "本机模式（未配置 hub）"
	case !st.Paired:
		return "未配对"
	case st.Connected:
		if st.Transport != "" {
			return "已连接（" + strings.ToUpper(st.Transport) + "）"
		}
		return "已连接"
	case st.LastError != "":
		return "未连接"
	default:
		return "连接中…"
	}
}
