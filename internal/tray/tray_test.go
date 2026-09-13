package tray

import (
	"testing"

	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

func TestLANIPPrefersDefaultRoute(t *testing.T) {
	// The whole point of preferring the default route's source address: on a
	// machine running a proxy, a virtual adapter often sorts first and its
	// address is unreachable from a phone. netinfo filters the obvious ones by
	// name, but a real second NIC (docking station, second Wi-Fi) is not
	// virtual and still must not win.
	ni := netinfo.Info{
		Outbound: "192.168.1.20",
		Ifaces: []netinfo.Iface{
			{Name: "以太网 2", IP: "10.8.0.3"},
			{Name: "WLAN", IP: "192.168.1.20"},
		},
	}
	if got := lanIP(ni); got != "192.168.1.20" {
		t.Errorf("lanIP = %q, want the default route's 192.168.1.20", got)
	}
}

func TestLANIPFallsBackToFirstInterface(t *testing.T) {
	// Outbound is empty when there is no route to the internet at all — an
	// air-gapped LAN still has a usable address, so the item must not go dark.
	ni := netinfo.Info{
		Ifaces: []netinfo.Iface{{Name: "WLAN", IP: "192.168.31.7"}},
	}
	if got := lanIP(ni); got != "192.168.31.7" {
		t.Errorf("lanIP = %q, want 192.168.31.7", got)
	}
}

func TestLANIPFallsBackToOutboundWhenNoInterfaceMatches(t *testing.T) {
	// Every interface was filtered as virtual but a route exists: the outbound
	// address is still better than nothing.
	ni := netinfo.Info{Outbound: "172.20.5.9"}
	if got := lanIP(ni); got != "172.20.5.9" {
		t.Errorf("lanIP = %q, want 172.20.5.9", got)
	}
}

func TestLANURLEmptyWhenNoAddress(t *testing.T) {
	// Empty is what disables the menu item. Returning "http://:8765/" instead
	// would give a clickable item that opens a broken page.
	d := Deps{Port: 8765}
	if got := d.LANURL(netinfo.Info{}); got != "" {
		t.Errorf("LANURL with no addressing = %q, want empty", got)
	}
}

func TestLANURLIncludesPort(t *testing.T) {
	d := Deps{Port: 18765}
	ni := netinfo.Info{Outbound: "192.168.1.20", Ifaces: []netinfo.Iface{{Name: "WLAN", IP: "192.168.1.20"}}}
	if got, want := d.LANURL(ni), "http://192.168.1.20:18765/"; got != want {
		t.Errorf("LANURL = %q, want %q", got, want)
	}
}

func TestLocalURLUsesLoopbackName(t *testing.T) {
	d := Deps{Port: 8765}
	if got, want := d.LocalURL(), "http://localhost:8765/"; got != want {
		t.Errorf("LocalURL = %q, want %q", got, want)
	}
}

func TestStatusTextDistinguishesUnpairedFromConnecting(t *testing.T) {
	// This distinction is the reason hub.Client grew a State method. Before,
	// a host that had never been paired looked identical to one whose hub was
	// simply down, and the only remedy offered for either was a restart.
	cases := []struct {
		name string
		st   hub.State
		want string
	}{
		{"local mode", hub.State{}, "本机模式（未配置 hub）"},
		{"configured but unpaired", hub.State{Configured: true}, "未配对"},
		{"paired, dialing", hub.State{Configured: true, Paired: true}, "连接中…"},
		{"paired, failed", hub.State{Configured: true, Paired: true, LastError: "dial tcp: timeout"}, "未连接"},
		{"connected, quic", hub.State{Configured: true, Paired: true, Connected: true, Transport: "quic"}, "已连接（QUIC）"},
		{"connected, ws", hub.State{Configured: true, Paired: true, Connected: true, Transport: "ws"}, "已连接（WS）"},
		{"connected, unknown transport", hub.State{Configured: true, Paired: true, Connected: true}, "已连接"},
	}
	for _, tc := range cases {
		if got := statusText(tc.st); got != tc.want {
			t.Errorf("%s: statusText = %q, want %q", tc.name, got, tc.want)
		}
	}
}
