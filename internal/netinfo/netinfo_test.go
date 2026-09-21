package netinfo

import "testing"

func TestPreferredIPPrefersDefaultRoute(t *testing.T) {
	// The whole point of preferring the default route's source address: on a
	// machine running a proxy, a virtual adapter often sorts first and its
	// address is unreachable from a phone. virtualIface filters the obvious
	// ones by name, but a real second NIC (docking station, second Wi-Fi) is
	// not virtual and still must not win.
	ni := Info{
		Outbound: "192.168.1.20",
		Ifaces: []Iface{
			{Name: "以太网 2", IP: "10.8.0.3"},
			{Name: "WLAN", IP: "192.168.1.20"},
		},
	}
	if got := PreferredIP(ni); got != "192.168.1.20" {
		t.Errorf("PreferredIP = %q, want the default route's 192.168.1.20", got)
	}
}

func TestPreferredIPFallsBackToFirstInterface(t *testing.T) {
	// Outbound is empty when there is no route to the internet at all — an
	// air-gapped LAN still has a usable address, so the caller must not be
	// handed an empty string.
	ni := Info{Ifaces: []Iface{{Name: "WLAN", IP: "192.168.31.7"}}}
	if got := PreferredIP(ni); got != "192.168.31.7" {
		t.Errorf("PreferredIP = %q, want 192.168.31.7", got)
	}
}

func TestPreferredIPFallsBackToOutbound(t *testing.T) {
	// Every interface was filtered as virtual but a route exists: the outbound
	// address is still better than nothing.
	ni := Info{Outbound: "172.20.5.9"}
	if got := PreferredIP(ni); got != "172.20.5.9" {
		t.Errorf("PreferredIP = %q, want 172.20.5.9", got)
	}
}

func TestPreferredIPEmptyWhenNothing(t *testing.T) {
	// Empty is what callers render as a disabled control. Returning something
	// plausible-looking here would produce a clickable item that opens a page
	// that cannot load.
	if got := PreferredIP(Info{}); got != "" {
		t.Errorf("PreferredIP = %q, want empty", got)
	}
}

func TestResolveHubExtractsHostAndSkipsLookupForLiterals(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		host string
		ip   string
	}{
		{name: "url", in: "https://hub.example.com/api/x", host: "hub.example.com"},
		// An IP literal needs no resolver, and asking for one invites a DNS
		// timeout on an offline machine.
		{name: "ip literal", in: "http://192.168.1.10:8787", host: "192.168.1.10", ip: "192.168.1.10"},
		{name: "empty", in: "", host: ""},
	} {
		host, ips, err := ResolveHub(tc.in)
		if tc.host == "" {
			if host != "" || err != nil {
				t.Errorf("%s: ResolveHub(%q) = %q, %v; want empty and no error", tc.name, tc.in, host, err)
			}
			continue
		}
		if host != tc.host {
			t.Errorf("%s: host = %q, want %q", tc.name, host, tc.host)
		}
		if tc.ip != "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", tc.name, err)
			} else if len(ips) != 1 || ips[0] != tc.ip {
				t.Errorf("%s: ips = %v, want [%s]", tc.name, ips, tc.ip)
			}
		}
	}
}

func TestVirtualIfaceFiltering(t *testing.T) {
	// These names are the reason the filter exists: each carries an address
	// that looks plausible and is useless for "open this on your phone".
	virtual := []string{
		"Clash", "clash-verge", "wintun", "Tailscale", "WireGuard",
		"VMware Network Adapter VMnet1", "vEthernet (Default Switch)",
		"vEthernet (WSL)", "Docker0", "Hyper-V Virtual Ethernet Adapter",
	}
	for _, name := range virtual {
		if !virtualIface.MatchString(name) {
			t.Errorf("virtualIface does not match %q; its address would be offered as the LAN address", name)
		}
	}
	// A real adapter must survive the filter.
	for _, name := range []string{"以太网", "WLAN", "Ethernet", "Wi-Fi", "en0"} {
		if virtualIface.MatchString(name) {
			t.Errorf("virtualIface matches the real adapter %q; the LAN address would be filtered away", name)
		}
	}
}
