// Package netinfo reports the addresses a person needs in order to reach this
// host from another device — the LAN IP to type into a phone, and the hub's
// resolved address for comparison when the relay looks down.
package netinfo

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Iface is one usable network interface.
type Iface struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

// Info is a snapshot of this machine's addressing.
type Info struct {
	// Outbound is the IPv4 the default route would use. Empty when there is
	// no route at all (fully offline).
	Outbound string `json:"outbound"`
	// Ifaces lists physical-looking interfaces with an IPv4, most likely
	// first. Virtual adapters are excluded — see virtualIface.
	Ifaces []Iface `json:"ifaces"`
}

// virtualIface matches adapter names created by VPNs, proxies, hypervisors and
// container runtimes. These carry addresses that look plausible but are useless
// for "open this on your phone", and on a machine running a proxy like Clash
// they frequently sort ahead of the real NIC.
var virtualIface = regexp.MustCompile(`(?i)clash|wintun|tailscale|zerotier|wireguard|openvpn|tap-|tun\d|vmware|virtualbox|vbox|hyper-v|vethernet|docker|wsl|npcap|radmin|bluetooth|loopback|teredo|isatap|pseudo`)

// Local reports this machine's addressing. It never fails: an unreachable
// network yields an Info with empty fields rather than an error, because every
// caller here is a status panel that should render "unknown" instead of
// nothing.
func Local() Info {
	return Info{Outbound: OutboundIP(), Ifaces: Interfaces()}
}

// OutboundIP returns the local IPv4 the kernel would source a packet from when
// routing to the public internet.
//
// The UDP "dial" sends nothing — connecting a UDP socket only asks the routing
// table which local address applies — so this needs no reachability and costs
// no packets. It is used instead of enumerating interfaces because it answers
// the question that actually matters (which address is the default route's)
// rather than guessing from adapter metrics, which is where the previous
// PowerShell tray picked wrong on machines running a proxy.
func OutboundIP() string {
	conn, err := net.DialTimeout("udp4", "8.8.8.8:80", 2*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || addr.IP == nil || addr.IP.IsUnspecified() {
		return ""
	}
	return addr.IP.String()
}

// Interfaces lists up interfaces holding a non-loopback IPv4, skipping virtual
// adapters. Ordering is stable and puts the likeliest LAN address first.
func Interfaces() []Iface {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []Iface
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		if virtualIface.MatchString(ifc.Name) {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipn.IP.To4()
			if ip4 == nil || ip4.IsLoopback() || ip4.IsLinkLocalUnicast() {
				continue
			}
			out = append(out, Iface{Name: ifc.Name, IP: ip4.String()})
		}
	}
	// Private LAN ranges first (that is what a phone on the same Wi-Fi can
	// reach), then by name so the list does not reshuffle between reads.
	sort.SliceStable(out, func(i, j int) bool {
		pi, pj := isPrivate(out[i].IP), isPrivate(out[j].IP)
		if pi != pj {
			return pi
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func isPrivate(s string) bool {
	ip := net.ParseIP(s)
	return ip != nil && ip.IsPrivate()
}

// ResolveHub extracts the host from a hub base URL and resolves it, so a
// status panel can show whether the name points where the operator expects.
// Returns the hostname, its addresses, and any resolution error.
func ResolveHub(hubURL string) (host string, ips []string, err error) {
	host = strings.TrimSpace(hubURL)
	if host == "" {
		return "", nil, nil
	}
	if u, perr := url.Parse(host); perr == nil && u.Host != "" {
		host = u.Hostname()
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return "", nil, nil
	}
	// An IP literal needs no lookup, and asking the resolver for one just
	// invites a DNS timeout on an offline machine.
	if ip := net.ParseIP(host); ip != nil {
		return host, []string{ip.String()}, nil
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return host, nil, fmt.Errorf("解析 %s 失败: %w", host, err)
	}
	return host, addrs, nil
}
