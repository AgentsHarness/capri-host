//go:build windows

package tray

import (
	"os"
	"strings"
	"testing"

	"github.com/ncruces/zenity"

	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

// fakeMenu builds a menu with no systray behind it. Every field the info text
// reads is plain data, so the text can be asserted without a message pump.
func fakeMenu(st hub.State, ni netinfo.Info) *menu {
	t := &menu{
		deps: Deps{
			Version:    "1.2.3",
			Port:       8765,
			HostID:     "pc",
			HostName:   "家里的 Windows",
			LogPath:    `C:\Users\me\.capri-host\logs\host.log`,
			ConfigPath: `C:\Users\me\.capri-host\config.toml`,
			HubState:   func() hub.State { return st },
		},
		net: ni,
	}
	return t
}

var pairedState = hub.State{
	Configured: true,
	Paired:     true,
	Connected:  true,
	Transport:  "quic",
	HubURL:     "https://198.51.100.7",
	UptimeSec:  3725,
}

var lanInfo = netinfo.Info{
	Outbound: "192.168.1.20",
	Ifaces:   []netinfo.Iface{{Name: "WLAN", IP: "192.168.1.20"}},
}

// TestInfoTextCarriesEveryRequestedField pins the contract of the 连接信息
// dialog: host name plus all three addresses plus the pairing verdict. These
// are the things a person opens the dialog to find, and a formatting change
// that quietly drops one would otherwise go unnoticed.
func TestInfoTextCarriesEveryRequestedField(t *testing.T) {
	got := fakeMenu(pairedState, lanInfo).infoText()

	for _, want := range []string{
		"家里的 Windows",               // 本机名字
		"http://localhost:8765/",    // 本机地址
		"http://192.168.1.20:8765/", // 内网地址
		"https://198.51.100.7",      // hub 地址
		"已配对",                       // 匹配信息
		"已连接（QUIC）",                 // link state
		"1 小时 2 分",                  // uptime, humanised
	} {
		if !strings.Contains(got, want) {
			t.Errorf("infoText missing %q\n--- got ---\n%s", want, got)
		}
	}
}

func TestInfoTextSaysUnpairedAndPointsAtTheFix(t *testing.T) {
	st := hub.State{Configured: true, HubURL: "https://198.51.100.7", LastError: "hub 拒绝配对: 配对码无效或已过期"}
	got := fakeMenu(st, lanInfo).infoText()

	if !strings.Contains(got, "未配对") {
		t.Errorf("infoText did not report 未配对:\n%s", got)
	}
	// An unpaired host should name the remedy, not just the symptom — the
	// menu item that fixes it is right there.
	if !strings.Contains(got, "配对 hub") {
		t.Errorf("infoText did not point at the 配对 hub action:\n%s", got)
	}
	if !strings.Contains(got, st.LastError) {
		t.Errorf("infoText dropped the last error, which is the only clue why:\n%s", got)
	}
}

func TestInfoTextInLocalModeDoesNotInventAHub(t *testing.T) {
	got := fakeMenu(hub.State{}, lanInfo).infoText()
	if !strings.Contains(got, "未配置") {
		t.Errorf("local mode should say hub 未配置:\n%s", got)
	}
	// No pairing or connection lines at all: there is nothing to pair with,
	// and printing "未连接" would read as a fault rather than a configuration.
	if strings.Contains(got, "配对状态") || strings.Contains(got, "连接状态") {
		t.Errorf("local mode should not show hub link fields:\n%s", got)
	}
}

func TestInfoTextReportsMissingLANAddress(t *testing.T) {
	got := fakeMenu(pairedState, netinfo.Info{}).infoText()
	if !strings.Contains(got, "未找到可用的局域网地址") {
		t.Errorf("infoText should say the LAN address is unavailable:\n%s", got)
	}
	// And must not emit a half-built URL that looks clickable.
	if strings.Contains(got, "http://:8765") {
		t.Errorf("infoText built a URL with no host:\n%s", got)
	}
}

func TestInfoTextMarksTheDefaultRouteInterface(t *testing.T) {
	ni := netinfo.Info{
		Outbound: "192.168.1.20",
		Ifaces: []netinfo.Iface{
			{Name: "以太网 2", IP: "10.8.0.3"},
			{Name: "WLAN", IP: "192.168.1.20"},
		},
	}
	got := fakeMenu(pairedState, ni).infoText()
	// The marker is what tells someone with two NICs which address the LAN
	// item actually chose.
	if !strings.Contains(got, "* 192.168.1.20") {
		t.Errorf("default-route interface not marked:\n%s", got)
	}
	if strings.Contains(got, "* 10.8.0.3") {
		t.Errorf("marked the wrong interface:\n%s", got)
	}
}

// TestShowInfoDialog renders the real dialog so it can be looked at. Skipped
// unless asked for: it blocks on a modal window, which no unattended run wants.
//
//	go test ./internal/tray -run TestShowInfoDialog -v   (with CAPRI_TRAY_DIALOG=1)
func TestShowInfoDialog(t *testing.T) {
	if os.Getenv("CAPRI_TRAY_DIALOG") == "" {
		t.Skip("set CAPRI_TRAY_DIALOG=1 to render the dialog")
	}
	_ = zenity.Info(fakeMenu(pairedState, lanInfo).infoText(),
		zenity.Title("Capri Host 连接信息"))
}
