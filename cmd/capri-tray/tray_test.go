// No build tag: the functions under test are the platform-independent ones
// (see urls.go and status.go), so they run everywhere `go test ./...` does
// rather than only inside a Windows binary somebody had to run by hand.

package main

import (
	"testing"

	"github.com/AgentsHarness/capri-host/internal/hubstate"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

func TestLocalURLUsesLoopbackName(t *testing.T) {
	if got, want := localURL(8765), "http://localhost:8765/"; got != want {
		t.Errorf("localURL = %q, want %q", got, want)
	}
}

func TestLANURLIncludesPort(t *testing.T) {
	ni := netinfo.Info{Outbound: "192.168.1.20", Ifaces: []netinfo.Iface{{Name: "WLAN", IP: "192.168.1.20"}}}
	if got, want := lanURL(ni, 18765), "http://192.168.1.20:18765/"; got != want {
		t.Errorf("lanURL = %q, want %q", got, want)
	}
}

func TestLANURLEmptyWhenNoAddress(t *testing.T) {
	// Empty is what disables the menu item. Returning "http://:8765/" instead
	// would give a clickable item that opens a broken page.
	if got := lanURL(netinfo.Info{}, 8765); got != "" {
		t.Errorf("lanURL with no addressing = %q, want empty", got)
	}
}

func TestMenuStatusLineShowsDialing(t *testing.T) {
	st := hubstate.State{Configured: true, Paired: true}
	got := menuStatusLine(true, 8765, st)
	if got != "运行中 · :8765 · Hub 连接中…" {
		t.Errorf("menuStatusLine = %q", got)
	}
}

func TestStatusText(t *testing.T) {
	cases := []struct {
		name     string
		running  bool
		st       hubstate.State
		stateErr string
		want     string
	}{
		// A stopped host is not a disconnected hub: saying "未连接" for a
		// process the user just stopped sends them looking at the network.
		{name: "host stopped", running: false, want: "Host 未运行"},
		{name: "host up, nothing else yet", running: true, stateErr: "connection refused", want: "Host 未就绪"},
		{name: "local mode", running: true, st: hubstate.State{}, want: "本机模式（未配置 hub）"},
		{name: "configured but unpaired", running: true, st: hubstate.State{Configured: true}, want: "未配对"},
		{name: "paired, dialing", running: true, st: hubstate.State{Configured: true, Paired: true}, want: "连接中…"},
		{name: "paired, failed", running: true, st: hubstate.State{Configured: true, Paired: true, LastError: "dial tcp: timeout"}, want: "未连接"},
		{name: "connected quic", running: true, st: hubstate.State{Configured: true, Paired: true, Connected: true, Transport: "quic"}, want: "已连接（QUIC）"},
		{name: "connected ws", running: true, st: hubstate.State{Configured: true, Paired: true, Connected: true, Transport: "ws"}, want: "已连接（WS）"},
		{name: "connected unknown transport", running: true, st: hubstate.State{Configured: true, Paired: true, Connected: true}, want: "已连接"},
	}
	for _, tc := range cases {
		if got := statusText(tc.running, tc.st, tc.stateErr); got != tc.want {
			t.Errorf("%s: statusText = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestHumanDuration(t *testing.T) {
	for _, tc := range []struct {
		sec  int64
		want string
	}{
		{0, "0 秒"},
		{59, "59 秒"},
		{60, "1 分钟"},
		{3600, "1 小时 0 分"},
		{3660, "1 小时 1 分"},
		{90000, "1 天 1 小时"},
	} {
		if got := humanDuration(tc.sec); got != tc.want {
			t.Errorf("humanDuration(%d) = %q, want %q", tc.sec, got, tc.want)
		}
	}
}
