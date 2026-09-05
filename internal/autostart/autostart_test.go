package autostart

import (
	"os"
	"testing"
)

func TestLaunchedAtLogon(t *testing.T) {
	orig := os.Args
	t.Cleanup(func() { os.Args = orig })

	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{"no args", []string{"capri-host.exe"}, false},
		{"double dash", []string{"capri-host.exe", "--autostart"}, true},
		{"single dash", []string{"capri-host.exe", "-autostart"}, true},
		{"windows slash", []string{"capri-host.exe", "/autostart"}, true},
		{"among others", []string{"capri-host.exe", "--verbose", "--autostart"}, true},
		{"unrelated flag", []string{"capri-host.exe", "--auto"}, false},
		// The program name itself must never count, or an exe that happened
		// to be named this way would permanently suppress the browser.
		{"program name only", []string{"--autostart"}, false},
	} {
		os.Args = tc.args
		if got := LaunchedAtLogon(); got != tc.want {
			t.Errorf("%s: LaunchedAtLogon(%v) = %v, want %v", tc.name, tc.args, got, tc.want)
		}
	}
}
