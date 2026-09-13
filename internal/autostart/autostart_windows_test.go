//go:build windows

package autostart

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows/registry"
)

// useScratchKey redirects the package at a throwaway registry key for the
// duration of a test. Without this, running the suite would add and remove a
// real logon-run entry on the developer's own account.
const scratchRoot = `Software\CapriHostTest`

func useScratchKey(t *testing.T) {
	t.Helper()
	origKey, origVal := runKey, valueName
	runKey = scratchRoot + `\` + t.Name()
	valueName = "CapriHost"
	t.Cleanup(func() {
		// Delete the value, then the key, then the shared root. All three may
		// be absent, and the root only goes if this was the last test holding
		// it — a suite that leaves registry keys behind is not clean.
		if k, err := registry.OpenKey(registry.CURRENT_USER, runKey, registry.SET_VALUE); err == nil {
			_ = k.DeleteValue(valueName)
			k.Close()
		}
		_ = registry.DeleteKey(registry.CURRENT_USER, runKey)
		_ = registry.DeleteKey(registry.CURRENT_USER, scratchRoot)
		runKey, valueName = origKey, origVal
	})
}

func TestGetReportsNoRegistrationWhenKeyMissing(t *testing.T) {
	useScratchKey(t)
	st, err := Get()
	if err != nil {
		t.Fatalf("Get on missing key: %v", err)
	}
	if st.Enabled {
		t.Fatalf("expected no registration, got %+v", st)
	}
}

func TestEnableThenDisableRoundTrips(t *testing.T) {
	useScratchKey(t)

	if err := Enable(); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	st, err := Get()
	if err != nil {
		t.Fatalf("Get after Enable: %v", err)
	}
	if !st.Enabled {
		t.Fatal("expected Enabled after Enable")
	}
	if st.Stale {
		t.Fatalf("freshly written registration reported stale: %q", st.Command)
	}

	// The value must carry the marker flag, otherwise a logon launch is
	// indistinguishable from a double-click and opens a browser tab.
	if !strings.Contains(st.Command, Flag) {
		t.Errorf("registration missing %s: %q", Flag, st.Command)
	}
	// And the path must be quoted, or Windows splits it on the spaces that
	// user-profile install paths routinely contain.
	if !strings.HasPrefix(st.Command, `"`) {
		t.Errorf("registration path not quoted: %q", st.Command)
	}
	exe, err := os.Executable()
	if err == nil && !strings.EqualFold(filepath.Base(exeOf(st.Command)), filepath.Base(exe)) {
		t.Errorf("registration points at %q, want basename of %q", exeOf(st.Command), exe)
	}

	if err := Disable(); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if Enabled() {
		t.Fatal("still enabled after Disable")
	}
}

func TestDisableIsIdempotent(t *testing.T) {
	useScratchKey(t)
	// Nothing registered: asking for "off" already holds, so this must not be
	// an error. Callers set a state, they do not fire an event.
	if err := Disable(); err != nil {
		t.Fatalf("Disable with nothing registered: %v", err)
	}
	if err := Enable(); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := Disable(); err != nil {
		t.Fatalf("first Disable: %v", err)
	}
	if err := Disable(); err != nil {
		t.Fatalf("second Disable: %v", err)
	}
}

func TestEnableOverwritesRatherThanDuplicating(t *testing.T) {
	useScratchKey(t)
	if err := Enable(); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	first, _ := Get()
	if err := Enable(); err != nil {
		t.Fatalf("second Enable: %v", err)
	}
	second, _ := Get()
	if first.Command != second.Command {
		t.Errorf("re-enabling changed the command: %q -> %q", first.Command, second.Command)
	}
}

func TestStaleRegistrationIsDetectedAndSynced(t *testing.T) {
	useScratchKey(t)

	// Simulate the exe having been moved or replaced since autostart was
	// switched on: the entry is still there, so the checkbox looks on, but
	// nothing would start at the next logon.
	k, _, err := registry.CreateKey(registry.CURRENT_USER, runKey, registry.SET_VALUE)
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	if err := k.SetStringValue(valueName, `"C:\nowhere\old-capri-host.exe" `+Flag); err != nil {
		k.Close()
		t.Fatalf("SetStringValue: %v", err)
	}
	k.Close()

	st, err := Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !st.Enabled || !st.Stale {
		t.Fatalf("expected enabled+stale, got %+v", st)
	}

	fixed, err := Sync()
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if !fixed {
		t.Fatal("Sync reported no change on a stale registration")
	}
	st, err = Get()
	if err != nil {
		t.Fatalf("Get after Sync: %v", err)
	}
	if st.Stale {
		t.Errorf("still stale after Sync: %q", st.Command)
	}

	// A healthy registration must not be rewritten — Sync is repair, not a
	// write on every startup.
	fixed, err = Sync()
	if err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	if fixed {
		t.Error("Sync rewrote a registration that was already correct")
	}
}

func TestSyncLeavesDisabledStateAlone(t *testing.T) {
	useScratchKey(t)
	fixed, err := Sync()
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if fixed {
		t.Error("Sync created a registration that was never asked for")
	}
	if Enabled() {
		t.Error("Sync enabled autostart on its own")
	}
}

func TestSetAppliesWantedState(t *testing.T) {
	useScratchKey(t)
	if err := Set(true); err != nil {
		t.Fatalf("Set(true): %v", err)
	}
	if !Enabled() {
		t.Error("Set(true) did not enable")
	}
	if err := Set(false); err != nil {
		t.Fatalf("Set(false): %v", err)
	}
	if Enabled() {
		t.Error("Set(false) did not disable")
	}
}

func TestExeOf(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"quoted with flag", `"C:\Program Files\capri-host.exe" --autostart`, `C:\Program Files\capri-host.exe`},
		{"quoted no flag", `"C:\a b\capri-host.exe"`, `C:\a b\capri-host.exe`},
		{"bare with flag", `C:\tools\capri-host.exe --autostart`, `C:\tools\capri-host.exe`},
		{"bare no flag", `C:\tools\capri-host.exe`, `C:\tools\capri-host.exe`},
		{"unterminated quote", `"C:\tools\capri-host.exe`, `C:\tools\capri-host.exe`},
		{"leading space", `  "C:\x.exe" --autostart`, `C:\x.exe`},
		{"empty", ``, ``},
	} {
		if got := exeOf(tc.in); got != tc.want {
			t.Errorf("%s: exeOf(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}
