package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// touch creates a file so the real os.Stat can find it.
func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// The shipped layout, produced by packaging/windows/make-exes.sh. If the build
// script and this search disagree, the user unzips a folder whose GUI cannot
// start — so the contract is pinned here rather than assumed.
func TestResolveEnginePrefersBinDirectory(t *testing.T) {
	dir := t.TempDir()
	touch(t, filepath.Join(dir, "bin", "Capri-host.exe"))
	touch(t, filepath.Join(dir, "Capri-host.exe")) // flattened copy also present

	got, err := resolveEngine("", dir, os.Stat)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "bin", "Capri-host.exe")
	if got != want {
		t.Errorf("resolveEngine = %q, want %q (bin/ is the shipped place)", got, want)
	}
}

func TestResolveEngineAcceptsFlattenedLayout(t *testing.T) {
	// Someone dragged the engine out of bin/ next to the GUI. That should keep
	// working: refusing to start over file placement would be gratuitous.
	dir := t.TempDir()
	exe := filepath.Join(dir, "Capri-host.exe")
	touch(t, exe)

	got, err := resolveEngine("", dir, os.Stat)
	if err != nil {
		t.Fatal(err)
	}
	if got != exe {
		t.Errorf("resolveEngine = %q, want %q", got, exe)
	}
}

func TestResolveEngineEnvOverrideWins(t *testing.T) {
	// CAPRI_HOST_BIN is how a developer points the GUI at a build tree, so it
	// must beat a perfectly good engine sitting in bin/.
	dir := t.TempDir()
	touch(t, filepath.Join(dir, "bin", "Capri-host.exe"))
	override := filepath.Join(dir, "elsewhere", "Capri-host.exe")
	touch(t, override)

	got, err := resolveEngine(override, dir, os.Stat)
	if err != nil {
		t.Fatal(err)
	}
	if got != override {
		t.Errorf("resolveEngine = %q, want the override %q", got, override)
	}
}

func TestResolveEngineRejectsMissingEnvOverride(t *testing.T) {
	// A set-but-wrong override is a mistake to report, not a reason to fall
	// back silently — that would run a different engine than the one asked for.
	dir := t.TempDir()
	touch(t, filepath.Join(dir, "bin", "Capri-host.exe"))

	_, err := resolveEngine(filepath.Join(dir, "nope"), dir, os.Stat)
	if err == nil || !strings.Contains(err.Error(), "CAPRI_HOST_BIN") {
		t.Errorf("err = %v, want a CAPRI_HOST_BIN complaint", err)
	}
}

func TestResolveEngineNotFoundNamesTheExpectedPlace(t *testing.T) {
	dir := t.TempDir()

	_, err := resolveEngine("", dir, os.Stat)
	if err == nil {
		t.Fatal("want an error when there is no engine")
	}
	// The message has to say where to put it, or the user has nothing to act on.
	if !strings.Contains(err.Error(), filepath.Join(dir, "bin")) {
		t.Errorf("err = %v, want it to name %q", err, filepath.Join(dir, "bin"))
	}
}
