package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotatingFileRotatesAndKeepsGenerations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Capri-host.log")
	// Small enough that a handful of writes crosses it.
	w, err := openRotating(path, 100, 2)
	if err != nil {
		t.Fatalf("openRotating: %v", err)
	}
	defer w.Close()

	line := strings.Repeat("x", 40) + "\n"
	for i := 0; i < 12; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// The live file must always exist and be under the cap after rotation.
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("live log missing: %v", err)
	}
	if st.Size() > 100 {
		t.Errorf("live log is %d bytes, want <= 100 (rotation did not happen)", st.Size())
	}

	// Generations 1..keep exist; anything older is deleted rather than
	// accumulating forever.
	for i := 1; i <= 2; i++ {
		if _, err := os.Stat(fmt.Sprintf("%s.%d", path, i)); err != nil {
			t.Errorf("generation %d missing: %v", i, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Errorf("generation 3 exists; keep=2 should have deleted it")
	}
}

func TestRotatingFileAppendsToExisting(t *testing.T) {
	// A restart must not truncate the log: the whole reason the tray owns the
	// file is that the interesting part is what happened before it restarted.
	dir := t.TempDir()
	path := filepath.Join(dir, "Capri-host.log")
	if err := os.WriteFile(path, []byte("earlier run\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := openRotating(path, 1<<20, 3)
	if err != nil {
		t.Fatalf("openRotating: %v", err)
	}
	if _, err := w.Write([]byte("this run\n")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(got), "earlier run\n") {
		t.Errorf("log was truncated on reopen: %q", got)
	}
	if !strings.Contains(string(got), "this run\n") {
		t.Errorf("new line not appended: %q", got)
	}
}

func TestRotatingFileWriteAfterCloseIsNotAnError(t *testing.T) {
	// The host's shutdown path can race a final log line. Failing there would
	// turn a log write into an error the caller has to handle, for no gain.
	dir := t.TempDir()
	w, err := openRotating(filepath.Join(dir, "tray.log"), 0, 0)
	if err != nil {
		t.Fatalf("openRotating: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	n, err := w.Write([]byte("after close\n"))
	if err != nil || n != len("after close\n") {
		t.Errorf("Write after Close = (%d, %v), want (%d, nil)", n, err, len("after close\n"))
	}
}

func TestRotatingFileDefaults(t *testing.T) {
	// Zero values must resolve to the documented defaults rather than a zero
	// max, which would rotate on every single line.
	dir := t.TempDir()
	w, err := openRotating(filepath.Join(dir, "tray.log"), 0, 0)
	if err != nil {
		t.Fatalf("openRotating: %v", err)
	}
	defer w.Close()
	if w.max != defaultLogMax {
		t.Errorf("max = %d, want %d", w.max, defaultLogMax)
	}
	if w.keep != defaultLogKeep {
		t.Errorf("keep = %d, want %d", w.keep, defaultLogKeep)
	}
}
