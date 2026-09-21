package config

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func clearHostEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"BIND", "HOST_BIND", "PORT", "GROK_BIN",
		"HUB_URL", "HUB_PAIR_CODE", "HOST_TOKEN",
		"HOST_ID", "HOST_NAME", "HUB_QUIC_PIN",
		"FE_TOKEN", "ACCESS_TOKEN", "RESIDENT_CAP", "USAGE_LEDGER",
		"PROXY", "NO_PROXY", "no_proxy",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadFileMissing(t *testing.T) {
	isolateConfig(t)
	f, err := LoadFile()
	if err != nil {
		t.Fatalf("missing file: %v", err)
	}
	if f != (File{}) {
		t.Fatalf("missing file → %+v, want zero", f)
	}
}

func TestSaveLoadFileRoundTrip(t *testing.T) {
	isolateConfig(t)
	on := true
	in := File{
		Bind:              "0.0.0.0",
		Port:              9000,
		HostID:            "mba",
		HostName:          "MacBook Air",
		HubURL:            "https://hub.example",
		FEToken:           "secret",
		GrokBin:           "/opt/grok",
		StartHostOnLaunch: &on,
	}
	if err := SaveFile(in); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(Path())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && st.Mode().Perm() != 0o600 {
		t.Fatalf("config.json mode = %o, want 0600", st.Mode().Perm())
	}
	got, err := LoadFile()
	if err != nil {
		t.Fatal(err)
	}
	if got.Bind != in.Bind || got.Port != in.Port || got.HostID != in.HostID ||
		got.HostName != in.HostName || got.HubURL != in.HubURL ||
		got.FEToken != in.FEToken || got.GrokBin != in.GrokBin {
		t.Fatalf("round-trip = %+v", got)
	}
	if !got.ShouldStartHostOnLaunch() {
		t.Fatal("start_host_on_launch true was lost")
	}
}

func TestLoadMergesFileThenEnv(t *testing.T) {
	isolateConfig(t)
	clearHostEnv(t)
	if err := SaveFile(File{
		Bind:     "0.0.0.0",
		Port:     9000,
		HostID:   "from-file",
		HostName: "From File",
		HubURL:   "https://file.example",
		FEToken:  "file-token",
		GrokBin:  "/from/file/grok",
	}); err != nil {
		t.Fatal(err)
	}

	cfg := Load()
	if cfg.BindAddr != "0.0.0.0" || cfg.Port != 9000 || cfg.HostID != "from-file" ||
		cfg.HostName != "From File" || cfg.HubURL != "https://file.example" ||
		cfg.AccessToken != "file-token" || cfg.GrokBin != "/from/file/grok" {
		t.Fatalf("file-only Load = %+v", cfg)
	}

	t.Setenv("BIND", "127.0.0.1")
	t.Setenv("PORT", "8765")
	t.Setenv("HOST_ID", "from-env")
	t.Setenv("HOST_NAME", "From Env")
	t.Setenv("HUB_URL", "https://env.example")
	t.Setenv("FE_TOKEN", "env-token")
	t.Setenv("GROK_BIN", "/from/env/grok")
	cfg = Load()
	if cfg.BindAddr != "127.0.0.1" || cfg.Port != 8765 || cfg.HostID != "from-env" ||
		cfg.HostName != "From Env" || cfg.HubURL != "https://env.example" ||
		cfg.AccessToken != "env-token" || cfg.GrokBin != "/from/env/grok" {
		t.Fatalf("env should win: %+v", cfg)
	}
}

func TestFileStartHostDefaultTrue(t *testing.T) {
	var f File
	if !f.ShouldStartHostOnLaunch() {
		t.Fatal("missing key should default to start on launch")
	}
	if f.ShouldStartAtLogin() {
		t.Fatal("missing key should default to not start at login")
	}
	off := false
	f.StartHostOnLaunch = &off
	if f.ShouldStartHostOnLaunch() {
		t.Fatal("explicit false")
	}
}

func TestDiscoverGrokBinPrefersHomeLocal(t *testing.T) {
	home := t.TempDir()
	want := filepath.Join(home, ".local", "bin", grokName())
	if err := os.MkdirAll(filepath.Dir(want), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(want, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	stat := func(p string) (os.FileInfo, error) { return os.Stat(p) }
	look := func(string) (string, error) { return "", os.ErrNotExist }
	got := discoverGrokBin(home, stat, look)
	if got != want {
		t.Fatalf("discover = %q, want %q", got, want)
	}
}

func TestDiscoverGrokBinEmpty(t *testing.T) {
	stat := func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
	look := func(string) (string, error) { return "", os.ErrNotExist }
	if got := discoverGrokBin(t.TempDir(), stat, look); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestResolveGrokBinKeepsExplicit(t *testing.T) {
	if got := resolveGrokBin("/opt/custom/grok"); got != "/opt/custom/grok" {
		t.Fatalf("explicit path = %q", got)
	}
}

func TestUpdateFileConcurrentWritersKeepBothKeys(t *testing.T) {
	isolateConfig(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := UpdateFile(func(f *File) { f.HostName = "a" }); err != nil {
			t.Errorf("host name: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := UpdateFile(func(f *File) { f.Proxy = "http://127.0.0.1:7890" }); err != nil {
			t.Errorf("proxy: %v", err)
		}
	}()
	wg.Wait()
	got, err := LoadFile()
	if err != nil {
		t.Fatal(err)
	}
	if got.HostName != "a" || got.Proxy != "http://127.0.0.1:7890" {
		t.Fatalf("merged file = %+v", got)
	}
}

func grokName() string {
	if runtime.GOOS == "windows" {
		return "grok.exe"
	}
	return "grok"
}
