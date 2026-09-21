package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The starter exists to teach, which only works if it is complete: a key that
// is missing from the file is a feature the user has no way to discover.
func TestStarterFileHasEveryTaggedKey(t *testing.T) {
	b, err := marshalAllKeys(StarterFile())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("starter is not valid JSON: %v\n%s", err, b)
	}

	tp := reflect.TypeOf(File{})
	for i := 0; i < tp.NumField(); i++ {
		name, _, _ := strings.Cut(tp.Field(i).Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		if _, ok := got[name]; !ok {
			t.Errorf("starter omits key %q; a user cannot set what they cannot see", name)
		}
	}
	if len(got) != tp.NumField() {
		t.Errorf("starter has %d keys, File has %d fields", len(got), tp.NumField())
	}
}

// Empty values must survive as explicit empty strings, not be dropped: that is
// the whole difference between a scaffold and {"port": 8765}.
func TestStarterFileKeepsEmptyValues(t *testing.T) {
	b, err := marshalAllKeys(StarterFile())
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, key := range []string{"hub_url", "hub_pair_code", "fe_token", "grok_bin", "proxy", "no_proxy"} {
		if !strings.Contains(s, `"`+key+`": ""`) {
			t.Errorf("starter should spell %q out as an empty string, got:\n%s", key, s)
		}
	}
	// The two switches must be concrete booleans, not null: a pointer field
	// renders as null when unset, which tells the user nothing.
	for _, key := range []string{"start_host_on_launch", "start_at_login"} {
		if strings.Contains(s, `"`+key+`": null`) {
			t.Errorf("starter wrote %q as null; it must show the value in force", key)
		}
	}
}

// What it writes must be what the host reads back — a scaffold that parses
// differently from its own source values would silently reconfigure the host.
func TestStarterFileRoundTripsThroughLoadFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(HomeEnv, dir)
	if created, err := WriteStarter(); err != nil || !created {
		t.Fatalf("WriteStarter = (%v, %v), want created", created, err)
	}

	got, err := LoadFile()
	if err != nil {
		t.Fatal(err)
	}
	want := StarterFile()
	if got.Port != want.Port || got.Bind != want.Bind {
		t.Errorf("port/bind = %d/%q, want %d/%q", got.Port, got.Bind, want.Port, want.Bind)
	}
	// The starter's port must be the one the host would have used anyway, or
	// opening settings would change which port the host listens on.
	if got.ListenPort() != 8765 {
		t.Errorf("ListenPort = %d, want the compiled default 8765", got.ListenPort())
	}
	if !got.ShouldStartHostOnLaunch() {
		t.Error("starter turned off start-on-launch; that is the opposite of its default")
	}
	if got.ShouldStartAtLogin() {
		t.Error("starter turned on start-at-login; that is the opposite of its default")
	}
}

func TestWriteStarterNeverClobbersAnExistingFile(t *testing.T) {
	// The file may be the user's own, or Capri.app's. Opening the editor is not
	// consent to rewrite it.
	dir := t.TempDir()
	t.Setenv(HomeEnv, dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	original := []byte(`{"port": 9999, "host_name": "我的机器"}`)
	if err := os.WriteFile(filepath.Join(dir, FileName), original, 0o600); err != nil {
		t.Fatal(err)
	}

	created, err := WriteStarter()
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Error("WriteStarter reported creating a file that already existed")
	}
	after, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Errorf("existing config was rewritten:\n got %s\nwant %s", after, original)
	}
}

func TestWriteStarterUsesPrivateMode(t *testing.T) {
	// The file can hold FE_TOKEN, so it follows config.json's 0600 rule rather
	// than whatever the umask would have given.
	if os.Getenv("GOOS") == "windows" {
		t.Skip("no POSIX modes on Windows")
	}
	dir := t.TempDir()
	t.Setenv(HomeEnv, dir)
	if _, err := WriteStarter(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Errorf("mode = %o, want 0600", perm)
	}
}
