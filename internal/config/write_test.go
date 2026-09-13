package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// useScratchDir points AppDir at a temp directory for one test, so writing a
// config never touches the developer's real ~/.capri-host.
func useScratchDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("CAPRI_HOST_DIR", dir)
	return dir
}

func TestSaveCreatesFileWithHeaderWhenAbsent(t *testing.T) {
	dir := useScratchDir(t)

	if err := Save(Settings{HubURL: String("https://hub.example.com")}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, ConfigFileName))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	body := string(b)
	if !strings.Contains(body, "hub_url = 'https://hub.example.com'") {
		t.Errorf("hub_url not written:\n%s", body)
	}
	// The header is the only documentation a user who never read a README will
	// see, so a freshly created file must carry it.
	if !strings.Contains(body, "# capri-host 设置") {
		t.Errorf("fresh file missing header:\n%s", body)
	}
	if !strings.Contains(body, "grok_bin") {
		t.Errorf("header should mention the other keys:\n%s", body)
	}
}

func TestSavePreservesCommentsAndUnknownKeys(t *testing.T) {
	dir := useScratchDir(t)
	path := filepath.Join(dir, ConfigFileName)

	original := "# 我自己写的注释，别删\n" +
		"port = 9000\n" +
		"grok_bin = 'C:\\tools\\grok.exe'\n" +
		"hub_url = 'https://old.example.com'\n" +
		"some_future_key = 'keep me'\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Save(Settings{HubURL: String("https://new.example.com")}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	body, _ := os.ReadFile(path)
	got := string(body)
	for _, want := range []string{
		"# 我自己写的注释，别删",
		"port = 9000",
		`grok_bin = 'C:\tools\grok.exe'`, // backslashes intact — literal string
		"hub_url = 'https://new.example.com'",
		"some_future_key = 'keep me'",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("lost %q after write:\n%s", want, got)
		}
	}
	if strings.Contains(got, "old.example.com") {
		t.Errorf("old value still present:\n%s", got)
	}
}

func TestSaveAppendsWhenKeyAbsent(t *testing.T) {
	dir := useScratchDir(t)
	path := filepath.Join(dir, ConfigFileName)
	if err := os.WriteFile(path, []byte("port = 8765\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Save(Settings{HubURL: String("https://h")}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, _ := os.ReadFile(path)
	if !strings.Contains(string(got), "port = 8765") || !strings.Contains(string(got), "hub_url = 'https://h'") {
		t.Errorf("append lost something:\n%s", got)
	}
}

func TestSaveAppendsToFileWithNoTrailingNewline(t *testing.T) {
	dir := useScratchDir(t)
	path := filepath.Join(dir, ConfigFileName)
	// No trailing newline: naive concatenation would produce
	// "port = 8765hub_url = ..." and silently corrupt both keys.
	if err := os.WriteFile(path, []byte("port = 8765"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Save(Settings{HubURL: String("https://h")}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, _ := os.ReadFile(path)
	if !strings.Contains(string(got), "port = 8765\n") {
		t.Errorf("keys ran together:\n%q", got)
	}
	// And the result must actually parse back.
	fc, err := loadFile(path)
	if err != nil {
		t.Fatalf("result does not parse: %v", err)
	}
	if fc.Port == nil || *fc.Port != 8765 || fc.HubURL == nil || *fc.HubURL != "https://h" {
		t.Errorf("round trip lost values: %+v", fc)
	}
}

func TestSaveRoundTripsThroughLoad(t *testing.T) {
	useScratchDir(t)
	if err := Save(Settings{
		HubURL:   String("https://hub.example.com"),
		HostID:   String("desk-01"),
		HostName: String("我的台式机"),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	c := Load()
	if c.ConfigError != nil {
		t.Fatalf("written config does not parse: %v", c.ConfigError)
	}
	if c.HubURL != "https://hub.example.com" {
		t.Errorf("HubURL = %q", c.HubURL)
	}
	if c.HostID != "desk-01" {
		t.Errorf("HostID = %q", c.HostID)
	}
	if c.HostName != "我的台式机" {
		t.Errorf("HostName = %q", c.HostName)
	}
}

func TestSaveIsIncremental(t *testing.T) {
	useScratchDir(t)
	if err := Save(Settings{HubURL: String("https://a"), HostID: String("id-1")}); err != nil {
		t.Fatal(err)
	}
	// A nil field must leave the stored value alone — otherwise saving one key
	// would blank the others.
	if err := Save(Settings{HubURL: String("https://b")}); err != nil {
		t.Fatal(err)
	}
	c := Load()
	if c.HubURL != "https://b" {
		t.Errorf("HubURL = %q, want the updated one", c.HubURL)
	}
	if c.HostID != "id-1" {
		t.Errorf("HostID = %q, want it untouched", c.HostID)
	}
}

func TestSaveRefusesValuesItCannotQuote(t *testing.T) {
	useScratchDir(t)
	// A TOML literal string has no escape for its own delimiter, so writing
	// this would produce a file that no longer parses. Refusing beats emitting
	// a config that breaks on the next start.
	for _, bad := range []string{"https://a'b", "line1\nline2", "a\rb"} {
		if err := Save(Settings{HubURL: String(bad)}); err == nil {
			t.Errorf("Save accepted unquotable value %q", bad)
		}
	}
}

func TestSaveRefusesDuplicateKey(t *testing.T) {
	dir := useScratchDir(t)
	path := filepath.Join(dir, ConfigFileName)
	// TOML lets the later assignment win, so editing only the first would
	// appear to do nothing at all. Say so instead.
	dup := "hub_url = 'https://one'\nport = 1\nhub_url = 'https://two'\n"
	if err := os.WriteFile(path, []byte(dup), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Save(Settings{HubURL: String("https://three")})
	if err == nil {
		t.Fatal("Save accepted a file with a duplicated key")
	}
	if !strings.Contains(err.Error(), "hub_url") {
		t.Errorf("error should name the key, got: %v", err)
	}
	// And the file must be untouched.
	got, _ := os.ReadFile(path)
	if string(got) != dup {
		t.Errorf("file was modified despite the error:\n%s", got)
	}
}

func TestSaveDoesNotDisturbCommentedOutKey(t *testing.T) {
	dir := useScratchDir(t)
	path := filepath.Join(dir, ConfigFileName)
	if err := os.WriteFile(path, []byte("# hub_url = 'https://commented'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Save(Settings{HubURL: String("https://real")}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, _ := os.ReadFile(path)
	body := string(got)
	if !strings.Contains(body, "# hub_url = 'https://commented'") {
		t.Errorf("commented example was edited:\n%s", body)
	}
	if !strings.Contains(body, "hub_url = 'https://real'") {
		t.Errorf("real value not appended:\n%s", body)
	}
	if c := Load(); c.HubURL != "https://real" {
		t.Errorf("Load read %q", c.HubURL)
	}
}

func TestSaveLeavesNoTempFilesBehind(t *testing.T) {
	dir := useScratchDir(t)
	if err := Save(Settings{HubURL: String("https://h")}); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}
}

func TestMachineHostIdentityIsUsableAndStable(t *testing.T) {
	id, name, ok := MachineHostIdentity()
	if !ok {
		t.Skip("this machine reports no hostname")
	}
	if id == "" || name == "" {
		t.Fatalf("ok but empty: id=%q name=%q", id, name)
	}
	// The id goes into a hub URL path and a JSON key, so it must not need
	// escaping, and it must not be the shared default that causes collisions.
	if id == DefaultHostID {
		t.Errorf("derived id equals the colliding default %q", DefaultHostID)
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			t.Errorf("id %q contains unsafe rune %q", id, r)
		}
	}
	if len(id) > 48 {
		t.Errorf("id too long: %d", len(id))
	}
	id2, name2, _ := MachineHostIdentity()
	if id != id2 || name != name2 {
		t.Errorf("not stable across calls: (%q,%q) then (%q,%q)", id, name, id2, name2)
	}
}
