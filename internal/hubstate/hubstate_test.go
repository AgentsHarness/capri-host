package hubstate

import (
	"os"
	"path/filepath"
	"testing"
)

// Usable is the whole basis for "reuse this credential instead of asking for a
// code", so its edge cases are the difference between a smooth reconnect and a
// failed one the user cannot explain.
func TestNormalizeURLMakesBareHostUsable(t *testing.T) {
	tok := &StoredToken{URL: "https://hub.example.com", Token: "tok"}
	got, err := NormalizeURL("hub.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !tok.Usable(got) {
		t.Fatalf("Usable(%q) = false, want a bare host to match the stored https URL", got)
	}
}

func TestUsable(t *testing.T) {
	const hub = "https://hub.example"

	cases := []struct {
		name string
		tok  *StoredToken
		url  string
		want bool
	}{
		{"exact match", &StoredToken{URL: hub, Token: "tok"}, hub, true},
		{"surrounding whitespace", &StoredToken{URL: " " + hub + " ", Token: " tok "}, hub, true},

		// A nil reader means there is no hub.json at all.
		{"no credential", nil, hub, false},
		{"empty file", &StoredToken{}, hub, false},

		// A token bound to another hub is not a credential for this one:
		// reusing it produces an authentication failure the user cannot act on.
		{"bound elsewhere", &StoredToken{URL: "https://other.example", Token: "tok"}, hub, false},
		{"url missing", &StoredToken{Token: "tok"}, hub, false},

		// A credential with no token is a half-written file, not a credential.
		{"token missing", &StoredToken{URL: hub}, hub, false},
		{"token blank", &StoredToken{URL: hub, Token: "   "}, hub, false},

		// No address to match against: the caller has nothing to reuse for.
		{"empty query", &StoredToken{URL: hub, Token: "tok"}, "", false},
	}
	for _, tc := range cases {
		if got := tc.tok.Usable(tc.url); got != tc.want {
			t.Errorf("%s: Usable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestReadToken(t *testing.T) {
	dir := t.TempDir()

	valid := filepath.Join(dir, "valid.json")
	if err := os.WriteFile(valid, []byte(`{"url":"https://h.example","hostId":"pc","token":"tok"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got := ReadToken(valid)
	if got == nil || got.URL != "https://h.example" || got.HostID != "pc" || got.Token != "tok" {
		t.Errorf("ReadToken = %+v, want the stored triple", got)
	}

	// Every failure mode means "needs a code", never "cannot start": hub.json
	// is a cache of something the user can always reproduce by pairing again.
	for _, tc := range []struct{ name, path, body string }{
		{"missing", filepath.Join(dir, "nope.json"), ""},
		{"empty path", "", ""},
		{"truncated", filepath.Join(dir, "bad.json"), `{"url":"https://h.example",`},
		{"not json", filepath.Join(dir, "text.json"), "not json at all"},
	} {
		if tc.body != "" {
			if err := os.WriteFile(tc.path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if tok := ReadToken(tc.path); tok != nil {
			t.Errorf("%s: ReadToken = %+v, want nil", tc.name, tok)
		}
	}
}

func TestURLOrEmptyOnNilToken(t *testing.T) {
	// A machine that has never paired has no hub.json, so every caller holds a
	// nil token. Reading the address must not be what crashes them.
	var tok *StoredToken
	if got := tok.URLOrEmpty(); got != "" {
		t.Errorf("nil URLOrEmpty = %q, want empty", got)
	}
	if got := (&StoredToken{URL: "  https://h.example  "}).URLOrEmpty(); got != "https://h.example" {
		t.Errorf("URLOrEmpty = %q, want it trimmed", got)
	}
}
