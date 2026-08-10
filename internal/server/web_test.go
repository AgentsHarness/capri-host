package server

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// webDistAssetNames lists the files embedded from acp-fe/dist (excluding
// index.html itself) so tests can reference a real asset path.
func webDistAssetNames(t *testing.T) []string {
	t.Helper()
	var names []string
	err := fs.WalkDir(webAssets, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && p != "index.html" {
			names = append(names, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk embedded web assets: %v", err)
	}
	return names
}

func get(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

// GET / serves the embedded acp-fe SPA shell (index.html).
func TestWebServesIndex(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	rec := get(t, s, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `<div id="root">`) {
		t.Fatalf("index.html missing #root: %s", body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
	// index.html 不可长缓存（每次进站拿到最新入口）。
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-cache") {
		t.Fatalf("Cache-Control = %q, want no-cache", cc)
	}
}

// Embedded assets (hashed JS/CSS/svg) are served as-is with a long cache
// lifetime — the hash in the filename makes them immutable.
func TestWebServesAssets(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	names := webDistAssetNames(t)
	if len(names) == 0 {
		t.Fatal("no embedded assets found")
	}
	js := ""
	for _, n := range names {
		if strings.HasSuffix(n, ".js") {
			js = "/" + n
			break
		}
	}
	if js == "" {
		t.Fatal("no .js asset embedded")
	}
	rec := get(t, s, js)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("Content-Type = %q, want javascript", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "immutable") {
		t.Fatalf("Cache-Control = %q, want immutable", cc)
	}
	if len(rec.Body.Bytes()) < 1000 {
		t.Fatalf("asset body suspiciously small: %d bytes", len(rec.Body.Bytes()))
	}
}

// Unknown non-API GET paths fall back to the SPA shell (client-side
// routing), so deep links always render the app.
func TestWebSpaFallback(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	for _, p := range []string{"/some/deep/link", "/session/abc123"} {
		rec := get(t, s, p)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", p, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `<div id="root">`) {
			t.Fatalf("%s: body is not the SPA shell", p)
		}
	}
}

// Unregistered /api/* GETs stay JSON 404s — the API surface must never
// silently serve the SPA shell.
func TestWebUnknownAPIGetsJSON404(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	for _, p := range []string{"/api/does-not-exist", "/api/", "/events/extra"} {
		rec := get(t, s, p)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404 (body=%s)", p, rec.Code, rec.Body.String())
		}
		if m := decodeBody(t, rec); m["ok"] != false {
			t.Fatalf("%s: resp = %s, want ok:false JSON envelope", p, rec.Body.String())
		}
	}
}
