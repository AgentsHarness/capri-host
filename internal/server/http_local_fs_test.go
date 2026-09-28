package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// jsonStr 把路径编成 JSON 字符串字面量（Windows 反斜杠在 JSON 里要转义，
// 手写 `{"path":"D:\aiwork"}` 会变成非法 JSON）。
func jsonStr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func mkTestDir(t *testing.T, parent, name string) string {
	t.Helper()
	p := filepath.Join(parent, name)
	if err := os.Mkdir(p, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", p, err)
	}
	return p
}

func dirNames(t *testing.T, m map[string]any) []string {
	t.Helper()
	raw, ok := m["dirs"].([]any)
	if !ok {
		t.Fatalf("dirs = %v, want an array", m["dirs"])
	}
	out := make([]string, 0, len(raw))
	for _, d := range raw {
		row, _ := d.(map[string]any)
		out = append(out, row["name"].(string))
	}
	return out
}

// ── POST /api/local/dirs ────────────────────────────────────────────

func TestLocalDirsEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	root := t.TempDir()
	mkTestDir(t, root, "zeta")
	mkTestDir(t, root, "alpha")
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 指向目录的符号链接算目录，断链不算（macOS 上工作区常在链接后面）。
	if err := os.Symlink(filepath.Join(root, "alpha"), filepath.Join(root, "link-dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "gone"), filepath.Join(root, "broken")); err != nil {
		t.Fatal(err)
	}

	rec := postJSON(t, s, "/api/local/dirs", `{"path":`+jsonStr(root)+`}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != true {
		t.Fatalf("resp = %s, want ok:true", rec.Body.String())
	}
	if m["path"] != root {
		t.Errorf("path = %v, want %q", m["path"], root)
	}
	if home, _ := m["home"].(string); home == "" {
		t.Errorf("home = %v, want the host home dir", m["home"])
	}
	if got, want := dirNames(t, m), []string{"alpha", "link-dir", "zeta"}; !equalStrings(got, want) {
		t.Errorf("dirs = %v, want %v (files and broken symlinks excluded, sorted)", got, want)
	}
	// 每项都带绝对路径，前端不再自己拼。
	first := m["dirs"].([]any)[0].(map[string]any)
	if first["path"] != filepath.Join(root, "alpha") {
		t.Errorf("entry path = %v, want %q", first["path"], filepath.Join(root, "alpha"))
	}
}

func TestLocalDirsErrors(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	root := t.TempDir()
	file := filepath.Join(root, "file.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 不存在的路径 → 400 + 人话 + 回显归一化后的 path。
	missing := filepath.Join(root, "nope")
	rec := postJSON(t, s, "/api/local/dirs", `{"path":`+jsonStr(missing)+`}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing status = %d, body=%s", rec.Code, rec.Body.String())
	}
	m := decodeBody(t, rec)
	if m["ok"] != false {
		t.Fatalf("resp = %s, want ok:false", rec.Body.String())
	}
	if msg, _ := m["error"].(string); !strings.Contains(msg, "目录不存在") || !strings.Contains(msg, missing) {
		t.Errorf("error = %q, want 目录不存在 + the path", msg)
	}
	if m["path"] != missing {
		t.Errorf("path = %v, want %q echoed back", m["path"], missing)
	}

	// 文件不是目录。
	rec = postJSON(t, s, "/api/local/dirs", `{"path":`+jsonStr(file)+`}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("file status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if msg, _ := decodeBody(t, rec)["error"].(string); !strings.Contains(msg, "不是目录") {
		t.Errorf("error = %q, want 不是目录", msg)
	}

	// 空路径 / "~" → 主目录。
	home, _ := os.UserHomeDir()
	for _, body := range []string{`{}`, `{"path":""}`, `{"path":"~"}`} {
		rec = postJSON(t, s, "/api/local/dirs", body)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body=%s", body, rec.Code, rec.Body.String())
		}
		if got := decodeBody(t, rec)["path"]; got != home {
			t.Errorf("%s path = %v, want home %q", body, got, home)
		}
	}

	// 坏 JSON → 400。
	if rec := postJSON(t, s, "/api/local/dirs", `{`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad-json status = %d, want 400", rec.Code)
	}
}

// ── POST /api/local/mkdir ───────────────────────────────────────────

func TestLocalMkdirEndpoint(t *testing.T) {
	s, _ := newFakeAgentServer(t)
	root := t.TempDir()

	rec := postJSON(t, s, "/api/local/mkdir", `{"parent":`+jsonStr(root)+`,"name":"newproj"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	want := filepath.Join(root, "newproj")
	if m := decodeBody(t, rec); m["ok"] != true || m["path"] != want {
		t.Fatalf("resp = %s, want ok:true path %q", rec.Body.String(), want)
	}
	if st, err := os.Stat(want); err != nil || !st.IsDir() {
		t.Fatalf("stat %s: %v (want a directory)", want, err)
	}

	// 重名 → 409（不是 400：目录已经在那儿了，只是冲突）。
	rec = postJSON(t, s, "/api/local/mkdir", `{"parent":`+jsonStr(root)+`,"name":"newproj"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("dup status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if msg, _ := decodeBody(t, rec)["error"].(string); !strings.Contains(msg, "已存在") {
		t.Errorf("error = %q, want 已存在", msg)
	}

	// 单层语义 + 空名：一律 400 + 说得出原因，且不落盘。
	for _, tc := range []struct{ name, want string }{
		{"", "不能为空"},
		{"   ", "不能为空"},
		{".", "名称无效"},
		{"..", "名称无效"},
		{"a/b", "分隔符"},
		{`a\b`, "分隔符"},
	} {
		body := `{"parent":` + jsonStr(root) + `,"name":` + jsonStr(tc.name) + `}`
		rec := postJSON(t, s, "/api/local/mkdir", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("name %q status = %d, body=%s; want 400", tc.name, rec.Code, rec.Body.String())
			continue
		}
		if msg, _ := decodeBody(t, rec)["error"].(string); !strings.Contains(msg, tc.want) {
			t.Errorf("name %q error = %q, want it to mention %q", tc.name, msg, tc.want)
		}
	}
	if entries, err := os.ReadDir(root); err != nil || len(entries) != 1 {
		t.Errorf("root entries = %v (err %v), want only newproj — invalid names must not create anything", entries, err)
	}

	// parent 不存在 / 不是目录。
	missing := filepath.Join(root, "no-such")
	rec = postJSON(t, s, "/api/local/mkdir", `{"parent":`+jsonStr(missing)+`,"name":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing parent status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if msg, _ := decodeBody(t, rec)["error"].(string); !strings.Contains(msg, "目录不存在") {
		t.Errorf("error = %q, want 目录不存在", msg)
	}
	fileRec := postJSON(t, s, "/api/local/mkdir", `{"parent":`+jsonStr(filepath.Join(root, "newproj"))+`,"name":""}`)
	if fileRec.Code != http.StatusBadRequest {
		t.Errorf("empty name status = %d, want 400", fileRec.Code)
	}

	if rec := postJSON(t, s, "/api/local/mkdir", `{`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad-json status = %d, want 400", rec.Code)
	}
}

// ── normalizeLocalPath ──────────────────────────────────────────────
//
// Windows 归一化是纯字符串处理，所以在 macOS/Linux 上也能测——这正是
// 「/d/aiwork 无法获取」那条需求的核心逻辑（Windows 宿主上 Go 进程拿到
// /d/aiwork 时 filepath 不认识它）。

func TestNormalizeLocalPathPosix(t *testing.T) {
	home := "/Users/ben"
	cases := []struct{ in, want string }{
		{"", home},
		{"~", home},
		{"~/proj", home + "/proj"},
		{"~/proj/../other", home + "/other"},
		{"/etc/../tmp", "/tmp"},
		{"/", "/"},
		{"rel", home + "/rel"},
		{"  /tmp/x  ", "/tmp/x"},
	}
	for _, tc := range cases {
		if got := normalizeLocalPath(tc.in, home, false); got != tc.want {
			t.Errorf("normalizeLocalPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// 没有主目录："" 与 "~" 无法解析 → 空串（handler 报 400）。
	for _, in := range []string{"", "~", "~/x"} {
		if got := normalizeLocalPath(in, "", false); got != "" {
			t.Errorf("normalizeLocalPath(%q, no home) = %q, want empty", in, got)
		}
	}
}

func TestNormalizeLocalPathWindows(t *testing.T) {
	home := `C:\Users\ben`
	cases := []struct{ in, want string }{
		{"", home},
		{"~", home},
		{`~\proj`, `C:\Users\ben\proj`},
		{"~/proj", `C:\Users\ben\proj`},
		{"rel", `C:\Users\ben\rel`},
		// MSYS/Git-Bash 盘符写法（用户报的就是这个）。
		{"/d/aiwork", `D:\aiwork`},
		{"/D/aiwork", `D:\aiwork`},
		{"/d", `D:\`},
		{"/d/aiwork/../other", `D:\other`},
		// 原生写法：正反斜杠、裸盘符、盘根。
		{`d:\aiwork`, `D:\aiwork`},
		{"d:/aiwork", `D:\aiwork`},
		{"d:", `D:\`},
		{`D:\`, `D:\`},
		// 根相对（不带盘符）→ 补主目录所在盘符；首段不是单个字母时不当作
		// MSYS 盘符（/Users 不能变成 U:\sers）。
		{"/", `C:\`},
		{"/Users/ben", `C:\Users\ben`},
		{`\aiwork`, `C:\aiwork`},
		// UNC 保留。
		{`\\server\share\dir`, `\\server\share\dir`},
		{"//server/share/dir", `\\server\share\dir`},
	}
	for _, tc := range cases {
		if got := normalizeLocalPath(tc.in, home, true); got != tc.want {
			t.Errorf("normalizeLocalPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestInvalidLocalDirName(t *testing.T) {
	for _, name := range []string{"", " ", ".", "..", "a/b", `a\b`} {
		if msg := invalidLocalDirName(strings.TrimSpace(name), false); msg == "" {
			t.Errorf("invalidLocalDirName(%q) = \"\", want a reason", name)
		}
	}
	// POSIX 上这些是合法目录名（含 Windows 非法字符、以点结尾）。
	for _, name := range []string{"proj", "带中文的目录", `a:b`, "con", "trailing."} {
		if msg := invalidLocalDirName(name, false); msg != "" {
			t.Errorf("invalidLocalDirName(%q, posix) = %q, want usable", name, msg)
		}
	}
	for _, name := range []string{`a:b`, `a<b`, `a|b`, "trailing.", "trailing ", "CON", "con.txt", "lpt1", "com9"} {
		if msg := invalidLocalDirName(name, true); msg == "" {
			t.Errorf("invalidLocalDirName(%q, windows) = \"\", want a reason", name)
		}
	}
	for _, name := range []string{"proj", "带中文的目录", "com0", "com10", "console"} {
		if msg := invalidLocalDirName(name, true); msg != "" {
			t.Errorf("invalidLocalDirName(%q, windows) = %q, want usable", name, msg)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
