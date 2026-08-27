package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// This file lets the host write settings back. It exists because the tray is
// now the only UI a new user has: they double-click an exe, and if pairing a
// hub could not persist itself they would have to re-enter the address and a
// fresh code after every restart — or hand-author a TOML file they were never
// told about.
//
// Only the three keys the tray can change are writable. A general
// string-keyed setter would let a caller invent keys that Load never reads,
// which fails silently and looks like a bug in the host.

// Settings is a partial update. A nil field is left as the file has it, which
// is what makes this safe to call for one key without knowing the rest.
type Settings struct {
	HubURL   *string
	HostID   *string
	HostName *string
}

// String is a helper for building a Settings literal.
func String(s string) *string { return &s }

// configHeader is written only when creating the file from scratch.
const configHeader = `# capri-host 设置。改完后重启 capri-host 即可生效。
# 环境变量优先于本文件；托盘里「配对 hub」会自动写回这里。
#
# port = 8765
# grok_bin = 'C:\Users\<你>\.grok\bin\grok.exe'
# fe_token = ''            # 网页端访问令牌，留空则不校验
# open_browser = true      # 启动时打开本机页面
# tray = true              # 系统托盘

`

// Save applies s to config.toml, creating the file when absent.
//
// Existing lines are edited in place and unknown keys are left alone, so
// comments and any hand-written settings survive a write. The file may already
// hold fe_token and host_token, so it is written atomically at 0600: a
// truncated config would strand the host with no credentials and no
// explanation.
func Save(s Settings) error {
	path := ConfigPath()

	var body string
	switch b, err := os.ReadFile(path); {
	case err == nil:
		body = string(b)
	case os.IsNotExist(err):
		body = configHeader
	default:
		return fmt.Errorf("读取 %s: %w", path, err)
	}

	var err error
	if s.HubURL != nil {
		if body, err = upsertKey(body, "hub_url", *s.HubURL); err != nil {
			return err
		}
	}
	if s.HostID != nil {
		if body, err = upsertKey(body, "host_id", *s.HostID); err != nil {
			return err
		}
	}
	if s.HostName != nil {
		if body, err = upsertKey(body, "host_name", *s.HostName); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return writeFileAtomic(path, []byte(body), 0o600)
}

// upsertKey replaces the value of an existing assignment or appends a new one.
func upsertKey(body, key, value string) (string, error) {
	// TOML literal strings take no escapes, which is what keeps a Windows
	// path's backslashes intact. The trade-off is that they cannot contain a
	// single quote at all, so a value carrying one is refused rather than
	// written as something that will not parse back.
	if strings.ContainsAny(value, "'\r\n") {
		return "", fmt.Errorf("%s 的值不能包含单引号或换行: %q", key, value)
	}
	line := key + " = '" + value + "'"

	re := regexp.MustCompile(`(?m)^[ \t]*` + regexp.QuoteMeta(key) + `[ \t]*=.*$`)
	if re.MatchString(body) {
		// Replace only the first assignment. A later duplicate would win in
		// TOML, so leaving one behind would make the write appear to do
		// nothing — refuse instead of writing a file whose meaning is unclear.
		if n := len(re.FindAllString(body, -1)); n > 1 {
			return "", fmt.Errorf("%s 在 %s 里出现了 %d 次，请先手动删掉多余的", key, ConfigPath(), n)
		}
		return re.ReplaceAllLiteralString(body, line), nil
	}

	if body != "" && !strings.HasSuffix(body, "\n") {
		body += "\n"
	}
	return body + line + "\n", nil
}

// writeFileAtomic writes via a temporary file in the same directory and
// renames, so a crash or a full disk cannot leave a half-written config where
// a complete one used to be.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op once the rename succeeded

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	// Flush before the rename: on a crash the directory entry could otherwise
	// point at a file whose contents never reached the disk.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, perm); err != nil {
		return err
	}
	// os.Rename on Windows uses MoveFileEx with MOVEFILE_REPLACE_EXISTING,
	// so it atomically overwrites an existing file. The old remove-then-
	// rename left a window where the file did not exist at all.
	return os.Rename(tmp, path)
}

// nonAlnum collapses anything that is not a letter or digit, so a machine name
// with spaces or CJK still yields a usable host id.
var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// MachineHostIdentity derives a per-machine host id and display name from the
// OS hostname.
//
// This matters because the hub keys its host table by host id: two people who
// both run an unconfigured host would arrive as the compiled default and
// silently displace each other on the hub. ok is false only when the OS gives
// us no hostname at all, in which case the caller should keep its own default.
func MachineHostIdentity() (id, name string, ok bool) {
	h, err := os.Hostname()
	if err != nil {
		return "", "", false
	}
	name = strings.TrimSpace(h)
	if name == "" {
		return "", "", false
	}
	// Strip a DNS suffix: "pc.lan" and "pc" are the same machine.
	if i := strings.IndexByte(name, '.'); i > 0 {
		name = name[:i]
	}

	id = strings.Trim(nonAlnum.ReplaceAllString(strings.ToLower(name), "-"), "-")
	if id == "" {
		// A purely non-ASCII hostname (common on Chinese Windows) leaves
		// nothing to slug. Hash it instead of picking at random, so the id is
		// stable across restarts — a host whose id changed every launch would
		// accumulate dead entries in the hub's host table.
		sum := sha256.Sum256([]byte(name))
		id = "host-" + hex.EncodeToString(sum[:4])
	}
	if len(id) > 48 {
		id = strings.Trim(id[:48], "-")
	}
	return id, name, true
}
