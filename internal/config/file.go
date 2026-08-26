package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
)

// This file gives the host a settings file. It exists because "one exe you
// double-click" removes the thing that used to supply configuration: a
// PowerShell launcher that set PORT / GROK_BIN / FE_TOKEN / HUB_URL as
// environment variables before exec'ing the binary. Nothing sources that when
// the exe is started from Explorer, so without a file the host would come up in
// local mode with no token — the settings would not be reported missing, they
// would silently not exist.
//
// Environment variables still win, so shell and service launches are unchanged.

// ConfigFileName is the settings file inside AppDir.
const ConfigFileName = "config.toml"

// AppDir is where the host keeps its settings, logs and hub token. It matches
// the directory the hub client already defaults its state file to, so all
// per-user state lives in one place.
func AppDir() string {
	if d := strings.TrimSpace(os.Getenv("CAPRI_HOST_DIR")); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".capri-host")
}

// ConfigPath is the full path to the settings file.
func ConfigPath() string { return filepath.Join(AppDir(), ConfigFileName) }

// LogDir is where the rotating log file lives.
func LogDir() string { return filepath.Join(AppDir(), "logs") }

// fileConfig mirrors config.toml. Every field is a pointer so an absent key is
// distinguishable from one explicitly set to a zero value — otherwise
// `open_browser = false` would be indistinguishable from not mentioning it and
// could never turn the default off.
type fileConfig struct {
	Port        *int    `toml:"port"`
	GrokBin     *string `toml:"grok_bin"`
	HostID      *string `toml:"host_id"`
	HostName    *string `toml:"host_name"`
	FEToken     *string `toml:"fe_token"`
	HubURL      *string `toml:"hub_url"`
	HubPairCode *string `toml:"hub_pair_code"`
	HostToken   *string `toml:"host_token"`
	HubQUICPin  *string `toml:"hub_quic_pin"`
	OpenBrowser *bool   `toml:"open_browser"`
	Tray        *bool   `toml:"tray"`
}

// loadFile reads config.toml if present. A missing file is not an error; a
// malformed one is, because silently ignoring a typo'd settings file is how a
// host ends up unpaired with no explanation.
func loadFile(path string) (*fileConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取 %s: %w", path, err)
	}
	var fc fileConfig
	if err := toml.Unmarshal(b, &fc); err != nil {
		return nil, fmt.Errorf("解析 %s: %w", path, err)
	}
	return &fc, nil
}

// apply overlays file settings onto c. Called before the environment is read,
// so env keeps priority.
func (fc *fileConfig) apply(c *Config) {
	if fc == nil {
		return
	}
	setInt(&c.Port, fc.Port)
	setStr(&c.GrokBin, fc.GrokBin)
	setStr(&c.HostID, fc.HostID)
	setStr(&c.HostName, fc.HostName)
	setStr(&c.AccessToken, fc.FEToken)
	setStr(&c.HubURL, fc.HubURL)
	setStr(&c.HubPairCode, fc.HubPairCode)
	setStr(&c.HostToken, fc.HostToken)
	setStr(&c.HubQUICPin, fc.HubQUICPin)
	setBool(&c.OpenBrowser, fc.OpenBrowser)
	setBool(&c.EnableTray, fc.Tray)
}

func setInt(dst *int, src *int) {
	if src != nil && *src > 0 {
		*dst = *src
	}
}

func setStr(dst *string, src *string) {
	if src != nil {
		if v := strings.TrimSpace(*src); v != "" {
			*dst = v
		}
	}
}

func setBool(dst *bool, src *bool) {
	if src != nil {
		*dst = *src
	}
}

// psEnvLine matches the `$env:NAME = "value"` assignments the old launcher
// used. Commented-out lines are skipped by the leading anchor.
var psEnvLine = regexp.MustCompile(`(?mi)^\s*\$env:([A-Z_][A-Z0-9_]*)\s*=\s*"([^"]*)"`)

// MigrateLegacyEnv converts a leftover env.ps1 into config.toml when no
// config.toml exists yet, and reports the path written.
//
// Without this, upgrading to the single exe would look like a regression: the
// old launcher held the only copy of HUB_URL and FE_TOKEN, so double-clicking
// the new binary would come up in local mode and the user would reasonably
// conclude the new build was broken. Values are copied verbatim; nothing is
// deleted, so the old launcher keeps working if they want to go back.
func MigrateLegacyEnv() (written string, err error) {
	cfgPath := ConfigPath()
	if _, err := os.Stat(cfgPath); err == nil {
		return "", nil // already configured; never overwrite
	}
	legacy := filepath.Join(AppDir(), "env.ps1")
	b, err := os.ReadFile(legacy)
	if err != nil {
		return "", nil // nothing to migrate
	}

	vals := map[string]string{}
	for _, m := range psEnvLine.FindAllStringSubmatch(string(b), -1) {
		vals[strings.ToUpper(m[1])] = m[2]
	}
	if len(vals) == 0 {
		return "", nil
	}

	var sb strings.Builder
	sb.WriteString("# capri-host 设置。改完后重启 capri-host 即可。\n")
	sb.WriteString("# 由旧的 env.ps1 自动迁移生成；环境变量仍然优先于本文件。\n\n")
	writeKV := func(key, envName string, quote bool) {
		v, ok := vals[envName]
		if !ok || strings.TrimSpace(v) == "" {
			return
		}
		if quote {
			// TOML basic strings treat \ as an escape, which matters for
			// Windows paths like C:\Users\… — a literal string does not.
			sb.WriteString(fmt.Sprintf("%s = '%s'\n", key, strings.ReplaceAll(v, "'", "")))
			return
		}
		sb.WriteString(fmt.Sprintf("%s = %s\n", key, v))
	}
	writeKV("port", "PORT", false)
	writeKV("grok_bin", "GROK_BIN", true)
	writeKV("host_id", "HOST_ID", true)
	writeKV("host_name", "HOST_NAME", true)
	writeKV("fe_token", "FE_TOKEN", true)
	writeKV("hub_url", "HUB_URL", true)
	writeKV("hub_pair_code", "HUB_PAIR_CODE", true)
	writeKV("host_token", "HOST_TOKEN", true)
	writeKV("hub_quic_pin", "HUB_QUIC_PIN", true)

	if err := os.MkdirAll(AppDir(), 0o755); err != nil {
		return "", err
	}
	// 0600: this file carries FE_TOKEN and possibly HOST_TOKEN, the same
	// secrets the hub state file is written with.
	if err := os.WriteFile(cfgPath, []byte(sb.String()), 0o600); err != nil {
		return "", err
	}
	return cfgPath, nil
}

// EnsureGrokOnPath prepends the grok binary's directory to PATH when GrokBin is
// an absolute path, mirroring what the old launcher did. Some grok builds
// resolve siblings (helpers, DLLs) relative to PATH rather than to their own
// image, so skipping this makes the agent fail to start with a confusing
// "not found" for something that is plainly on disk.
func EnsureGrokOnPath(grokBin string) {
	if grokBin == "" || !filepath.IsAbs(grokBin) {
		return
	}
	dir := filepath.Dir(grokBin)
	if dir == "" || dir == "." {
		return
	}
	cur := os.Getenv("PATH")
	for _, p := range filepath.SplitList(cur) {
		if strings.EqualFold(strings.TrimRight(p, string(os.PathSeparator)),
			strings.TrimRight(dir, string(os.PathSeparator))) {
			return
		}
	}
	_ = os.Setenv("PATH", dir+string(os.PathListSeparator)+cur)
}
