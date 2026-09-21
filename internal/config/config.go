package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// DefaultBindAddr 是默认监听地址：capri-host 的 /api/* 能驱动 agent 进程，
// 默认只对本机开放。要局域网访问（例如手机开内嵌前端）显式设 BIND=0.0.0.0
// —— 那必须同时设 FE_TOKEN，见 CheckBindPolicy。
const DefaultBindAddr = "127.0.0.1"

type Config struct {
	Port int
	// BindAddr 是 HTTP 监听地址（BIND / HOST_BIND）。空 = DefaultBindAddr。
	BindAddr string
	GrokBin  string
	// Hub relay mode: set HUB_URL to pair with capri-hub and serve
	// requests through it (hub is the browser-facing endpoint).
	HubURL      string
	HubPairCode string
	HostToken   string
	HostID      string
	HostName    string
	// HUB_QUIC_PIN: pin the hub's QUIC certificate by SPKI sha256
	// fingerprint (hex or base64) instead of the system CA path — the
	// self-signed-hub replacement for disabling verification. See
	// docs/DEPLOY.md for the openssl one-liner that generates it.
	HubQUICPin string
	// Inbound access token for this host's own HTTP API (/api/*, /events).
	// Set FE_TOKEN (or ACCESS_TOKEN) to require it; empty = open (local
	// trusted default, matching the pre-token behavior). Same secret
	// semantics as the hub's FE_TOKEN — deploy the same value so the
	// browser gate and the host port share one credential.
	AccessToken string
	// OpenBrowser opens the local web UI once the server is listening.
	// Default true — it replaces the launcher script's final step, which is
	// the only reason a double-clicked exe shows anything at all.
	OpenBrowser bool
	// EnableTray runs the system tray (Windows only). Default true.
	EnableTray bool
	// ConfigSource is the settings file that was read, empty when none was
	// found. Recorded for the startup log so a misplaced config is visible.
	ConfigSource string
	// ConfigError is a malformed settings file, surfaced by the caller
	// rather than swallowed here — Load has no way to report it otherwise
	// and a silently ignored config is indistinguishable from a broken host.
	ConfigError error
	// ResidentCap is passed to the grok bridge idle-unload supervisor.
	// 0 = use the bridge default (4). Negative = disable (RESIDENT_CAP=0).
	ResidentCap int
	// UsageLedger picks the usage ledger mode read from USAGE_LEDGER:
	// "" / "1" / "on" = default (enabled), "0" / "off" = disabled. The
	// ledger copies per-turn usage into ~/.capri-host/usage-ledger.jsonl
	// before the agent's 30-day session cleanup can delete the source
	// updates.jsonl, so /usage history outlives that cleanup.
	UsageLedger string
	// Proxy is the HTTP(S) proxy exported to this process and to the grok
	// agent child as HTTPS_PROXY/HTTP_PROXY/ALL_PROXY. Empty before
	// WithSystemProxy means "use macOS system proxy if enabled".
	Proxy string
	// NoProxy is NO_PROXY: the comma-separated hosts that bypass Proxy.
	NoProxy string
	// proxyFromSystem 表示 Proxy 来自 macOS 系统网络设置，只给日志用。
	proxyFromSystem bool
}

// DefaultHostID and DefaultHostName are the compiled-in identity used when
// nothing else supplies one. Exported so a caller can tell "the user never
// chose an identity" from "the user chose this one" — the hub keys its host
// table by id, so leaving every unconfigured host on the same default makes
// two of them displace each other on the same hub.
const (
	DefaultHostID   = "local"
	DefaultHostName = "Local Host"
)

func Load() Config {
	c := Config{
		Port:        8765,
		GrokBin:     "grok",
		HostID:      DefaultHostID,
		HostName:    DefaultHostName,
		OpenBrowser: true,
		EnableTray:  true,
	}

	// Layer 1: config.toml (Windows tray / single-exe path).
	path := ConfigPath()
	fc, err := loadFile(path)
	if err != nil {
		c.ConfigError = err
	} else if fc != nil {
		fc.apply(&c)
		c.ConfigSource = path
	}

	// Layer 2: config.json (macOS Capri.app / upstream). Fills empty fields
	// only so a hand-written toml still wins over the JSON defaults.
	jf, jerr := LoadFile()
	if jerr != nil && c.ConfigError == nil {
		c.ConfigError = jerr
	}
	applyJSONFile(&c, jf)

	// Layer 3: environment (highest priority).
	if v := os.Getenv("PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Port = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("GROK_BIN")); v != "" {
		c.GrokBin = v
	}
	envSet(&c.HubURL, "HUB_URL")
	envSet(&c.HubPairCode, "HUB_PAIR_CODE")
	envSet(&c.HostToken, "HOST_TOKEN")
	envSet(&c.HostID, "HOST_ID")
	envSet(&c.HostName, "HOST_NAME")
	envSet(&c.HubQUICPin, "HUB_QUIC_PIN")
	if v := firstNonEmpty(os.Getenv("FE_TOKEN"), os.Getenv("ACCESS_TOKEN")); v != "" {
		c.AccessToken = v
	}
	envBool(&c.OpenBrowser, "CAPRI_OPEN_BROWSER")
	envBool(&c.EnableTray, "CAPRI_TRAY")
	// BIND / HOST_BIND: env wins; else config.json bind; else loopback.
	c.BindAddr = bindAddr(jf.Bind)
	c.ResidentCap = envResidentCap()
	c.UsageLedger = strings.TrimSpace(os.Getenv("USAGE_LEDGER"))
	c.GrokBin = resolveGrokBin(c.GrokBin)
	// PROXY / NO_PROXY: env wins over config.json (filled earlier when empty).
	c.Proxy = proxyURL(os.Getenv("PROXY"), c.Proxy)
	if v := strings.TrimSpace(os.Getenv("NO_PROXY")); v != "" {
		c.NoProxy = v
	}

	return c
}

// applyJSONFile overlays upstream config.json onto c for fields still at
// compiled defaults / empty, so Capri.app settings are visible to the host.
func applyJSONFile(c *Config, f File) {
	if f.Port > 0 && c.Port == 8765 {
		c.Port = f.Port
	}
	if v := strings.TrimSpace(f.GrokBin); v != "" && (c.GrokBin == "" || c.GrokBin == "grok") {
		c.GrokBin = v
	}
	if v := strings.TrimSpace(f.HostID); v != "" && c.HostID == DefaultHostID {
		c.HostID = v
	}
	if v := strings.TrimSpace(f.HostName); v != "" && c.HostName == DefaultHostName {
		c.HostName = v
	}
	if v := strings.TrimSpace(f.HubURL); v != "" && c.HubURL == "" {
		c.HubURL = v
	}
	if v := strings.TrimSpace(f.HubPairCode); v != "" && c.HubPairCode == "" {
		c.HubPairCode = v
	}
	if v := strings.TrimSpace(f.FEToken); v != "" && c.AccessToken == "" {
		c.AccessToken = v
	}
	if v := strings.TrimSpace(f.HubQUICPin); v != "" && c.HubQUICPin == "" {
		c.HubQUICPin = v
	}
	if c.Proxy == "" {
		c.Proxy = proxyURL("", f.Proxy)
	}
	if v := strings.TrimSpace(f.NoProxy); v != "" && c.NoProxy == "" {
		c.NoProxy = v
	}
	if c.ConfigSource == "" {
		if _, err := os.Stat(Path()); err == nil {
			c.ConfigSource = Path()
		}
	}
}

// proxyURL 归一化代理地址：文件里可以只写 host:port，这里补上 http://
// 前缀，让 net/http 与 grok 都能直接当 URL 用。
func proxyURL(envVal, fileVal string) string {
	v := strings.TrimSpace(envVal)
	if v == "" {
		v = strings.TrimSpace(fileVal)
	}
	if v == "" {
		return ""
	}
	if !strings.Contains(v, "://") {
		v = "http://" + v
	}
	return v
}

// UsageLedgerDisabled reports whether USAGE_LEDGER explicitly turns the
// ledger off ("0" / "off" / "false" / "no"). Unset means enabled: the
// ledger is what keeps /usage history past the agent's 30-day cleanup.
func (c Config) UsageLedgerDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(c.UsageLedger)) {
	case "0", "off", "false", "no", "disable", "disabled":
		return true
	}
	return false
}

// envResidentCap reads RESIDENT_CAP. Unset → 0 (bridge default of 4).
// 0 or a non-positive / unparsable value → -1 (disable idle-unload).
func envResidentCap() int {
	v := strings.TrimSpace(os.Getenv("RESIDENT_CAP"))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return -1
	}
	return n
}

// envSet overwrites dst when the named variable is set and non-empty.
func envSet(dst *string, key string) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		*dst = v
	}
}

// envBool accepts 1/0, true/false, yes/no. Anything else leaves dst alone.
func envBool(dst *bool, key string) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		*dst = true
	case "0", "false", "no", "off":
		*dst = false
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func strOr(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// bindAddr reads BIND (alias HOST_BIND), then config.json, then loopback.
func bindAddr(fileBind string) string {
	v := strings.TrimSpace(os.Getenv("BIND"))
	if v == "" {
		v = strings.TrimSpace(os.Getenv("HOST_BIND"))
	}
	if v == "" {
		v = strings.TrimSpace(fileBind)
	}
	if v == "" {
		return DefaultBindAddr
	}
	return strings.Trim(v, "[]")
}

// BindIsLoopback reports whether this bind address only accepts connections
// from the same machine. Empty means the Load() default (loopback).
func (c Config) BindIsLoopback() bool {
	switch c.BindAddr {
	case "", DefaultBindAddr, "localhost":
		return true
	}
	ip := net.ParseIP(c.BindAddr)
	return ip != nil && ip.IsLoopback()
}

// CheckBindPolicy refuses a token-free host API exposed beyond loopback:
// withAuth is open by design when FE_TOKEN is unset ("local trusted"), which
// is only safe while the socket is loopback-only. Non-loopback therefore
// requires a token (mirrors the hub's REQUIRE_FE_TOKEN fail-fast).
func CheckBindPolicy(c Config) error {
	if c.BindIsLoopback() || strings.TrimSpace(c.AccessToken) != "" {
		return nil
	}
	return fmt.Errorf(
		"listening on non-loopback %s without FE_TOKEN exposes the agent API to the local network; set FE_TOKEN, or use BIND=%s for same-machine only",
		c.BindAddr, DefaultBindAddr)
}
