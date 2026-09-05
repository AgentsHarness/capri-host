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

	// Settings file first, environment second: env wins so shell and service
	// launches behave exactly as before this file existed.
	path := ConfigPath()
	fc, err := loadFile(path)
	if err != nil {
		c.ConfigError = err
	} else if fc != nil {
		fc.apply(&c)
		c.ConfigSource = path
	}

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
	if v := envOr("FE_TOKEN", os.Getenv("ACCESS_TOKEN")); v != "" {
		c.AccessToken = v
	}
	envBool(&c.OpenBrowser, "CAPRI_OPEN_BROWSER")
	envBool(&c.EnableTray, "CAPRI_TRAY")
	// BIND / HOST_BIND: upstream default is loopback-only; env always wins here.
	c.BindAddr = bindAddr()

	return c
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

// bindAddr reads BIND (alias HOST_BIND), defaulting to loopback.
func bindAddr() string {
	v := strings.TrimSpace(os.Getenv("BIND"))
	if v == "" {
		v = strings.TrimSpace(os.Getenv("HOST_BIND"))
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
