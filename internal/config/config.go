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
}

func Load() Config {
	port := 8765
	if v := os.Getenv("PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			port = n
		}
	}
	bin := os.Getenv("GROK_BIN")
	if bin == "" {
		bin = "grok"
	}
	return Config{
		Port:        port,
		BindAddr:    bindAddr(),
		GrokBin:     bin,
		HubURL:      os.Getenv("HUB_URL"),
		HubPairCode: os.Getenv("HUB_PAIR_CODE"),
		HostToken:   os.Getenv("HOST_TOKEN"),
		HostID:      envOr("HOST_ID", "local"),
		HostName:    envOr("HOST_NAME", "Local Host"),
		HubQUICPin:  strings.TrimSpace(os.Getenv("HUB_QUIC_PIN")),
		AccessToken: envOr("FE_TOKEN", os.Getenv("ACCESS_TOKEN")),
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
