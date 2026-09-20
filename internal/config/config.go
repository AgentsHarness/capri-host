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

func Load() Config {
	file, _ := LoadFile()
	return merge(file)
}

// merge 叠三层：内置默认 < config.json < 环境变量。环境变量仍是
// launchd / 脚本部署的最高优先级，GUI 写入的文件不会盖掉它们。
func merge(file File) Config {
	port := 8765
	if file.Port > 0 {
		port = file.Port
	}
	if v := os.Getenv("PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			port = n
		}
	}
	bin := strings.TrimSpace(file.GrokBin)
	if v := os.Getenv("GROK_BIN"); v != "" {
		bin = v
	}
	return Config{
		Port:        port,
		BindAddr:    bindAddr(file.Bind),
		GrokBin:     resolveGrokBin(bin),
		HubURL:      envOr("HUB_URL", strings.TrimSpace(file.HubURL)),
		HubPairCode: envOr("HUB_PAIR_CODE", strings.TrimSpace(file.HubPairCode)),
		HostToken:   os.Getenv("HOST_TOKEN"),
		HostID:      envOr("HOST_ID", strOr(strings.TrimSpace(file.HostID), "local")),
		HostName:    envOr("HOST_NAME", strOr(strings.TrimSpace(file.HostName), "Local Host")),
		HubQUICPin:  strings.TrimSpace(envOr("HUB_QUIC_PIN", strings.TrimSpace(file.HubQUICPin))),
		AccessToken: firstNonEmpty(os.Getenv("FE_TOKEN"), os.Getenv("ACCESS_TOKEN"), strings.TrimSpace(file.FEToken)),
		ResidentCap: envResidentCap(),
		UsageLedger: strings.TrimSpace(os.Getenv("USAGE_LEDGER")),
		Proxy:       proxyURL(os.Getenv("PROXY"), file.Proxy),
		NoProxy:     envOr("NO_PROXY", strings.TrimSpace(file.NoProxy)),
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
