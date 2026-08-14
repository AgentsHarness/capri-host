package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port    int
	GrokBin string
	// Hub relay mode: set HUB_URL to pair with capri-hub and serve
	// requests through it (hub is the browser-facing endpoint).
	HubURL      string
	HubPairCode string
	HostToken   string
	HostID      string
	HostName    string
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
		GrokBin:     bin,
		HubURL:      os.Getenv("HUB_URL"),
		HubPairCode: os.Getenv("HUB_PAIR_CODE"),
		HostToken:   os.Getenv("HOST_TOKEN"),
		HostID:      envOr("HOST_ID", "local"),
		HostName:    envOr("HOST_NAME", "Local Host"),
		AccessToken: envOr("FE_TOKEN", os.Getenv("ACCESS_TOKEN")),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
