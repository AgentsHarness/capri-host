package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port    int
	GrokBin string
	// Hub mode (optional, Phase later)
	HubEnabled bool
	HubURL     string
	HostToken  string
	HostID     string
	HostName   string
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
		Port:       port,
		GrokBin:    bin,
		HubEnabled: os.Getenv("HUB_ENABLED") == "1" || os.Getenv("HUB_ENABLED") == "true",
		HubURL:     os.Getenv("HUB_URL"),
		HostToken:  os.Getenv("HOST_TOKEN"),
		HostID:     envOr("HOST_ID", "local"),
		HostName:   envOr("HOST_NAME", "Local Host"),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
