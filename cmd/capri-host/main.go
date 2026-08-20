package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/server"
)

// version rides acp.Version, which release builds stamp via
// -ldflags "-X .../internal/acp.Version=<tag>"; local builds show "dev".
var version = acp.Version

func main() {
	log.Printf("[capri-host] version %s", version)
	cfg := config.Load()
	bridge := acp.NewBridge(acp.GrokConfig{
		Bin:      cfg.GrokBin,
		HostID:   cfg.HostID,
		HostName: cfg.HostName,
	})
	srv := server.New(cfg, bridge)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Hub relay mode: pair with capri-hub and serve requests through it.
	// The browser talks to the hub; this host keeps working locally too.
	if cfg.HubURL != "" {
		hc := hub.NewClient(hub.Config{
			URL:         cfg.HubURL,
			HostID:      cfg.HostID,
			HostName:    cfg.HostName,
			PairCode:    cfg.HubPairCode,
			Token:       cfg.HostToken,
			LocalBase:   fmt.Sprintf("http://127.0.0.1:%d", cfg.Port),
			AccessToken: cfg.AccessToken,
			// Optional: bypass proxy/fake-ip DNS for the QUIC transport
			// (e.g. HUB_QUIC_HOST=203.0.113.10).
			QUICHost: os.Getenv("HUB_QUIC_HOST"),
			// Escape hatch for a self-signed hub on a trusted network.
			// Leave unset in production: the QUIC transport otherwise
			// verifies the hub certificate whenever HUB_URL is https
			// (a failure just falls back to WebSocket over verified TLS).
			QUICInsecure: os.Getenv("HUB_QUIC_INSECURE") == "1",
		})
		go hc.Run(ctx, bridge)
	}

	// Eager boot in background (non-fatal if grok missing until first prompt).
	// Boot only warms the agent process — it does NOT create a session, so
	// capri-fe opening does not auto-start a new conversation. The first
	// prompt restores the last known session when one exists; only a machine
	// with no last-session pointer creates a new chat on demand.
	go func() {
		bootCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := bridge.Boot(bootCtx); err != nil {
			log.Printf("[capri-host] initial boot: %v", err)
		}
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("[capri-host] server stopped: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[capri-host] shutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	bridge.Shutdown()
}
