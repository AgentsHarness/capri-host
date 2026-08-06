package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benin/acp-host/internal/acp"
	"github.com/benin/acp-host/internal/config"
	"github.com/benin/acp-host/internal/server"
)

func main() {
	cfg := config.Load()
	bridge := acp.NewBridge(acp.GrokConfig{
		Bin:      cfg.GrokBin,
		HostID:   cfg.HostID,
		HostName: cfg.HostName,
	})
	srv := server.New(cfg, bridge)

	// Eager boot in background (non-fatal if grok missing until first prompt)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := bridge.Boot(ctx, acp.SessionConfig{}); err != nil {
			log.Printf("[acp-host] initial boot: %v", err)
		}
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("[acp-host] server stopped: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("[acp-host] shutting down…")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	bridge.Shutdown()
}
