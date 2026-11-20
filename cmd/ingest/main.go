package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adanic/monitoring/internal/config"
	"github.com/adanic/monitoring/internal/ingest"
	"github.com/adanic/monitoring/internal/queue"
	"github.com/adanic/monitoring/internal/wal"
)

func main() {
	cfg := config.Load()
	log := config.NewLogger(cfg.LogLevel)

	producer, err := queue.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	w, err := wal.Open(cfg.WALDir, cfg.WALMaxBytes)
	if err != nil {
		log.Error("open wal", "err", err)
		os.Exit(1)
	}
	defer w.Close()

	srv := ingest.New(producer, w,
		cfg.IngestAPIKey, cfg.InternalToken, cfg.WALMaxBytes, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go srv.ReplayLoop(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Info("ingest listening", "addr", cfg.HTTPAddr, "node", cfg.NodeID)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
