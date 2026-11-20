package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adanic/monitoring/internal/config"
	"github.com/adanic/monitoring/internal/consumer"
	"github.com/adanic/monitoring/internal/queue"
	"github.com/adanic/monitoring/internal/store"
)

func main() {
	cfg := config.Load()
	log := config.NewLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	cl, err := queue.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.KafkaTopic)
	if err != nil {
		log.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	dlq, err := queue.NewProducer(cfg.KafkaBrokers, cfg.DLQTopic)
	if err != nil {
		log.Error("dlq producer", "err", err)
		os.Exit(1)
	}
	defer dlq.Close()

	c := consumer.New(cfg.BatchSize, cl, st, dlq, cfg.DLQTopic, log)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if st.Ping(r.Context()) != nil || !dlq.Healthy(r.Context()) {
			http.Error(w, "database or broker unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("consumer listening", "addr", cfg.HTTPAddr, "node", cfg.NodeID)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		defer stop()
		c.Run(ctx)
	}()
	<-ctx.Done()
	log.Info("shutting down consumer")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	<-consumerDone
}
