package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adanic/monitoring/internal/config"
	"github.com/adanic/monitoring/internal/simdata"
)

func main() {
	cfg := config.Load()
	log := config.NewLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	const readyFile = "/tmp/simulator.ready"
	if err := os.Remove(readyFile); err != nil && !os.IsNotExist(err) {
		log.Error("clear simulator readiness", "err", err)
		os.Exit(1)
	}
	if cfg.SimDevices < 1 || cfg.SimOffset < 1 || cfg.SimOffset > math.MaxInt32 || cfg.SimDevices-1 > math.MaxInt32-cfg.SimOffset ||
		cfg.SimStep < time.Microsecond || cfg.SimStep > 24*time.Hour || cfg.SimBackfill < 0 || cfg.SimBackfill > 6 ||
		cfg.SimRate < 1.0/86400 || cfg.SimRate > 1000 || math.IsNaN(cfg.SimRate) {
		log.Error("invalid simulator settings: use positive device IDs, 0–6 history days, a step from 1µs to 24h and a rate from 1 per day to 1000 per second")
		os.Exit(1)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	ids := make([]int, cfg.SimDevices)
	for i := range ids {
		ids[i] = cfg.SimOffset + i
	}
	log.Info("simulator start", "devices", len(ids), "backfill_days", cfg.SimBackfill, "step", cfg.SimStep, "rate", cfg.SimRate)

	if cfg.SimBackfill > 0 {
		if err := backfill(ctx, client, cfg, ids, log); err != nil {
			log.Error("demo history incomplete", "err", err)
			os.Exit(1)
		}
		if err := refresh(ctx, client, cfg, log); err != nil {
			log.Error("demo history refresh failed", "err", err)
			os.Exit(1)
		}
	}
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0o644); err != nil {
		log.Error("write simulator readiness", "err", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runDevice(ctx, client, cfg, id, log)
		}(id)
	}
	wg.Wait()
	log.Info("simulator stopped")
}

func backfill(ctx context.Context, client *http.Client, cfg config.Config, ids []int, log *slog.Logger) error {
	end := time.Now().UTC().Truncate(cfg.SimStep)
	start := end.Add(-time.Duration(cfg.SimBackfill) * 24 * time.Hour)
	log.Info("backfill", "from", start, "to", end)
	const batch = 500
	var buf []simdata.Sample
	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		body, _ := json.Marshal(buf)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.BackfillURL, bytes.NewReader(body))
		if err != nil {
			log.Error("build backfill request", "err", err)
			return false
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Internal-Token", cfg.InternalToken)
		resp, err := client.Do(req)
		if err != nil {
			log.Error("backfill post", "err", err)
			return false
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			log.Error("backfill status", "status", resp.StatusCode)
			return false
		}
		buf = buf[:0]
		return true
	}
	for _, id := range ids {
		r := rand.New(rand.NewSource(int64(id) * 7919))
		for t := start; t.Before(end); t = t.Add(cfg.SimStep) {
			buf = append(buf, simdata.Generate(id, t, r))
			if len(buf) >= batch {
				if !flush() {
					return fmt.Errorf("backfill batch was not acknowledged")
				}
			}
		}
	}
	if !flush() {
		return fmt.Errorf("final backfill batch was not acknowledged")
	}
	// Backfill ACK means Kafka accepted it. Refresh only after the consumer
	// writes the last timestamp for every device in this run.
	wait, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	db, err := pgx.Connect(wait, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	last := end.Add(-cfg.SimStep)
	last = time.UnixMicro(last.UnixMicro()).UTC()
	for {
		var count int
		if err := db.QueryRow(wait, `SELECT count(*) FROM metrics_raw WHERE ts=$1 AND device_id BETWEEN $2 AND $3`, last, ids[0], ids[len(ids)-1]).Scan(&count); err != nil {
			return err
		}
		if count == len(ids) {
			return nil
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func refresh(ctx context.Context, client *http.Client, cfg config.Config, log *slog.Logger) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.RefreshURL, nil)
	if err != nil {
		return fmt.Errorf("build refresh request: %w", err)
	}
	req.Header.Set("X-Internal-Token", cfg.InternalToken)
	refreshClient := *client
	refreshClient.Timeout = 5*time.Minute + 2*time.Second
	resp, err := refreshClient.Do(req)
	if err != nil {
		return fmt.Errorf("refresh aggregates: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("refresh aggregates: HTTP %d", resp.StatusCode)
	}
	log.Info("demo history ready")
	return nil
}

// runDevice implements the device contract: it sends ts, keeps a bounded buffer
// (drop oldest), and resends the same sample with the same ts on non-2xx.
func runDevice(ctx context.Context, client *http.Client, cfg config.Config, id int, log *slog.Logger) {
	buf := make(chan simdata.Sample, 1000)
	go sender(ctx, client, cfg, buf, log)

	interval := time.Second
	if cfg.SimRate > 0 {
		interval = time.Duration(float64(time.Second) / cfg.SimRate)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	r := rand.New(rand.NewSource(int64(id) * 104729))
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s := simdata.Generate(id, now.UTC(), r)
			select {
			case buf <- s:
			default:
				select {
				case <-buf:
				default:
				}
				select {
				case buf <- s:
				default:
				}
			}
		}
	}
}

func sender(ctx context.Context, client *http.Client, cfg config.Config, buf <-chan simdata.Sample, log *slog.Logger) {
	backoff := 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-buf:
			for {
				err := post(ctx, client, cfg.IngestURL, cfg.IngestAPIKey, s)
				if err == nil {
					backoff = 100 * time.Millisecond
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 5*time.Second {
					backoff *= 2
				}
			}
		}
	}
}

func post(ctx context.Context, client *http.Client, url, key string, s simdata.Sample) error {
	body, _ := json.Marshal(s)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
