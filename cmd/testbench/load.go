package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adanic/monitoring/internal/simdata"
)

type stageResult struct {
	Devices     int     `json:"devices"`
	Offered     int     `json:"offered"`
	Acked       int     `json:"acknowledged"`
	Stored      *int    `json:"persisted"`
	AckRate     float64 `json:"ack_per_second"`
	WriteRate   float64 `json:"persisted_per_second_including_drain"`
	IngestP99   float64 `json:"ingest_p99_ms"`
	ChartP99    float64 `json:"chart_p99_ms"`
	ChartReads  int     `json:"one_year_queries"`
	ChartErrors int     `json:"chart_errors"`
	Drain       float64 `json:"drain_seconds_from_end_of_offered_window"`
	Passed      bool    `json:"passed"`
}

type loadResult struct {
	Seconds      int           `json:"stage_seconds"`
	Maximum      int           `json:"highest_passing_devices"`
	FirstFailure int           `json:"lowest_failing_devices,omitempty"`
	Ceiling      int           `json:"configured_device_ceiling"`
	Stages       []stageResult `json:"stages"`
}

func load(start, max, seconds int) error {
	if start > max || seconds < 3 || seconds > 120 || max > 600000/seconds {
		return fmt.Errorf("require start <= max and 3-120 seconds, with at most 600000 requests per stage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	const chartDevice = 90
	to, err := seedYearChart(ctx, chartDevice)
	if err != nil {
		return err
	}

	result := loadResult{Seconds: seconds, Ceiling: max}
	firstID := 10_000
	runStage := func(devices int) (bool, error) {
		r, err := measure(ctx, devices, firstID, seconds, chartDevice, to)
		firstID += devices
		if err != nil {
			return false, err
		}
		result.Stages = append(result.Stages, r)
		if r.Passed {
			if devices > result.Maximum {
				result.Maximum = devices
			}
		} else if result.FirstFailure == 0 || devices < result.FirstFailure {
			result.FirstFailure = devices
		}
		stored := "unmeasured"
		if r.Stored != nil {
			stored = fmt.Sprint(*r.Stored)
		}
		fmt.Fprintf(os.Stderr, "%d devices: acked %d/%d, stored %s, ingest p99 %.1f ms, year chart p99 %.1f ms, pass=%t\n", devices, r.Acked, r.Offered, stored, r.IngestP99, r.ChartP99, r.Passed)
		if !r.Passed {
			client.CloseIdleConnections()
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
		return r.Passed, nil
	}
	for _, devices := range loadStages(start, max) {
		ok, err := runStage(devices)
		if err != nil {
			return err
		}
		if !ok || devices == max {
			break
		}
	}
	for i := 0; i < 2 && result.Maximum > 0 && result.FirstFailure-result.Maximum > 1; i++ {
		if _, err := runStage((result.Maximum + result.FirstFailure) / 2); err != nil {
			return err
		}
	}
	sort.Slice(result.Stages, func(i, j int) bool { return result.Stages[i].Devices < result.Stages[j].Devices })
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return err
	}
	if result.Maximum == 0 {
		return fmt.Errorf("no load stage met the thresholds")
	}
	return nil
}

// loadStages starts with the requested size, follows the standard ramp
// checkpoints, then doubles until it reaches the configured ceiling.
func loadStages(start, max int) []int {
	stages := []int{start}
	last := start
	for _, checkpoint := range []int{300, 700, 1000} {
		if checkpoint > last && checkpoint <= max {
			stages = append(stages, checkpoint)
			last = checkpoint
		}
	}
	for last < max {
		next := min(last*2, max)
		if next <= last {
			break
		}
		stages = append(stages, next)
		last = next
	}
	return stages
}

// Send a sparse but year-long device history through backfill and refresh.
func seedYearChart(ctx context.Context, device int) (time.Time, error) {
	to := time.Now().UTC().Truncate(4 * time.Hour)
	rng := rand.New(rand.NewSource(int64(device)))
	var samples []simdata.Sample
	for at := to.Add(-364 * 24 * time.Hour); at.Before(to); at = at.Add(7 * 24 * time.Hour) {
		samples = append(samples, simdata.Generate(device, at, rng))
	}
	for at := to.Add(-3 * 24 * time.Hour); at.Before(to); at = at.Add(4 * time.Hour) {
		samples = append(samples, simdata.Generate(device, at, rng))
	}
	body, err := json.Marshal(samples)
	if err != nil {
		return to, err
	}
	status, _, response, err := request(ctx, http.MethodPost, "http://nginx/internal/backfill", string(body))
	if err != nil || status != 200 {
		return to, fmt.Errorf("prepare year chart: HTTP %d, %v: %s", status, err, response)
	}
	db, err := openDB(ctx)
	if err != nil {
		return to, err
	}
	defer db.Close(context.Background())
	for {
		var stored int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM metrics_raw WHERE device_id=$1`, device).Scan(&stored); err != nil {
			return to, err
		}
		if stored == len(samples) {
			break
		}
		select {
		case <-ctx.Done():
			return to, fmt.Errorf("waiting for simulator samples: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	// Only this disposable database has year-old raw samples. The normal
	// refresh endpoint never recomputes data outside the raw retention window.
	for _, view := range []string{"metrics_1m", "metrics_5m", "metrics_1h", "metrics_4h"} {
		if _, err := db.Exec(ctx, `CALL refresh_continuous_aggregate($1::text::regclass, $2::timestamptz, $3::timestamptz)`, view, to.Add(-365*24*time.Hour), to); err != nil {
			return to, fmt.Errorf("refresh %s: %w", view, err)
		}
	}
	return to, chart(ctx, device, to)
}

var chartSequence atomic.Int64

func chart(ctx context.Context, id int, to time.Time) error {
	status, headers, body, err := request(ctx, http.MethodGet, chartURL(id, to.Add(-365*24*time.Hour), to), "")
	if err != nil {
		return err
	}
	var result struct {
		Tier   string                             `json:"tier"`
		TS     []int64                            `json:"ts"`
		Series map[string]struct{ Count []int64 } `json:"series"`
	}
	if status != 200 || headers.Get("X-Cache") != "miss" {
		return fmt.Errorf("one-year chart HTTP %d, cache=%q", status, headers.Get("X-Cache"))
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return err
	}
	counts := result.Series["cpu"].Count
	populated := 0
	for _, n := range counts {
		if n > 0 {
			populated++
		}
	}
	if result.Tier != "4h" || len(result.TS) < 2190 || len(counts) != len(result.TS) || populated < 60 {
		return fmt.Errorf("incomplete year chart: tier %q, %d points, %d populated", result.Tier, len(result.TS), populated)
	}

	return nil
}

func measure(ctx context.Context, devices, firstID, seconds, chartID int, to time.Time) (stageResult, error) {
	r := stageResult{Devices: devices, Offered: devices * seconds}
	db, err := openDB(ctx)
	if err != nil {
		return r, err
	}
	defer db.Close(context.Background())
	type reads struct {
		times  []time.Duration
		errors int
	}
	chartDone := make(chan reads, 1)
	chartCtx, stopChart := context.WithCancel(ctx)
	defer stopChart()
	go func() {
		var out reads
		for {
			then := time.Now()
			err := chart(chartCtx, chartID, to.Add(time.Duration(chartSequence.Add(1))*time.Millisecond))
			if chartCtx.Err() != nil {
				break
			}
			if err != nil {
				out.errors++
			} else {
				out.times = append(out.times, time.Since(then))
			}
			select {
			case <-chartCtx.Done():
				chartDone <- out
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
		chartDone <- out
	}()
	start := time.Now()
	trafficEnd := start.Add(time.Duration(seconds) * time.Second)
	trafficCtx, cancel := context.WithDeadline(ctx, trafficEnd.Add(30*time.Second))
	defer cancel()
	type persistence struct {
		count *int
		drain float64
		err   error
	}
	persisted := make(chan persistence, 1)
	go func() {
		timing, stop := context.WithDeadline(ctx, trafficEnd.Add(30*time.Second))
		defer stop()
		var observed persistence
		defer func() {
			observed.drain = max(0, time.Since(trafficEnd).Seconds())
			persisted <- observed
		}()
		select {
		case <-timing.Done():
			return
		case <-time.After(time.Until(trafficEnd)):
		}
		for timing.Err() == nil {
			var count int
			err := db.QueryRow(timing, `SELECT count(*) FROM metrics_raw
				WHERE device_id >= $1 AND device_id < $2 AND ts >= $3 AND ts < $4`, firstID, firstID+devices, start.Truncate(time.Microsecond), trafficEnd).Scan(&count)
			if err != nil {
				if timing.Err() == nil {
					observed.err = err
				}
				return
			}
			observed.count = &count
			if count >= r.Offered {
				return
			}
			select {
			case <-timing.Done():
			case <-time.After(150 * time.Millisecond):
			}
		}
	}()
	success := make(chan time.Duration, r.Offered)
	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(int64(firstID)))
schedule:
	for second := 0; second < seconds; second++ {
		for offset := 0; offset < devices; offset++ {
			if trafficCtx.Err() != nil {
				break schedule
			}
			target := start.Add(time.Duration(second)*time.Second + time.Duration(offset)*time.Second/time.Duration(devices))
			if delay := time.Until(target); delay > 0 {
				time.Sleep(delay)
			}
			wg.Add(1)
			sample := simdata.Generate(firstID+offset, target, rng)
			go func(sample simdata.Sample, target time.Time) {
				defer wg.Done()
				body, err := json.Marshal(sample)
				if err != nil {
					return
				}
				status, _, err := postBody(trafficCtx, ingestURL, string(body))
				if err == nil && status == http.StatusOK {
					success <- time.Since(target)
				}
			}(sample, target)
		}
	}
	if delay := time.Until(trafficEnd); delay > 0 {
		time.Sleep(delay)
	}
	wg.Wait()
	close(success)
	stopChart()
	readings := <-chartDone
	r.ChartReads = len(readings.times)
	r.ChartErrors = readings.errors
	r.ChartP99 = p99(readings.times)
	var ackTimes []time.Duration
	for latency := range success {
		ackTimes = append(ackTimes, latency)
	}
	r.Acked = len(ackTimes)
	r.IngestP99 = p99(ackTimes)
	r.AckRate = float64(r.Acked) / float64(seconds)
	storage := <-persisted
	if storage.err != nil {
		return r, storage.err
	}
	r.Stored, r.Drain = storage.count, storage.drain
	if r.Stored != nil {
		r.WriteRate = float64(*r.Stored) / (float64(seconds) + r.Drain)
	}
	r.Passed = r.Acked*1000 >= r.Offered*999 && r.Stored != nil && *r.Stored == r.Offered &&
		r.IngestP99 > 0 && r.IngestP99 < 200 && r.ChartReads > 0 && r.ChartErrors == 0 && r.ChartP99 < 1000 && r.Drain <= 30
	return r, nil
}

func p99(times []time.Duration) float64 {
	if len(times) == 0 {
		return 0
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return float64(times[int(math.Ceil(.99*float64(len(times))))-1]) / float64(time.Millisecond)
}
