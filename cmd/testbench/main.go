// testbench drives HTTP traffic and verifies persisted samples in disposable Compose stacks.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const ingestURL = "http://nginx/v1/ingest"
const apiURL = "http://nginx"
const fillManifest = "/tmp/wal-accepted.json"

var client = &http.Client{
	Timeout:   12 * time.Second,
	Transport: &http.Transport{MaxIdleConns: 2048, MaxIdleConnsPerHost: 2048, IdleConnTimeout: 45 * time.Second},
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "expected a benchmark action")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(command string, args []string) error {
	switch command {
	case "now":
		fmt.Println(time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano))
	case "smoke":
		ts := stamp(args[0])
		if err := smoke(ts); err != nil {
			return err
		}
	case "year":
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := seedYearChart(ctx, 90); err != nil {
			return err
		}
		fmt.Println("One-year chart: verified simulated data after backfill and aggregate refresh.")
	case "send", "fill":
		if command == "fill" {
			return send(number(args[0]), stamp(args[1]), 192, 0, args[2], "", true)
		}
		interval, err := time.ParseDuration(args[3])
		if err != nil {
			return err
		}
		var marker string
		if len(args) > 5 {
			marker = args[5]
		}
		return send(number(args[0]), stamp(args[1]), number(args[2]), interval, args[4], marker, false)
	case "absent":
		return absent(number(args[0]))
	case "verify":
		want := make([]int, number(args[2]))
		for i := range want {
			want[i] = i
		}
		return verify(number(args[0]), stamp(args[1]), want)
	case "verify-fill":
		raw, err := os.ReadFile(fillManifest)
		if err != nil {
			return err
		}
		var want []int
		if err := json.Unmarshal(raw, &want); err != nil {
			return err
		}
		return verify(number(args[0]), stamp(args[1]), want)
	case "load":
		return load(number(args[0]), number(args[1]), number(args[2]))
	case "report":
		return report(args[0])
	default:
		return fmt.Errorf("unknown action %q", command)
	}
	return nil
}

func number(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		panic("expected positive integer, got " + s)
	}
	return n
}

func stamp(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func request(ctx context.Context, method, target, body string) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("X-API-Key", os.Getenv("INGEST_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
	}
	if token := os.Getenv("UI_TOKEN"); token != "" {
		req.Header.Set("X-UI-Token", token)
	}
	if method == http.MethodPost {
		req.Header.Set("X-Internal-Token", os.Getenv("INTERNAL_TOKEN"))
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err == nil && resp.StatusCode != http.StatusOK && !(resp.StatusCode == http.StatusServiceUnavailable && resp.Header.Get("Retry-After") != "") {
		err = fmt.Errorf("%s", strings.TrimSpace(string(data)))
	}
	return resp.StatusCode, resp.Header, data, err
}

func post(ctx context.Context, target string, id int, ts time.Time) (int, http.Header, error) {
	body := fmt.Sprintf(`{"device_id":%d,"ts":%q,"cpu":null,"mem":0,"disk":101.25,"disk_io":4,"net_in":5,"net_out":6,"fan":7}`, id, ts.Format(time.RFC3339Nano))
	return postBody(ctx, target, body)
}

func postBody(ctx context.Context, target, body string) (int, http.Header, error) {
	status, headers, data, err := request(ctx, http.MethodPost, target, body)
	if err == nil && status == http.StatusOK && !strings.Contains(string(data), `"ok":true`) {
		err = fmt.Errorf("unexpected acknowledgement: %s", data)
	}
	return status, headers, err
}

func send(device int, base time.Time, count int, interval time.Duration, target, marker string, fill bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(count)*interval+55*time.Second)
	defer cancel()
	type result struct {
		index  int
		status int
		err    error
	}
	out := make(chan result, count)
	var first sync.Once
	start := time.Now()
	for i := 0; i < count; i++ {
		delay := time.Until(start.Add(time.Duration(i) * interval))
		if delay > 0 {
			time.Sleep(delay)
		}
		go func(i int) {
			var resultErr error
			ts := base.Add(time.Duration(i) * time.Millisecond)
			for attempt := 0; ; attempt++ {
				status, headers, err := post(ctx, target, device, ts)
				if err == nil && status == 200 {
					if marker != "" {
						first.Do(func() { resultErr = os.WriteFile(marker, nil, 0600) })
					}
					out <- result{i, status, resultErr}
					return
				}
				if fill && status == 503 && headers.Get("Retry-After") != "" {
					out <- result{i, status, nil}
					return
				}
				if fill || attempt >= 8 || ctx.Err() != nil || (err == nil && status < 500) {
					out <- result{i, status, fmt.Errorf("HTTP %d: %v", status, err)}
					return
				}
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
			}
		}(i)
	}
	accepted := make([]int, 0, count)
	rejected := 0
	for i := 0; i < count; i++ {
		r := <-out
		if r.err != nil {
			return fmt.Errorf("sample %d: %w", r.index, r.err)
		}
		if r.status == 200 {
			accepted = append(accepted, r.index)
		} else {
			rejected++
		}
	}
	fmt.Printf("Device %d: %d acknowledged, %d rejected with Retry-After\n", device, len(accepted), rejected)
	if fill {
		if len(accepted) == 0 || rejected == 0 {
			return fmt.Errorf("expected both WAL acknowledgements and WAL-full rejections")
		}
		data, err := json.Marshal(accepted)
		if err != nil {
			return err
		}
		return os.WriteFile(fillManifest, data, 0600)
	}
	return nil
}

func openDB(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
}

func absent(device int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	db, err := openDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	var n int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM metrics_raw WHERE device_id=$1", device).Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("device %d had %d stored rows while Kafka was stopped", device, n)
	}
	return nil
}

func verify(device int, base time.Time, want []int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := openDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	expected := make(map[int64]bool, len(want))
	for _, i := range want {
		expected[base.Add(time.Duration(i)*time.Millisecond).UnixMicro()] = true
	}
	for {
		rows, err := db.Query(ctx, `SELECT ts, (cpu IS NULL AND mem=0 AND disk=10125 AND
			disk_io=400 AND net_in=500 AND net_out=600 AND fan=700) IS TRUE
			FROM metrics_raw WHERE device_id=$1`, device)
		if err != nil {
			return err
		}
		seen := make(map[int64]bool, len(want))
		for rows.Next() {
			var ts time.Time
			var good bool
			if err := rows.Scan(&ts, &good); err != nil {
				rows.Close()
				return err
			}
			key := ts.UnixMicro()
			if !good || !expected[key] || seen[key] {
				rows.Close()
				return fmt.Errorf("device %d unexpected, changed or duplicated sample: %s", device, ts)
			}
			seen[key] = true
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(seen) == len(expected) {
			fmt.Printf("Device %d: verified exactly %d distinct persisted samples and their payloads\n", device, len(seen))
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("device %d: %d/%d persisted", device, len(seen), len(expected))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func smoke(ts time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, path := range []string{"/healthz", "/", "/v1/devices"} {
		status, _, data, err := request(ctx, http.MethodGet, apiURL+path, "")
		if err != nil || status != 200 || len(data) == 0 {
			return fmt.Errorf("GET %s: HTTP %d: %v", path, status, err)
		}
	}
	if err := send(100, ts, 2, 0, ingestURL, "", false); err != nil {
		return err
	}
	if err := verify(100, ts, []int{0, 1}); err != nil {
		return err
	}
	status, _, body, err := request(ctx, http.MethodGet, chartURL(100, ts, ts.Add(time.Second)), "")
	if err != nil || status != 200 || !strings.Contains(string(body), `"mem":{"min":[0`) || !strings.Contains(string(body), `"cpu":{"min":[null`) {
		return fmt.Errorf("raw chart did not preserve zero and null: HTTP %d: %v: %s", status, err, body)
	}
	return nil
}

func chartURL(device int, from, to time.Time) string {
	return fmt.Sprintf("%s/v1/devices/%d/metrics?from=%s&to=%s&max_points=2500", apiURL, device, from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano))
}
