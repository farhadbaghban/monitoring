//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adanic/monitoring/internal/store"
)

func TestNullableSensorsThroughCompose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := pgx.Connect(ctx, requiredEnv(t, "DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())

	var nullable int
	err = db.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'metrics_raw'
		  AND column_name IN ('cpu','mem','disk','disk_io','net_in','net_out','fan')
		  AND is_nullable = 'YES'`).Scan(&nullable)
	if err != nil || nullable != 7 {
		t.Fatalf("schema has %d nullable sensor columns, want 7: %v", nullable, err)
	}

	// Use a recent, uncompressed chunk and a timestamp the previous skew check rejected.
	ts := time.Now().UTC().Add(-25 * time.Hour).Truncate(time.Minute)
	device := int32(1_000_000_000 + rand.IntN(1_000_000_000))
	var exists bool
	if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM devices WHERE id=$1)", device).Scan(&exists); err != nil || exists {
		t.Fatalf("test device %d must be unused: exists=%t, err=%v", device, exists, err)
	}
	refreshMinute := func(ctx context.Context) error {
		_, err := db.Exec(ctx, "CALL refresh_continuous_aggregate('metrics_1m', $1::timestamptz, $2::timestamptz)", ts, ts.Add(time.Minute))
		return err
	}
	refreshed := false
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := db.Exec(cleanupCtx, "DELETE FROM metrics_raw WHERE device_id=$1 AND ts IN ($2, $3)", device, ts, ts.Add(time.Second)); err != nil {
			t.Errorf("remove test readings: %v", err)
		}
		if _, err := db.Exec(cleanupCtx, "DELETE FROM devices WHERE id=$1", device); err != nil {
			t.Errorf("remove test device: %v", err)
		}
		if refreshed {
			if err := refreshMinute(cleanupCtx); err != nil {
				t.Errorf("remove test rollup: %v", err)
			}
		}
	}()

	ingestURL := requiredEnv(t, "INGEST_URL")
	apiURL := requiredEnv(t, "TEST_API_URL")
	apiKey := requiredEnv(t, "INGEST_API_KEY")
	client := &http.Client{Timeout: 8 * time.Second}
	post := func(at time.Time, fields string) {
		t.Helper()
		payload := fmt.Sprintf(`{"device_id":%d,"ts":%q%s}`, device, at.Format(time.RFC3339Nano), fields)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ingestURL, bytes.NewBufferString(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", apiKey)
		if body := response(t, client, req, http.StatusOK); !bytes.Contains(body, []byte(`"ok":true`)) {
			t.Fatalf("ingest acknowledgement: %s", body)
		}
	}
	post(ts, `,"cpu":null,"mem":0,"disk":101.25`)
	post(ts.Add(time.Second), `,"cpu":null,"mem":null,"disk":null,"disk_io":null,"net_in":null,"net_out":null,"fan":null`)

	for i := 0; i < 2; i++ {
		var values [7]*int16
		at := ts.Add(time.Duration(i) * time.Second)
		for {
			err := db.QueryRow(ctx, `SELECT cpu, mem, disk, disk_io, net_in, net_out, fan
				FROM metrics_raw WHERE device_id=$1 AND ts=$2`, device, at).
				Scan(&values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6])
			if err == nil {
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("read sample at %v: %v", at, err)
			}
			select {
			case <-ctx.Done():
				t.Fatalf("sample at %v was acknowledged but never reached PostgreSQL", at)
			case <-time.After(200 * time.Millisecond):
			}
		}
		for j, value := range values {
			switch {
			case i == 0 && j == 1:
				if value == nil || *value != 0 {
					t.Fatal("mem=0 must be stored as zero")
				}
			case i == 0 && j == 2:
				if value == nil || *value != 10125 {
					t.Fatal("disk=101.25 must be stored as 10125")
				}
			case value != nil:
				t.Errorf("row %d, sensor column %d: got %d, want SQL NULL", i, j, *value)
			}
		}
	}

	query := func(urlPath string, status int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+urlPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token := os.Getenv("UI_TOKEN"); token != "" {
			req.Header.Set("X-UI-Token", token)
		}
		return response(t, client, req, status)
	}
	params := url.Values{
		"from": {ts.Format(time.RFC3339)},
		"to":   {ts.Add(2 * time.Second).Format(time.RFC3339)},
	}
	var result store.QueryResult
	if err := json.Unmarshal(query(fmt.Sprintf("/v1/devices/%d/metrics?%s", device, params.Encode()), http.StatusOK), &result); err != nil {
		t.Fatal(err)
	}
	if result.Tier != "raw" || len(result.TS) < 2 || result.TS[0] != ts.UnixMilli() {
		t.Fatalf("expected two raw buckets starting at %s, got tier=%s ts=%v", ts, result.Tier, result.TS)
	}
	for _, sensor := range []string{"cpu", "mem", "disk", "disk_io", "net_in", "net_out", "fan"} {
		want := map[string]float64{"mem": 0, "disk": 101.25}
		series := result.Series[sensor]
		if len(series.Count) < 2 || len(series.Min) < 2 || len(series.Avg) < 2 || len(series.Max) < 2 {
			t.Fatalf("missing buckets for sensor %s: %+v", sensor, series)
		}
		for i := 0; i < 2; i++ {
			value, present := want[sensor]
			if i == 0 && present {
				if series.Count[i] != 1 || !matches(series.Min[i], value) || !matches(series.Avg[i], value) || !matches(series.Max[i], value) {
					t.Errorf("%s bucket %d should contain one reading of %v", sensor, i, value)
				}
			} else if series.Count[i] != 0 || series.Min[i] != nil || series.Avg[i] != nil || series.Max[i] != nil {
				t.Errorf("%s bucket %d should be a gap", sensor, i)
			}
		}
	}
	if err := refreshMinute(ctx); err != nil {
		t.Fatalf("refresh one-minute aggregate: %v", err)
	}
	refreshed = true
	params.Set("to", ts.Add(2*time.Hour).Format(time.RFC3339))
	var rolled store.QueryResult
	if err := json.Unmarshal(query(fmt.Sprintf("/v1/devices/%d/metrics?%s", device, params.Encode()), http.StatusOK), &rolled); err != nil {
		t.Fatal(err)
	}
	if rolled.Tier != "1m" {
		t.Fatalf("expected 1m tier, got %s", rolled.Tier)
	}
	for _, sensor := range []string{"cpu", "mem", "disk"} {
		series := rolled.Series[sensor]
		if len(series.Count) == 0 || len(series.Avg) == 0 {
			t.Fatalf("missing rollup for %s", sensor)
		}
		switch sensor {
		case "cpu":
			if series.Count[0] != 0 || series.Avg[0] != nil {
				t.Errorf("cpu rollup must be a gap")
			}
		case "mem":
			if series.Count[0] != 1 || !matches(series.Avg[0], 0) {
				t.Errorf("mem rollup must contain one zero reading")
			}
		case "disk":
			if series.Count[0] != 1 || !matches(series.Avg[0], 101.25) {
				t.Errorf("disk rollup must contain one reading of 101.25")
			}
		}
	}
	query("/metrics", http.StatusNotFound)
}

func matches(value *float64, expected float64) bool {
	return value != nil && *value == expected
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required; run this test through Docker Compose", name)
	}
	return value
}

func response(t *testing.T, client *http.Client, req *http.Request, want int) []byte {
	t.Helper()
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != want {
		t.Fatalf("%s %s: got HTTP %d, want %d: %s", req.Method, req.URL, res.StatusCode, want, body)
	}
	return body
}
