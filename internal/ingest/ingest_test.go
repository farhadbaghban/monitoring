package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adanic/monitoring/internal/wal"
)

type fakeProducer struct {
	mu       sync.Mutex
	fail     bool
	produced int
	value    []byte
}

func (f *fakeProducer) Produce(_ context.Context, _ []byte, value []byte) error {
	if f.fail {
		return errors.New("kafka down")
	}
	f.mu.Lock()
	f.value = value
	f.produced++
	f.mu.Unlock()
	return nil
}

func (f *fakeProducer) ProduceBatch(_ context.Context, _, values [][]byte) error {
	if f.fail {
		return errors.New("kafka down")
	}
	f.mu.Lock()
	f.produced += len(values)
	f.mu.Unlock()
	return nil
}

func (f *fakeProducer) Healthy(context.Context) bool { return !f.fail }

func newServer(t *testing.T, fp *fakeProducer, walMax int64) *Server {
	t.Helper()
	w, err := wal.Open(t.TempDir(), walMax)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(fp, w, "secret", "internal", walMax, log)
}

func body(ts time.Time, v int8) string {
	return fmt.Sprintf(`{"device_id":1,"ts":%q,"cpu":%v,"mem":%v,"disk":%v,"disk_io":%v,"net_in":%v,"net_out":%v,"fan":%v}`,
		ts.UTC().Format(time.RFC3339Nano), v, v, v, v, v, v, v)
}

func do(s *Server, path, key, payload string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(payload))
	req.Header.Set("X-API-Key", key)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

func TestIngestOK(t *testing.T) {
	fp := &fakeProducer{}
	s := newServer(t, fp, 1<<20)
	rr := do(s, "/v1/ingest", "secret", fmt.Sprintf(`{"device_id":1,"ts":%q,"cpu":null,"mem":0,"disk":100}`, time.Now().Add(-48*time.Hour).Format(time.RFC3339Nano)))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	if fp.produced != 1 {
		t.Fatalf("produced %d", fp.produced)
	}
	for _, field := range []string{`"cpu":null`, `"fan":null`, `"mem":0`, `"disk":10000`} {
		if !strings.Contains(string(fp.value), field) {
			t.Errorf("published sample is missing %s: %s", field, fp.value)
		}
	}
}

func TestIngestUnauthorized(t *testing.T) {
	s := newServer(t, &fakeProducer{}, 1<<20)
	if rr := do(s, "/v1/ingest", "wrong", body(time.Now(), 1)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rr.Code)
	}
}

func TestIngestAcceptsBurstWithoutRateLimiting(t *testing.T) {
	fp := &fakeProducer{}
	h := newServer(t, fp, 1<<20).Handler()
	payload := body(time.Now(), 5)
	const requests = 2100
	for i := 0; i < requests; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(payload))
		req.Header.Set("X-API-Key", "secret")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: got HTTP %d: %s", i+1, rr.Code, rr.Body)
		}
	}
	if fp.produced != requests {
		t.Fatalf("published %d samples, want %d", fp.produced, requests)
	}
}

func TestIngestValidation(t *testing.T) {
	s := newServer(t, &fakeProducer{}, 1<<20)
	if rr := do(s, "/v1/ingest", "secret", `{"device_id":1,"ts":"nope"}`); rr.Code != http.StatusBadRequest {
		t.Errorf("bad timestamp: status %d", rr.Code)
	}
	for _, value := range []string{"-1", "101", "12.5", "128"} {
		payload := fmt.Sprintf(`{"device_id":1,"ts":%q,"cpu":%s}`, time.Now().Format(time.RFC3339Nano), value)
		if rr := do(s, "/v1/ingest", "secret", payload); rr.Code != http.StatusBadRequest {
			t.Errorf("cpu=%s: status %d, want 400", value, rr.Code)
		}
	}
}

func TestWALFallback(t *testing.T) {
	fp := &fakeProducer{fail: true}
	s := newServer(t, fp, 1<<20)
	rr := do(s, "/v1/ingest", "secret", body(time.Now(), 10))
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200 via WAL, got %d", rr.Code)
	}
	if s.wal.Size() == 0 {
		t.Fatal("expected WAL to hold the sample")
	}
}

func TestReplayDrainsPartialSegmentWithoutRestart(t *testing.T) {
	fp := &fakeProducer{fail: true}
	s := newServer(t, fp, 1<<20)
	if rr := do(s, "/v1/ingest", "secret", body(time.Now(), 10)); rr.Code != http.StatusOK {
		t.Fatalf("WAL fallback returned %d", rr.Code)
	}
	fp.fail = false
	s.replayOnce(context.Background())
	if fp.produced != 1 || s.wal.Size() != 0 {
		t.Fatalf("got %d broker records and %d WAL bytes; want one record and an empty WAL", fp.produced, s.wal.Size())
	}
}

func TestNoSink503(t *testing.T) {
	fp := &fakeProducer{fail: true}
	s := newServer(t, fp, 0)
	rr := do(s, "/v1/ingest", "secret", body(time.Now(), 10))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
}

func TestBackfillHistoricalSample(t *testing.T) {
	fp := &fakeProducer{}
	s := newServer(t, fp, 1<<20)
	old := time.Now().Add(-400 * 24 * time.Hour)
	payload := "[" + body(old, 5) + "]"
	req := httptest.NewRequest(http.MethodPost, "/internal/backfill", strings.NewReader(payload))
	req.Header.Set("X-Internal-Token", "internal")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	if fp.produced != 1 {
		t.Fatalf("produced %d", fp.produced)
	}
}
