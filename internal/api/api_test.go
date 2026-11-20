package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	want := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if got, err := parseTime("2026-05-01T00:00:00Z"); err != nil || !got.Equal(want) {
		t.Fatalf("rfc3339: %v %v", got, err)
	}
	if got, err := parseTime("1777593600"); err != nil || !got.Equal(want) {
		t.Fatalf("unix seconds: %v %v", got, err)
	}
	if got, err := parseTime("1777593600000"); err != nil || !got.Equal(want) {
		t.Fatalf("unix millis: %v %v", got, err)
	}
	if _, err := parseTime("garbage"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCacheKeyStable(t *testing.T) {
	from := time.Unix(1000, 0)
	to := time.Unix(2000, 0)
	a := cacheKey(1, from, to, "cpu", 100)
	b := cacheKey(1, from, to, "cpu", 100)
	c := cacheKey(2, from, to, "cpu", 100)
	if a != b || a == c {
		t.Fatalf("cache keys not stable/distinct: %q %q %q", a, b, c)
	}
	if a == cacheKey(1, from.Add(time.Microsecond), to, "cpu", 100) {
		t.Fatal("cache keys must distinguish sub-millisecond time ranges")
	}
}

func TestUIAuth(t *testing.T) {
	s := &Server{uiToken: "tok"}
	h := s.uiAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/devices", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rr.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/devices", nil)
	req.Header.Set("X-UI-Token", "tok")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz must bypass auth, got %d", rr.Code)
	}
}
