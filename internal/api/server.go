package api

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adanic/monitoring/internal/store"
)

//go:embed web/index.html
var indexHTML []byte

type Server struct {
	st        *store.Store
	uiToken   string
	internal  string
	cacheTTL  time.Duration
	maxPoints int
	log       *slog.Logger

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	body    []byte
	expires time.Time
}

func New(st *store.Store, uiToken, internal string, cacheTTL time.Duration, maxPoints int, log *slog.Logger) *Server {
	return &Server{
		st: st, uiToken: uiToken, internal: internal, cacheTTL: cacheTTL,
		maxPoints: maxPoints, log: log, cache: make(map[string]cacheEntry),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if s.st != nil && s.st.Ping(r.Context()) != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/internal/refresh-caggs", s.handleRefresh)
	mux.HandleFunc("/v1/devices", s.handleDevices)
	mux.HandleFunc("/v1/devices/", s.handleDeviceMetrics)
	mux.HandleFunc("/", s.handleIndex)
	return s.uiAuth(mux)
}

func (s *Server) uiAuth(next http.Handler) http.Handler {
	if s.uiToken == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/internal/refresh-caggs" {
			next.ServeHTTP(w, r)
			return
		}
		tok := r.Header.Get("X-UI-Token")
		if tok == "" {
			tok = r.URL.Query().Get("token")
		}
		if subtle.ConstantTimeCompare([]byte(tok), []byte(s.uiToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.internal == "" || r.Header.Get("X-Internal-Token") != s.internal {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5*time.Minute + time.Second))
	if err := s.st.RefreshCAGGs(ctx); err != nil {
		s.log.Error("refresh caggs", "err", err)
		http.Error(w, "refresh failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	cursor, _ := strconv.Atoi(q.Get("cursor"))
	devices, err := s.st.SearchDevices(r.Context(), strings.TrimSpace(q.Get("q")), limit, cursor)
	if err != nil {
		s.log.Error("search devices", "err", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	next := 0
	if len(devices) > 0 {
		next = devices[len(devices)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": devices, "next_cursor": next})
}

func (s *Server) handleDeviceMetrics(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/devices/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[1] != "metrics" {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil || id <= 0 {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return
	}
	deviceID := int(id)
	q := r.URL.Query()
	to, err := parseTime(q.Get("to"))
	if err != nil {
		if q.Get("to") != "" {
			http.Error(w, "bad to timestamp", http.StatusBadRequest)
			return
		}
		to = time.Now().UTC()
	}
	from, err := parseTime(q.Get("from"))
	if err != nil {
		if q.Get("from") != "" {
			http.Error(w, "bad from timestamp", http.StatusBadRequest)
			return
		}
		from = to.Add(-time.Hour)
	}
	if !from.Before(to) {
		http.Error(w, "from must be before to", http.StatusBadRequest)
		return
	}
	if to.Sub(from) > 365*24*time.Hour {
		http.Error(w, "range exceeds 365 days", http.StatusBadRequest)
		return
	}
	maxPoints := min(2500, max(1, s.maxPoints))
	if v, err := strconv.Atoi(q.Get("max_points")); err == nil && v > 0 {
		maxPoints = min(v, maxPoints)
	}
	sensors := q.Get("sensors")

	key := cacheKey(deviceID, from, to, sensors, maxPoints)
	if body, ok := s.getCache(key); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "hit")
		_, _ = w.Write(body)
		return
	}

	exists, err := s.st.DeviceExists(r.Context(), deviceID)
	if err != nil {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "device not found", http.StatusNotFound)
		return
	}

	res, err := s.st.QueryMetrics(r.Context(), deviceID, from, to, sensors, maxPoints)
	if err != nil {
		s.log.Error("query metrics", "err", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(res)
	if err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	if to.Sub(from) > time.Hour {
		s.putCache(key, body)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "miss")
	_, _ = w.Write(body)
}

func (s *Server) getCache(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[key]
	if !ok || time.Now().After(e.expires) {
		if ok {
			delete(s.cache, key)
		}
		return nil, false
	}
	return e.body, true
}

func (s *Server) putCache(key string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) > 1000 {
		s.cache = make(map[string]cacheEntry)
	}
	s.cache[key] = cacheEntry{body: body, expires: time.Now().Add(s.cacheTTL)}
}

func cacheKey(deviceID int, from, to time.Time, sensors string, maxPoints int) string {
	return strconv.Itoa(deviceID) + "|" +
		from.UTC().Format(time.RFC3339Nano) + "|" +
		to.UTC().Format(time.RFC3339Nano) + "|" +
		sensors + "|" + strconv.Itoa(maxPoints)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, strconv.ErrSyntax
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	if n > 1_000_000_000_000 {
		return time.UnixMilli(n).UTC(), nil
	}
	return time.Unix(n, 0).UTC(), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
