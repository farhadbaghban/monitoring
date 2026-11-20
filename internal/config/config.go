package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// NewLogger builds the shared JSON logger using the configured slog level.
// Unknown level strings fall back to info instead of disabling application logs.
func NewLogger(raw string) *slog.Logger {
	level := new(slog.LevelVar)
	parsed, err := parseLogLevel(raw)
	if err != nil {
		parsed = slog.LevelInfo
	}
	level.Set(parsed)
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func parseLogLevel(raw string) (slog.Level, error) {
	var level slog.Level
	err := level.UnmarshalText([]byte(raw))
	return level, err
}

// Config holds every tunable read from the environment. Each service reads the
// subset it needs; unused fields simply stay at their defaults.
type Config struct {
	NodeID   string
	HTTPAddr string
	LogLevel string

	KafkaBrokers []string
	KafkaTopic   string
	DLQTopic     string

	DatabaseURL string

	IngestAPIKey  string
	InternalToken string
	UIToken       string

	WALDir      string
	WALMaxBytes int64

	ConsumerGroup string
	BatchSize     int

	CacheTTL         time.Duration
	MaxPointsDefault int

	IngestURL   string
	BackfillURL string
	RefreshURL  string
	SimDevices  int
	SimBackfill int
	SimRate     float64
	SimOffset   int
	SimStep     time.Duration
}

func Load() Config {
	return Config{
		NodeID:   getenv("NODE_ID", "node"),
		HTTPAddr: getenv("HTTP_ADDR", ":8081"),
		LogLevel: getenv("LOG_LEVEL", "info"),

		KafkaBrokers: split(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getenv("KAFKA_TOPIC", "metrics"),
		DLQTopic:     getenv("KAFKA_DLQ_TOPIC", "metrics-dlq"),

		DatabaseURL: getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/metrics?sslmode=disable"),

		IngestAPIKey:  getenv("INGEST_API_KEY", "dev-api-key-change-me"),
		InternalToken: getenv("INTERNAL_TOKEN", "internal-dev-token"),
		UIToken:       getenv("UI_TOKEN", ""),

		WALDir:      getenv("WAL_DIR", "/tmp/wal"),
		WALMaxBytes: getenvInt64("WAL_MAX_BYTES", 512<<20),

		ConsumerGroup: getenv("CONSUMER_GROUP", "metrics-consumer"),
		BatchSize:     getenvInt("BATCH_SIZE", 2000),

		CacheTTL:         getenvDur("CACHE_TTL", 10*time.Second),
		MaxPointsDefault: getenvInt("MAX_POINTS_DEFAULT", 2500),

		IngestURL:   getenv("INGEST_URL", "http://localhost:8080/v1/ingest"),
		BackfillURL: getenv("BACKFILL_URL", "http://localhost:8080/internal/backfill"),
		RefreshURL:  getenv("REFRESH_URL", "http://localhost:8082/internal/refresh-caggs"),
		SimDevices:  getenvInt("SIM_DEVICES", 20),
		SimBackfill: getenvInt("SIM_BACKFILL_DAYS", 3),
		SimRate:     getenvFloat("SIM_RATE", 1),
		SimOffset:   getenvInt("SIM_DEVICE_OFFSET", 1),
		SimStep:     getenvDur("SIM_BACKFILL_STEP", time.Minute),
	}
}

func getenv(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvInt64(k string, def int64) int64 {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getenvFloat(k string, def float64) float64 {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func getenvDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
