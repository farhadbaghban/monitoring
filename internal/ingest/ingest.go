package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/adanic/monitoring/internal/auth"
	"github.com/adanic/monitoring/internal/model"
	"github.com/adanic/monitoring/internal/wal"
)

// Producer is the durable sink the ingest handler writes to. It is an interface
// so handlers can be tested without a broker.
type Producer interface {
	Produce(ctx context.Context, key, value []byte) error
	ProduceBatch(ctx context.Context, keys, values [][]byte) error
	Healthy(ctx context.Context) bool
}

type Server struct {
	producer Producer
	wal      *wal.WAL
	keyHash  string
	internal string
	walMaxB  int64
	log      *slog.Logger
}

func New(producer Producer, w *wal.WAL,
	apiKey, internalToken string, walMax int64, log *slog.Logger) *Server {
	return &Server{
		producer: producer,
		wal:      w,
		keyHash:  auth.Hash(apiKey),
		internal: internalToken,
		walMaxB:  walMax,
		log:      log,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/ingest", s.handleIngest)
	mux.HandleFunc("/internal/backfill", s.handleBackfill)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", s.handleReady)
	return mux
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if s.producer.Healthy(ctx) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "kafka\n")
		return
	}
	if s.wal.Size() < s.walMaxB {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "wal\n")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, "no sink\n")
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.Equal(auth.Hash(r.Header.Get("X-API-Key")), s.keyHash) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<10))
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var smp model.Sample
	if err := json.Unmarshal(body, &smp); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	wire, err := encodeSample(smp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.persist(r.Context(), wire); err != nil {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "no durable sink", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func (s *Server) handleBackfill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.internal == "" || r.Header.Get("X-Internal-Token") != s.internal {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var samples []model.Sample
	if err := json.Unmarshal(body, &samples); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	keys := make([][]byte, 0, len(samples))
	values := make([][]byte, 0, len(samples))
	for _, smp := range samples {
		wire, err := encodeSample(smp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		payload, err := json.Marshal(wire)
		if err != nil {
			http.Error(w, "encode", http.StatusInternalServerError)
			return
		}
		keys = append(keys, []byte(strconv.Itoa(int(wire.DeviceID))))
		values = append(values, payload)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := s.producer.ProduceBatch(ctx, keys, values); err != nil {
		http.Error(w, "kafka unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"accepted":%d}`, len(values))
}

func encodeSample(smp model.Sample) (model.WireSample, error) {
	if smp.DeviceID <= 0 || smp.DeviceID > math.MaxInt32 {
		return model.WireSample{}, errors.New("device_id must be a positive 32-bit integer")
	}
	ts, err := time.Parse(time.RFC3339Nano, smp.TS)
	if err != nil {
		return model.WireSample{}, errors.New("ts must be RFC3339")
	}
	vals := []*float64{smp.CPU, smp.Mem, smp.Disk, smp.DiskIO, smp.NetIn, smp.NetOut, smp.Fan}
	var out [7]*int16
	for i, v := range vals {
		if v == nil {
			continue
		}
		scaled := math.Round(*v * 100)
		if scaled < math.MinInt16 || scaled > math.MaxInt16 {
			return model.WireSample{}, fmt.Errorf("sensor %s cannot be stored with two decimal places", model.Sensors[i])
		}
		n := int16(scaled)
		out[i] = &n
	}
	return model.WireSample{
		DeviceID: int32(smp.DeviceID),
		TS:       ts.UTC().UnixMicro(),
		CPU:      out[0], Mem: out[1], Disk: out[2], DiskIO: out[3],
		NetIn: out[4], NetOut: out[5], Fan: out[6],
	}, nil
}

// persist tries Kafka first, then the local WAL. A nil error means the sample is
// durable and the caller may answer 200.
func (s *Server) persist(ctx context.Context, wire model.WireSample) error {
	payload, err := json.Marshal(wire)
	if err != nil {
		return err
	}
	key := []byte(strconv.Itoa(int(wire.DeviceID)))
	pctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	err = s.producer.Produce(pctx, key, payload)
	cancel()
	if err == nil {
		return nil
	}
	if werr := s.wal.Append(payload); werr != nil {
		return fmt.Errorf("kafka: %v; wal: %w", err, werr)
	}
	return nil
}

// ReplayLoop drains closed WAL segments to Kafka whenever Kafka is reachable.
func (s *Server) ReplayLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.replayOnce(ctx)
		}
	}
}

func (s *Server) replayOnce(ctx context.Context) {
	if !s.producer.Healthy(ctx) {
		return
	}
	if err := s.wal.Seal(); err != nil {
		s.log.Error("seal wal segment", "err", err)
		return
	}
	segs, err := s.wal.ClosedSegments()
	if err != nil || len(segs) == 0 {
		return
	}
	for _, seg := range segs {
		records, err := wal.ReadSegment(seg)
		if err != nil {
			s.log.Error("read wal segment", "segment", seg, "err", err)
			return
		}
		keys := make([][]byte, 0, len(records))
		for _, rec := range records {
			var wire model.WireSample
			if json.Unmarshal(rec, &wire) == nil {
				keys = append(keys, []byte(strconv.Itoa(int(wire.DeviceID))))
			} else {
				keys = append(keys, []byte("0"))
			}
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = s.producer.ProduceBatch(pctx, keys, records)
		cancel()
		if err != nil {
			return
		}
		if err := s.wal.RemoveSegment(seg); err != nil {
			s.log.Error("remove wal segment", "segment", seg, "err", err)
			return
		}
	}
}
