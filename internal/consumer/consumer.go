package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/adanic/monitoring/internal/model"
	"github.com/adanic/monitoring/internal/queue"
	"github.com/adanic/monitoring/internal/store"
)

type Consumer struct {
	batchSize int
	cl        *queue.Consumer
	store     *store.Store
	dlq       *queue.Producer
	dlqTopic  string
	log       *slog.Logger

	mu      sync.Mutex
	devices map[int32]struct{}
}

func New(batchSize int, cl *queue.Consumer,
	st *store.Store, dlq *queue.Producer, dlqTopic string, log *slog.Logger) *Consumer {
	return &Consumer{
		batchSize: max(1, batchSize),
		cl:        cl, store: st, dlq: dlq, dlqTopic: dlqTopic, log: log,
		devices: make(map[int32]struct{}),
	}
}

// Run polls batches, writes them, and commits offsets only after the DB commit.
// On a DB error the batch is retried in place; offsets stay uncommitted, so a
// crash redelivers. Duplicates are harmless thanks to ON CONFLICT DO NOTHING.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.cl.Poll(ctx, c.batchSize)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
		})
		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			continue
		}
		if err := c.processWithRetry(ctx, recs); err != nil {
			c.log.Error("batch failed, not committing", "err", err)
			continue
		}
		if err := c.cl.Commit(ctx, recs); err != nil {
			c.log.Error("commit failed", "err", err)
		}
	}
}

func (c *Consumer) processWithRetry(ctx context.Context, recs []*kgo.Record) error {
	backoff := 200 * time.Millisecond
	for {
		err := c.process(ctx, recs)
		if err == nil {
			return nil
		}
		c.log.Error("process batch", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (c *Consumer) process(ctx context.Context, recs []*kgo.Record) error {
	valid := make([]model.WireSample, 0, len(recs))
	for _, r := range recs {
		var w model.WireSample
		if err := json.Unmarshal(r.Value, &w); err != nil || w.DeviceID <= 0 {
			if err := c.toDLQ(ctx, r, err); err != nil {
				return err
			}
			continue
		}
		valid = append(valid, w)
	}
	if len(valid) > 0 {
		if err := c.registerDevices(ctx, valid); err != nil {
			return err
		}
		if _, err := c.store.InsertBatch(ctx, valid); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) toDLQ(ctx context.Context, r *kgo.Record, cause error) error {
	msg := map[string]any{
		"error":      errStr(cause),
		"topic":      r.Topic,
		"partition":  r.Partition,
		"offset":     r.Offset,
		"raw_base64": r.Value,
	}
	payload, _ := json.Marshal(msg)
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.dlq.ProduceTo(pctx, c.dlqTopic, r.Key, payload); err != nil {
		c.log.Error("dlq produce", "err", err)
		return err
	}
	return nil
}

func (c *Consumer) registerDevices(ctx context.Context, samples []model.WireSample) error {
	c.mu.Lock()
	var unknown []int32
	for _, s := range samples {
		if _, ok := c.devices[s.DeviceID]; !ok {
			unknown = append(unknown, s.DeviceID)
			c.devices[s.DeviceID] = struct{}{}
		}
	}
	c.mu.Unlock()
	if len(unknown) == 0 {
		return nil
	}
	if err := c.store.EnsureDevices(ctx, unknown); err != nil {
		// Roll back the cache so a later batch retries the registration.
		c.mu.Lock()
		for _, id := range unknown {
			delete(c.devices, id)
		}
		c.mu.Unlock()
		return err
	}
	return nil
}

func errStr(err error) string {
	if err == nil {
		return "invalid sample"
	}
	return err.Error()
}
