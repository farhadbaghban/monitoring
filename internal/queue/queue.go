package queue

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is a durable, idempotent Kafka producer. franz-go enables the
// idempotent producer by default; we pin acks=all and a short delivery timeout
// so a produce failure surfaces inside the HTTP request budget.
type Producer struct {
	cl    *kgo.Client
	topic string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.RecordDeliveryTimeout(2*time.Second),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(100_000),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, topic: topic}, nil
}

// Produce waits for a broker acknowledgement or returns as soon as the caller
// deadline expires. The caller can then durably append the record to its WAL.
func (p *Producer) Produce(ctx context.Context, key, value []byte) error {
	rec := &kgo.Record{Topic: p.topic, Key: key, Value: value}
	result := make(chan error, 1)
	p.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		result <- err
	})
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ProduceTo writes to an explicit topic (used for the dead-letter queue).
func (p *Producer) ProduceTo(ctx context.Context, topic string, key, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	return p.cl.ProduceSync(ctx, rec).FirstErr()
}

// ProduceBatch writes many records and waits for one broker acknowledgement.
func (p *Producer) ProduceBatch(ctx context.Context, keys, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(values))
	for i := range values {
		recs[i] = &kgo.Record{Topic: p.topic, Key: keys[i], Value: values[i]}
	}
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Healthy reports whether at least one broker answers a metadata request.
func (p *Producer) Healthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return p.cl.Ping(ctx) == nil
}

func (p *Producer) Close() { p.cl.Close() }

// Consumer reads from a topic group without auto-committing offsets.
type Consumer struct {
	cl *kgo.Client
}

func NewConsumer(brokers []string, group, topic string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(64<<20),
		kgo.FetchMaxPartitionBytes(16<<20),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl}, nil
}

func (c *Consumer) Poll(ctx context.Context, maxRecords int) kgo.Fetches {
	return c.cl.PollRecords(ctx, maxRecords)
}

func (c *Consumer) Commit(ctx context.Context, recs []*kgo.Record) error {
	return c.cl.CommitRecords(ctx, recs...)
}

func (c *Consumer) Close() { c.cl.Close() }
