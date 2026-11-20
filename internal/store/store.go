package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adanic/monitoring/internal/model"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.pool.Ping(ctx)
}

var stageColumns = []string{"ts", "device_id", "cpu", "mem", "disk", "disk_io", "net_in", "net_out", "fan"}

// InsertBatch writes samples through a temp table + COPY + ON CONFLICT DO
// NOTHING, so redeliveries and retries are idempotent. It returns the number of
// rows actually inserted (duplicates excluded).
func (s *Store) InsertBatch(ctx context.Context, samples []model.WireSample) (int64, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE IF NOT EXISTS ingest_stage (
		ts timestamptz NOT NULL,
		device_id integer NOT NULL,
		cpu smallint, mem smallint, disk smallint,
		disk_io smallint, net_in smallint, net_out smallint,
		fan smallint
	) ON COMMIT DROP`); err != nil {
		return 0, err
	}

	rows := make([][]any, len(samples))
	for i, smp := range samples {
		rows[i] = []any{
			time.UnixMicro(smp.TS).UTC(), smp.DeviceID,
			smp.CPU, smp.Mem, smp.Disk, smp.DiskIO, smp.NetIn, smp.NetOut, smp.Fan,
		}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ingest_stage"}, stageColumns, pgx.CopyFromRows(rows)); err != nil {
		return 0, fmt.Errorf("copy: %w", err)
	}

	tag, err := tx.Exec(ctx, `INSERT INTO metrics_raw (ts, device_id, cpu, mem, disk, disk_io, net_in, net_out, fan)
		SELECT ts, device_id, cpu, mem, disk, disk_io, net_in, net_out, fan FROM ingest_stage
		ON CONFLICT (device_id, ts) DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// EnsureDevices registers unknown device ids. Names are placeholders; a real
// fleet would populate them from provisioning data.
func (s *Store) EnsureDevices(ctx context.Context, ids []int32) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO devices (id, name)
		SELECT id, 'device-' || id FROM unnest($1::int[]) AS id
		ON CONFLICT (id) DO NOTHING`, ids)
	return err
}

// SearchDevices returns devices ordered by id, using the id as a keyset cursor.
func (s *Store) SearchDevices(ctx context.Context, q string, limit, cursor int) ([]model.Device, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT id, name, created_at FROM devices
		WHERE ($1 = '' OR name ILIKE '%' || $1 || '%' OR id::text = $1)
		  AND ($2 = 0 OR id > $2)
		ORDER BY id
		LIMIT $3`, q, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Device
	for rows.Next() {
		var d model.Device
		if err := rows.Scan(&d.ID, &d.Name, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DeviceExists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// RefreshCAGGs forces a manual refresh of every continuous aggregate, used after
// a simulator backfill so historical buckets are materialised immediately.
func (s *Store) RefreshCAGGs(ctx context.Context) error {
	now := time.Now().UTC()
	for _, tier := range tiers[1:] {
		// Stay inside raw retention even on a volume that already holds years of rollups.
		from := now.Add(-6 * 24 * time.Hour).Truncate(tier.Bucket)
		to := now.Truncate(tier.Bucket)
		if _, err := s.pool.Exec(ctx, `CALL refresh_continuous_aggregate($1::text::regclass, $2::timestamptz, $3::timestamptz)`, tier.Source, from, to); err != nil {
			return fmt.Errorf("refresh %s: %w", tier.Source, err)
		}
	}
	return nil
}
