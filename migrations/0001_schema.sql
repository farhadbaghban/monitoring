-- Schema for the sensor-metrics platform.
-- Idempotent: safe to run on every `make up`.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS devices (
    id           integer PRIMARY KEY,
    name         text NOT NULL,
    api_key_hash text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS devices_name_trgm ON devices USING gin (name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS metrics_raw (
    ts        timestamptz NOT NULL,
    device_id integer NOT NULL,
    cpu       smallint,
    mem       smallint,
    disk      smallint,
    disk_io   smallint,
    net_in    smallint,
    net_out   smallint,
    fan       smallint
);

-- Upgrade existing installations whose sensor columns were required.
ALTER TABLE metrics_raw
    ALTER COLUMN cpu DROP NOT NULL,
    ALTER COLUMN mem DROP NOT NULL,
    ALTER COLUMN disk DROP NOT NULL,
    ALTER COLUMN disk_io DROP NOT NULL,
    ALTER COLUMN net_in DROP NOT NULL,
    ALTER COLUMN net_out DROP NOT NULL,
    ALTER COLUMN fan DROP NOT NULL;

-- ADR-013: 1-day chunks are too large at 10k devices; use 6-hour chunks.
SELECT create_hypertable('metrics_raw', 'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE);

-- ADR-005/ADR-013: the unique key doubles as the only index; no separate
-- (device_id, ts DESC) index is created.
CREATE UNIQUE INDEX IF NOT EXISTS metrics_raw_device_ts_uidx
    ON metrics_raw (device_id, ts);

-- ---------------------------------------------------------------- aggregates
-- Group by the SELECT expression; higher tiers also have an input named bucket.
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', ts) AS bucket,
       device_id,
       min(cpu)::smallint      AS cpu_min,      max(cpu)::smallint      AS cpu_max,      sum(cpu)::bigint      AS cpu_sum,      count(cpu)      AS cpu_cnt,
       min(mem)::smallint      AS mem_min,      max(mem)::smallint      AS mem_max,      sum(mem)::bigint      AS mem_sum,      count(mem)      AS mem_cnt,
       min(disk)::smallint     AS disk_min,     max(disk)::smallint     AS disk_max,     sum(disk)::bigint     AS disk_sum,     count(disk)     AS disk_cnt,
       min(disk_io)::smallint  AS disk_io_min,  max(disk_io)::smallint  AS disk_io_max,  sum(disk_io)::bigint  AS disk_io_sum,  count(disk_io)  AS disk_io_cnt,
       min(net_in)::smallint   AS net_in_min,   max(net_in)::smallint   AS net_in_max,   sum(net_in)::bigint   AS net_in_sum,   count(net_in)   AS net_in_cnt,
       min(net_out)::smallint  AS net_out_min,  max(net_out)::smallint  AS net_out_max,  sum(net_out)::bigint  AS net_out_sum,  count(net_out)  AS net_out_cnt,
       min(fan)::smallint      AS fan_min,      max(fan)::smallint      AS fan_max,      sum(fan)::bigint      AS fan_sum,      count(fan)      AS fan_cnt
FROM metrics_raw
GROUP BY 1, device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5m
WITH (timescaledb.continuous) AS
SELECT time_bucket('5 minutes', bucket) AS bucket,
       device_id,
       min(cpu_min)::smallint      AS cpu_min,      max(cpu_max)::smallint      AS cpu_max,      sum(cpu_sum)::bigint      AS cpu_sum,      sum(cpu_cnt)      AS cpu_cnt,
       min(mem_min)::smallint      AS mem_min,      max(mem_max)::smallint      AS mem_max,      sum(mem_sum)::bigint      AS mem_sum,      sum(mem_cnt)      AS mem_cnt,
       min(disk_min)::smallint     AS disk_min,     max(disk_max)::smallint     AS disk_max,     sum(disk_sum)::bigint     AS disk_sum,     sum(disk_cnt)     AS disk_cnt,
       min(disk_io_min)::smallint  AS disk_io_min,  max(disk_io_max)::smallint  AS disk_io_max,  sum(disk_io_sum)::bigint  AS disk_io_sum,  sum(disk_io_cnt)  AS disk_io_cnt,
       min(net_in_min)::smallint   AS net_in_min,   max(net_in_max)::smallint   AS net_in_max,   sum(net_in_sum)::bigint   AS net_in_sum,   sum(net_in_cnt)   AS net_in_cnt,
       min(net_out_min)::smallint  AS net_out_min,  max(net_out_max)::smallint  AS net_out_max,  sum(net_out_sum)::bigint  AS net_out_sum,  sum(net_out_cnt)  AS net_out_cnt,
       min(fan_min)::smallint      AS fan_min,      max(fan_max)::smallint      AS fan_max,      sum(fan_sum)::bigint      AS fan_sum,      sum(fan_cnt)      AS fan_cnt
FROM metrics_1m
GROUP BY 1, device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket) AS bucket,
       device_id,
       min(cpu_min)::smallint      AS cpu_min,      max(cpu_max)::smallint      AS cpu_max,      sum(cpu_sum)::bigint      AS cpu_sum,      sum(cpu_cnt)      AS cpu_cnt,
       min(mem_min)::smallint      AS mem_min,      max(mem_max)::smallint      AS mem_max,      sum(mem_sum)::bigint      AS mem_sum,      sum(mem_cnt)      AS mem_cnt,
       min(disk_min)::smallint     AS disk_min,     max(disk_max)::smallint     AS disk_max,     sum(disk_sum)::bigint     AS disk_sum,     sum(disk_cnt)     AS disk_cnt,
       min(disk_io_min)::smallint  AS disk_io_min,  max(disk_io_max)::smallint  AS disk_io_max,  sum(disk_io_sum)::bigint  AS disk_io_sum,  sum(disk_io_cnt)  AS disk_io_cnt,
       min(net_in_min)::smallint   AS net_in_min,   max(net_in_max)::smallint   AS net_in_max,   sum(net_in_sum)::bigint   AS net_in_sum,   sum(net_in_cnt)   AS net_in_cnt,
       min(net_out_min)::smallint  AS net_out_min,  max(net_out_max)::smallint  AS net_out_max,  sum(net_out_sum)::bigint  AS net_out_sum,  sum(net_out_cnt)  AS net_out_cnt,
       min(fan_min)::smallint      AS fan_min,      max(fan_max)::smallint      AS fan_max,      sum(fan_sum)::bigint      AS fan_sum,      sum(fan_cnt)      AS fan_cnt
FROM metrics_5m
GROUP BY 1, device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_4h
WITH (timescaledb.continuous) AS
SELECT time_bucket('4 hours', bucket) AS bucket,
       device_id,
       min(cpu_min)::smallint      AS cpu_min,      max(cpu_max)::smallint      AS cpu_max,      sum(cpu_sum)::bigint      AS cpu_sum,      sum(cpu_cnt)      AS cpu_cnt,
       min(mem_min)::smallint      AS mem_min,      max(mem_max)::smallint      AS mem_max,      sum(mem_sum)::bigint      AS mem_sum,      sum(mem_cnt)      AS mem_cnt,
       min(disk_min)::smallint     AS disk_min,     max(disk_max)::smallint     AS disk_max,     sum(disk_sum)::bigint     AS disk_sum,     sum(disk_cnt)     AS disk_cnt,
       min(disk_io_min)::smallint  AS disk_io_min,  max(disk_io_max)::smallint  AS disk_io_max,  sum(disk_io_sum)::bigint  AS disk_io_sum,  sum(disk_io_cnt)  AS disk_io_cnt,
       min(net_in_min)::smallint   AS net_in_min,   max(net_in_max)::smallint   AS net_in_max,   sum(net_in_sum)::bigint   AS net_in_sum,   sum(net_in_cnt)   AS net_in_cnt,
       min(net_out_min)::smallint  AS net_out_min,  max(net_out_max)::smallint  AS net_out_max,  sum(net_out_sum)::bigint  AS net_out_sum,  sum(net_out_cnt)  AS net_out_cnt,
       min(fan_min)::smallint      AS fan_min,      max(fan_max)::smallint      AS fan_max,      sum(fan_sum)::bigint      AS fan_sum,      sum(fan_cnt)      AS fan_cnt
FROM metrics_1h
GROUP BY 1, device_id
WITH NO DATA;

-- ---------------------------------------------------------------- policies
DO $$
DECLARE
    v text;
BEGIN
    FOREACH v IN ARRAY ARRAY['metrics_1m','metrics_5m','metrics_1h','metrics_4h'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_refresh_continuous_aggregate'
              AND hypertable_schema = current_schema()
              AND hypertable_name = v
        ) THEN
            PERFORM add_continuous_aggregate_policy(v,
                start_offset => CASE WHEN v = 'metrics_4h' THEN INTERVAL '12 hours' ELSE INTERVAL '3 hours' END,
                end_offset   => INTERVAL '1 minute',
                schedule_interval => INTERVAL '1 minute');
        END IF;
    END LOOP;
END $$;

-- Compression: only after the late-data window (ADR-008).
ALTER TABLE metrics_raw SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'ts DESC'
);
ALTER MATERIALIZED VIEW metrics_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'bucket DESC'
);
ALTER MATERIALIZED VIEW metrics_5m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'bucket DESC'
);
ALTER MATERIALIZED VIEW metrics_1h SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'bucket DESC'
);
ALTER MATERIALIZED VIEW metrics_4h SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'bucket DESC'
);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['metrics_raw','metrics_1m','metrics_5m','metrics_1h','metrics_4h'] LOOP
        PERFORM add_compression_policy(t, INTERVAL '2 days', if_not_exists => TRUE);
    END LOOP;
END $$;

-- Retention (ADR-008). 1h/4h are kept long-term and have no retention policy.
DO $$
DECLARE
    t text;
    d interval;
BEGIN
    FOR t, d IN SELECT * FROM (VALUES
        ('metrics_raw', INTERVAL '7 days'),
        ('metrics_1m',  INTERVAL '30 days'),
        ('metrics_5m',  INTERVAL '365 days')
    ) AS x(t, d) LOOP
        PERFORM add_retention_policy(t, d, if_not_exists => TRUE);
    END LOOP;
END $$;
