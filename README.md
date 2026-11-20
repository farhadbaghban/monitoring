# Sensor Monitoring

A Docker-based sensor monitoring service. Devices send timestamped JSON readings for seven sensors. The service accepts partial readings (including `null` sensor values), stores them as time-series data, and provides a web UI and HTTP API for querying device history.

## Contents

- [What runs](#what-runs)
- [Requirements and configuration](#requirements-and-configuration)
- [Start the application](#start-the-application)
- [Prepare the one-year chart fixture](#prepare-the-one-year-chart-fixture)
- [Send and query data](#send-and-query-data)
- [Database and retention](#database-and-retention)
- [Tests and load testing](#tests-and-load-testing)
- [Make commands](#make-commands)
- [Stop or reset the stack](#stop-or-reset-the-stack)
- [Project layout](#project-layout)

## What runs

All application processes and the database run in Docker Compose.

```text
Device / simulator
        |
        v
   nginx :8080  -- serves the web UI and routes HTTP requests
      /     \
     /       \-----------------------> api :8082 -- chart queries and device list
    v
ingest1 / ingest2 / ingest3
    |
    +----> Redpanda (Kafka API, :9092) ----> consumer1/2/3 ----> TimescaleDB (:5432)
    |
    +----> per-ingest durable WAL when Redpanda is unavailable
```

| Service | Role |
| --- | --- |
| `nginx` | Local entry point, web UI proxy, and load balancer for ingest requests. |
| `ingest1`, `ingest2`, `ingest3` | Authenticate and accept readings; forward them to Redpanda or persist them to a local write-ahead log (WAL) during broker outages. |
| `redpanda` | Kafka-compatible message broker. |
| `consumer1`, `consumer2`, `consumer3` | Consume readings in batches and write them to TimescaleDB. |
| `api` | Serves the device and chart APIs and the embedded web page. |
| `timescaledb` | PostgreSQL with TimescaleDB extensions and persistent Compose volumes. |
| `migrate` | One-shot service that applies `migrations/0001_schema.sql`; it exits successfully when done. |
| `simulator` | Creates recent sample history on startup, then sends live readings at the configured rate. |
| `seed-year` | One-shot command container used by `make seed-year` to add and verify a year-long chart fixture. |

## Requirements and configuration

Requirements: Docker Engine, Docker Compose v2, and Make. Go commands in the Makefile run in the `golang:1.25` Docker image; Go does not need to be installed on the host.

Compose supplies development defaults. To customize them, copy `.env.example` to `.env` and edit the values:

```sh
cp .env.example .env
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `POSTGRES_PASSWORD` | `postgres` | Local database password. |
| `INGEST_API_KEY` | `dev-api-key-change-me` | Key required by `POST /v1/ingest`. |
| `INTERNAL_TOKEN` | `internal-dev-token` | Token for internal backfill and aggregate-refresh routes. |
| `UI_TOKEN` | empty | Optional token required by the web UI and query API. |
| `SIM_DEVICES` | `20` | Number of simulated devices. |
| `SIM_DEVICE_OFFSET` | `1` | First simulated device ID. |
| `SIM_BACKFILL_DAYS` | `3` | Recent simulator history in days; allowed range is 0–6. Set to `0` to disable that history. |
| `SIM_BACKFILL_STEP` | `1m` | Time between historical simulator samples. |
| `SIM_RATE` | `1` | Live samples per second per simulated device. |
| `LOG_LEVEL` | `info` | Application log level. |
| `GOPROXY` | Go's default proxy | Optional Go module proxy used when building images and running Go commands. |

Ports are bound to loopback for local use: web UI `8080`, ingest `8081`, API `8082`, PostgreSQL `5432`, and Redpanda Kafka `9092`. The default keys and passwords are for local development only.

## Start the application

From the project directory:

```sh
make up
```

This builds the shared app image and the nginx image, starts the stack, and waits for the migration and health checks. `migrate` normally ends with `Exited (0)`. Open [http://localhost:8080](http://localhost:8080). The simulator sends integer readings and, by default, creates three days of recent history before sending live readings. The one-year fixture runs separately. To check service status and logs:

```sh
make ps
make logs
```

The UI starts with the first available device and a one-hour range. Search for a device by ID or name, select sensors, then choose a time range. Available ranges include `5m`, `15m`, `1h`, `6h`, `1d`, `7d`, `30d`, `90d`, and `1y`; custom start and end times can also be entered.

## Prepare the one-year chart fixture

On a local stack with disposable data, run `make up` first, then:

```sh
make seed-year
```

The command writes 2,190 synthetic samples for device `90`: one every four hours over 365 days. It refreshes the shared continuous aggregates for that window and verifies that the API returns readings in all 2,190 buckets. Use a disposable database because refreshing older buckets can affect other devices whose raw history has expired. Compose removes the seed container when it finishes.

To view it in the UI, open `http://localhost:8080`, search for `90`, select `90 · device-90`, then choose `1y`. The chart uses the four-hour aggregate tier for this range.

The new fixture sends integers from 0 to 100 for every sensor. The chart displays bucket averages with two decimal places, so an integer such as 42 appears as `42.00`, and a bucket with multiple readings can have a fractional average. Re-running the seed does not overwrite existing samples with the same `(device_id, ts)`. A fresh test volume is the way to verify that the chart contains only samples from the current generator.

## Send and query data

### Ingest a reading

`POST /v1/ingest` accepts one JSON object and requires the `X-API-Key` header. Example:

```sh
curl -X POST http://localhost:8080/v1/ingest \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-api-key-change-me' \
  -d '{"device_id":42,"ts":"2026-11-20T12:00:00Z","cpu":12,"mem":null,"disk":0}'
```

The seven sensor fields are `cpu`, `mem`, `disk`, `disk_io`, `net_in`, `net_out`, and `fan`. A sensor may be an integer from 0 to 100, `null`, or omitted. `null` and omitted readings are stored as SQL `NULL`; a numeric zero remains zero. `device_id` must be a positive 32-bit integer and `ts` must be a valid timestamp. The queue and database keep the existing scaled integer format (percentage × 100). Chart averages can contain decimals.

### Query devices and metrics

```sh
curl 'http://localhost:8080/v1/devices?limit=50&q=90'
curl 'http://localhost:8080/v1/devices/90/metrics?from=2025-11-20T00:00:00Z&to=2026-11-20T00:00:00Z&sensors=cpu,mem&max_points=2500'
```

`GET /v1/devices` supports `q`, `limit`, and `cursor`. `GET /v1/devices/{id}/metrics` accepts optional `from`, `to`, `sensors`, and `max_points`; the maximum query interval is 365 days. If `UI_TOKEN` is configured, provide it with `X-UI-Token` or the UI's `?token=...` query parameter. API responses include time buckets, per-sensor min/average/max values, and reading counts. Empty buckets and buckets containing only null readings have no sensor value.

Other routes:

| Route | Purpose | Authentication |
| --- | --- | --- |
| `GET /` | Web UI. | Optional `UI_TOKEN`. |
| `GET /healthz` | Health endpoint. | None. |
| `GET /readyz` on ingest services | Reports whether Kafka or available WAL can accept data. | None. |
| `POST /internal/backfill` | Batch insert historical simulator or fixture readings. | `X-Internal-Token`. |
| `POST /internal/refresh-caggs` | Refresh continuous aggregates after bulk historical inserts. | `X-Internal-Token`. |

## Database and retention

The migration creates the `metrics` database objects in schema `public`:

- `devices`: registered device IDs and display names.
- `metrics_raw`: raw timestamped sensor values, including nullable columns.
- `metrics_1m`, `metrics_5m`, `metrics_1h`, and `metrics_4h`: TimescaleDB continuous aggregate materialized views used for longer-range charts.

Connect to the database from the host through the Compose service:

```sh
docker compose exec timescaledb psql -U postgres -d metrics
```

Inside `psql`, use `\dt` to list regular tables and `\dm` to list materialized views. For example, to inspect the one-year fixture:

```sql
SELECT device_id, count(*) AS samples, min(ts) AS oldest, max(ts) AS newest
FROM metrics_raw
WHERE device_id = 90
GROUP BY device_id;
```

The time-series table is a hypertable partitioned into six-hour chunks. Queries select an appropriate tier and bucket size to keep chart responses bounded. Current retention policy removes raw readings after 7 days, one-minute aggregates after 30 days, and five-minute aggregates after 365 days. Hourly and four-hour aggregates have no retention policy. Compression policies apply after two days.

Database and broker data live in named Docker volumes. `make down` stops and removes containers but preserves those volumes.

## Tests and load testing

`make test-null`, `make smoke`, `make chaos`, and `make load` each create an isolated Compose project. The runner removes that project's containers, network, and volumes on exit, including on failure. The temporary frontend is available at `http://localhost:18080` while the stack is up. The Go unit test targets use a disposable Go container without starting the application stack.

| Command | What it checks |
| --- | --- |
| `make test` | All Go unit tests. |
| `make test-unit` | Unit tests under `internal/`. |
| `make test-null` | Unit tests plus Compose integration checks for `null`, omitted, and zero sensor readings through ingestion, database storage, and chart output. |
| `make smoke` | End-to-end ingest and persistence, a year-long chart query, and recovery from a brief Kafka outage. |
| `make chaos` | Kafka outage, ingest failover, WAL capacity behavior, and database recovery. |
| `make load` | Live traffic ramp with concurrent year-chart reads, persistence checks, resource sampling, and HTML result graphs. The test stack and its volumes are removed afterward. |

The load test defaults to 100 devices, then uses checkpoints of 300, 700, and 1000 before doubling up to 10000. Override the run with `LOAD_START_DEVICES`, `LOAD_MAX_DEVICES`, and `LOAD_STAGE_SECONDS`; set `LOAD_OUTPUT_DIR` to change the local report directory (default `docs/loadtest`). Output contains JSON results, resource samples, and three HTML graphs.

The test runner can also be invoked without Make:

```sh
bash scripts/compose-test.sh null
bash scripts/compose-test.sh smoke
bash scripts/compose-test.sh chaos
bash scripts/compose-test.sh load
```

## Make commands

| Command | Purpose |
| --- | --- |
| `make help` | Show the command summary. |
| `make up` | Build and start all normal services, waiting for health checks. |
| `make down` | Stop and remove normal Compose containers and network; preserve volumes. |
| `make ps` | Show normal Compose service status. |
| `make logs` | Follow recent logs from the normal stack. |
| `make build` | Compile all Go packages in Docker. |
| `make test` | Run all Go unit tests in Docker. |
| `make test-unit` | Run internal-package tests in Docker. |
| `make test-null` | Run isolated null-sensor integration tests. |
| `make smoke` | Run isolated end-to-end smoke tests. |
| `make chaos` | Run isolated recovery/failure tests. |
| `make load` | Run isolated load tests and create local reports. |
| `make migrate` | Apply the SQL migration to the normal running database. |
| `make seed-year` | Insert and verify the persistent one-year chart fixture. |
| `make fmt` | Format Go source files. |
| `make vet` | Run `go vet` across Go packages in Docker. |
| `make deps` | Run `go mod tidy` in Docker. |

## Stop or reset the stack

Stop the local services while retaining database and broker state:

```sh
make down
```

To remove all normal Compose volumes and start with an empty database and broker next time:

```sh
docker compose down --volumes --remove-orphans
make up
```

The `down --volumes` command permanently removes the normal stack's local database, broker, and WAL data. Test reports stored in the project directory are kept.

## Project layout

| Path | Contents |
| --- | --- |
| `cmd/api` | API service entry point. |
| `cmd/consumer` | Kafka consumer entry point. |
| `cmd/ingest` | Ingest HTTP service entry point. |
| `cmd/simulator` | Demo device and recent-history simulator. |
| `cmd/testbench` | Integration, smoke, chaos, load, chart fixture, and report helpers. |
| `internal/api` | HTTP API handlers and embedded frontend. |
| `internal/auth` | API-key hashing and comparison helpers. |
| `internal/config` | Environment configuration and structured logging. |
| `internal/consumer` | Kafka consumption and database writes. |
| `internal/ingest` | JSON parsing, nullable sensor handling, authentication, Kafka publishing, and WAL fallback. |
| `internal/model` | Shared sample, wire, device, and sensor types. |
| `internal/queue` | Kafka producer and consumer setup. |
| `internal/simdata` | Deterministic synthetic sensor readings. |
| `internal/store` | Database writes, continuous aggregate refresh, device lookup, and chart queries. |
| `internal/wal` | Durable local write-ahead log used during broker failures. |
| `migrations` | Database schema, continuous aggregates, compression, and retention policies. |
| `nginx` | Local reverse proxy configuration and image definition. |
| `scripts/compose-test.sh` | Isolated Docker test orchestration and cleanup. |
| `docker-compose.yml` | Normal application stack and one-shot commands. |
| `docker-compose.test.yml` | Test-only overrides and benchmark container. |
