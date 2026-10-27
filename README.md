# 🌦️ Weather Data Pipeline — Batch ETL + Kafka Streaming + Airflow Orchestration

A production-style weather data project that ingests daily data from the Open-Meteo API into PostgreSQL through **two parallel paths** — a batch ETL pipeline (pandas) orchestrated by Apache Airflow, and a Kafka streaming pipeline (confluent-kafka) — visualized with a Streamlit dashboard.

![CI](https://github.com/Kacper277/etl-pipeline/actions/workflows/ci.yml/badge.svg)

## 📑 Contents

- [Description](#-description)
- [Architecture](#-architecture)
- [Quick Start (Batch Path)](#-quick-start-batch-path)
- [Streaming Path Quick Start](#-streaming-path-quick-start)
- [Orchestration (Apache Airflow)](#-orchestration-apache-airflow)
- [Configuration](#️-configuration)
- [Usage Examples](#-usage-examples)
- [API Documentation](#-api-documentation)
- [Database Schema](#️-database-schema)
- [SQL Query Examples](#-sql-query-examples)
- [Design Decisions: Batch vs Streaming](#-design-decisions-batch-vs-streaming)
- [Testing](#-testing)
- [Development](#️-development)
- [Technologies Used](#-technologies-used)
- [Future Improvements](#-future-improvements)
- [Author & Contact](#-author--contact)
- [License](#-license)

## 📖 Description

**What this project does:** It runs the same ingestion problem two ways over the same source and the same analytical layer — with Airflow orchestrating the batch path.

- The **batch path** pulls up to 16 forecast days plus up to 92 past days (~3 months) of daily weather from the free Open-Meteo API, saves the raw JSON, cleans it with pandas, validates data quality, exports a processed CSV, and upserts it into PostgreSQL.
- The **streaming path** publishes individual readings to Kafka, a micro-batching consumer validates them and writes them to PostgreSQL with idempotent upserts, routing rejects to a dead-letter topic.
- A **Streamlit dashboard** visualizes the stored data.

**Why it was built:** As a hands-on portfolio project to demonstrate practical data engineering fundamentals: REST APIs, pandas processing, SQL and relational databases, SQLAlchemy, Kafka, testing, Docker, and CI/CD — and, above all, knowing when to reach for each ingestion pattern.

**Target audience:** Hiring managers evaluating candidates for junior/trainee Data Engineer positions.

## 🏗️ Architecture

```
              ORCHESTRATION (Airflow 2.11.2, LocalExecutor)
┌─────────────────────────────────────────────────────────────────────┐
│  DAG weather_batch_etl_pipeline  (cron 0 0 * * *, catchup=False)    │
│    extract >> transform >> validate >> load >> summary              │
│    reuses src/extract, src/transform, src/load as-is                │
│    schedules BATCH only - does NOT manage the streaming path        │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
                BATCH PATH (default, fastest to run)
┌─────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  OPEN-METEO │   │   EXTRACT    │   │  TRANSFORM   │   │   POSTGRES   │
│  PUBLIC API │-->│ (fetch_data) │-->│ (process_)   │-->│ (SQLAlchemy) │
│             │   │              │   │              │   │              │
└─────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
 JSON Response     Raw JSON File      Clean CSV/Table    weather_daily
                                                                ▲
                STREAMING PATH (low-latency, NOT scheduled)     │
┌─────────────┐   ┌──────────┐   ┌──────────┐   ┌───────────┐   │
│  OPEN-METEO │   │ PRODUCER │-->│  KAFKA   │-->│ CONSUMER  │---┘
│  PUBLIC API │   │          │   │ 3 topics │   │ micro-    │--> weather_readings
└─────────────┘   └──────────┘   └────┬─────┘   │ batch UPSERT
                                      ▼         └──────┬───►
                                 DLQ topic      (rejected records)
```

- **Batch:** `src/extract/` → `src/transform/` → `src/load/` — fetch, pandas cleaning + validation, upsert into `weather_daily`.
- **Streaming:** `src/streaming/` — producer, JSON serialization, schema validation, micro-batching consumer with DLQ routing; rows land in `weather_readings`.
- **Convergence:** the view `weather_readings_daily` is shaped exactly like `weather_daily`, so the analytical SQL in `sql/queries.sql` runs unchanged against either table.

### 🗃️ Data lake zones (local)

The `data/` directory is organized as a miniature lake, mirroring the zone pattern used in cloud lakes:

| Zone | Path | Format | Analogy |
|---|---|---|---|
| Bronze (raw) | `data/raw/*.json` | as-received API JSON | S3 raw zone |
| Silver (curated) | `data/processed/weather_daily/date=YYYY-MM-DD/*.parquet` | columnar, date-partitioned | S3 curated zone |
| Serving | PostgreSQL tables | relational | warehouse/serving layer |

The silver zone is written by `DataTransformer.export_partitioned_parquet` (idempotent — re-runs overwrite, never duplicate). `terraform/` contains a reviewable AWS IaC scaffold (versioned, encrypted S3 bucket + least-privilege IAM) showing where the curated zone would live in the cloud.

Why both paths in one repository: batch covers history, backfill and replay; streaming covers near-real-time arrival. Shipping both over one source and one analytical layer shows the trade-off instead of telling it — see [Design Decisions](#-design-decisions-batch-vs-streaming).

## 🚀 Quick Start (Batch Path)

1. **Clone the repository**
   ```bash
   git clone https://github.com/Kacper277/etl-pipeline.git
   cd etl-pipeline
   ```

2. **Install dependencies** (Python 3.11 recommended)
   ```bash
   python -m venv .venv
   .venv\Scripts\activate        # Windows  (use source .venv/bin/activate on Linux/macOS)
   pip install -r requirements.txt
   ```

3. **Start the database**
   ```bash
   docker compose up -d postgres
   ```

4. **Configure environment**
   ```bash
   copy .env.example .env        # Windows
   # cp .env.example .env        # Linux/macOS
   # Edit .env: set POSTGRES_PASSWORD and DATABASE_URL
   ```

5. **Run the pipeline**
   ```bash
   python -m src.pipeline
   ```

6. **Verify results**
   ```bash
   docker compose exec postgres psql -U etl_user -d weather_db -c "SELECT * FROM weather_daily LIMIT 7;"
   ```

7. **Launch the dashboard**
   ```bash
   streamlit run src/dashboard/app.py
   # or: make dashboard
   ```

That is the complete batch path — everything below is optional.

## 📡 Streaming Path Quick Start

Prerequisites: steps 1, 2 and 4 from the batch Quick Start (repo, dependencies, `.env`).

1. **Start Kafka and the UI** (waits for the broker healthcheck)
   ```bash
   docker compose up -d --wait kafka kafka-ui
   ```

2. **Create the topics** (idempotent — safe to re-run)
   ```bash
   python scripts/create_topics.py
   # Created topic weather.readings.raw / processed / dlq
   ```

3. **Publish a demo batch of readings** (from the host; note the env override)
   ```bash
   # PowerShell: $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
   # bash:       export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   python -m src.streaming.produce_demo
   # Published 30 readings to topic weather.readings.raw
   ```

4. **Run the consumer** (Ctrl+C to stop gracefully)
   ```bash
   python -m src.streaming.consume
   # Batch: 30 loaded, 0 rejected, 30 rows written
   # Rows now in weather_readings: 30
   ```

5. **Verify the rows landed**
   ```bash
   docker compose exec postgres psql -U etl_user -d weather_db ^
     -c "SELECT station_id, COUNT(*) FROM weather_readings GROUP BY station_id ORDER BY station_id;"
   ```

6. **Inspect visually** — open [http://localhost:8088](http://localhost:8088): the Kafka UI shows the three topics, partition layout, and the consumer group's committed offsets.

## ⏰ Orchestration (Apache Airflow)

**Purpose:** Apache Airflow schedules and monitors the batch ETL — daily runs at 00:00 UTC, retries with backoff, task-level observability in the UI, and safe backfills. It orchestrates the **batch path only**; the Kafka consumer is a long-lived process supervised by the container runtime, not a scheduled task, and is deliberately invisible to Airflow.

**Minimal stack** (single node, `LocalExecutor` — no Redis, no workers, no flower; the official Celery quick-start bundle is documented as not production-ready and is overkill here):

```
postgres-airflow (metadata DB, dedicated) ── airflow-init (migrations + admin user, runs once)
                                │
              airflow-scheduler ──── airflow-webserver (:8080)
                     └── tasks run in-process (LocalExecutor), importing the mounted ./src
```

**Quick start** (requires the batch Quick Start steps 1–4 first):

```bash
docker build -f docker/Dockerfile.airflow -t weather-airflow:2.11.2 .   # or: docker compose --profile airflow build
make airflow-up        # POSIX shell; = docker compose --profile airflow up -d --wait ...
# -> postgres_airflow Healthy, airflow_init Exited(0), scheduler + webserver Healthy
```

1. Open [http://localhost:8080](http://localhost:8080) and log in with the documented local defaults (`admin` / `admin` from `.env.example` — override them for anything beyond a demo).
2. Find the DAG `weather_batch_etl_pipeline` (the only DAG — example DAGs are disabled), unpause it, then trigger it manually from the UI or:
   ```bash
   make airflow-trigger   # = airflow dags trigger weather_batch_etl_pipeline
   ```
3. A one-shot run without the scheduler (useful for local verification):
   ```bash
   docker exec airflow_scheduler airflow dags test weather_batch_etl_pipeline 2026-10-21T03:00
   # -> "Load complete: 81 rows in table (2 new)" on first run; "(0 new)" on re-runs (idempotent upsert)
   ```
4. Re-run the same command: the row delta is 0 — the `ON CONFLICT DO NOTHING` upsert makes retried/orchestrated runs idempotent.

**How the DAG reuses `src/`:** every task is a thin callable that resolves parameters from environment variables (same names and defaults as the batch CLI) and delegates — `DataExtractor` + `save_raw_json` (extract), `DataTransformer` (transform), `DataValidator` (validate), `DatabaseLoader` (load). No business logic lives in the DAG file; tasks hand off files via a named volume and push only compact values (paths, counts, a 5-float stats dict) through XCom.

**Task list:**

| Task | What it does | Retry policy |
|---|---|---|
| `extract` | Fetches the 16+92-day window via `DataExtractor`, saves raw JSON | retries on 5xx / timeout / connection errors only |
| `transform` | `DataTransformer`: daily metrics + `temp_range`/`temp_category`, exports processed CSV | standard retries |
| `validate` | `DataValidator` gate: impossible values **fail the run**; nulls at the window edge **warn** (reported via XCom) | standard retries |
| `load` | `DatabaseLoader` upsert into `weather_daily`; row counts cross-checked against the table delta | standard retries (idempotent) |
| `summary` | Logs + XCom-pushes fetched/transformed/inserted counts and duration | — |

**Configuration** (added to `.env.example`):

| Variable | Purpose | Example |
|---|---|---|
| `AIRFLOW_META_USER` / `AIRFLOW_META_DB` | Metadata DB credentials (dedicated instance) | `airflow_meta` / `airflow_meta` |
| `AIRFLOW_META_PASSWORD` | Metadata DB password | *(secret — set in `.env`)* |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Webserver admin (documented local defaults) | `admin` / `admin` |
| `AIRFLOW_UID` | Host UID for mounted volumes on Linux | `50000` |
| `AIRFLOW_CONN_WEATHER_DB` | Built by Compose from the `POSTGRES_*` vars — no separate value needed | `postgres://etl_user:***@postgres:5432/weather_db` |

Make targets (POSIX shell required): `airflow-up`, `airflow-down` (keeps metadata), `airflow-logs`, `airflow-trigger`, `airflow-reinit` (drops metadata volume — deletes run history).

## ⚙️ Configuration

| Variable | Purpose | Example |
|---|---|---|
| `POSTGRES_HOST` | Database host | `localhost` |
| `POSTGRES_PORT` | Database port | `5432` |
| `POSTGRES_USER` | Database user | `etl_user` |
| `POSTGRES_PASSWORD` | Database password | *(secret — set in `.env`)* |
| `POSTGRES_DB` | Database name | `weather_db` |
| `DATABASE_URL` | SQLAlchemy connection string | `postgresql+psycopg2://user:pass@localhost:5432/weather_db` |
| `OPEN_METEO_BASE_URL` | Open-Meteo endpoint | `https://api.open-meteo.com/v1/forecast` |
| `LATITUDE` | Forecast latitude | `52.23` |
| `LONGITUDE` | Forecast longitude | `21.01` |
| `FORECAST_DAYS` | Days of forecast (1–16, API max) | `16` |
| `PAST_DAYS` | Days of past data (0–92, API max) | `92` |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker address **from inside Docker** | `kafka:29092` |
| `KAFKA_BOOTSTRAP_SERVERS_HOST` | Broker address **from the host machine** | `localhost:9092` |
| `KAFKA_TOPIC_RAW` | Raw readings topic | `weather.readings.raw` |
| `KAFKA_TOPIC_PROCESSED` | Validated readings topic | `weather.readings.processed` |
| `KAFKA_TOPIC_DLQ` | Dead-letter topic for rejects | `weather.readings.dlq` |
| `KAFKA_CONSUMER_GROUP` | Consumer group id | `weather-ingest-v1` |
| `KAFKA_BATCH_SIZE` | Max records per consumer micro-batch | `50` |
| `KAFKA_BATCH_TIMEOUT_S` | Seconds before flushing a partial batch | `5` |

**Two bootstrap addresses on purpose:** the broker advertises two listeners — an internal one (`kafka:29092`, resolvable only on the Compose network) and an external one (`localhost:9092`, for host-side scripts, pytest and CI). Using the wrong one produces the classic "connects, then times out waiting for node assignment" failure.

**Database connection:** the app talks to PostgreSQL via SQLAlchemy. Inside Docker Compose use `postgres` as the host (service name); locally use `localhost`.

**API endpoint documentation:** see [Open-Meteo Docs](https://open-meteo.com/en/docs).

## 💻 Usage Examples

Run the full batch pipeline (extract + transform + load):

```bash
python -m src.pipeline
```

Use individual batch modules programmatically:

```python
from src.extract.fetch_data import DataExtractor
from src.extract.save_raw import save_raw_json

extractor = DataExtractor("https://api.open-meteo.com/v1/forecast")
data = extractor.fetch_forecast(52.23, 21.01, days=16, past_days=92)
save_raw_json(data)
```

```python
from src.transform.process_data import DataTransformer
from src.transform.validate_data import DataValidator

transformer = DataTransformer("data/raw/weather_raw.json")
df = transformer.add_derived_columns(transformer.parse_daily_metrics())
print(DataValidator().full_validation_report(df))
transformer.export_to_csv(df)
```

Publish and consume streaming readings (see the [Streaming Path Quick Start](#-streaming-path-quick-start) for setup):

```bash
python -m src.streaming.produce_demo --stations 3 --per-station 10
python -m src.streaming.consume
```

### 📊 Dashboard

A Streamlit app (`src/dashboard/app.py`) reads **both serving tables** — `weather_daily` (batch) and `weather_readings` (streaming, with per-station charts) — and shows KPI cards (avg max/min temp, total rain, rainy-day count), temperature line and rainfall bar charts, weekly averages, and sidebar filters. Run it with `streamlit run src/dashboard/app.py` or `docker compose up -d dashboard` (then open [http://localhost:8501](http://localhost:8501)).

*(Screenshot placeholders: `docs/screenshots/pipeline_run.png`, `docs/screenshots/psql_results.png`, `docs/screenshots/kafka_ui.png`)*

## 🌐 API Documentation

**Endpoint used:** `GET https://api.open-meteo.com/v1/forecast`

**Request parameters:**

| Parameter | Type | Description |
|---|---|---|
| `latitude` | float | Decimal degrees (-90..90) |
| `longitude` | float | Decimal degrees (-180..180) |
| `daily` | csv | `temperature_2m_max,temperature_2m_min,rain_sum` |
| `timezone` | string | `auto` |
| `forecast_days` | int | 1–16 (API maximum) |
| `past_days` | int | 0–92, ~3 months of past data (API maximum) |

**Sample call:**

```
https://api.open-meteo.com/v1/forecast?latitude=52.23&longitude=21.01&daily=temperature_2m_max,temperature_2m_min,rain_sum&timezone=auto&forecast_days=16&past_days=92
```

**Response structure (relevant part):**

```json
{
  "daily": {
    "time": ["2026-10-10", "2026-10-11"],
    "temperature_2m_max": [18.4, 19.1],
    "temperature_2m_min": [9.2, 10.0],
    "rain_sum": [0.3, 0.0]
  }
}
```

## 🗄️ Database Schema

**Batch table** — one row per day ([`sql/schema.sql`](sql/schema.sql)):

```
┌────────────────────────────────────────────┐
│               weather_daily                │
├────────────┬───────────────┬───────────────┤
│ id         │ SERIAL        │ PK            │
│ date       │ DATE          │ UNIQUE, NOT N │
│ temp_max_c │ DECIMAL(5,2)  │ max temp (°C) │
│ temp_min_c │ DECIMAL(5,2)  │ min temp (°C) │
│ temp_range │ DECIMAL(5,2)  │ max - min     │
│ rain_mm    │ DECIMAL(6,2)  │ rain (mm)     │
│ loaded_at  │ TIMESTAMP     │ default NOW() │
└────────────┴───────────────┴───────────────┘
```

**Streaming table** — one row per station reading ([`sql/schema_streaming.sql`](sql/schema_streaming.sql)):

| Column | Type | Notes |
|---|---|---|
| `id` | SERIAL | PK |
| `station_id` | TEXT NOT NULL | Kafka partition key |
| `observed_at` | TIMESTAMPTZ NOT NULL | reading timestamp (UTC) |
| `temp_max_c` / `temp_min_c` | NUMERIC(5,2) | °C |
| `rain_mm` | NUMERIC(6,2) | mm |
| `ingested_at` | TIMESTAMPTZ DEFAULT NOW() | load time |

**Why `UNIQUE (station_id, observed_at)`:** the consumer commits Kafka offsets only *after* a successful database write, so a crash between the two re-delivers the batch (at-least-once). The unique constraint plus `ON CONFLICT DO NOTHING` makes those duplicates collapse idempotently instead of double-counting. Indexes: `observed_at DESC`, `station_id`.

**Convergence view:** `weather_readings_daily` (in the same file) rolls the stream to daily grain with exactly the `weather_daily` columns, so `sql/queries.sql` runs unchanged against either path.

## 🔍 SQL Query Examples

```sql
-- Basic statistics (runs as-is against weather_daily
-- or weather_readings_daily — same columns)
SELECT
    ROUND(AVG(temp_max_c), 2) AS avg_temp_max,
    ROUND(AVG(temp_min_c), 2) AS avg_temp_min,
    ROUND(SUM(rain_mm), 2)    AS total_rain
FROM weather_daily;

-- Rainy days percentage
SELECT
    COUNT(*) FILTER (WHERE rain_mm > 0) AS rainy_days,
    COUNT(*)                            AS total_days,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rain_mm > 0) / COUNT(*), 1) AS rainy_pct
FROM weather_daily;

-- Streaming table: readings per station
SELECT station_id, COUNT(*) AS readings,
       MIN(observed_at) AS first, MAX(observed_at) AS last
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;
```

Sample output of the last query against the demo run (real captured output, 3 demo stations):

```
 station_id | readings |          first           |          last
------------+----------+--------------------------+-------------------------
 gdansk-3   |       18 | 2026-10-19 23:17:15+00   | 2026-10-20 00:10:31+00
 krakow-2   |       18 | 2026-10-19 23:17:15+00   | 2026-10-20 00:10:31+00
 warsaw-1   |       18 | 2026-10-19 23:17:15+00   | 2026-10-20 00:10:31+00
```

More analytical queries (hot days, temperature swings, weekly aggregation, a window-function "latest reading per station", and a batch-vs-stream daily comparison through the convergence view) are in [`sql/queries.sql`](sql/queries.sql).

## ⚖️ Design Decisions: Batch vs Streaming

**When batch is the right choice** — and why it is this project's default: the data arrives in large historical chunks (up to ~3 months in one API call), so one request, one DataFrame pass and one upsert are simpler, cheaper and easier to reason about than any streaming setup. Batch is also the natural tool for backfill and replay: re-running `python -m src.pipeline` is idempotent and re-derives everything from source.

**When streaming is the right choice:** readings arrive continuously and consumers want them with low latency — dashboards, alerting, downstream event-driven systems. Kafka also decouples producers from consumers and retains every record, so a consumer can be re-deployed, re-partitioned or added later and replay from `earliest`.

**Why a plain producer + consumer, not a stream processing framework:** the per-record work here is stateless — decode, validate, map to a row, upsert. A framework (Kafka Streams, Flink, Faust) adds a runtime, state management and dependency risk for capabilities this problem does not need. A micro-batching consumer (N records or T seconds, configurable) gets the write-efficiency benefit without the machinery. If this pipeline ever grows joins or windowed aggregation, that is the point to revisit.

**Why the partition key is the station identifier:** Kafka guarantees ordering only within a partition. Keying by station keeps each station's readings ordered and co-located, so per-station logic stays correct if partitions are added for throughput.

**Why at-least-once plus an idempotent upsert:** committing offsets only after a successful database write risks re-delivery on crash; committing before it risks data loss. For this workload, re-delivery is free to accept because `UNIQUE (station_id, observed_at)` + `ON CONFLICT DO NOTHING` collapses duplicates — so the simple, at-least-once design is correct without transactions. Exactly-once (consumer + producer in one transaction) would add complexity that the upsert already makes unnecessary.

**Why Airflow orchestrates the batch path only:** batch work is naturally scheduled (a daily window closes, one run processes it) and benefits from retries, observability and backfill — exactly what a scheduler provides. The streaming consumer is the opposite shape: a long-lived process whose "scheduling" is container supervision (`restart: always`), not cron. Wrapping it in a DAG would mean a task that never ends — a well-known Airflow anti-pattern. If cross-path coordination is ever needed (e.g. "trigger batch only after N streaming readings"), the path to it is sensors/assets, listed in Future Improvements.

**Why Airflow 2.11.2 and not 3.x:** Airflow 3.3.x is the current line and 2.x has left its support window, but this project pins `apache/airflow:2.11.2-python3.11` deliberately: the 2.x authoring surface (`with DAG(...)`, `PythonOperator`, cron schedules) is what a trainee is expected to know, is vastly better documented, and every 2.x idiom used here maps forward. The DAG uses the 2.4+ `schedule=` parameter (not the deprecated `schedule_interval`), so the eventual 3.x upgrade is a config change, not a rewrite. Trade-off accepted knowingly: 2.11.2 will not receive security fixes.

**Why LocalExecutor and the minimal stack:** one machine, one DAG, a handful of tasks — LocalExecutor runs tasks in-process in the scheduler container with no Redis, no workers, no flower. The official Celery quick-start bundle is explicitly documented as not production-ready and would triple the footprint. Scaling out is a documented upgrade path (CeleryExecutor), not a present need.

**Why a separate metadata database:** Airflow's migrations and run history must never share a schema with business data — a metadata outage or migration must not be able to touch `weather_daily`, and the weather database's size/backup profile is unrelated. Hence the dedicated `postgres-airflow` service; the DAG reaches the weather DB via an Airflow-native connection (`AIRFLOW_CONN_WEATHER_DB`) built from the same `POSTGRES_*` variables the app uses.

**Why the Airflow dependency file is split from `requirements.txt`:** Airflow pins its own dependency universe via constraints files. Notably, Airflow 2.x requires **SQLAlchemy <2.0** — the Airflow runtime runs `SQLAlchemy==1.4.54` (verified end-to-end: the orchestrated load task upserts correctly), while the app/test environment keeps `SQLAlchemy>=2.0.0`. Mixing the two in one file would force one environment to lie about its dependencies or break CI. `docker/requirements-airflow.txt` pins the Airflow-runtime set to the constraint-resolved versions; the `src/` code paths used by the DAG are API-identical under both (verified by a real DAG run).

## 🧪 Testing

Two tiers, separated by a pytest marker:

```bash
pytest                       # unit tests only — fast, no broker required (default)
pytest -m integration -v     # integration tests — needs a live broker (docker compose up -d --wait kafka)
pytest --cov=src             # unit tests with coverage
```

- **Unit tests** (run in every CI push): mocked HTTP responses (extract), DataFrame building, derived columns, statistics, validation (transform), mocked engines (load), and the whole streaming logic — producer encoding/queueing, consumer batch/DLQ/commit semantics, and the validation gate — against mocked `confluent_kafka` objects. **No real API calls, database connections or brokers.**
- **Integration tests** (`tests/integration/`, marker `integration`, deselected by default): a full round trip against a live broker — create topic, produce, consume, assert transformed rows — plus malformed-record → DLQ routing. Auto-skipped when `KAFKA_BOOTSTRAP_SERVERS_HOST` is unset, so a fresh clone's plain `pytest` never fails.
- **DAG integrity tests** (`tests/test_dag_integrity.py`): assert exactly one DAG is discovered with no import errors, the 5-task chain, `catchup=False`, the daily schedule, `retries >= 1` and an `execution_timeout` on every task, and a static guard against oversized XCom payloads. They need the Airflow dependency set (`docker/requirements-airflow.txt`, installed under the official constraints file in CI) but **no Airflow infrastructure** — no scheduler, webserver or containers. On machines without Airflow they skip with a coherent message. Run them explicitly with `pytest tests/test_dag_integrity.py`.
- Coverage is printed to the terminal; use `--cov-report=html` for `htmlcov/index.html`.

## 🛠️ Development

**Code structure:**

```
src/
├── extract/     # API client + raw persistence            (batch)
├── transform/   # pandas processing + validation          (batch)
├── load/        # schema + database loading               (batch)
├── streaming/   # config, schemas, serialization,         (streaming)
│                # producer, handlers, consumer
├── dashboard/   # Streamlit visualization                 (analytics)
airflow/
└── dags/        # weather_batch_etl_pipeline.py           (orchestration)
docker/
├── Dockerfile.airflow         # pinned Airflow image + runtime deps
└── requirements-airflow.txt   # Airflow-runtime dependency set
terraform/        # AWS IaC scaffold for the lake (S3 + IAM)  (cloud)
scripts/
└── create_topics.py   # idempotent Kafka topic provisioning
sql/
├── schema.sql           # batch table DDL
├── schema_streaming.sql # streaming table + convergence view
└── queries.sql          # analytical queries (both paths)
```

**Adding a new daily metric** (batch): add the field to `DataExtractor.DAILY_FIELDS` → map it in `parse_daily_metrics` (`src/transform/process_data.py`) → extend `src/load/schema.py` and `sql/schema.sql` → add tests.

**Adding a new metric to the stream:** extend `WeatherEvent` + `validate_event` (`src/streaming/schemas.py`) → map it in `event_to_row` (`src/streaming/handlers.py`) → extend `weather_readings` in `sql/schema_streaming.sql` → add tests at both tiers.

**Adding a new task to the DAG:** write a thin callable in `airflow/dags/weather_batch_etl_pipeline.py` that delegates to a `src/` module (no business logic in the DAG), wire it into the chain, extend `tests/test_dag_integrity.py` if the chain shape changes.

**Adding a new source** (e.g. another weather provider): implement it behind the existing extract/streaming interfaces; the transform and load layers are source-agnostic by design.

**Contribution guidelines:** create a feature branch, keep PEP 8 (flake8 must pass), add tests for new behavior at the appropriate tier, and keep PRs focused.

## 🧰 Technologies Used

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.11 | Main language |
| requests | ≥ 2.31 | HTTP client |
| pandas | ≥ 2.1 | Data processing |
| pyarrow | ≥ 15 | Columnar Parquet lake output |
| SQLAlchemy | ≥ 2.0 | Database ORM/engine |
| psycopg2-binary | ≥ 2.9 | PostgreSQL driver |
| PostgreSQL | 15 (Alpine) | Target database |
| Apache Kafka | 4.3.1 (pinned, KRaft) | Streaming message broker |
| confluent-kafka | ≥ 2.5 | Kafka Python client (librdkafka) |
| kafka-ui (provectuslabs) | v0.7.2 (pinned) | Broker/topic visualization |
| Apache Airflow | 2.11.2-python3.11 (pinned image) | Batch path orchestration |
| pytest | ≥ 7.4 | Testing |
| flake8 | ≥ 6.1 | Linting |
| Docker / Compose | ≥ 20.10.4 | Local infrastructure |
| Streamlit | ≥ 1.31 | Dashboard visualization |
| Terraform | ≥ 1.5 (scaffold) | IaC for the AWS lake (not applied by CI) |
| GitHub Actions | — | CI/CD |

Kafka has been KRaft-only since 4.0 (no ZooKeeper); the broker runs in single-node combined mode via the official `apache/kafka:4.3.1` image.

## 🔮 Future Improvements

- **Airflow 3.x upgrade** (the DAG already uses the 3.x-compatible `schedule=` parameter; the remaining work is the image pin and config keys)
- **CeleryExecutor** for the Airflow stack when multiple concurrent DAG runs need more than one worker
- **Alerting** on DAG failure via `on_failure_callback` (email/Slack) instead of UI-only observability
- **Asset/sensor-based coordination** between the paths (e.g. trigger batch maintenance after streaming volume thresholds) once on Airflow 3 assets
- **Schema Registry** for the Kafka events (evolving the event contract safely)
- **Exactly-once semantics** via consumer/producer transactions, replacing the upsert-based deduplication
- Cloud **dashboard hosting** (deploy the Streamlit app or build a custom web UI)
- Deployment to the **cloud** (e.g. AWS RDS + ECS / MSK) — the `terraform/` scaffold is the starting point
- **Data quality framework** (Great Expectations) instead of the basic validator
- **PySpark lab** on the Parquet lake (the pandas transform stage is the unit of work a Spark job would parallelize) and a **Databricks** trial on the curated zone

## 👤 Author & Contact

- **GitHub:** [Kacper277](https://github.com/Kacper277)

## 📄 License

MIT License — see the [LICENSE](LICENSE) file.
