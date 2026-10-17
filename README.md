# 🌦️ Weather ETL Pipeline

A production-style ETL pipeline that extracts daily weather data from the Open-Meteo API, transforms it with pandas, and loads it into PostgreSQL.

![CI](https://github.com/Kacper277/etl-pipeline/actions/workflows/ci.yml/badge.svg)

## 📑 Contents

- [Description](#-description)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Usage Examples](#-usage-examples)
- [Dashboard](#-dashboard)
- [API Documentation](#-api-documentation)
- [Database Schema](#-database-schema)
- [SQL Query Examples](#-sql-query-examples)
- [Testing](#-testing)
- [Development](#-development)
- [Technologies Used](#-technologies-used)
- [Future Improvements](#-future-improvements)
- [Author & Contact](#-author--contact)
- [License](#-license)

## 📖 Description

**What this project does:** It runs a full Extract → Transform → Load cycle. The extract stage pulls up to 16 forecast days plus up to 92 past days (~3 months) of daily weather from the free Open-Meteo API and saves the raw JSON to disk. The transform stage cleans the payload with pandas, derives extra metrics, validates data quality, and exports a processed CSV. The load stage inserts the records into a PostgreSQL table using idempotent UPSERTs. A Streamlit dashboard visualizes the stored data.

**Why it was built:** As a hands-on portfolio project to demonstrate practical data engineering fundamentals: working with REST APIs, pandas-based data processing, SQL and relational databases, SQLAlchemy, testing, Docker, and CI/CD.

**Target audience:** Hiring managers evaluating candidates for junior/trainee Data Engineer positions.

## 🏗️ Architecture

```
┌─────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  OPEN-METEO │   │   EXTRACT    │   │  TRANSFORM   │   │   POSTGRES   │
│  PUBLIC API │-->│ (fetch_data) │-->│ (process_)   │-->│ (SQLAlchemy) │
│             │   │              │   │              │   │              │
└─────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
 JSON Response     Raw JSON File      Clean CSV/Table    Analytical Queries
```

- **Extract** — `src/extract/` — HTTP fetch with timeouts & error handling, raw JSON persistence.
- **Transform** — `src/transform/` — DataFrame cleaning, derived columns, statistics, validation.
- **Load** — `src/load/` — SQLAlchemy engine, DDL, UPSERT inserts (`ON CONFLICT DO NOTHING`).

## 🚀 Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/etl-pipeline.git
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

## 📊 Dashboard

A Streamlit app (`src/dashboard/app.py`) reads the `weather_daily` table and shows:

- KPI cards (avg max/min temp, total rain, rainy-day count)
- Temperature line chart and daily rainfall bar chart
- Weekly averages
- Sidebar filters for date range and future-days-only view

Run it locally with `streamlit run src/dashboard/app.py`, or via Docker Compose:

```bash
docker compose up -d dashboard   # then open http://localhost:8501
```

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

**Database connection:** the app talks to PostgreSQL via SQLAlchemy. Inside Docker Compose use `postgres` as the host (service name); locally use `localhost`.

**API endpoint documentation:** see [Open-Meteo Docs](https://open-meteo.com/en/docs).

## 💻 Usage Examples

Run the full pipeline (extract + transform + load):

```bash
python -m src.pipeline
```

Use individual modules programmatically:

```python
from src.extract.fetch_data import DataExtractor
from src.extract.save_raw import save_raw_json

extractor = DataExtractor("https://api.open-meteo.com/v1/forecast")
data = extractor.fetch_forecast(52.23, 21.01, days=7)
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

Expected output (`data/processed/weather_processed.csv`):

```
date,temp_max_c,temp_min_c,rain_mm,temp_range,temp_category
2026-10-10,18.4,9.2,0.3,9.2,mild
...
```

*(Screenshot placeholders: `docs/screenshots/pipeline_run.png`, `docs/screenshots/psql_results.png`)*

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

Full DDL lives in [`sql/schema.sql`](sql/schema.sql).

## 🔍 SQL Query Examples

```sql
-- Basic statistics
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
```

More analytical queries (hot days, temperature swings, weekly aggregation) are in [`sql/queries.sql`](sql/queries.sql).

## 🧪 Testing

```bash
pytest -v --cov=src
```

- Coverage report is printed to the terminal; use `--cov-report=html` for `htmlcov/index.html`.
- **What is tested:** URL/coordinate validation and mocked HTTP responses (extract), DataFrame building, derived columns, statistics and validation (transform), DDL execution, UPSERT counting, query execution and connection cleanup with mocked engines (load).
- **No real API calls or database connections** are used in tests.

## 🛠️ Development

**Code structure:**

```
src/
├── extract/    # API client + raw persistence
├── transform/  # pandas processing + validation
└── load/       # schema + database loading
```

**Adding a new feature** (e.g. a new daily metric):

1. Add the field to `DataExtractor.DAILY_FIELDS` (`src/extract/fetch_data.py`).
2. Map it in `parse_daily_metrics` (`src/transform/process_data.py`).
3. Extend the table DDL in `src/load/schema.py` and `sql/schema.sql`.
4. Add/adjust tests and run `pytest` and `flake8`.

**Contribution guidelines:** create a feature branch, keep PEP 8 (flake8 must pass), add tests for new behavior, and keep PRs focused.

## 🧰 Technologies Used

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.11 | Main language |
| requests | ≥ 2.31 | HTTP client |
| pandas | ≥ 2.1 | Data processing |
| SQLAlchemy | ≥ 2.0 | Database ORM/engine |
| psycopg2-binary | ≥ 2.9 | PostgreSQL driver |
| PostgreSQL | 15 (Alpine) | Target database |
| pytest | ≥ 7.4 | Testing |
| flake8 | ≥ 6.1 | Linting |
| Docker / Compose | latest | Local infrastructure |
| Streamlit | ≥ 1.31 | Dashboard visualization |
| GitHub Actions | — | CI/CD |

## 🔮 Future Improvements

- Orchestration with **Apache Airflow** (scheduling, retries, backfills)
- Cloud **dashboard hosting** (deploy the Streamlit app or build a custom web UI)
- Deployment to the **cloud** (e.g. AWS RDS + ECS)
- Incremental/historical backfill mode with partitioned tables
- Data quality framework (Great Expectations) instead of basic checks

## 📄 License

MIT License — see the [LICENSE](LICENSE) file.
