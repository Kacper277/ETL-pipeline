-- ============================================================
-- Schema for the STREAMING path (Kafka consumer -> PostgreSQL)
-- Additive only: the batch table `weather_daily` is untouched.
-- Target: PostgreSQL 15+
-- All statements are idempotent (IF NOT EXISTS / CREATE OR REPLACE).
-- ============================================================

-- ------------------------------------------------------------
-- Table: weather_readings
-- One row per (station, reading) delivered by the streaming path.
--
-- The UNIQUE constraint on (station_id, observed_at) is what makes
-- the pipeline's AT-LEAST-ONCE delivery safe: Kafka may redeliver a
-- batch (e.g. after a crash between write and offset commit), and the
-- consumer's loader uses ON CONFLICT (station_id, observed_at)
-- DO NOTHING, so duplicates collapse idempotently instead of
-- double-counting.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_readings (
    id          SERIAL PRIMARY KEY,
    station_id  TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temp_max_c  NUMERIC(5, 2),
    temp_min_c  NUMERIC(5, 2),
    rain_mm     NUMERIC(6, 2),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_weather_readings_station_time UNIQUE (station_id, observed_at)
);

-- Analytical queries typically read the newest data first.
CREATE INDEX IF NOT EXISTS idx_weather_readings_observed_at
    ON weather_readings (observed_at DESC);

-- Per-station filtering / joins.
CREATE INDEX IF NOT EXISTS idx_weather_readings_station
    ON weather_readings (station_id);

-- ------------------------------------------------------------
-- View: weather_readings_daily
--
-- CONVERGENCE POINT: this view rolls the stream up to the daily
-- grain and is deliberately shaped with the SAME columns as the
-- batch table `weather_daily` (date, temp_max_c, temp_min_c,
-- temp_range, rain_mm). The analytical queries in sql/queries.sql
-- therefore run UNCHANGED against either the batch table or this
-- view - one analytical layer, two ingestion paths.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW weather_readings_daily AS
SELECT
    (observed_at AT TIME ZONE 'UTC')::date AS date,
    MAX(temp_max_c)                        AS temp_max_c,
    MIN(temp_min_c)                        AS temp_min_c,
    MAX(temp_max_c) - MIN(temp_min_c)      AS temp_range,
    SUM(rain_mm)                           AS rain_mm
FROM weather_readings
GROUP BY (observed_at AT TIME ZONE 'UTC')::date;
