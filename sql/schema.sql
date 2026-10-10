-- ============================================================
-- Schema for the weather ETL pipeline
-- Target: PostgreSQL 15+
-- ============================================================

CREATE TABLE IF NOT EXISTS weather_daily (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL UNIQUE,
    temp_max_c  DECIMAL(5, 2),
    temp_min_c  DECIMAL(5, 2),
    temp_range  DECIMAL(5, 2),
    rain_mm     DECIMAL(6, 2),
    loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Helpful index for range queries over dates
CREATE INDEX IF NOT EXISTS idx_weather_daily_date ON weather_daily (date);
