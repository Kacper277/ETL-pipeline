"""SQL schema definitions for the weather pipeline."""

TABLE_NAME = "weather_daily"

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    temp_max_c DECIMAL(5, 2),
    temp_min_c DECIMAL(5, 2),
    temp_range DECIMAL(5, 2),
    rain_mm DECIMAL(6, 2),
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);
""".format(table=TABLE_NAME)

UPSERT_SQL = """
INSERT INTO {table} (date, temp_max_c, temp_min_c, temp_range, rain_mm)
VALUES (:date, :temp_max_c, :temp_min_c, :temp_range, :rain_mm)
ON CONFLICT (date) DO NOTHING
""".format(table=TABLE_NAME)

COUNT_SQL = "SELECT COUNT(*) FROM {table}".format(table=TABLE_NAME)
