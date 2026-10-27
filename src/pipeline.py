"""Entry point for running the full ETL pipeline as a module."""

import logging
import os

from dotenv import load_dotenv

from src.extract.fetch_data import DataExtractor
from src.extract.save_raw import save_raw_json
from src.load.store_data import DatabaseLoader
from src.transform.process_data import DataTransformer
from src.transform.validate_data import DataValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> None:
    """Run the full extract -> transform -> load pipeline."""
    load_dotenv()

    base_url = os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
    lat = float(os.getenv("LATITUDE", "52.23"))
    lon = float(os.getenv("LONGITUDE", "21.01"))
    days = int(os.getenv("FORECAST_DAYS", "16"))
    past_days = int(os.getenv("PAST_DAYS", "92"))
    database_url = os.environ["DATABASE_URL"]

    # Extract
    extractor = DataExtractor(base_url)
    raw = extractor.fetch_forecast(lat, lon, days, past_days)
    save_raw_json(raw, "data/raw/weather_raw.json")

    # Transform
    transformer = DataTransformer("data/raw/weather_raw.json")
    df = transformer.parse_daily_metrics()
    df = transformer.add_derived_columns(df)
    report = DataValidator().full_validation_report(df)
    print(report)
    transformer.export_to_csv(df)
    transformer.export_partitioned_parquet(df)

    # Load
    loader = DatabaseLoader(database_url)
    loader.create_table()
    records = df[["date", "temp_max_c", "temp_min_c", "temp_range", "rain_mm"]].to_dict("records")
    inserted = loader.insert_records(records)
    print(f"Rows in table: {loader.get_record_count()} (inserted {inserted})")
    loader.close()


if __name__ == "__main__":
    main()
