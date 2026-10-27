"""Transform raw Open-Meteo JSON into a clean pandas DataFrame."""

import json
import logging
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

TEMP_HOT_THRESHOLD = 25.0
TEMP_COLD_THRESHOLD = 10.0


class TransformError(Exception):
    """Raised when the raw payload cannot be transformed."""


class DataTransformer:
    """Transform raw Open-Meteo API responses into analysis-ready data.

    Attributes:
        raw_data_path: Path to the raw JSON file.
        raw_data: Parsed raw JSON payload.
    """

    OUTPUT_COLUMNS = ["date", "temp_max_c", "temp_min_c", "rain_mm"]

    def __init__(self, raw_data_path: str) -> None:
        """Load the raw JSON payload from disk.

        Args:
            raw_data_path: Path to a raw Open-Meteo JSON file.

        Raises:
            FileNotFoundError: If the raw file does not exist.
            TransformError: If the file cannot be parsed as JSON.
        """
        path = Path(raw_data_path)
        if not path.is_file():
            raise FileNotFoundError(f"Raw data file not found: {raw_data_path}")
        try:
            with path.open("r", encoding="utf-8") as fh:
                self.raw_data: dict[str, Any] = json.load(fh)
        except json.JSONDecodeError as exc:
            raise TransformError(f"Invalid JSON in {raw_data_path}: {exc}") from exc
        self.raw_data_path = raw_data_path

    def parse_daily_metrics(self) -> pd.DataFrame:
        """Parse the raw API response into a tidy DataFrame.

        Returns:
            DataFrame with columns ``date`` (datetime64),
            ``temp_max_c``, ``temp_min_c`` and ``rain_mm`` (float).

        Raises:
            TransformError: If the payload lacks the expected structure.
        """
        daily = self.raw_data.get("daily")
        if not daily:
            raise TransformError("Raw payload does not contain a 'daily' section")

        expected = {
            "time": "date",
            "temperature_2m_max": "temp_max_c",
            "temperature_2m_min": "temp_min_c",
            "rain_sum": "rain_mm",
        }
        missing = [key for key in expected if key not in daily]
        if missing:
            raise TransformError(f"Missing daily fields: {', '.join(missing)}")

        df = pd.DataFrame(daily)
        df = df.rename(columns=expected)[self.OUTPUT_COLUMNS]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for col in ("temp_max_c", "temp_min_c", "rain_mm"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if df["date"].isna().any():
            raise TransformError("One or more dates could not be parsed")

        # Rows at the very edge of the requested window can come back
        # with no measurements at all (all metrics null) - drop them.
        metric_cols = ["temp_max_c", "temp_min_c", "rain_mm"]
        empty_rows = df[metric_cols].isna().all(axis=1).sum()
        if empty_rows:
            logger.warning("Dropping %d rows with no measurements", empty_rows)
            df = df.dropna(subset=metric_cols, how="all").reset_index(drop=True)

        logger.info("Parsed %d daily records", len(df))
        return df

    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ``temp_range`` and ``temp_category`` columns.

        Args:
            df: DataFrame produced by :meth:`parse_daily_metrics`.

        Returns:
            A copy of the DataFrame with the derived columns.

        Raises:
            KeyError: If required temperature columns are missing.
        """
        required = {"temp_max_c", "temp_min_c"}
        if not required.issubset(df.columns):
            raise KeyError(f"DataFrame must contain columns {sorted(required)}")

        out = df.copy()
        out["temp_range"] = out["temp_max_c"] - out["temp_min_c"]

        def categorize(avg_temp: float) -> str:
            if pd.isna(avg_temp):
                return "unknown"
            if avg_temp >= TEMP_HOT_THRESHOLD:
                return "hot"
            if avg_temp <= TEMP_COLD_THRESHOLD:
                return "cold"
            return "mild"

        out["temp_category"] = out.apply(
            lambda row: categorize((row["temp_max_c"] + row["temp_min_c"]) / 2), axis=1
        )
        return out

    def calculate_statistics(self, df: pd.DataFrame) -> dict[str, float]:
        """Compute summary statistics for the dataset.

        Args:
            df: DataFrame with temperature and rain columns.

        Returns:
            Dict with keys ``avg_temp_max``, ``avg_temp_min``,
            ``total_rain``, ``max_temp_recorded`` and
            ``min_temp_recorded``; values rounded to 2 decimals.

        Raises:
            KeyError: If required columns are missing.
        """
        required = {"temp_max_c", "temp_min_c", "rain_mm"}
        if not required.issubset(df.columns):
            raise KeyError(f"DataFrame must contain columns {sorted(required)}")

        stats = {
            "avg_temp_max": round(float(df["temp_max_c"].mean(skipna=True)), 2),
            "avg_temp_min": round(float(df["temp_min_c"].mean(skipna=True)), 2),
            "total_rain": round(float(df["rain_mm"].sum(skipna=True)), 2),
            "max_temp_recorded": round(float(df["temp_max_c"].max(skipna=True)), 2),
            "min_temp_recorded": round(float(df["temp_min_c"].min(skipna=True)), 2),
        }
        logger.info("Computed statistics: %s", stats)
        return stats

    def export_to_csv(
        self, df: pd.DataFrame, filepath: str = "data/processed/weather_processed.csv"
    ) -> None:
        """Export the processed DataFrame to CSV.

        Args:
            df: DataFrame to export.
            filepath: Destination CSV path (directories are created).
        """
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info("Processed data exported to %s", path)

    def export_partitioned_parquet(
        self, df: pd.DataFrame, base_dir: str = "data/processed"
    ) -> str:
        """Export the processed DataFrame as a partitioned Parquet dataset.

        Writes a hive-style layout (``<base_dir>/weather_daily/date=YYYY-MM-DD/``),
        which is the on-disk pattern used by data lakes: columnar files,
        partitioned by date, selectively readable by tools such as
        Spark, DuckDB or pandas.

        Args:
            df: DataFrame with a ``date`` datetime column.
            base_dir: Base directory of the local lake.

        Returns:
            The dataset root path.

        Raises:
            KeyError: If the ``date`` column is missing.
        """
        if "date" not in df.columns:
            raise KeyError("DataFrame must contain a 'date' column")

        dataset_path = Path(base_dir) / "weather_daily"
        # Idempotent overwrite: pyarrow would otherwise append a new
        # part-file per partition on every run, duplicating rows on read.
        if dataset_path.exists():
            shutil.rmtree(dataset_path)
        dataset_path.mkdir(parents=True, exist_ok=True)
        out = df.copy()
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        out.to_parquet(dataset_path, partition_cols=["date"], index=False)
        logger.info("Partitioned Parquet dataset written to %s", dataset_path)
        return str(dataset_path)
