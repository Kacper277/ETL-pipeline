"""Tests for the transform module."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.transform.process_data import DataTransformer
from src.transform.validate_data import DataValidator

SAMPLE_RAW = {
    "daily": {
        "time": ["2026-10-01", "2026-10-02", "2026-10-03"],
        "temperature_2m_max": [20.5, 26.125, 15.0],
        "temperature_2m_min": [10.0, 12.0, 8.0],
        "rain_sum": [0.0, 3.5, 1.0],
    }
}


@pytest.fixture
def raw_file(tmp_path: Path) -> Path:
    """Write the sample raw payload to a temp JSON file."""
    path = tmp_path / "weather_raw.json"
    path.write_text(json.dumps(SAMPLE_RAW), encoding="utf-8")
    return path


@pytest.fixture
def transformer(raw_file: Path) -> DataTransformer:
    return DataTransformer(str(raw_file))


@pytest.fixture
def df(transformer: DataTransformer) -> pd.DataFrame:
    return transformer.parse_daily_metrics()


class TestParseDailyMetrics:
    def test_dataframe_columns(self, df: pd.DataFrame) -> None:
        assert list(df.columns) == ["date", "temp_max_c", "temp_min_c", "rain_mm"]

    def test_dates_are_datetime(self, df: pd.DataFrame) -> None:
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_row_count(self, df: pd.DataFrame) -> None:
        assert len(df) == 3

    def test_missing_file_raises_error(self) -> None:
        with pytest.raises(FileNotFoundError):
            DataTransformer("does/not/exist.json")

    def test_invalid_structure_raises_error(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.json"
        path.write_text(json.dumps({"wrong": {}}), encoding="utf-8")
        with pytest.raises(Exception):
            DataTransformer(str(path)).parse_daily_metrics()


class TestDerivedColumns:
    def test_temp_range_calculated(self, df: pd.DataFrame, transformer: DataTransformer) -> None:
        out = transformer.add_derived_columns(df)
        expected = df["temp_max_c"] - df["temp_min_c"]
        assert out["temp_range"].tolist() == pytest.approx(expected.tolist())

    def test_temp_category_values(self, df: pd.DataFrame, transformer: DataTransformer) -> None:
        out = transformer.add_derived_columns(df)
        assert set(out["temp_category"]).issubset({"hot", "cold", "mild", "unknown"})

    def test_hot_day_detected(self, df: pd.DataFrame, transformer: DataTransformer) -> None:
        out = transformer.add_derived_columns(df)
        # avg of (26.125, 12.0) = 19.06 -> mild; make an explicit hot check instead
        hot = pd.DataFrame({"temp_max_c": [30.0], "temp_min_c": [20.0], "rain_mm": [0.0]})
        out2 = transformer.add_derived_columns(hot)
        assert out2["temp_category"].iloc[0] == "hot"
        assert out is not df  # original DataFrame untouched


class TestStatistics:
    def test_statistic_rounding(self, df: pd.DataFrame, transformer: DataTransformer) -> None:
        stats = transformer.calculate_statistics(df)
        for value in stats.values():
            assert value == round(value, 2)

    def test_statistic_values(self, transformer: DataTransformer) -> None:
        simple = pd.DataFrame(
            {"temp_max_c": [10.0, 20.0], "temp_min_c": [5.0, 15.0], "rain_mm": [1.0, 3.0]}
        )
        stats = transformer.calculate_statistics(simple)
        assert stats["avg_temp_max"] == 15.0
        assert stats["avg_temp_min"] == 10.0
        assert stats["total_rain"] == 4.0
        assert stats["max_temp_recorded"] == 20.0
        assert stats["min_temp_recorded"] == 5.0

    def test_missing_columns_raise_keyerror(self, transformer: DataTransformer) -> None:
        with pytest.raises(KeyError):
            transformer.calculate_statistics(pd.DataFrame({"date": ["2026-01-01"]}))


class TestValidator:
    def test_null_check(self) -> None:
        df = pd.DataFrame({"a": [1, None], "b": [1, 2]})
        result = DataValidator().check_null_values(df)
        assert result == {"a": True, "b": False}

    def test_range_check_flags_out_of_bounds(self) -> None:
        df = pd.DataFrame({"temp_max_c": [999.0], "rain_mm": [0.0]})
        result = DataValidator().check_value_ranges(df)
        assert result["temp_max_c"] is False
        assert result["rain_mm"] is True

    def test_report_contains_verdict(self, df: pd.DataFrame) -> None:
        report = DataValidator().full_validation_report(df)
        assert "PASS" in report or "FAIL" in report
