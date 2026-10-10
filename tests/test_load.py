"""Tests for the load module (mocked database engine, no real DB)."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.load.schema import TABLE_DDL
from src.load.store_data import DatabaseLoader

VALID_URL = "postgresql+psycopg2://user:pass@localhost:5432/testdb"
SAMPLE_RECORDS = [
    {"date": "2026-10-01", "temp_max_c": 20.5, "temp_min_c": 10.0,
     "temp_range": 10.5, "rain_mm": 0.0},
    {"date": "2026-10-02", "temp_max_c": 22.3, "temp_min_c": 11.5,
     "temp_range": 10.8, "rain_mm": 1.2},
]


@pytest.fixture
def loader() -> DatabaseLoader:
    return DatabaseLoader(VALID_URL)


class TestDatabaseLoader:
    def test_empty_connection_string_raises_error(self) -> None:
        with pytest.raises(ValueError):
            DatabaseLoader("")

    def test_create_table_executes_ddl(self, loader: DatabaseLoader) -> None:
        loader.engine = MagicMock()
        conn = loader.engine.begin.return_value.__enter__.return_value
        loader.create_table()
        assert conn.execute.called
        assert "weather_daily" in str(conn.execute.call_args.args[0])

    def test_insert_records_returns_count(self, loader: DatabaseLoader) -> None:
        loader.engine = MagicMock()
        conn = loader.engine.begin.return_value.__enter__.return_value
        conn.execute.side_effect = [MagicMock(scalar_one=lambda: 0),
                                    MagicMock(),
                                    MagicMock(),
                                    MagicMock(scalar_one=lambda: 2)]
        inserted = loader.insert_records(SAMPLE_RECORDS)
        assert inserted == 2

    def test_insert_empty_records_returns_zero(self, loader: DatabaseLoader) -> None:
        assert loader.insert_records([]) == 0

    def test_normalize_record_parses_date(self) -> None:
        normalized = DatabaseLoader._normalize_record(SAMPLE_RECORDS[0])
        assert normalized["date"] == date(2026, 10, 1)
        assert normalized["temp_max_c"] == pytest.approx(20.5)

    def test_execute_query_returns_rows(self, loader: DatabaseLoader) -> None:
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(1, "2026-10-01")]
        with patch.object(loader.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__ = MagicMock(return_value=conn)
            mock_connect.return_value.__exit__ = MagicMock(return_value=False)
            rows = loader.execute_query("SELECT id, date FROM weather_daily")
        assert rows == [(1, "2026-10-01")]

    def test_get_record_count(self, loader: DatabaseLoader) -> None:
        with patch.object(loader, "execute_query", return_value=[(42,)]):
            assert loader.get_record_count() == 42

    def test_close_disposes_engine(self, loader: DatabaseLoader) -> None:
        with patch.object(loader.engine, "dispose") as mock_dispose:
            loader.close()
        assert mock_dispose.called


def test_ddl_contains_expected_columns() -> None:
    ddl = TABLE_DDL.lower()
    for column in ("date", "temp_max_c", "temp_min_c", "temp_range", "rain_mm", "loaded_at"):
        assert column in ddl
