"""Database loading operations for processed weather data."""

import logging
from datetime import date as date_type
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.load.schema import COUNT_SQL, TABLE_DDL, UPSERT_SQL

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Raised when a database operation fails."""


class DatabaseLoader:
    """Load processed weather records into PostgreSQL via SQLAlchemy.

    Attributes:
        connection_string: SQLAlchemy database URL.
        engine: SQLAlchemy engine instance.
    """

    def __init__(self, connection_string: str) -> None:
        """Create the SQLAlchemy engine.

        Args:
            connection_string: Database URL, e.g.
                ``postgresql+psycopg2://user:pass@host:5432/db``.

        Raises:
            ValueError: If the connection string is empty.
        """
        if not connection_string:
            raise ValueError("connection_string must not be empty")
        self.connection_string = connection_string
        self.engine: Engine = create_engine(connection_string, future=True)
        logger.info("Database engine created")

    def create_table(self) -> None:
        """Create the ``weather_daily`` table if it does not exist.

        Raises:
            LoadError: If the DDL execution fails.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(TABLE_DDL))
            logger.info("Table 'weather_daily' is ready")
        except Exception as exc:
            raise LoadError(f"Failed to create table: {exc}") from exc

    def insert_records(self, records: list[dict[str, Any]]) -> int:
        """Insert records using UPSERT (ON CONFLICT DO NOTHING).

        Args:
            records: List of dicts with keys ``date``, ``temp_max_c``,
                ``temp_min_c``, ``temp_range`` and ``rain_mm``.

        Returns:
            Number of rows actually inserted (duplicates skipped).

        Raises:
            LoadError: If the insert fails.
        """
        if not records:
            logger.info("No records to insert")
            return 0

        try:
            with self.engine.begin() as conn:
                before = conn.execute(text(COUNT_SQL)).scalar_one()
                for record in records:
                    conn.execute(
                        text(UPSERT_SQL),
                        self._normalize_record(record),
                    )
                after = conn.execute(text(COUNT_SQL)).scalar_one()
            inserted = after - before
            logger.info("Inserted %d of %d records (%d duplicates skipped)",
                        inserted, len(records), len(records) - inserted)
            return inserted
        except Exception as exc:
            raise LoadError(f"Failed to insert records: {type(exc).__name__}: {exc}") from exc

    @staticmethod
    def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
        """Coerce a record's values into DB-friendly types.

        Args:
            record: Raw record dict.

        Returns:
            Dict with ``date`` as ``datetime.date`` and numeric fields
            as floats (``None`` when missing/unparseable).
        """
        raw_date = record.get("date")
        if isinstance(raw_date, date_type):
            parsed_date: date_type = raw_date
        else:
            parsed_date = date_type.fromisoformat(str(raw_date)[:10])

        def to_float(value: Any) -> float | None:
            try:
                return None if value is None else float(value)
            except (TypeError, ValueError):
                return None

        return {
            "date": parsed_date,
            "temp_max_c": to_float(record.get("temp_max_c")),
            "temp_min_c": to_float(record.get("temp_min_c")),
            "temp_range": to_float(record.get("temp_range")),
            "rain_mm": to_float(record.get("rain_mm")),
        }

    def execute_query(self, query: str) -> list[tuple]:
        """Execute a generic SQL query and fetch all rows.

        Args:
            query: SQL statement to execute.

        Returns:
            List of row tuples.

        Raises:
            LoadError: If the query fails.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [tuple(row) for row in result.fetchall()]
        except Exception as exc:
            raise LoadError(f"Query failed: {exc}") from exc

    def get_record_count(self) -> int:
        """Return the total number of rows in ``weather_daily``.

        Raises:
            LoadError: If the count query fails.
        """
        rows = self.execute_query(COUNT_SQL)
        return int(rows[0][0])

    def close(self) -> None:
        """Dispose of the engine and release all connections."""
        self.engine.dispose()
        logger.info("Database engine disposed")
