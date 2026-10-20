"""Run the streaming consumer loop (entry point).

Entry point::

    python -m src.streaming.consume

Reads raw readings from Kafka, validates them, persists valid rows to
the ``weather_readings`` table and routes rejects to the DLQ topic.
Stops gracefully on SIGINT/SIGTERM (Ctrl+C): the current batch is
finished, committed and connections are closed.
"""

from __future__ import annotations

import logging
import os
import signal
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.streaming.config import StreamingConfig
from src.streaming.consumer import WeatherEventConsumer
from src.streaming.handlers import process_reading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consume")

INSERT_READINGS_SQL = """
INSERT INTO weather_readings
    (station_id, observed_at, temp_max_c, temp_min_c, rain_mm)
VALUES
    (:station_id, :observed_at, :temp_max_c, :temp_min_c, :rain_mm)
ON CONFLICT (station_id, observed_at) DO NOTHING
"""

SELECT_COUNT_SQL = "SELECT COUNT(*) FROM weather_readings"


class StreamingDatabaseLoader:
    """Persist validated reading rows to PostgreSQL.

    The UNIQUE constraint on ``(station_id, observed_at)`` combined
    with ``ON CONFLICT DO NOTHING`` makes re-delivered batches
    (at-least-once semantics) harmless.
    """

    def __init__(self, connection_string: str) -> None:
        """Create the SQLAlchemy engine.

        Args:
            connection_string: Database URL (from ``DATABASE_URL``).
        """
        self._engine: Engine = create_engine(connection_string, future=True)

    def insert_readings(self, rows: list[dict[str, Any]]) -> int:
        """Insert rows idempotently.

        Args:
            rows: Row dicts from :func:`src.streaming.handlers.event_to_row`.

        Returns:
            Number of rows actually inserted (duplicates skipped).

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database failure.
        """
        with self._engine.begin() as conn:
            result = conn.execute(text(INSERT_READINGS_SQL), rows)
            return int(result.rowcount or 0)

    def count(self) -> int:
        """Return the total number of rows in ``weather_readings``.

        Returns:
            Current row count.
        """
        with self._engine.connect() as conn:
            return int(conn.execute(text(SELECT_COUNT_SQL)).scalar_one())

    def close(self) -> None:
        """Dispose of the engine."""
        self._engine.dispose()


def main() -> int:
    """Run the consumer until interrupted.

    Returns:
        Process exit code: 0 on graceful shutdown, 1 on failure.
    """
    load_dotenv()
    config = StreamingConfig.from_env(dict(os.environ))
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL is not set - cannot connect to PostgreSQL")
        return 1

    consumer = WeatherEventConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        topics=[config.topic_raw],
        dlq_topic=config.topic_dlq,
        batch_size=config.batch_size,
        batch_timeout_s=config.batch_timeout_s,
    )
    loader = StreamingDatabaseLoader(database_url)
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)

    try:
        total = consumer.run(handler=process_reading, loader=loader.insert_readings)
        logger.info(
            "Rows now in weather_readings: %d (records processed: %d)", loader.count(), total
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level failure report
        logger.error("Consumer run failed: %s", exc)
        return 1
    finally:
        loader.close()


if __name__ == "__main__":
    raise SystemExit(main())
