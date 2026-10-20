"""Pure transformation logic for the streaming consumer.

These functions perform no I/O: they decode a raw Kafka value, validate
it and map it to a database row dict. The consumer decides what happens
on failure (DLQ routing); this module only raises. That keeps the whole
validation gate unit-testable without a broker or a database.
"""

from __future__ import annotations

from typing import Any

from src.streaming.schemas import WeatherEvent, event_from_dict, validate_event
from src.streaming.serialization import decode_event


def event_to_row(event: WeatherEvent) -> dict[str, Any]:
    """Map a validated event to a ``weather_readings`` row dict.

    Args:
        event: A validated :class:`WeatherEvent`.

    Returns:
        Dict with keys ``station_id``, ``observed_at`` (datetime),
        ``temp_max_c``, ``temp_min_c`` and ``rain_mm``.
    """
    return {
        "station_id": event.station_id,
        "observed_at": event.observed_at,
        "temp_max_c": event.temp_max_c,
        "temp_min_c": event.temp_min_c,
        "rain_mm": event.rain_mm,
    }


def process_reading(raw: bytes | None) -> dict[str, Any]:
    """Validation gate: decode, validate and map one raw record.

    This is the single gate a Kafka record must pass through. Any
    failure raises, which the consumer translates into a DLQ entry -
    the raising convention (instead of returning a status tuple) keeps
    the happy path composable and the failure reason explicit.

    Args:
        raw: Raw Kafka message value (UTF-8 JSON) or ``None``.

    Returns:
        A database row dict ready for :meth:`insert_readings`.

    Raises:
        SerializationError: If the payload is empty or malformed JSON.
        EventValidationError: If the decoded event violates the schema.
    """
    payload = decode_event(raw)
    event = event_from_dict(payload)
    validate_event(event)
    return event_to_row(event)
