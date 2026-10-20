"""Canonical event schema for streaming weather readings.

The event shape mirrors the batch pipeline's clean columns, so the same
analytical layer applies to both paths. Range constants are reused from
the batch validator instead of being redeclared here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from src.transform.validate_data import (
    RAIN_MAX_ALLOWED,
    RAIN_MIN_ALLOWED,
    TEMP_MAX_ALLOWED,
    TEMP_MIN_ALLOWED,
)


class EventValidationError(Exception):
    """Raised when an event violates the canonical schema."""


@dataclass(frozen=True)
class WeatherEvent:
    """A single weather reading from one station.

    Attributes:
        station_id: Stable station identifier; Kafka partition key.
        observed_at: UTC timestamp of the reading.
        temp_max_c: Maximum temperature in Celsius.
        temp_min_c: Minimum temperature in Celsius.
        rain_mm: Rainfall in millimeters.
    """

    station_id: str
    observed_at: datetime
    temp_max_c: float
    temp_min_c: float
    rain_mm: float

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable dict (ISO-8601 timestamp).

        Returns:
            Dict with ``observed_at`` rendered as an ISO-8601 string.
        """
        payload = asdict(self)
        payload["observed_at"] = self.observed_at.isoformat()
        return payload


def event_from_dict(data: dict[str, object]) -> WeatherEvent:
    """Build a :class:`WeatherEvent` from a decoded JSON dict.

    Args:
        data: Dict with keys ``station_id``, ``observed_at`` (ISO-8601
            string), ``temp_max_c``, ``temp_min_c`` and ``rain_mm``.

    Returns:
        The constructed event.

    Raises:
        EventValidationError: If a key is missing or of the wrong type.
    """
    missing = {"station_id", "observed_at", "temp_max_c", "temp_min_c", "rain_mm"} - set(data)
    if missing:
        raise EventValidationError(f"Missing keys: {sorted(missing)}")

    try:
        observed_at = datetime.fromisoformat(str(data["observed_at"]))
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            f"observed_at must be an ISO-8601 timestamp, got {data['observed_at']!r}"
        ) from exc

    try:
        return WeatherEvent(
            station_id=str(data["station_id"]),
            observed_at=observed_at,
            temp_max_c=float(data["temp_max_c"]),  # type: ignore[arg-type]
            temp_min_c=float(data["temp_min_c"]),  # type: ignore[arg-type]
            rain_mm=float(data["rain_mm"]),  # type: ignore[arg-type]
        )
    except (TypeError, ValueError) as exc:
        raise EventValidationError(f"Metrics must be numeric: {exc}") from exc


def validate_event(event: WeatherEvent) -> None:
    """Validate an event against physical range constraints.

    Ranges are the same as the batch pipeline's
    :mod:`src.transform.validate_data` (temperature -50..+50 °C,
    rain 0..500 mm) so both paths reject the same data.

    Args:
        event: The event to validate.

    Raises:
        EventValidationError: On the first violated constraint.
    """
    if not event.station_id.strip():
        raise EventValidationError("station_id must be a non-empty string")
    if event.observed_at.tzinfo is None:
        raise EventValidationError("observed_at must be timezone-aware (UTC)")
    if event.observed_at > datetime.now(timezone.utc):
        raise EventValidationError(f"observed_at {event.observed_at.isoformat()} is in the future")

    for name, value, low, high in (
        ("temp_max_c", event.temp_max_c, TEMP_MIN_ALLOWED, TEMP_MAX_ALLOWED),
        ("temp_min_c", event.temp_min_c, TEMP_MIN_ALLOWED, TEMP_MAX_ALLOWED),
        ("rain_mm", event.rain_mm, RAIN_MIN_ALLOWED, RAIN_MAX_ALLOWED),
    ):
        if not low <= value <= high:
            raise EventValidationError(
                f"{name}={value} outside allowed range [{low}, {high}]"
            )
    if event.temp_min_c > event.temp_max_c:
        raise EventValidationError(
            f"temp_min_c {event.temp_min_c} greater than temp_max_c {event.temp_max_c}"
        )
