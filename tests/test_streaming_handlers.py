"""Unit tests for the streaming handler functions (pure, no I/O)."""

import pytest

from src.streaming.handlers import event_to_row, process_reading
from src.streaming.serialization import encode_event, SerializationError
from src.streaming.schemas import EventValidationError, event_from_dict
from datetime import datetime, timedelta, timezone


def iso_in_past(minutes: int = 5) -> str:
    """Return an ISO-8601 timestamp a few minutes in the past."""
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def valid_payload(station: str = "warsaw-1") -> dict:
    """Return a payload that passes the full validation gate."""
    return {
        "station_id": station,
        "observed_at": iso_in_past(),
        "temp_max_c": 15.0,
        "temp_min_c": 5.0,
        "rain_mm": 2.5,
    }


class TestEventToRow:
    def test_maps_all_columns(self) -> None:
        observed_at = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
        event = event_from_dict({
            "station_id": "gdansk-3",
            "observed_at": observed_at.isoformat(),
            "temp_max_c": 12.5,
            "temp_min_c": 3.5,
            "rain_mm": 0.0,
        })
        row = event_to_row(event)
        assert row == {
            "station_id": "gdansk-3",
            "observed_at": observed_at,
            "temp_max_c": 12.5,
            "temp_min_c": 3.5,
            "rain_mm": 0.0,
        }


class TestProcessReading:
    def test_valid_record_passes_gate(self) -> None:
        raw = encode_event(valid_payload())
        row = process_reading(raw)
        assert row["station_id"] == "warsaw-1"
        assert row["temp_max_c"] == 15.0

    def test_malformed_json_raises_serialization_error(self) -> None:
        with pytest.raises(SerializationError):
            process_reading(b"not json")

    def test_missing_keys_raise_validation_error(self) -> None:
        raw = encode_event({"station_id": "s1"})
        with pytest.raises(EventValidationError):
            process_reading(raw)


class TestRangeBoundaries:
    """Boundary values must match the batch validator exactly."""

    @pytest.mark.parametrize("metric,value", [
        ("temp_max_c", -50.0),   # lowest allowed temperature
        ("temp_max_c", 50.0),    # highest allowed temperature
        ("temp_min_c", -50.0),
        ("temp_min_c", 50.0),
        ("rain_mm", 0.0),        # no rain
        ("rain_mm", 500.0),      # highest allowed rainfall
    ])
    def test_boundaries_pass(self, metric: str, value: float) -> None:
        payload = valid_payload()
        payload[metric] = value
        if metric == "temp_max_c":
            payload["temp_min_c"] = value  # keep min <= max
        elif metric == "temp_min_c":
            payload["temp_max_c"] = value
        raw = encode_event(payload)
        row = process_reading(raw)  # must not raise
        assert row[metric] == value

    @pytest.mark.parametrize("metric,value", [
        ("temp_max_c", -50.1),
        ("temp_max_c", 50.1),
        ("temp_min_c", -50.1),
        ("temp_min_c", 50.1),
        ("rain_mm", -0.1),
        ("rain_mm", 500.1),
    ])
    def test_just_outside_boundaries_fail(self, metric: str, value: float) -> None:
        payload = valid_payload()
        payload[metric] = value
        raw = encode_event(payload)
        with pytest.raises(EventValidationError):
            process_reading(raw)

    def test_min_above_max_fails(self) -> None:
        payload = valid_payload()
        payload["temp_max_c"] = 10.0
        payload["temp_min_c"] = 20.0
        with pytest.raises(EventValidationError):
            process_reading(encode_event(payload))

    def test_naive_timestamp_fails(self) -> None:
        payload = valid_payload()
        payload["observed_at"] = "2026-10-19T12:00:00"  # no timezone
        with pytest.raises(EventValidationError):
            process_reading(encode_event(payload))

    def test_future_timestamp_fails(self) -> None:
        payload = valid_payload()
        payload["observed_at"] = (
            datetime.now(timezone.utc) + timedelta(hours=1)
        ).isoformat()
        with pytest.raises(EventValidationError):
            process_reading(encode_event(payload))
