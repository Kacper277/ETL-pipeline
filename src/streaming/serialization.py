"""JSON serialization helpers for Kafka payloads.

Events travel as UTF-8 JSON with ``ensure_ascii=False`` so station
names and identifiers stay human-readable in tooling such as the
Kafka UI.
"""

from __future__ import annotations

import json
from typing import Any


class SerializationError(Exception):
    """Raised when a payload cannot be encoded or decoded."""


def encode_event(payload: dict[str, Any]) -> bytes:
    """Encode a payload dict to JSON bytes.

    Args:
        payload: JSON-serializable mapping.

    Returns:
        UTF-8 encoded JSON bytes.

    Raises:
        SerializationError: If the payload is not JSON-serializable.
    """
    try:
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SerializationError(f"Payload is not JSON-serializable: {exc}") from exc


def decode_event(raw: bytes | None) -> dict[str, Any]:
    """Decode JSON bytes into a payload dict.

    Args:
        raw: Raw Kafka message value (UTF-8 JSON), or ``None`` for
            tombstone records.

    Returns:
        The decoded mapping.

    Raises:
        SerializationError: If the value is empty or not valid JSON.
    """
    if not raw:
        raise SerializationError("Empty message value (tombstone or no payload)")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SerializationError(f"Malformed JSON payload: {exc}") from exc
    if not isinstance(decoded, dict):
        raise SerializationError(
            f"Expected a JSON object, got {type(decoded).__name__}"
        )
    return decoded
