"""Persist raw API responses to disk as JSON files."""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def save_raw_json(data: dict[str, Any], filepath: str = "data/raw/weather_raw.json") -> str:
    """Save a raw JSON payload to disk.

    Parent directories are created automatically if they do not exist.

    Args:
        data: Raw (parsed) API response to persist.
        filepath: Destination path of the JSON file.

    Returns:
        The path of the file that was written.

    Raises:
        TypeError: If ``data`` is not JSON-serializable.
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)

    logger.info("Raw JSON saved to %s", path)
    return str(path)
