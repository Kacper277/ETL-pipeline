"""Central configuration for the streaming path.

All Kafka settings come from environment variables (loaded from a local
``.env`` file by :mod:`python-dotenv`). This module is the single source
of truth: the producer, the consumer and the entry-point scripts all
take a :class:`StreamingConfig` built here.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

#: Safe local defaults used when the corresponding env var is absent.
_DEFAULTS: dict[str, str] = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_TOPIC_RAW": "weather.readings.raw",
    "KAFKA_TOPIC_PROCESSED": "weather.readings.processed",
    "KAFKA_TOPIC_DLQ": "weather.readings.dlq",
    "KAFKA_CONSUMER_GROUP": "weather-ingest-v1",
    "KAFKA_BATCH_SIZE": "50",
    "KAFKA_BATCH_TIMEOUT_S": "5",
}


class ConfigError(Exception):
    """Raised when streaming configuration is missing or malformed."""


def _get(env: dict[str, str], key: str) -> str:
    """Fetch a config value, raising a clear error when it is absent.

    Args:
        env: Mapping of environment variables.
        key: Environment variable name.

    Returns:
        The raw string value (empty string if unset).

    Raises:
        ConfigError: If the variable exists but is empty.
    """
    value = env.get(key, _DEFAULTS.get(key, ""))
    if not value:
        raise ConfigError(
            f"Missing required environment variable {key}. "
            f"Set it in your environment or .env file (see .env.example)."
        )
    return value


def _get_int(env: dict[str, str], key: str, minimum: int) -> int:
    """Fetch an integer config value.

    Args:
        env: Mapping of environment variables.
        key: Environment variable name.
        minimum: Minimum accepted value (inclusive).

    Returns:
        The parsed integer.

    Raises:
        ConfigError: If the value is not an integer or below ``minimum``.
    """
    raw = _get(env, key)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{key} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class StreamingConfig:
    """Validated, immutable streaming configuration.

    Attributes:
        bootstrap_servers: ``host:port`` list for Kafka clients.
        topic_raw: Topic receiving unprocessed readings.
        topic_processed: Topic receiving validated readings.
        topic_dlq: Dead-letter topic for rejected records.
        consumer_group: Consumer group id of the ingestion consumer.
        batch_size: Maximum records per consumer micro-batch.
        batch_timeout_s: Seconds to wait before flushing a partial batch.
    """

    bootstrap_servers: str
    topic_raw: str
    topic_processed: str
    topic_dlq: str
    consumer_group: str
    batch_size: int
    batch_timeout_s: float

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "StreamingConfig":
        """Build a config from environment variables.

        Args:
            env: Environment mapping; defaults to ``os.environ`` after
                loading a local ``.env`` file.

        Returns:
            A frozen :class:`StreamingConfig`.

        Raises:
            ConfigError: If any required value is missing or malformed.
        """
        if env is None:
            load_dotenv()
            env = dict(os.environ)

        config = cls(
            bootstrap_servers=_get(env, "KAFKA_BOOTSTRAP_SERVERS"),
            topic_raw=_get(env, "KAFKA_TOPIC_RAW"),
            topic_processed=_get(env, "KAFKA_TOPIC_PROCESSED"),
            topic_dlq=_get(env, "KAFKA_TOPIC_DLQ"),
            consumer_group=_get(env, "KAFKA_CONSUMER_GROUP"),
            batch_size=_get_int(env, "KAFKA_BATCH_SIZE", minimum=1),
            batch_timeout_s=float(_get_int(env, "KAFKA_BATCH_TIMEOUT_S", minimum=1)),
        )
        logger.debug("Streaming config loaded: %s", config)
        return config
