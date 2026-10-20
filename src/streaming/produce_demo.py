"""Produce a bounded demo batch of weather readings to Kafka.

Entry point::

    python -m src.streaming.produce_demo [--stations N] [--per-station M]

Generates synthetic-but-plausible readings for a few stations
(timestamps in the recent past, metrics inside the validated ranges)
and publishes them to the raw topic, keyed by station id.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from src.streaming.config import StreamingConfig
from src.streaming.producer import WeatherEventProducer
from src.streaming.schemas import WeatherEvent, validate_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("produce_demo")

DEMO_STATIONS = ["warsaw-1", "krakow-2", "gdansk-3"]


def generate_event(station_id: str, observed_at: datetime, rng: random.Random) -> WeatherEvent:
    """Generate one plausible reading for a station.

    Args:
        station_id: Station identifier (used as the Kafka key).
        observed_at: Reading timestamp (UTC).
        rng: Random source (seedable for reproducible demos).

    Returns:
        A valid :class:`WeatherEvent`.
    """
    temp_min = round(rng.uniform(-5.0, 15.0), 1)
    temp_max = round(temp_min + rng.uniform(2.0, 10.0), 1)
    rain = round(rng.choice([0.0, 0.0, 0.0, rng.uniform(0.1, 12.0)]), 1)
    return WeatherEvent(
        station_id=station_id,
        observed_at=observed_at,
        temp_max_c=temp_max,
        temp_min_c=temp_min,
        rain_mm=rain,
    )


def main() -> int:
    """Publish the demo batch and flush before exiting.

    Returns:
        Process exit code: 0 on success, 1 if messages remained unsent.
    """
    load_dotenv()
    parser = argparse.ArgumentParser(description="Publish demo weather readings to Kafka")
    parser.add_argument("--stations", type=int, default=len(DEMO_STATIONS),
                        help="Number of demo stations")
    parser.add_argument("--per-station", type=int, default=10,
                        help="Readings per station")
    args = parser.parse_args()

    env = dict(os.environ)
    config = StreamingConfig.from_env(env)
    rng = random.Random(42)  # reproducible demo data
    now = datetime.now(timezone.utc)

    producer = WeatherEventProducer(
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic_raw,
        client_id="weather-producer-demo",
    )

    total = 0
    try:
        for station in DEMO_STATIONS[: max(1, args.stations)]:
            for i in range(max(1, args.per_station)):
                # Readings spaced 5 minutes apart, ending "now".
                observed_at = now - timedelta(minutes=5 * (args.per_station - i))
                event = generate_event(station, observed_at, rng)
                validate_event(event)  # demo data must pass the same gate as real data
                producer.publish(key=event.station_id, payload=event.to_dict())
                total += 1
            time.sleep(0.1)
    finally:
        unsent = producer.flush(timeout=15.0)

    logger.info("Published %d readings to topic %s", total, config.topic_raw)
    return 1 if unsent else 0


if __name__ == "__main__":
    raise SystemExit(main())
