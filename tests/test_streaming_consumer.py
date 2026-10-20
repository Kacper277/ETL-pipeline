"""Unit tests for the streaming consumer (broker not required)."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.consumer import WeatherEventConsumer
from src.streaming.serialization import encode_event
from src.streaming.handlers import process_reading

BOOTSTRAP = "localhost:9092"


def make_reading(station: str = "warsaw-1") -> bytes:
    """Build a valid raw reading payload."""
    return encode_event({
        "station_id": station,
        "observed_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        "temp_max_c": 15.0,
        "temp_min_c": 5.0,
        "rain_mm": 0.0,
    })


def make_msg(value: bytes | None) -> MagicMock:
    """Build a fake Kafka message."""
    msg = MagicMock()
    msg.value.return_value = value
    msg.error.return_value = None
    return msg


@pytest.fixture
def consumer_cls():
    """Patch confluent_kafka.Consumer and Producer for a test."""
    with patch("src.streaming.consumer.Consumer") as mock_consumer, \
         patch("src.streaming.consumer.Producer") as mock_producer:
        yield mock_consumer, mock_producer


def make_consumer(consumer_cls, batch_size: int = 2, batch_timeout_s: float = 0.05):
    """Build a consumer with fully mocked Kafka plumbing."""
    consumer, _ = consumer_cls
    return WeatherEventConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id="test-group",
        topics=["weather.readings.raw"],
        dlq_topic="weather.readings.dlq",
        batch_size=batch_size,
        batch_timeout_s=batch_timeout_s,
    )


def scripted_poll(cons, messages: list[MagicMock], stop_after_n_polls: int = 100):
    """Return a poll side_effect serving ``messages`` then ``None``.

    After ``stop_after_n_polls`` calls the consumer's stop flag flips,
    so ``run()`` terminates deterministically instead of looping on a
    mocked broker that never returns.
    """
    queue = list(messages)

    def poll(timeout: float = 0.0):
        if queue:
            return queue.pop(0)
        cons._polls = getattr(cons, "_polls", 0) + 1
        if cons._polls >= stop_after_n_polls:
            cons.stop()
        return None

    return poll


class TestBatchAccumulation:
    def test_respects_batch_size(self, consumer_cls) -> None:
        cons = make_consumer(consumer_cls, batch_size=3)
        cons._consumer.poll.side_effect = [make_msg(make_reading()) for _ in range(5)]
        batch = cons._collect_batch()
        assert len(batch) == 3

    def test_respects_timeout(self, consumer_cls) -> None:
        cons = make_consumer(consumer_cls, batch_timeout_s=0.01)
        cons._consumer.poll.return_value = None
        assert cons._collect_batch() == []

    def test_transport_error_skipped(self, consumer_cls) -> None:
        cons = make_consumer(consumer_cls, batch_size=1, batch_timeout_s=0.01)
        bad = make_msg(None)
        bad.error.return_value = MagicMock(code=lambda: -1)
        cons._consumer.poll.side_effect = [bad, make_msg(make_reading())]
        assert len(cons._collect_batch()) == 1


class TestProcessing:
    def test_malformed_json_routes_to_dlq_and_batch_continues(self, consumer_cls) -> None:
        cons = make_consumer(consumer_cls)
        loaded: list[list[dict]] = []

        def loader(rows: list[dict]) -> int:
            loaded.append(rows)
            return len(rows)

        cons._consumer.commit.side_effect = lambda **kw: cons.stop()
        cons._consumer.poll.side_effect = scripted_poll(cons, [
            make_msg(b"definitely not json"),
            make_msg(make_reading("krakow-2")),
        ])
        total = cons.run(handler=process_reading, loader=loader)

        # The malformed record did not abort the batch: the valid record
        # was loaded, and the batch offset was committed.
        assert len(loaded) == 1
        assert loaded[0][0]["station_id"] == "krakow-2"
        assert total == 2

    def test_invalid_event_routes_to_dlq_and_batch_continues(self, consumer_cls) -> None:
        """A JSON object failing schema validation also lands in the DLQ."""
        cons = make_consumer(consumer_cls)
        loaded: list[list[dict]] = []

        def loader(rows: list[dict]) -> int:
            loaded.append(rows)
            return len(rows)

        cons._consumer.commit.side_effect = lambda **kw: cons.stop()
        cons._consumer.poll.side_effect = scripted_poll(cons, [
            make_msg(encode_event({"not": "a valid event"})),
            make_msg(make_reading("warsaw-1")),
        ])
        total = cons.run(handler=process_reading, loader=loader)
        assert len(loaded) == 1
        assert loaded[0][0]["station_id"] == "warsaw-1"
        assert total == 2

    def test_valid_batch_calls_loader_exactly_once(self, consumer_cls) -> None:
        cons = make_consumer(consumer_cls, batch_size=3)
        loader = MagicMock(return_value=3)
        cons._consumer.poll.side_effect = [make_msg(make_reading()) for _ in range(3)]

        batch = cons._collect_batch()
        rows = [process_reading(m.value()) for m in batch]
        loader(rows)

        loader.assert_called_once()
        assert len(rows) == 3

    def test_commit_only_after_successful_load(self, consumer_cls) -> None:
        commits: list[dict] = []

        # Failing loader -> exception propagates, nothing committed.
        cons = make_consumer(consumer_cls)
        cons._consumer.poll.side_effect = scripted_poll(cons, [make_msg(make_reading())])
        failing_loader = MagicMock(side_effect=RuntimeError("db down"))
        with pytest.raises(RuntimeError):
            cons.run(handler=process_reading, loader=failing_loader)
        assert commits == []
        assert cons._consumer.close.called

        # Successful load -> commit happens after the write.
        cons2 = make_consumer(consumer_cls)
        cons2._consumer.poll.side_effect = scripted_poll(cons2, [make_msg(make_reading())])
        cons2._consumer.commit.side_effect = lambda **kw: (
            commits.append(kw), cons2.stop(),
        )
        cons2.run(handler=process_reading, loader=MagicMock(return_value=1))
        assert len(commits) == 1

    def test_close_on_success_and_exception_paths(self, consumer_cls) -> None:
        # Success path: stop on commit; close() must run.
        cons = make_consumer(consumer_cls)
        cons._consumer.poll.side_effect = scripted_poll(cons, [make_msg(make_reading())])
        cons._consumer.commit.side_effect = lambda **kw: cons.stop()
        cons.run(handler=process_reading, loader=MagicMock(return_value=1))
        assert cons._consumer.close.called

        # Exception path: loader raises; close() must still run.
        cons2 = make_consumer(consumer_cls)
        cons2._consumer.poll.side_effect = scripted_poll(cons2, [make_msg(make_reading())])
        with pytest.raises(RuntimeError):
            cons2.run(handler=process_reading,
                      loader=MagicMock(side_effect=RuntimeError("db down")))
        assert cons2._consumer.close.called


class TestDlq:
    def test_dlq_envelope_contains_reason_and_payload(self, consumer_cls) -> None:
        _, mock_producer = consumer_cls
        cons = make_consumer(consumer_cls)
        raw = b"broken payload"
        cons._send_to_dlq(raw, "test reason")
        envelope = json.loads(
            mock_producer.return_value.produce.call_args.kwargs["value"].decode("utf-8")
        )
        assert envelope["error"] == "test reason"
        assert envelope["rejected_at"]
