"""Unit tests for the streaming producer (broker not required)."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from src.streaming.producer import PublishError, WeatherEventProducer

BOOTSTRAP = "localhost:9092"
TOPIC = "weather.readings.raw"
PAYLOAD = {"station_id": "warsaw-1", "temp_max_c": 15.0}


@pytest.fixture
def producer_cls():
    """Patch confluent_kafka.Producer for the duration of a test."""
    with patch("src.streaming.producer.Producer") as mock:
        yield mock


class TestConstruction:
    def test_valid_config_creates_producer(self, producer_cls) -> None:
        WeatherEventProducer(BOOTSTRAP, TOPIC)
        config = producer_cls.call_args.args[0]
        assert config["bootstrap.servers"] == BOOTSTRAP
        assert config["enable.idempotence"] is True
        assert config["acks"] == "all"
        assert 0 < config["retries"] < 100  # bounded

    def test_producer_failure_raises_publish_error(self, producer_cls) -> None:
        producer_cls.side_effect = Exception("boom")
        with pytest.raises(PublishError):
            WeatherEventProducer(BOOTSTRAP, TOPIC)


class TestPublish:
    def test_encodes_key_and_payload(self, producer_cls) -> None:
        producer = WeatherEventProducer(BOOTSTRAP, TOPIC)
        producer.publish("warsaw-1", PAYLOAD)

        args, kwargs = producer_cls.return_value.produce.call_args
        assert kwargs["topic"] == TOPIC
        assert kwargs["key"] == "warsaw-1".encode("utf-8")
        assert json.loads(kwargs["value"].decode("utf-8")) == PAYLOAD
        assert kwargs["on_delivery"] == WeatherEventProducer._delivery_report

    def test_non_ascii_survives_ensure_ascii_false(self, producer_cls) -> None:
        producer = WeatherEventProducer(BOOTSTRAP, TOPIC)
        producer.publish("s", {"note": "zażółć gęślą jaźń"})
        value = producer_cls.return_value.produce.call_args.kwargs["value"]
        assert "zażółć gęślą jaźń" in value.decode("utf-8")

    def test_buffer_error_polls_and_retries(self, producer_cls) -> None:
        producer = WeatherEventProducer(BOOTSTRAP, TOPIC)
        mock_producer = producer_cls.return_value
        mock_producer.produce.side_effect = [BufferError(), None]
        producer.publish("k", PAYLOAD)
        assert mock_producer.poll.called

    def test_persistent_buffer_error_raises(self, producer_cls) -> None:
        producer = WeatherEventProducer(BOOTSTRAP, TOPIC)
        mock_producer = producer_cls.return_value
        mock_producer.produce.side_effect = BufferError()
        with pytest.raises(PublishError):
            producer.publish("k", PAYLOAD)
        assert mock_producer.poll.call_count >= 1


class TestDeliveryReport:
    def test_logs_on_error(self, caplog: pytest.LogCaptureFixture) -> None:
        err = KafkaError(1, "connection refused", fatal=False)
        with caplog.at_level(logging.ERROR, logger="src.streaming.producer"):
            WeatherEventProducer._delivery_report(err, None)
        assert "Delivery failed" in caplog.text

    def test_success_does_not_log_error(self, caplog: pytest.LogCaptureFixture) -> None:
        msg = MagicMock()
        msg.topic.return_value = TOPIC
        with caplog.at_level(logging.ERROR, logger="src.streaming.producer"):
            WeatherEventProducer._delivery_report(None, msg)
        assert "Delivery failed" not in caplog.text


class TestFlush:
    def test_flush_propagates_unsent_count(self, producer_cls) -> None:
        producer = WeatherEventProducer(BOOTSTRAP, TOPIC)
        producer_cls.return_value.flush.return_value = 7
        assert producer.flush(timeout=5.0) == 7

    def test_flush_zero_when_drained(self, producer_cls) -> None:
        producer = WeatherEventProducer(BOOTSTRAP, TOPIC)
        producer_cls.return_value.flush.return_value = 0
        assert producer.flush() == 0
