"""
Unit tests for Algorithm A.

Tests are split into two classes:
  TestAlgorithmAProcess         — pure logic: process() with no broker side-effects
  TestAlgorithmABrokerInteraction — queue consume/publish via process_one / process_all
"""

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A
from tests.helpers import make_audio_message

_REQUIRED_OUTPUT_FIELDS = {
    "message_id",
    "source_message_id",
    "feature_type",
    "sensor_id",
    "timestamp",
    "processed_at",
    "features",
}

_REQUIRED_FEATURE_FIELDS = {
    "mfcc",
    "spectral_centroid",
    "zero_crossing_rate",
    "rms_energy",
}


@pytest.mark.unit
class TestAlgorithmAProcess:
    """Tests for the core processing logic of AlgorithmA.process()."""

    async def test_valid_audio_returns_feature_type_a(self, algo_a):
        result = algo_a.process(make_audio_message())
        assert result["feature_type"] == "A"

    async def test_output_contains_all_required_fields(self, algo_a):
        result = algo_a.process(make_audio_message())
        assert _REQUIRED_OUTPUT_FIELDS.issubset(result.keys())

    async def test_output_features_contains_all_required_fields(self, algo_a):
        result = algo_a.process(make_audio_message())
        assert _REQUIRED_FEATURE_FIELDS.issubset(result["features"].keys())

    async def test_output_mfcc_has_13_coefficients(self, algo_a):
        result = algo_a.process(make_audio_message())
        assert len(result["features"]["mfcc"]) == 13

    async def test_output_preserves_sensor_id(self, algo_a):
        result = algo_a.process(make_audio_message(sensor_id="sensor-XYZ"))
        assert result["sensor_id"] == "sensor-XYZ"

    async def test_output_preserves_timestamp(self, algo_a):
        result = algo_a.process(make_audio_message(timestamp="2024-06-01T12:00:00+00:00"))
        assert result["timestamp"] == "2024-06-01T12:00:00+00:00"

    async def test_output_links_source_message_id(self, algo_a):
        msg = make_audio_message()
        result = algo_a.process(msg)
        assert result["source_message_id"] == msg["message_id"]

    async def test_feature_extraction_is_deterministic(self, algo_a):
        """Same audio_data must always produce identical feature values."""
        msg = make_audio_message()
        result_1 = algo_a.process(msg)
        result_2 = algo_a.process(msg)
        assert result_1["features"] == result_2["features"]

    @pytest.mark.parametrize("missing_field", [
        "message_id", "sensor_id", "timestamp", "audio_data",
    ])
    async def test_missing_required_field_raises_value_error(self, algo_a, missing_field):
        msg = make_audio_message()
        del msg[missing_field]
        with pytest.raises(ValueError):
            algo_a.process(msg)

    async def test_empty_audio_data_raises_value_error(self, algo_a):
        with pytest.raises(ValueError, match="audio_data cannot be empty"):
            algo_a.process(make_audio_message(audio_data=""))

    @pytest.mark.parametrize("bad_timestamp", [
        "not-a-date",
        "15/01/2024",
        "",
    ])
    async def test_invalid_timestamp_raises_value_error(self, algo_a, bad_timestamp):
        with pytest.raises(ValueError):
            algo_a.process(make_audio_message(timestamp=bad_timestamp))


@pytest.mark.unit
class TestAlgorithmABrokerInteraction:
    """Tests for consume/publish behaviour via process_one() and process_all()."""

    async def test_process_one_returns_none_when_audio_queue_is_empty(self, algo_a):
        assert await algo_a.process_one() is None

    async def test_process_one_consumes_message_from_audio_queue(self, algo_a, broker):
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        assert broker.work_queue_depth(AUDIO_STREAM) == 0

    async def test_process_one_returns_feature_a(self, algo_a, broker):
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        result = await algo_a.process_one()
        assert result is not None
        assert result["feature_type"] == "A"

    async def test_process_one_publishes_to_features_a_fanout(self, algo_a, broker):
        # Subscribe before publishing so the message is captured
        subscriber = broker.subscribe_fanout(FEATURES_A)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        assert not subscriber.empty()

    async def test_process_one_increments_processed_count(self, algo_a, broker):
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        assert algo_a.processed_count == 1

    async def test_process_all_drains_queue_and_returns_count(self, algo_a, broker):
        for _ in range(5):
            await broker.publish_work(AUDIO_STREAM, make_audio_message())
        count = await algo_a.process_all()
        assert count == 5
        assert broker.work_queue_depth(AUDIO_STREAM) == 0

    async def test_process_all_on_empty_queue_returns_zero(self, algo_a):
        assert await algo_a.process_all() == 0
