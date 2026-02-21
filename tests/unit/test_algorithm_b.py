"""
Unit tests for Algorithm B.

Tests are split into two classes:
  TestAlgorithmBProcess         — pure logic: process() with no broker side-effects
  TestAlgorithmBBrokerInteraction — fanout inbox consume/publish via process_one
"""

import pytest

from mocks.rabbitmq import FEATURES_A, FEATURES_B
from tests.helpers import make_feature_a_message, make_feature_b_message

_REQUIRED_OUTPUT_FIELDS = {
    "message_id",
    "source_message_id",
    "feature_type",
    "sensor_id",
    "timestamp",
    "processed_at",
    "features",
}

_VALID_CLASSIFICATIONS = {"speech", "music", "noise", "silence", "mixed"}


@pytest.mark.unit
class TestAlgorithmBProcess:
    """Tests for the core processing logic of AlgorithmB.process()."""

    async def test_valid_feature_a_returns_feature_type_b(self, algo_b):
        result = algo_b.process(make_feature_a_message())
        assert result["feature_type"] == "B"

    async def test_output_contains_all_required_fields(self, algo_b):
        result = algo_b.process(make_feature_a_message())
        assert _REQUIRED_OUTPUT_FIELDS.issubset(result.keys())

    async def test_output_preserves_sensor_id(self, algo_b):
        result = algo_b.process(make_feature_a_message(sensor_id="sensor-99"))
        assert result["sensor_id"] == "sensor-99"

    async def test_output_preserves_timestamp(self, algo_b):
        result = algo_b.process(make_feature_a_message(timestamp="2024-03-10T08:00:00+00:00"))
        assert result["timestamp"] == "2024-03-10T08:00:00+00:00"

    async def test_output_links_source_message_id(self, algo_b):
        msg = make_feature_a_message()
        result = algo_b.process(msg)
        assert result["source_message_id"] == msg["message_id"]

    async def test_output_classification_is_a_known_value(self, algo_b):
        result = algo_b.process(make_feature_a_message())
        assert result["features"]["classification"] in _VALID_CLASSIFICATIONS

    async def test_output_confidence_is_between_0_and_1(self, algo_b):
        result = algo_b.process(make_feature_a_message())
        confidence = result["features"]["confidence"]
        assert 0.0 <= confidence <= 1.0

    async def test_output_derived_metrics_present(self, algo_b):
        result = algo_b.process(make_feature_a_message())
        metrics = result["features"]["derived_metrics"]
        assert {"mfcc_mean", "spectral_spread", "activity_score"}.issubset(metrics.keys())

    @pytest.mark.parametrize(
        "missing_field",
        [
            "message_id",
            "sensor_id",
            "timestamp",
            "feature_type",
            "features",
        ],
    )
    async def test_missing_required_field_raises_value_error(self, algo_b, missing_field):
        msg = make_feature_a_message()
        del msg[missing_field]
        with pytest.raises(ValueError):
            algo_b.process(msg)

    async def test_wrong_feature_type_raises_type_error(self, algo_b):
        with pytest.raises(TypeError, match="Expected feature_type 'A'"):
            algo_b.process(make_feature_b_message())

    @pytest.mark.parametrize("bad_type", ["B", "C", "", None])
    async def test_non_a_feature_type_raises_type_error(self, algo_b, bad_type):
        msg = make_feature_a_message(feature_type=bad_type)
        with pytest.raises(TypeError):
            algo_b.process(msg)


@pytest.mark.unit
class TestAlgorithmBBrokerInteraction:
    """Tests for fanout inbox consume/publish via process_one()."""

    async def test_process_one_returns_none_when_inbox_is_empty(self, algo_b):
        assert await algo_b.process_one() is None

    async def test_process_one_consumes_from_features_a_fanout(self, algo_b, broker):
        # algo_b subscribed to FEATURES_A at construction; publish after subscription
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        result = await algo_b.process_one()
        assert result is not None
        assert result["feature_type"] == "B"

    async def test_process_one_publishes_to_features_b_fanout(self, algo_b, broker):
        downstream = broker.subscribe_fanout(FEATURES_B)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await algo_b.process_one()
        assert not downstream.empty()

    async def test_process_one_increments_processed_count(self, algo_b, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await algo_b.process_one()
        assert algo_b.processed_count == 1

    async def test_process_all_drains_inbox_and_returns_count(self, algo_b, broker):
        for _ in range(4):
            await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        count = await algo_b.process_all()
        assert count == 4
