"""
Contract tests — verify that the output schema produced by one component exactly matches
the input schema expected by the next component in the pipeline.

In a distributed system a silent schema mismatch between AlgorithmA's output and
AlgorithmB's input could cause data loss with no visible error. These tests catch
that class of bugs before they reach production by exercising the real component
code end-to-end and asserting on the shape of every message at every boundary.
"""

import base64
import uuid
from datetime import datetime

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, FEATURES_B
from mocks.sensor import Sensor
from tests.helpers import make_audio_message, make_feature_a_message, make_feature_b_message

pytestmark = pytest.mark.contract


# ---------------------------------------------------------------------------
# Sensor → AlgorithmA boundary
# ---------------------------------------------------------------------------


class TestAudioMessageContract:
    """Sensor output schema exactly satisfies AlgorithmA's required input fields."""

    async def test_sensor_output_satisfies_algo_a_required_fields(self, broker):
        sensor = Sensor(broker, sensor_id="contract-sensor")
        await sensor.publish_audio()
        message = await broker.consume_work(AUDIO_STREAM)
        required = {"message_id", "sensor_id", "timestamp", "audio_data"}
        assert required.issubset(set(message.keys()))

    async def test_sensor_audio_data_is_valid_base64(self, broker):
        sensor = Sensor(broker, sensor_id="contract-sensor")
        msg = await sensor.publish_audio()
        decoded = base64.b64decode(msg["audio_data"])
        assert len(decoded) > 0

    async def test_sensor_timestamp_is_valid_iso8601(self, broker):
        sensor = Sensor(broker, sensor_id="contract-sensor")
        msg = await sensor.publish_audio()
        parsed = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
        assert parsed is not None

    async def test_sensor_message_id_is_valid_uuid(self, broker):
        sensor = Sensor(broker, sensor_id="contract-sensor")
        msg = await sensor.publish_audio()
        parsed_uuid = uuid.UUID(msg["message_id"])
        assert parsed_uuid.version == 4


# ---------------------------------------------------------------------------
# AlgorithmA → AlgorithmB boundary
# ---------------------------------------------------------------------------


class TestFeatureAContract:
    """AlgorithmA output schema exactly satisfies AlgorithmB's required input fields."""

    async def test_algo_a_output_satisfies_algo_b_required_fields(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        required = {
            "message_id",
            "source_message_id",
            "feature_type",
            "sensor_id",
            "timestamp",
            "processed_at",
            "features",
        }
        assert required.issubset(set(feature_a.keys()))

    async def test_feature_a_type_field_is_exactly_A(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        assert feature_a["feature_type"] == "A"

    async def test_feature_a_features_dict_has_mfcc_list_of_13(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        mfcc = feature_a["features"]["mfcc"]
        assert isinstance(mfcc, list)
        assert len(mfcc) == 13

    async def test_feature_a_features_dict_has_spectral_centroid_float(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        centroid = feature_a["features"]["spectral_centroid"]
        assert isinstance(centroid, float)
        assert centroid > 0

    async def test_feature_a_features_dict_has_zero_crossing_rate_in_range(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        zcr = feature_a["features"]["zero_crossing_rate"]
        assert 0 <= zcr <= 1

    async def test_feature_a_features_dict_has_rms_energy_positive(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        assert feature_a["features"]["rms_energy"] > 0

    async def test_feature_a_timestamp_format_matches_sensor_input(self, broker):
        inbox = broker.subscribe_fanout(FEATURES_A)
        algo_a = AlgorithmA(broker)
        original_ts = "2024-06-01T12:00:00+00:00"
        await broker.publish_work(AUDIO_STREAM, make_audio_message(timestamp=original_ts))
        await algo_a.process_one()
        feature_a = inbox.get_nowait()
        assert feature_a["timestamp"] == original_ts


# ---------------------------------------------------------------------------
# AlgorithmB → DataWriter / REST API boundary
# ---------------------------------------------------------------------------


class TestFeatureBContract:
    """AlgorithmB output schema satisfies DataWriter's and the REST API's requirements."""

    async def test_algo_b_output_satisfies_datawriter_required_fields(self, broker):
        algo_b = AlgorithmB(broker)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        feature_b = await algo_b.process_one()
        required = {
            "message_id",
            "source_message_id",
            "feature_type",
            "sensor_id",
            "timestamp",
            "processed_at",
            "features",
        }
        assert required.issubset(set(feature_b.keys()))

    async def test_feature_b_type_field_is_exactly_B(self, broker):
        algo_b = AlgorithmB(broker)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        feature_b = await algo_b.process_one()
        assert feature_b["feature_type"] == "B"

    async def test_feature_b_classification_is_one_of_known_values(self, broker):
        algo_b = AlgorithmB(broker)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        feature_b = await algo_b.process_one()
        known = {"speech", "music", "noise", "silence", "mixed"}
        assert feature_b["features"]["classification"] in known

    async def test_feature_b_confidence_is_float_between_0_and_1(self, broker):
        algo_b = AlgorithmB(broker)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        feature_b = await algo_b.process_one()
        conf = feature_b["features"]["confidence"]
        assert isinstance(conf, float)
        assert 0 <= conf <= 1

    async def test_feature_b_derived_metrics_has_required_keys(self, broker):
        algo_b = AlgorithmB(broker)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        feature_b = await algo_b.process_one()
        required_keys = {"mfcc_mean", "spectral_spread", "activity_score"}
        assert required_keys.issubset(set(feature_b["features"]["derived_metrics"].keys()))

    async def test_feature_b_preserves_sensor_id_from_feature_a(self, broker):
        algo_b = AlgorithmB(broker)
        feature_a = make_feature_a_message(sensor_id="sensor-contract-lineage")
        await broker.publish_fanout(FEATURES_A, feature_a)
        feature_b = await algo_b.process_one()
        assert feature_b["sensor_id"] == "sensor-contract-lineage"


# ---------------------------------------------------------------------------
# DataWriter DB read/write roundtrip
# ---------------------------------------------------------------------------


class TestDataWriterDatabaseContract:
    """Messages written to the in-memory DB can be read back with full fidelity."""

    async def test_written_feature_a_is_queryable_by_feature_type(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await data_writer.flush()
        results = data_writer.query(feature_type="A")
        assert len(results) == 1
        assert results[0]["feature_type"] == "A"

    async def test_written_feature_b_is_queryable_by_feature_type(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_B, make_feature_b_message())
        await data_writer.flush()
        results = data_writer.query(feature_type="B")
        assert len(results) == 1
        assert results[0]["feature_type"] == "B"

    async def test_written_record_timestamp_survives_storage_and_retrieval(
        self, data_writer, broker
    ):
        ts = "2024-03-15T09:30:00+00:00"
        await broker.publish_fanout(FEATURES_A, make_feature_a_message(timestamp=ts))
        await data_writer.flush()
        results = data_writer.query(feature_type="A")
        assert results[0]["timestamp"] == ts

    async def test_written_record_features_dict_survives_storage_and_retrieval(
        self, data_writer, broker
    ):
        original_features = {
            "mfcc": [round(i * 0.1, 1) for i in range(13)],
            "spectral_centroid": 999.99,
            "zero_crossing_rate": 0.123,
            "rms_energy": 0.456,
        }
        await broker.publish_fanout(FEATURES_A, make_feature_a_message(features=original_features))
        await data_writer.flush()
        results = data_writer.query(feature_type="A")
        assert results[0]["features"] == original_features

    async def test_query_by_sensor_id_returns_only_matching_records(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message(sensor_id="sensor-target"))
        await broker.publish_fanout(FEATURES_A, make_feature_a_message(sensor_id="sensor-other"))
        await data_writer.flush()
        results = data_writer.query(sensor_id="sensor-target")
        assert len(results) == 1
        assert results[0]["sensor_id"] == "sensor-target"
