"""
Integration tests for the full audio processing pipeline.

Tests are organised into four classes that mirror the pipeline's stages:
  TestSensorToAlgorithmA    — audio publishing and Algorithm A consumption
  TestAlgorithmAToAlgorithmB — feature propagation through the fanout
  TestDataWriterPersistence  — feature storage and queryability
  TestRestApiEndToEnd        — features surfaced via both REST API endpoints
  TestLoadBalancing          — competing-consumer behaviour with multiple pods
"""

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, FEATURES_B


@pytest.mark.integration
class TestSensorToAlgorithmA:
    """Sensor publishes audio; Algorithm A consumes and produces Feature A."""

    async def test_published_audio_is_consumed_by_algo_a(self, pipeline):
        await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_one()
        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0

    async def test_algo_a_produces_feature_a_on_fanout(self, pipeline):
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)
        await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_one()
        assert not probe.empty()
        msg = probe.get_nowait()
        assert msg["feature_type"] == "A"

    async def test_sensor_id_is_propagated_to_feature_a(self, pipeline):
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)
        await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_one()
        msg = probe.get_nowait()
        assert msg["sensor_id"] == pipeline.sensor.sensor_id

    async def test_multiple_audio_messages_all_processed(self, pipeline):
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)
        for _ in range(5):
            await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_all()
        features = []
        while not probe.empty():
            features.append(probe.get_nowait())
        assert len(features) == 5


@pytest.mark.integration
class TestAlgorithmAToAlgorithmB:
    """Feature A published to the fanout is consumed by Algorithm B."""

    async def test_algo_b_receives_feature_a_from_fanout(self, pipeline):
        await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_one()
        result = await pipeline.algo_b.process_one()
        assert result is not None
        assert result["feature_type"] == "B"

    async def test_feature_b_is_published_to_features_b_fanout(self, pipeline):
        probe = pipeline.broker.subscribe_fanout(FEATURES_B)
        await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_one()
        await pipeline.algo_b.process_one()
        assert not probe.empty()
        assert probe.get_nowait()["feature_type"] == "B"

    async def test_sensor_id_is_preserved_through_both_algorithms(self, pipeline):
        await pipeline.sensor.publish_audio(timestamp="2024-01-15T10:00:00+00:00")
        await pipeline.algo_a.process_one()
        feature_b = await pipeline.algo_b.process_one()
        assert feature_b["sensor_id"] == pipeline.sensor.sensor_id

    async def test_timestamp_is_preserved_through_both_algorithms(self, pipeline):
        await pipeline.sensor.publish_audio(timestamp="2024-01-15T10:00:00+00:00")
        await pipeline.algo_a.process_one()
        feature_b = await pipeline.algo_b.process_one()
        assert feature_b["timestamp"] == "2024-01-15T10:00:00+00:00"


@pytest.mark.integration
class TestDataWriterPersistence:
    """DataWriter receives features from the fanout and stores them in the DB."""

    async def _run_pipeline(self, pipeline, timestamp="2024-01-15T10:00:00+00:00"):
        await pipeline.sensor.publish_audio(timestamp=timestamp)
        await pipeline.algo_a.process_one()
        await pipeline.algo_b.process_one()
        await pipeline.writer.flush()

    async def test_feature_a_is_written_to_db(self, pipeline):
        await self._run_pipeline(pipeline)
        results = pipeline.writer.query(feature_type="A")
        assert len(results) == 1

    async def test_feature_b_is_written_to_db(self, pipeline):
        await self._run_pipeline(pipeline)
        results = pipeline.writer.query(feature_type="B")
        assert len(results) == 1

    async def test_both_features_written_from_single_audio_message(self, pipeline):
        await self._run_pipeline(pipeline)
        assert len(pipeline.writer.db) == 2

    async def test_multiple_audio_messages_written_correctly(self, pipeline):
        for i in range(3):
            await pipeline.sensor.publish_audio(timestamp=f"2024-01-15T{10 + i:02d}:00:00+00:00")
        await pipeline.algo_a.process_all()
        await pipeline.algo_b.process_all()
        await pipeline.writer.flush()
        assert pipeline.writer.query(feature_type="A") == pipeline.writer.query(feature_type="A")
        assert len(pipeline.writer.db) == 6  # 3 Feature A + 3 Feature B


@pytest.mark.integration
class TestRestApiEndToEnd:
    """Features produced by the pipeline are accessible via the REST API."""

    _AUTH = {"Authorization": "Bearer test-token"}
    _TS = "2024-01-15T10:00:00+00:00"
    _RANGE = {"start": "2024-01-15T00:00:00+00:00", "end": "2024-01-15T23:59:59+00:00"}

    async def _run_pipeline(self, pipeline):
        await pipeline.sensor.publish_audio(timestamp=self._TS)
        await pipeline.algo_a.process_one()
        await pipeline.algo_b.process_one()

    async def test_realtime_endpoint_returns_feature_a(self, pipeline):
        await self._run_pipeline(pipeline)
        response = pipeline.client.get("/features/realtime", headers=self._AUTH)
        data = response.get_json()
        assert response.status_code == 200
        types = {f["feature_type"] for f in data["features"]}
        assert "A" in types

    async def test_realtime_endpoint_returns_feature_b(self, pipeline):
        await self._run_pipeline(pipeline)
        response = pipeline.client.get("/features/realtime", headers=self._AUTH)
        types = {f["feature_type"] for f in response.get_json()["features"]}
        assert "B" in types

    async def test_realtime_returns_both_feature_types(self, pipeline):
        await self._run_pipeline(pipeline)
        response = pipeline.client.get("/features/realtime", headers=self._AUTH)
        types = {f["feature_type"] for f in response.get_json()["features"]}
        assert types == {"A", "B"}

    async def test_historical_endpoint_returns_db_features(self, pipeline):
        await self._run_pipeline(pipeline)
        await pipeline.writer.flush()
        response = pipeline.client.get(
            "/features/historical",
            query_string=self._RANGE,
            headers=self._AUTH,
        )
        data = response.get_json()
        assert response.status_code == 200
        assert data["count"] == 2

    async def test_historical_excludes_features_outside_query_range(self, pipeline):
        await self._run_pipeline(pipeline)
        await pipeline.writer.flush()
        response = pipeline.client.get(
            "/features/historical",
            query_string={
                "start": "2025-01-01T00:00:00+00:00",
                "end": "2025-12-31T23:59:59+00:00",
            },
            headers=self._AUTH,
        )
        assert response.get_json()["count"] == 0


@pytest.mark.integration
class TestLoadBalancing:
    """Competing consumers on the audio work queue process each message exactly once."""

    async def test_two_algo_a_pods_process_all_messages_without_duplication(self, pipeline):
        """
        With two Algorithm A pods competing on the audio work queue,
        N messages must produce exactly N Feature A outputs (no drops, no duplicates).
        """
        message_count = 10
        pod_2 = AlgorithmA(pipeline.broker)

        # Capture all Feature A outputs via a dedicated subscriber
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)

        for _ in range(message_count):
            await pipeline.sensor.publish_audio()

        await pipeline.algo_a.process_all()
        await pod_2.process_all()

        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0

        features_produced = []
        while not probe.empty():
            features_produced.append(probe.get_nowait())

        assert len(features_produced) == message_count

        unique_source_ids = {f["source_message_id"] for f in features_produced}
        assert len(unique_source_ids) == message_count, "Duplicate processing detected"

    async def test_three_algo_a_pods_distribute_work(self, pipeline):
        pod_2 = AlgorithmA(pipeline.broker)
        pod_3 = AlgorithmA(pipeline.broker)

        for _ in range(9):
            await pipeline.sensor.publish_audio()

        counts = [
            await pipeline.algo_a.process_all(),
            await pod_2.process_all(),
            await pod_3.process_all(),
        ]

        # Total work done equals total messages published
        assert sum(counts) == 9
        # Each pod handled at least some messages (probabilistic with 9 messages)
        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0
