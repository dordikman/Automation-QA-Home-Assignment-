"""
Real-service integration tests.

These tests verify behaviour that the in-memory mocks deliberately cannot
reproduce — they require a live RabbitMQ broker and PostgreSQL database.
Run `docker-compose up -d` before executing this module.

What these tests cover that the in-memory tests do not
------------------------------------------------------
RabbitMQ
  - Actual AMQP protocol: message persistence, acknowledgement, routing
  - Fanout exchange delivers independent copies to multiple bound queues
  - Competing consumers on a durable work queue (no duplicate delivery)
  - Queue depth reflects true broker state, not a Python counter

PostgreSQL
  - UNIQUE constraint on message_id enforces idempotency at the DB level
  - JSONB column stores and retrieves nested feature dicts correctly
  - Parameterised SQL prevents injection (real query planner involved)
  - Timestamp-range queries use real TIMESTAMPTZ semantics
  - TRUNCATE / isolation between tests via real transactions
"""

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A
from tests.helpers import make_audio_message, make_feature_a_message, make_feature_b_message


@pytest.mark.real
class TestRealRabbitMQWorkQueue:
    """AMQP work-queue (competing consumer) behaviour."""

    async def test_published_message_increments_queue_depth(self, real_broker):
        await real_broker.publish_work(AUDIO_STREAM, make_feature_a_message())
        assert real_broker.work_queue_depth(AUDIO_STREAM) == 1

    async def test_consumed_message_decrements_queue_depth(self, real_broker):
        await real_broker.publish_work(AUDIO_STREAM, make_feature_a_message())
        await real_broker.consume_work(AUDIO_STREAM)
        assert real_broker.work_queue_depth(AUDIO_STREAM) == 0

    async def test_consume_from_empty_queue_returns_none(self, real_broker):
        assert await real_broker.consume_work(AUDIO_STREAM) is None

    async def test_message_content_survives_broker_round_trip(self, real_broker):
        original = make_feature_a_message(sensor_id="sensor-round-trip")
        await real_broker.publish_work(AUDIO_STREAM, original)
        received = await real_broker.consume_work(AUDIO_STREAM)
        assert received["sensor_id"] == "sensor-round-trip"
        assert received["message_id"] == original["message_id"]

    async def test_two_competing_consumers_process_all_messages_without_duplication(
        self, real_broker
    ):
        """
        Core competing-consumer guarantee: N messages → N unique processed results.
        With real RabbitMQ this is enforced by the AMQP acknowledgement protocol,
        not by Python queue semantics.
        """
        probe = real_broker.subscribe_fanout(FEATURES_A)

        for _ in range(6):
            await real_broker.publish_work(AUDIO_STREAM, make_audio_message())

        pod_1 = AlgorithmA(real_broker)
        pod_2 = AlgorithmA(real_broker)
        await pod_1.process_all()
        await pod_2.process_all()

        assert real_broker.work_queue_depth(AUDIO_STREAM) == 0

        features = []
        while not probe.empty():
            features.append(probe.get_nowait())

        assert len(features) == 6
        source_ids = {f["source_message_id"] for f in features}
        assert len(source_ids) == 6, "Duplicate processing detected"


@pytest.mark.real
class TestRealRabbitMQFanout:
    """AMQP fanout exchange: each subscriber receives an independent copy."""

    async def test_single_subscriber_receives_published_message(self, real_broker):
        sub = real_broker.subscribe_fanout(FEATURES_A)
        await real_broker.publish_fanout(FEATURES_A, make_feature_a_message())
        assert not sub.empty()

    async def test_two_subscribers_each_receive_independent_copy(self, real_broker):
        sub_1 = real_broker.subscribe_fanout(FEATURES_A)
        sub_2 = real_broker.subscribe_fanout(FEATURES_A)
        await real_broker.publish_fanout(FEATURES_A, make_feature_a_message())
        # Both queues must have a copy — not competing for the same message
        assert not sub_1.empty()
        assert not sub_2.empty()

    async def test_subscriber_registered_before_publish_receives_message(self, real_broker):
        """A subscriber created before the publish must receive the message."""
        sub = real_broker.subscribe_fanout(FEATURES_A)
        await real_broker.publish_fanout(FEATURES_A, make_feature_a_message())
        msg = sub.get_nowait()
        assert msg["feature_type"] == "A"

    async def test_message_content_survives_fanout_round_trip(self, real_broker):
        original = make_feature_a_message(sensor_id="sensor-fanout")
        sub = real_broker.subscribe_fanout(FEATURES_A)
        await real_broker.publish_fanout(FEATURES_A, original)
        received = sub.get_nowait()
        assert received["sensor_id"] == "sensor-fanout"
        assert received["features"] == original["features"]


@pytest.mark.real
class TestRealPostgresDatabase:
    """PostgreSQL write/query behaviour including constraint enforcement."""

    async def test_write_feature_a_persists_record(self, real_db):
        msg = make_feature_a_message()
        assert real_db.write(msg) is True
        results = real_db.query(feature_type="A")
        assert len(results) == 1
        assert results[0]["message_id"] == msg["message_id"]

    async def test_write_feature_b_persists_record(self, real_db):
        msg = make_feature_b_message()
        assert real_db.write(msg) is True
        assert real_db.query(feature_type="B")[0]["feature_type"] == "B"

    async def test_jsonb_features_column_round_trips_correctly(self, real_db):
        msg = make_feature_a_message()
        real_db.write(msg)
        stored = real_db.query()[0]
        assert stored["features"]["mfcc"] == msg["features"]["mfcc"]
        assert stored["features"]["spectral_centroid"] == msg["features"]["spectral_centroid"]

    async def test_duplicate_message_id_rejected_by_unique_constraint(self, real_db):
        """Idempotency enforced at the database level, not just in application code."""
        msg = make_feature_a_message()
        assert real_db.write(msg) is True
        assert real_db.write(msg) is False  # ON CONFLICT DO NOTHING
        assert len(real_db.query()) == 1

    async def test_query_filters_by_feature_type(self, real_db):
        real_db.write(make_feature_a_message())
        real_db.write(make_feature_b_message())
        assert len(real_db.query(feature_type="A")) == 1
        assert len(real_db.query(feature_type="B")) == 1

    async def test_query_filters_by_sensor_id(self, real_db):
        real_db.write(make_feature_a_message(sensor_id="sensor-X"))
        real_db.write(make_feature_a_message(sensor_id="sensor-Y"))
        results = real_db.query(sensor_id="sensor-X")
        assert len(results) == 1
        assert results[0]["sensor_id"] == "sensor-X"

    async def test_query_filters_by_timestamp_range(self, real_db):
        real_db.write(make_feature_a_message(timestamp="2024-01-15T08:00:00+00:00"))
        real_db.write(make_feature_a_message(timestamp="2024-01-15T20:00:00+00:00"))
        results = real_db.query(
            start="2024-01-15T06:00:00+00:00",
            end="2024-01-15T12:00:00+00:00",
        )
        assert len(results) == 1

    async def test_sql_injection_attempt_in_sensor_id_is_harmless(self, real_db):
        """
        Parameterised queries mean injected SQL is treated as a literal string,
        not executed. The query returns zero results; the table is untouched.
        """
        real_db.write(make_feature_a_message())
        results = real_db.query(sensor_id="' OR '1'='1")
        assert results == []
        assert len(real_db.query()) == 1  # original record still intact


@pytest.mark.real
class TestRealEndToEnd:
    """Full pipeline: sensor → RabbitMQ → algorithms → PostgreSQL."""

    async def test_feature_a_and_b_written_to_db_after_pipeline_run(self, real_pipeline):
        await real_pipeline.sensor.publish_audio(timestamp="2024-01-15T10:00:00+00:00")

        await real_pipeline.algo_a.process_one()  # audio → Feature A
        await real_pipeline.algo_b.process_one()  # Feature A → Feature B
        await real_pipeline.writer.flush()  # Features A+B → PostgreSQL

        results = real_pipeline.db.query()
        assert len(results) == 2
        assert {r["feature_type"] for r in results} == {"A", "B"}

    async def test_sensor_id_preserved_through_full_pipeline(self, real_pipeline):
        await real_pipeline.sensor.publish_audio()
        await real_pipeline.algo_a.process_one()
        await real_pipeline.algo_b.process_one()
        await real_pipeline.writer.flush()

        for record in real_pipeline.db.query():
            assert record["sensor_id"] == real_pipeline.sensor.sensor_id

    async def test_historical_query_returns_only_features_in_range(self, real_pipeline):
        await real_pipeline.sensor.publish_audio(timestamp="2024-01-15T10:00:00+00:00")
        await real_pipeline.algo_a.process_one()
        await real_pipeline.algo_b.process_one()
        await real_pipeline.writer.flush()

        in_range = real_pipeline.db.query(
            start="2024-01-15T00:00:00+00:00",
            end="2024-01-15T23:59:59+00:00",
        )
        out_of_range = real_pipeline.db.query(
            start="2025-01-01T00:00:00+00:00",
            end="2025-12-31T23:59:59+00:00",
        )
        assert len(in_range) == 2
        assert len(out_of_range) == 0

    async def test_multiple_sensors_data_isolated_by_sensor_id(self, real_pipeline):
        sensor_2 = type(real_pipeline.sensor)(real_pipeline.broker, sensor_id="sensor-other")

        await real_pipeline.sensor.publish_audio()
        await sensor_2.publish_audio()

        await real_pipeline.algo_a.process_all()
        await real_pipeline.algo_b.process_all()
        await real_pipeline.writer.flush()

        results_1 = real_pipeline.db.query(sensor_id=real_pipeline.sensor.sensor_id)
        results_2 = real_pipeline.db.query(sensor_id="sensor-other")

        assert len(results_1) == 2  # Feature A + B for sensor 1
        assert len(results_2) == 2  # Feature A + B for sensor 2
