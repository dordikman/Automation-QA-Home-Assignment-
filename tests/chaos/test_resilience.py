"""
Chaos / resilience tests — verify that the system degrades gracefully under failure
conditions rather than silently corrupting data or crashing.

Each test class targets a distinct failure mode:
  - Malformed / unexpected messages arriving on a queue
  - Idempotency guarantees holding under duplicate or concurrent delivery
  - Edge-case queue states (empty queues, mid-flight purges, zero subscribers)
"""

import asyncio

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, InMemoryBroker
from tests.helpers import make_audio_message, make_feature_a_message, make_feature_b_message

pytestmark = pytest.mark.chaos


# ---------------------------------------------------------------------------
# Malformed-message resilience
# ---------------------------------------------------------------------------


class TestBrokerResilienceUnderMalformedMessages:
    """Components handle malformed messages without crashing or losing valid data."""

    async def test_algo_a_skips_message_with_missing_fields_and_continues_processing(
        self, broker
    ):
        """
        A bad message (missing required fields) raises ValueError from AlgorithmA.process().
        Wrapping each process_one() call in try/except must still allow the two
        subsequent valid messages to be processed successfully.
        """
        algo_a = AlgorithmA(broker)

        # Inject: 1 malformed message followed by 2 valid ones
        bad_message = {"message_id": "bad-id", "sensor_id": "sensor-1"}  # missing audio_data
        await broker.publish_work(AUDIO_STREAM, bad_message)
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        await broker.publish_work(AUDIO_STREAM, make_audio_message())

        successes = 0
        for _ in range(3):
            try:
                result = await algo_a.process_one()
                if result is not None:
                    successes += 1
            except ValueError:
                pass  # Expected for the malformed message

        assert successes == 2

    async def test_algo_b_skips_wrong_feature_type_and_continues(self, broker):
        """
        A FeatureB message published to the features_a fanout has the wrong feature_type.
        AlgorithmB._validate() raises TypeError for it; the two subsequent valid
        FeatureA messages must still be processed successfully.
        """
        algo_b = AlgorithmB(broker)

        # Publish 1 wrong-type message then 2 valid FeatureA messages to the same fanout
        await broker.publish_fanout(FEATURES_A, make_feature_b_message())  # feature_type="B"
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())

        successes = 0
        for _ in range(3):
            try:
                result = await algo_b.process_one()
                if result is not None:
                    successes += 1
            except TypeError:
                pass  # Expected for the wrong-type message

        assert successes == 2

    async def test_datawriter_handles_messages_with_missing_fields_gracefully(
        self, data_writer, broker
    ):
        """
        Writing an empty dict directly to _write() must not corrupt previously
        persisted valid records.
        """
        # Persist one valid FeatureA record first
        valid_msg = make_feature_a_message()
        await broker.publish_fanout(FEATURES_A, valid_msg)
        await data_writer.flush()
        assert len(data_writer.db) == 1

        # Attempt to write a completely empty dict — should raise or silently skip
        try:
            data_writer._write({})
        except Exception:
            pass  # Any exception is acceptable; the important check is below

        # The valid record already in DB must remain intact and uncorrupted
        assert data_writer.db[0]["message_id"] == valid_msg["message_id"]


# ---------------------------------------------------------------------------
# Idempotency under stress
# ---------------------------------------------------------------------------


class TestDataWriterIdempotencyUnderStress:
    """Idempotency guarantees hold under high-duplicate and concurrent conditions."""

    async def test_idempotency_holds_with_100_duplicate_deliveries(
        self, data_writer, broker
    ):
        """Publishing the same message 100 times must result in exactly 1 DB record."""
        feature_a = make_feature_a_message()
        for _ in range(100):
            await broker.publish_fanout(FEATURES_A, feature_a)
        await data_writer.flush()
        assert len(data_writer.db) == 1

    async def test_concurrent_flush_calls_do_not_create_duplicates(
        self, data_writer, broker
    ):
        """
        Five simultaneous flush() coroutines on 10 unique messages must produce
        exactly 10 DB records — no double-writes despite concurrent access.
        """
        for _ in range(10):
            await broker.publish_fanout(FEATURES_A, make_feature_a_message())

        await asyncio.gather(*[data_writer.flush() for _ in range(5)])

        assert len(data_writer.db) == 10

    async def test_db_accumulates_correctly_across_50_flush_cycles(
        self, data_writer, broker
    ):
        """
        Publish-one / flush-one repeated 50 times must accumulate exactly 50
        distinct records with no duplicates or omissions.
        """
        for _ in range(50):
            await broker.publish_fanout(FEATURES_A, make_feature_a_message())
            await data_writer.flush()

        assert len(data_writer.db) == 50


# ---------------------------------------------------------------------------
# Queue edge cases
# ---------------------------------------------------------------------------


class TestQueueBehaviourUnderEdgeCases:
    """Broker and queue handle boundary conditions without errors."""

    async def test_empty_broker_processes_nothing_without_error(self, broker):
        """process_all() on a broker with no messages must return 0 and not raise."""
        algo_a = AlgorithmA(broker)
        count = await algo_a.process_all()
        assert count == 0

    async def test_purge_mid_processing_leaves_broker_in_clean_state(self, broker):
        """
        After processing 2 of 5 messages, purge_all() must zero the queue.
        Subsequent publish + process must still work correctly.
        """
        algo_a = AlgorithmA(broker)

        for _ in range(5):
            await broker.publish_work(AUDIO_STREAM, make_audio_message())

        # Process 2 messages, then purge the remaining 3
        await algo_a.process_one()
        await algo_a.process_one()
        broker.purge_all()

        assert broker.work_queue_depth(AUDIO_STREAM) == 0

        # Broker must still accept new messages and process them after purge
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        result = await algo_a.process_one()
        assert result is not None

    async def test_fanout_with_zero_subscribers_does_not_raise(self, broker):
        """
        Publishing to a fanout topic with no subscribers must complete silently.
        After the zero-subscriber publish, subscribing and receiving a second
        message must work correctly, proving the broker is in a consistent state.
        """
        # Publish with no subscribers — must not raise
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())

        # Subscribe and verify a fresh publish is delivered correctly
        inbox = broker.subscribe_fanout(FEATURES_A)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        msg = inbox.get_nowait()
        assert msg is not None
