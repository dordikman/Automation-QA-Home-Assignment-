"""
Load tests — DataWriter flush() throughput and idempotency under bulk load.

Covers:
  TestDataWriterThroughput — flush rate and deduplication correctness at scale

SLA target:
  DataWriter flush rate : ≥ 50 features / second

All tests run against the InMemoryBroker (Python queue.Queue).
"""

import time

import pytest

from mocks.data_writer import DataWriter
from mocks.rabbitmq import FEATURES_A, FEATURES_B
from tests.helpers import make_feature_a_message, make_feature_b_message

_SLA_DATAWRITER_FEATS_PER_SEC = 50  # minimum DataWriter flush throughput


# ---------------------------------------------------------------------------
# 4. DataWriter throughput and idempotency under bulk load
# ---------------------------------------------------------------------------


@pytest.mark.load
class TestDataWriterThroughput:
    """
    Measure DataWriter flush() throughput and verify idempotency at scale.

    Why: DataWriter is the bridge between the message broker and the database.
    Under sustained load it must keep pace with the feature fanouts to avoid
    the DB falling behind the real-time cache. Idempotency (dedup by message_id)
    must hold even when the broker re-delivers messages.
    """

    async def test_flush_rate_meets_sla_with_1000_features(self, broker):
        """
        Pre-populate 500 Feature A and 500 Feature B messages in the fanout.
        A single flush() must write all 1 000 records at ≥ 50 features/second.
        """
        writer = DataWriter(broker)
        feature_count = 500

        for _ in range(feature_count):
            await broker.publish_fanout(FEATURES_A, make_feature_a_message())
            await broker.publish_fanout(FEATURES_B, make_feature_b_message())

        start = time.perf_counter()
        written = await writer.flush()
        elapsed = time.perf_counter() - start

        throughput = written / elapsed
        assert (
            written == feature_count * 2
        ), f"Expected {feature_count * 2} records written, got {written}"
        assert throughput >= _SLA_DATAWRITER_FEATS_PER_SEC, (
            f"DataWriter flush throughput {throughput:.1f} features/s is below SLA of "
            f"{_SLA_DATAWRITER_FEATS_PER_SEC} features/s"
        )

    async def test_idempotency_under_bulk_duplicate_delivery(self, broker):
        """
        Simulate at-least-once broker delivery by publishing each message twice.
        DataWriter must deduplicate by message_id and produce exactly N unique DB records.

        This mirrors real RabbitMQ behaviour where a message can be redelivered after
        a consumer crash before acknowledging.
        """
        writer = DataWriter(broker)
        messages = [make_feature_a_message() for _ in range(300)]

        for msg in messages:
            await broker.publish_fanout(FEATURES_A, msg)
            await broker.publish_fanout(FEATURES_A, msg)  # duplicate delivery

        await writer.flush()

        assert len(writer.db) == len(messages), (
            f"Expected {len(messages)} unique DB records after deduplication, "
            f"got {len(writer.db)} — idempotency violated under bulk load"
        )

    async def test_multiple_flush_cycles_accumulate_without_duplication(self, broker):
        """
        Ten sequential flush() cycles, each receiving 100 unique features,
        must accumulate exactly 1 000 records — no double-counting across cycles.
        """
        writer = DataWriter(broker)
        cycles = 10
        features_per_cycle = 100

        for _ in range(cycles):
            for _ in range(features_per_cycle):
                await broker.publish_fanout(FEATURES_A, make_feature_a_message())
            await writer.flush()

        assert (
            len(writer.db) == cycles * features_per_cycle
        ), f"Expected {cycles * features_per_cycle} DB records, got {len(writer.db)}"
