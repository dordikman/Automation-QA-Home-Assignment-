"""
Load tests — Algorithm A throughput and multi-pod scalability.

Covers:
  TestAudioQueueThroughput  — single-pod processing rate (msgs/sec)
  TestMultiPodScalability   — competing-consumer correctness under high volume

SLA targets:
  AlgorithmA throughput (single pod) : ≥ 100 messages / second
  Message loss under burst           : 0 (zero tolerance)
  Multi-pod deduplication            : 0 duplicate DB records

All tests run against the InMemoryBroker (Python queue.Queue).
"""

import asyncio
import time

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A

_SLA_ALGO_A_MSGS_PER_SEC = 100  # minimum AlgorithmA throughput


# ---------------------------------------------------------------------------
# 1. Algorithm A throughput
# ---------------------------------------------------------------------------


@pytest.mark.load
class TestAudioQueueThroughput:
    """
    Measure how fast a single AlgorithmA pod can drain the audio work queue.

    Why: The audio queue is the pipeline's entry point. If AlgorithmA cannot
    keep up with sensor publishing rates, queue depth grows unboundedly and
    the system accumulates backpressure that affects all downstream stages.
    """

    async def test_single_pod_processes_at_least_100_messages_per_second(self, pipeline):
        """
        Publish 500 audio messages in a batch, then time process_all().
        Throughput must be ≥ 100 msgs/sec to meet the pipeline SLA.
        """
        message_count = 500
        for _ in range(message_count):
            await pipeline.sensor.publish_audio()

        start = time.perf_counter()
        processed = await pipeline.algo_a.process_all()
        elapsed = time.perf_counter() - start

        throughput = processed / elapsed
        assert (
            processed == message_count
        ), f"Expected all {message_count} messages processed, got {processed}"
        assert throughput >= _SLA_ALGO_A_MSGS_PER_SEC, (
            f"AlgorithmA throughput {throughput:.1f} msgs/s is below SLA of "
            f"{_SLA_ALGO_A_MSGS_PER_SEC} msgs/s"
        )

    async def test_algorithm_a_publishes_one_feature_a_per_audio_message(self, pipeline):
        """
        Every audio message consumed must produce exactly one Feature A on the fanout.
        Verifies the 1-to-1 mapping between audio input and feature output.
        """
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)
        message_count = 200

        for _ in range(message_count):
            await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_all()

        features_produced = []
        while not probe.empty():
            features_produced.append(probe.get_nowait())

        assert (
            len(features_produced) == message_count
        ), f"Expected {message_count} Feature A messages, got {len(features_produced)}"
        unique_sources = {f["source_message_id"] for f in features_produced}
        assert len(unique_sources) == message_count, (
            "Duplicate Feature A outputs detected — each audio message must produce "
            "exactly one Feature A"
        )


# ---------------------------------------------------------------------------
# 2. Multi-pod scalability (competing consumers)
# ---------------------------------------------------------------------------


@pytest.mark.load
class TestMultiPodScalability:
    """
    Verify competing-consumer correctness at high volume with multiple Algorithm A pods.

    Why: The assignment specifies that multiple Algorithm A pods share the audio work
    queue. Each message must be processed by exactly one pod — no drops, no duplicates.
    This is enforced by the work-queue (not fanout) pattern on the audio stream.
    """

    async def test_two_pods_process_all_1000_messages_without_loss_or_duplication(self, pipeline):
        """
        Publish 1 000 audio messages. Two competing Algorithm A pods must together
        process every message exactly once.
        """
        message_count = 1_000
        pod_2 = AlgorithmA(pipeline.broker)
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)

        for _ in range(message_count):
            await pipeline.sensor.publish_audio()

        count_1, count_2 = await asyncio.gather(
            pipeline.algo_a.process_all(),
            pod_2.process_all(),
        )

        assert count_1 + count_2 == message_count, (
            f"Total processed ({count_1 + count_2}) != published ({message_count}) "
            f"— messages were dropped"
        )
        assert (
            pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0
        ), "Audio queue is not empty after both pods drained it"

        features = []
        while not probe.empty():
            features.append(probe.get_nowait())

        assert (
            len(features) == message_count
        ), f"Feature A output count ({len(features)}) != messages published ({message_count})"
        unique_sources = {f["source_message_id"] for f in features}
        assert len(unique_sources) == message_count, (
            f"Only {len(unique_sources)} unique source IDs out of {message_count} — "
            "at least one message was processed more than once"
        )

    async def test_three_pods_process_3000_messages_without_loss_or_duplication(self, pipeline):
        """
        Three competing pods on 3 000 messages — stricter volume test for the
        work-queue guarantee that every message is processed exactly once.
        """
        message_count = 3_000
        pod_2 = AlgorithmA(pipeline.broker)
        pod_3 = AlgorithmA(pipeline.broker)
        probe = pipeline.broker.subscribe_fanout(FEATURES_A)

        for _ in range(message_count):
            await pipeline.sensor.publish_audio()

        counts = await asyncio.gather(
            pipeline.algo_a.process_all(),
            pod_2.process_all(),
            pod_3.process_all(),
        )

        assert (
            sum(counts) == message_count
        ), f"Total processed {sum(counts)} != published {message_count}"
        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0

        features = []
        while not probe.empty():
            features.append(probe.get_nowait())

        unique_sources = {f["source_message_id"] for f in features}
        assert (
            len(unique_sources) == message_count
        ), f"Duplication detected: {message_count - len(unique_sources)} duplicate(s)"
