"""
Load tests — queue backpressure and fanout delivery under high volume.

Covers:
  TestQueueBackpressure — no message loss when producers outpace consumers

SLA target:
  Message loss under burst : 0 (zero tolerance)

All tests run against the InMemoryBroker (Python queue.Queue).
"""

import pytest

from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A

# ---------------------------------------------------------------------------
# 6. Queue backpressure — no message loss when producers outpace consumers
# ---------------------------------------------------------------------------


@pytest.mark.load
class TestQueueBackpressure:
    """
    Verify that the pipeline loses no messages when a burst of audio is published
    faster than the algorithms can process, and that queues drain completely once
    processing catches up.

    Why: Real sensors may burst-publish hundreds of audio frames simultaneously.
    If the system silently drops messages under backpressure, features are lost
    and the DB becomes incomplete — a critical correctness failure.
    """

    async def test_burst_of_1000_messages_is_fully_processed_with_no_loss(self, pipeline):
        """
        Publish a burst of 1 000 audio messages before any processing starts,
        simulating a producer outpacing consumers. After draining all stages,
        the DB must contain exactly 1 000 Feature A and 1 000 Feature B records.
        """
        burst_size = 1_000

        for _ in range(burst_size):
            await pipeline.sensor.publish_audio()

        assert (
            pipeline.broker.work_queue_depth(AUDIO_STREAM) == burst_size
        ), "Queue depth should equal the published burst before any processing"

        await pipeline.algo_a.process_all()
        await pipeline.algo_b.process_all()
        await pipeline.writer.flush()

        assert (
            pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0
        ), "Audio queue must be empty after full processing"
        assert (
            len(pipeline.writer.query(feature_type="A")) == burst_size
        ), f"Expected {burst_size} Feature A records in DB, message loss detected"
        assert (
            len(pipeline.writer.query(feature_type="B")) == burst_size
        ), f"Expected {burst_size} Feature B records in DB, message loss detected"

    async def test_queue_depth_returns_to_zero_after_processing(self, pipeline):
        """
        After alternating bursts and processing cycles, the audio queue depth
        must always return to zero — no residual messages accumulate over time.
        """
        for cycle in range(5):
            burst = 100 * (cycle + 1)
            for _ in range(burst):
                await pipeline.sensor.publish_audio()

            await pipeline.algo_a.process_all()
            assert (
                pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0
            ), f"Queue not empty after cycle {cycle + 1} with burst of {burst}"

    async def test_fanout_delivers_to_all_subscribers_under_high_volume(self, pipeline):
        """
        The features_a fanout must deliver an independent copy to every subscriber
        (DataWriter, REST API, AlgorithmB) even under high message volume.
        Each subscriber must receive exactly N copies for N published messages.
        """
        n = 500
        extra_probe = pipeline.broker.subscribe_fanout(FEATURES_A)

        for _ in range(n):
            await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_all()

        # Count messages that reached the extra probe subscriber
        received = 0
        while not extra_probe.empty():
            extra_probe.get_nowait()
            received += 1

        assert received == n, (
            f"Fanout delivered {received} messages to probe subscriber instead of {n} "
            f"— fanout guarantee violated under load"
        )
