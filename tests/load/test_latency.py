"""
Load tests — end-to-end pipeline latency (sensor → DB, p99).

Covers:
  TestEndToEndPipelineLatency — wall-clock time from publish_audio() to DB write

SLA target:
  End-to-end latency p99 : ≤ 2 000 ms

All tests run against the InMemoryBroker (Python queue.Queue).
"""

import time
from statistics import quantiles

import pytest

_SLA_E2E_LATENCY_P99_MS = 2_000  # maximum end-to-end pipeline p99 latency (ms)


# ---------------------------------------------------------------------------
# 3. End-to-end pipeline latency
# ---------------------------------------------------------------------------


@pytest.mark.load
class TestEndToEndPipelineLatency:
    """
    Measure the wall-clock time from sensor.publish_audio() to features
    appearing in the DataWriter DB.

    Why: A slow pipeline means features become available in the DB with high
    delay, which degrades the historical API and causes the real-time cache to
    serve stale data. The SLA of ≤ 2 s p99 reflects the system requirement that
    audio processing lag must stay below 2 seconds at peak sensor rate.
    """

    async def test_full_pipeline_p99_latency_is_under_2_seconds(self, pipeline):
        """
        Process 50 audio messages individually through the complete pipeline
        (sensor → AlgoA → AlgoB → DataWriter) and assert p99 ≤ 2 000 ms.
        """
        sample_count = 50
        latencies_ms = []

        for _ in range(sample_count):
            t0 = time.perf_counter()
            await pipeline.sensor.publish_audio()
            await pipeline.algo_a.process_one()
            await pipeline.algo_b.process_one()
            await pipeline.writer.flush()
            latencies_ms.append((time.perf_counter() - t0) * 1_000)

        latencies_ms.sort()
        p99_ms = quantiles(latencies_ms, n=100)[98]

        assert p99_ms <= _SLA_E2E_LATENCY_P99_MS, (
            f"End-to-end p99 latency {p99_ms:.1f} ms exceeds SLA of "
            f"{_SLA_E2E_LATENCY_P99_MS} ms"
        )

    async def test_pipeline_produces_feature_a_and_b_for_every_audio_message(self, pipeline):
        """
        After processing N audio messages through the full pipeline, exactly
        N Feature A records and N Feature B records must appear in the DB.
        Validates that no stage silently drops messages under sustained throughput.
        """
        n = 200

        for _ in range(n):
            await pipeline.sensor.publish_audio()

        await pipeline.algo_a.process_all()
        await pipeline.algo_b.process_all()
        await pipeline.writer.flush()

        feature_a_count = len(pipeline.writer.query(feature_type="A"))
        feature_b_count = len(pipeline.writer.query(feature_type="B"))

        assert feature_a_count == n, f"Expected {n} Feature A records in DB, got {feature_a_count}"
        assert feature_b_count == n, f"Expected {n} Feature B records in DB, got {feature_b_count}"

    async def test_sensor_id_is_preserved_through_full_pipeline_at_scale(self, pipeline):
        """
        Every Feature A and Feature B record written to the DB must carry the
        correct sensor_id from the originating audio message — data lineage must
        not be corrupted under high volume.
        """
        n = 100

        for _ in range(n):
            await pipeline.sensor.publish_audio()

        await pipeline.algo_a.process_all()
        await pipeline.algo_b.process_all()
        await pipeline.writer.flush()

        for record in pipeline.writer.db:
            assert record["sensor_id"] == pipeline.sensor.sensor_id, (
                f"sensor_id mismatch in {record['feature_type']} record "
                f"{record['message_id']}: expected {pipeline.sensor.sensor_id!r}, "
                f"got {record['sensor_id']!r}"
            )
