"""
Pipeline throughput, latency, and backpressure load tests.

These tests are deliberately excluded from the standard test run
(norecursedirs = tests/load in pytest.ini) and are executed on demand
or in the CI nightly schedule.

To run:
    pytest tests/load/test_pipeline_throughput.py -v -s

Each class maps to a distinct system-level concern from the assignment:

  TestAudioQueueThroughput       — Algorithm A pod processing rate (msgs/sec)
  TestMultiPodScalability        — Competing-consumer correctness under high volume
  TestEndToEndPipelineLatency    — Sensor → DB wall-clock latency (p99)
  TestDataWriterThroughput       — DataWriter flush rate and idempotency under bulk load
  TestRestApiResponseTime        — REST API latency (p99) under sequential load
  TestQueueBackpressure          — No message loss when producers outpace consumers

SLA targets (in-memory baseline; real services use the same thresholds):
  AlgorithmA throughput (single pod) : ≥ 100 messages / second
  End-to-end latency p99             : ≤ 2 000 ms
  DataWriter flush rate              : ≥ 50 features / second
  REST API p99 (both endpoints)      : ≤ 500 ms
  Message loss under burst           : 0 (zero tolerance)
"""

import asyncio
import time
from statistics import quantiles

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, FEATURES_B
from tests.helpers import make_feature_a_message, make_feature_b_message

# ---------------------------------------------------------------------------
# SLA constants
# ---------------------------------------------------------------------------
_SLA_ALGO_A_MSGS_PER_SEC = 100       # minimum AlgorithmA throughput
_SLA_E2E_LATENCY_P99_MS = 2_000      # maximum end-to-end pipeline p99 latency (ms)
_SLA_DATAWRITER_FEATS_PER_SEC = 50   # minimum DataWriter flush throughput
_SLA_API_P99_MS = 500                # maximum REST API p99 response time (ms)

_AUTH = {"Authorization": "Bearer test-token"}


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
        assert processed == message_count, (
            f"Expected all {message_count} messages processed, got {processed}"
        )
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

        assert len(features_produced) == message_count, (
            f"Expected {message_count} Feature A messages, got {len(features_produced)}"
        )
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

    async def test_two_pods_process_all_1000_messages_without_loss_or_duplication(
        self, pipeline
    ):
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
        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0, (
            "Audio queue is not empty after both pods drained it"
        )

        features = []
        while not probe.empty():
            features.append(probe.get_nowait())

        assert len(features) == message_count, (
            f"Feature A output count ({len(features)}) != messages published ({message_count})"
        )
        unique_sources = {f["source_message_id"] for f in features}
        assert len(unique_sources) == message_count, (
            f"Only {len(unique_sources)} unique source IDs out of {message_count} — "
            "at least one message was processed more than once"
        )

    async def test_three_pods_process_3000_messages_without_loss_or_duplication(
        self, pipeline
    ):
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

        assert sum(counts) == message_count, (
            f"Total processed {sum(counts)} != published {message_count}"
        )
        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0

        features = []
        while not probe.empty():
            features.append(probe.get_nowait())

        unique_sources = {f["source_message_id"] for f in features}
        assert len(unique_sources) == message_count, (
            f"Duplication detected: {message_count - len(unique_sources)} duplicate(s)"
        )


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

        assert feature_a_count == n, (
            f"Expected {n} Feature A records in DB, got {feature_a_count}"
        )
        assert feature_b_count == n, (
            f"Expected {n} Feature B records in DB, got {feature_b_count}"
        )

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
        assert written == feature_count * 2, (
            f"Expected {feature_count * 2} records written, got {written}"
        )
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

        assert len(writer.db) == cycles * features_per_cycle, (
            f"Expected {cycles * features_per_cycle} DB records, got {len(writer.db)}"
        )


# ---------------------------------------------------------------------------
# 5. REST API response time under sequential load
# ---------------------------------------------------------------------------

@pytest.mark.load
class TestRestApiResponseTime:
    """
    Measure REST API endpoint latency under sequential load using the Flask
    test client. Both endpoints must satisfy the ≤ 500 ms p99 SLA.

    Why: The REST API is the external-facing surface of the system. All
    internet-connected clients depend on it. Breaching the latency SLA means
    degraded user experience and potential SLA contract violations.
    """

    async def test_realtime_endpoint_p99_is_under_500ms(self, pipeline):
        """
        Warm up the pipeline cache with 20 features, then send 80 sequential
        requests to /features/realtime. p99 must be ≤ 500 ms.

        Request count (80) is kept below the per-client rate limit (100/60 s)
        so this test measures pure latency, not rate-limiting behaviour.
        Rate-limit correctness is covered separately in test_rate_limiter_*.
        """
        for _ in range(20):
            await pipeline.sensor.publish_audio()
        await pipeline.algo_a.process_all()

        request_count = 80
        latencies_ms = []

        for _ in range(request_count):
            t0 = time.perf_counter()
            resp = pipeline.client.get("/features/realtime", headers=_AUTH)
            latencies_ms.append((time.perf_counter() - t0) * 1_000)
            assert resp.status_code == 200, f"Unexpected status {resp.status_code}"

        latencies_ms.sort()
        p99_ms = quantiles(latencies_ms, n=100)[98]

        assert p99_ms <= _SLA_API_P99_MS, (
            f"/features/realtime p99 {p99_ms:.1f} ms exceeds SLA of {_SLA_API_P99_MS} ms"
        )

    async def test_historical_endpoint_p99_is_under_500ms_with_large_dataset(self, pipeline):
        """
        Pre-populate the DB with 2 000 feature records spanning a wide date range,
        then issue 100 sequential /features/historical requests that match all records.
        p99 must be ≤ 500 ms even with a full linear scan of the in-memory DB.
        """
        records_per_type = 1_000
        for i in range(records_per_type):
            day = (i % 28) + 1
            hour = i % 24
            ts = f"2024-01-{day:02d}T{hour:02d}:00:00+00:00"
            pipeline.writer.db.append(make_feature_a_message(timestamp=ts))
            pipeline.writer.db.append(make_feature_b_message(timestamp=ts))

        assert len(pipeline.writer.db) == records_per_type * 2

        qs = {"start": "2024-01-01T00:00:00+00:00", "end": "2024-01-31T23:59:59+00:00"}
        request_count = 100
        latencies_ms = []

        for _ in range(request_count):
            t0 = time.perf_counter()
            resp = pipeline.client.get(
                "/features/historical", query_string=qs, headers=_AUTH
            )
            latencies_ms.append((time.perf_counter() - t0) * 1_000)
            assert resp.status_code == 200
            assert resp.get_json()["count"] == records_per_type * 2

        latencies_ms.sort()
        p99_ms = quantiles(latencies_ms, n=100)[98]

        assert p99_ms <= _SLA_API_P99_MS, (
            f"/features/historical p99 {p99_ms:.1f} ms exceeds SLA of {_SLA_API_P99_MS} ms"
        )

    async def test_rate_limiter_allows_exactly_100_requests_then_blocks(self, pipeline):
        """
        RATE_LIMIT_MAX = 100 requests / 60 s per client IP.
        Sending 110 rapid requests must yield exactly 100 successes and 10 rejections.

        This validates that the rate limiter does not degrade under a burst of
        requests — a common DoS vector against public APIs.
        """
        statuses = [
            pipeline.client.get("/features/realtime", headers=_AUTH).status_code
            for _ in range(110)
        ]

        successes = statuses.count(200)
        rate_limited = statuses.count(429)

        assert successes == 100, (
            f"Expected exactly 100 successful requests before rate limiting, got {successes}"
        )
        assert rate_limited == 10, (
            f"Expected 10 rate-limited responses (429), got {rate_limited}"
        )


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

        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == burst_size, (
            "Queue depth should equal the published burst before any processing"
        )

        await pipeline.algo_a.process_all()
        await pipeline.algo_b.process_all()
        await pipeline.writer.flush()

        assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0, (
            "Audio queue must be empty after full processing"
        )
        assert len(pipeline.writer.query(feature_type="A")) == burst_size, (
            f"Expected {burst_size} Feature A records in DB, message loss detected"
        )
        assert len(pipeline.writer.query(feature_type="B")) == burst_size, (
            f"Expected {burst_size} Feature B records in DB, message loss detected"
        )

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
            assert pipeline.broker.work_queue_depth(AUDIO_STREAM) == 0, (
                f"Queue not empty after cycle {cycle + 1} with burst of {burst}"
            )

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
