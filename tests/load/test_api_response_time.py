"""
Load tests — REST API response time (p99) and rate-limiter correctness.

Covers:
  TestRestApiResponseTime — sequential latency on both endpoints + rate-limit boundary

SLA target:
  REST API p99 (both endpoints) : ≤ 500 ms

All tests run against the InMemoryBroker (Python queue.Queue).
"""

import time
from statistics import quantiles

import pytest

from tests.helpers import make_feature_a_message, make_feature_b_message

_SLA_API_P99_MS = 500  # maximum REST API p99 response time (ms)

_AUTH = {"Authorization": "Bearer test-token"}


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

        assert (
            p99_ms <= _SLA_API_P99_MS
        ), f"/features/realtime p99 {p99_ms:.1f} ms exceeds SLA of {_SLA_API_P99_MS} ms"

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
            # Direct _write() is acceptable here because we are testing API latency,
            # not DataWriter correctness. This is intentional state setup for load testing.
            pipeline.writer._write(make_feature_a_message(timestamp=ts))
            pipeline.writer._write(make_feature_b_message(timestamp=ts))

        assert len(pipeline.writer.db) == records_per_type * 2

        qs = {"start": "2024-01-01T00:00:00+00:00", "end": "2024-01-31T23:59:59+00:00"}
        request_count = 100
        latencies_ms = []

        for _ in range(request_count):
            t0 = time.perf_counter()
            resp = pipeline.client.get("/features/historical", query_string=qs, headers=_AUTH)
            latencies_ms.append((time.perf_counter() - t0) * 1_000)
            assert resp.status_code == 200
            assert resp.get_json()["count"] == records_per_type * 2

        latencies_ms.sort()
        p99_ms = quantiles(latencies_ms, n=100)[98]

        assert (
            p99_ms <= _SLA_API_P99_MS
        ), f"/features/historical p99 {p99_ms:.1f} ms exceeds SLA of {_SLA_API_P99_MS} ms"

    async def test_rate_limiter_allows_exactly_100_requests_then_blocks(self, pipeline):
        """
        RATE_LIMIT_MAX = 100 requests / 60 s per client IP.
        Sending 110 rapid requests must yield exactly 100 successes and 10 rejections.

        This validates that the rate limiter does not degrade under a burst of
        requests — a common DoS vector against public APIs.
        """
        statuses = [
            pipeline.client.get("/features/realtime", headers=_AUTH).status_code for _ in range(110)
        ]

        successes = statuses.count(200)
        rate_limited = statuses.count(429)

        assert (
            successes == 100
        ), f"Expected exactly 100 successful requests before rate limiting, got {successes}"
        assert rate_limited == 10, f"Expected 10 rate-limited responses (429), got {rate_limited}"
