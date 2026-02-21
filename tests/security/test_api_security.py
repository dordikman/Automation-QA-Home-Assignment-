"""
Security tests for the REST API.

Tests are split into three classes:
  TestAuthentication  — all auth failure modes across both endpoints
  TestInputValidation — injection attempts and malformed parameters
  TestRateLimiting    — per-client request rate enforcement
"""

import pytest

from mocks.rabbitmq import FEATURES_A
from tests.helpers import make_feature_a_message

_VALID_HEADERS = {"Authorization": "Bearer test-token"}


@pytest.mark.security
class TestAuthentication:
    """Every endpoint must reject requests that lack or present invalid credentials."""

    @pytest.mark.parametrize(
        "endpoint",
        [
            "/features/realtime",
            "/features/historical?start=2024-01-01T00:00:00+00:00&end=2024-12-31T00:00:00+00:00",
        ],
    )
    async def test_no_auth_header_returns_401(self, client, endpoint):
        response = client.get(endpoint)
        assert response.status_code == 401

    @pytest.mark.parametrize(
        "endpoint",
        [
            "/features/realtime",
            "/features/historical?start=2024-01-01T00:00:00+00:00&end=2024-12-31T00:00:00+00:00",
        ],
    )
    async def test_invalid_token_returns_403(self, client, endpoint):
        response = client.get(endpoint, headers={"Authorization": "Bearer wrong-token"})
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "bad_header",
        [
            "no-scheme-prefix",  # missing 'Bearer'
            "Basic dXNlcjpwYXNz",  # wrong scheme
            "Bearer",  # scheme with no token
            "",  # empty string
        ],
    )
    async def test_malformed_auth_header_returns_401(self, client, bad_header):
        response = client.get(
            "/features/realtime",
            headers={"Authorization": bad_header},
        )
        assert response.status_code == 401

    @pytest.mark.parametrize("valid_token", ["test-token", "valid-token"])
    async def test_all_valid_tokens_are_accepted(self, client, valid_token):
        response = client.get(
            "/features/realtime",
            headers={"Authorization": f"Bearer {valid_token}"},
        )
        assert response.status_code == 200


@pytest.mark.security
class TestInputValidation:
    """
    Malformed or injected inputs must be rejected before reaching business logic.
    The goal is to verify that the API never passes unsanitised strings to
    downstream processing (e.g., date parsing, DB queries).
    """

    @pytest.mark.parametrize(
        "injected_value",
        [
            "' OR '1'='1",
            "'; DROP TABLE features; --",
            "2024-01-15T00:00:00+00:00' OR '1'='1",
            "<script>alert(1)</script>",
            "../../../etc/passwd",
        ],
    )
    async def test_sql_and_injection_attempts_in_start_param_return_400(
        self, client, injected_value
    ):
        response = client.get(
            "/features/historical",
            query_string={
                "start": injected_value,
                "end": "2024-01-15T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    @pytest.mark.parametrize(
        "injected_value",
        [
            "' OR '1'='1",
            "'; DROP TABLE features; --",
        ],
    )
    async def test_injection_attempts_in_end_param_return_400(self, client, injected_value):
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-15T00:00:00+00:00",
                "end": injected_value,
            },
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    async def test_start_after_end_returns_400(self, client):
        """Logically invalid range must be rejected, not silently return empty results."""
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-12-31T23:59:59+00:00",
                "end": "2024-01-01T00:00:00+00:00",
            },
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    async def test_equal_start_and_end_returns_400(self, client):
        ts = "2024-01-15T10:00:00+00:00"
        response = client.get(
            "/features/historical",
            query_string={"start": ts, "end": ts},
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    async def test_missing_start_param_returns_400(self, client):
        response = client.get(
            "/features/historical",
            query_string={"end": "2024-01-15T23:59:59+00:00"},
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    async def test_missing_end_param_returns_400(self, client):
        response = client.get(
            "/features/historical",
            query_string={"start": "2024-01-15T00:00:00+00:00"},
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    async def test_injection_does_not_affect_db_contents(self, client, data_writer, broker):
        """DB records written before an injection attempt must remain intact."""
        msg = make_feature_a_message(timestamp="2024-01-15T10:00:00+00:00")
        await broker.publish_fanout(FEATURES_A, msg)
        await data_writer.flush()

        # Injection attempt
        client.get(
            "/features/historical",
            query_string={
                "start": "' OR '1'='1",
                "end": "2024-01-15T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )

        # Original record must still be present and uncorrupted
        assert len(data_writer.db) == 1
        assert data_writer.db[0]["message_id"] == msg["message_id"]


@pytest.mark.security
class TestRateLimiting:
    """Clients that exceed the request-rate threshold must receive HTTP 429."""

    async def test_requests_within_limit_all_succeed(self, client):
        for _ in range(10):
            response = client.get("/features/realtime", headers=_VALID_HEADERS)
            assert response.status_code == 200

    async def test_requests_exceeding_limit_receive_429(self, client):
        """
        RATE_LIMIT_MAX is 100 requests per 60s.
        Exactly 100 requests must succeed, then all subsequent must be blocked.
        """
        statuses = [
            client.get("/features/realtime", headers=_VALID_HEADERS).status_code for _ in range(110)
        ]

        successes = statuses.count(200)
        rate_limited = statuses.count(429)

        assert (
            successes == 100
        ), f"Expected exactly 100 successful requests before rate limiting, got {successes}"
        assert rate_limited == 10, f"Expected 10 rate-limited (429) responses, got {rate_limited}"
        # Verify the 429s come AFTER the 200s (not scattered randomly)
        first_429_index = next(i for i, s in enumerate(statuses) if s == 429)
        assert first_429_index == 100, (
            f"First 429 should be at index 100, got index {first_429_index} "
            "— rate limiter fired too early or too late"
        )

    async def test_rate_limit_response_body_describes_error(self, client):
        for _ in range(110):
            response = client.get("/features/realtime", headers=_VALID_HEADERS)
        assert response.status_code == 429
        assert "error" in response.get_json()

    async def test_rate_limit_also_enforced_on_historical_endpoint(self, client):
        """Rate limiting must apply to the historical endpoint, not only to realtime."""
        qs = {"start": "2024-01-01T00:00:00+00:00", "end": "2024-12-31T23:59:59+00:00"}
        statuses = [
            client.get("/features/historical", query_string=qs, headers=_VALID_HEADERS).status_code
            for _ in range(110)
        ]
        assert 429 in statuses, "Rate limiting was not enforced on /features/historical"
