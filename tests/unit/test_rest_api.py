"""
Unit tests for the REST API.

Tests are split into three classes:
  TestAuthentication    — token validation on both endpoints
  TestRealtimeEndpoint  — cache population and response shape
  TestHistoricalEndpoint — DB querying, param validation, and error handling
"""

import pytest

from mocks.rabbitmq import FEATURES_A, FEATURES_B
from tests.helpers import make_feature_a_message, make_feature_b_message

_VALID_HEADERS = {"Authorization": "Bearer test-token"}


@pytest.mark.unit
class TestAuthentication:
    """Both endpoints must enforce Bearer token authentication."""

    @pytest.mark.parametrize("endpoint", [
        "/features/realtime",
        "/features/historical?start=2024-01-01T00:00:00+00:00&end=2024-12-31T00:00:00+00:00",
    ])
    async def test_missing_auth_header_returns_401(self, client, endpoint):
        response = client.get(endpoint)
        assert response.status_code == 401

    @pytest.mark.parametrize("endpoint", [
        "/features/realtime",
        "/features/historical?start=2024-01-01T00:00:00+00:00&end=2024-12-31T00:00:00+00:00",
    ])
    async def test_invalid_token_returns_403(self, client, endpoint):
        response = client.get(endpoint, headers={"Authorization": "Bearer bad-token"})
        assert response.status_code == 403

    @pytest.mark.parametrize("bad_header", [
        "bad-token",          # missing 'Bearer' scheme
        "Basic dXNlcjpwYXNz", # wrong scheme
        "",                   # empty string
    ])
    async def test_malformed_auth_header_returns_401(self, client, bad_header):
        response = client.get(
            "/features/realtime",
            headers={"Authorization": bad_header},
        )
        assert response.status_code == 401

    async def test_valid_token_returns_200(self, client):
        response = client.get("/features/realtime", headers=_VALID_HEADERS)
        assert response.status_code == 200


@pytest.mark.unit
class TestRealtimeEndpoint:
    """Tests for GET /features/realtime."""

    async def test_empty_cache_returns_empty_list(self, client):
        response = client.get("/features/realtime", headers=_VALID_HEADERS)
        data = response.get_json()
        assert data["features"] == []
        assert data["count"] == 0

    async def test_returns_feature_a_published_to_fanout(self, client, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        response = client.get("/features/realtime", headers=_VALID_HEADERS)
        data = response.get_json()
        assert data["count"] == 1
        assert data["features"][0]["feature_type"] == "A"

    async def test_returns_feature_b_published_to_fanout(self, client, broker):
        await broker.publish_fanout(FEATURES_B, make_feature_b_message())
        response = client.get("/features/realtime", headers=_VALID_HEADERS)
        data = response.get_json()
        assert data["count"] == 1
        assert data["features"][0]["feature_type"] == "B"

    async def test_returns_both_feature_types_together(self, client, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await broker.publish_fanout(FEATURES_B, make_feature_b_message())
        response = client.get("/features/realtime", headers=_VALID_HEADERS)
        data = response.get_json()
        assert data["count"] == 2
        types_returned = {f["feature_type"] for f in data["features"]}
        assert types_returned == {"A", "B"}

    async def test_response_includes_count_field(self, client, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        response = client.get("/features/realtime", headers=_VALID_HEADERS)
        data = response.get_json()
        assert data["count"] == len(data["features"])


@pytest.mark.unit
class TestHistoricalEndpoint:
    """Tests for GET /features/historical."""

    async def test_returns_features_within_time_range(self, client, data_writer):
        data_writer.db.append(make_feature_a_message(timestamp="2024-01-15T10:00:00+00:00"))
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-15T00:00:00+00:00",
                "end": "2024-01-15T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )
        data = response.get_json()
        assert response.status_code == 200
        assert data["count"] == 1

    async def test_excludes_features_outside_time_range(self, client, data_writer):
        data_writer.db.append(make_feature_a_message(timestamp="2024-01-15T10:00:00+00:00"))
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-16T00:00:00+00:00",
                "end": "2024-01-16T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )
        assert response.get_json()["count"] == 0

    async def test_empty_db_returns_empty_list(self, client):
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-01T00:00:00+00:00",
                "end": "2024-12-31T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )
        data = response.get_json()
        assert data["features"] == []
        assert data["count"] == 0

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

    async def test_start_after_end_returns_400(self, client):
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-15T23:59:59+00:00",
                "end": "2024-01-15T00:00:00+00:00",
            },
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    @pytest.mark.parametrize("bad_ts", [
        "not-a-date",
        "15/01/2024",
        "2024-13-01T00:00:00+00:00",
    ])
    async def test_invalid_timestamp_format_returns_400(self, client, bad_ts):
        response = client.get(
            "/features/historical",
            query_string={"start": bad_ts, "end": "2024-01-15T23:59:59+00:00"},
            headers=_VALID_HEADERS,
        )
        assert response.status_code == 400

    async def test_records_with_malformed_timestamps_are_skipped(self, client, data_writer):
        """A DB record with a bad timestamp must be silently skipped; valid records returned."""
        from tests.helpers import make_feature_a_message
        data_writer.db.append(make_feature_a_message(timestamp="2024-01-15T10:00:00+00:00"))
        data_writer.db.append({"message_id": "bad-ts-id", "timestamp": "NOT-A-DATE",
                                "feature_type": "A", "sensor_id": "s"})
        response = client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-15T00:00:00+00:00",
                "end": "2024-01-15T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )
        data = response.get_json()
        assert response.status_code == 200
        assert data["count"] == 1

    async def test_historical_endpoint_with_no_db_returns_empty(self, flask_app):
        """When the app is created without a DB reference, historical returns an empty list."""
        from mocks.rest_api import create_app
        from mocks.rabbitmq import InMemoryBroker
        no_db_app = create_app(InMemoryBroker(), db=None)
        no_db_app.config["TESTING"] = True
        no_db_client = no_db_app.test_client()
        response = no_db_client.get(
            "/features/historical",
            query_string={
                "start": "2024-01-01T00:00:00+00:00",
                "end": "2024-12-31T23:59:59+00:00",
            },
            headers=_VALID_HEADERS,
        )
        data = response.get_json()
        assert response.status_code == 200
        assert data["features"] == []
        assert data["count"] == 0
