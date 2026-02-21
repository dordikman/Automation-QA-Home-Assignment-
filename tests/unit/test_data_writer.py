"""
Unit tests for DataWriter.

Tests are split into two classes:
  TestDataWriterFlush — verify messages are written correctly via flush()
  TestDataWriterQuery — verify filtered queries return the right records
"""

import pytest

from mocks.rabbitmq import FEATURES_A, FEATURES_B
from tests.helpers import make_feature_a_message, make_feature_b_message


@pytest.mark.unit
class TestDataWriterFlush:
    """Tests for flush(): draining inboxes and writing records to the DB."""

    async def test_flush_writes_feature_a_to_db(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        written = await data_writer.flush()
        assert written == 1
        assert len(data_writer.db) == 1

    async def test_flush_writes_feature_b_to_db(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_B, make_feature_b_message())
        written = await data_writer.flush()
        assert written == 1
        assert data_writer.db[0]["feature_type"] == "B"

    async def test_flush_drains_both_inboxes_in_one_call(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await broker.publish_fanout(FEATURES_B, make_feature_b_message())
        written = await data_writer.flush()
        assert written == 2
        feature_types = {r["feature_type"] for r in data_writer.db}
        assert feature_types == {"A", "B"}

    async def test_flush_returns_zero_when_inboxes_are_empty(self, data_writer):
        assert await data_writer.flush() == 0

    async def test_flush_persists_all_fields_unchanged(self, data_writer, broker):
        msg = make_feature_a_message(sensor_id="sensor-check")
        await broker.publish_fanout(FEATURES_A, msg)
        await data_writer.flush()
        stored = data_writer.db[0]
        assert stored["message_id"] == msg["message_id"]
        assert stored["sensor_id"] == "sensor-check"
        assert stored["features"] == msg["features"]

    async def test_flush_skips_duplicate_message_id(self, data_writer, broker):
        """Idempotency: re-delivering the same message must not create a duplicate record."""
        msg = make_feature_a_message()
        await broker.publish_fanout(FEATURES_A, msg)
        await broker.publish_fanout(FEATURES_A, msg)
        await data_writer.flush()
        assert len(data_writer.db) == 1

    async def test_flush_duplicate_returns_zero_for_second_write(self, data_writer, broker):
        msg = make_feature_a_message()
        await broker.publish_fanout(FEATURES_A, msg)
        await data_writer.flush()
        await broker.publish_fanout(FEATURES_A, msg)
        second_write = await data_writer.flush()
        assert second_write == 0

    async def test_flush_multiple_times_accumulates_records(self, data_writer, broker):
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await data_writer.flush()
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())
        await data_writer.flush()
        assert len(data_writer.db) == 2


@pytest.mark.unit
class TestDataWriterQuery:
    """Tests for query(): filtering the in-memory database."""

    @pytest.fixture(autouse=True)
    async def _populate_db(self, data_writer, broker):
        """Pre-populate the DB with two Feature A and one Feature B record."""
        await broker.publish_fanout(
            FEATURES_A,
            make_feature_a_message(
                sensor_id="sensor-01",
                timestamp="2024-01-15T08:00:00+00:00",
            ),
        )
        await broker.publish_fanout(
            FEATURES_A,
            make_feature_a_message(
                sensor_id="sensor-02",
                timestamp="2024-01-15T12:00:00+00:00",
            ),
        )
        await broker.publish_fanout(
            FEATURES_B,
            make_feature_b_message(
                sensor_id="sensor-01",
                timestamp="2024-01-15T08:00:01+00:00",
            ),
        )
        await data_writer.flush()

    async def test_query_with_no_filters_returns_all_records(self, data_writer):
        assert len(data_writer.query()) == 3

    async def test_query_filters_by_feature_type_a(self, data_writer):
        results = data_writer.query(feature_type="A")
        assert len(results) == 2
        assert all(r["feature_type"] == "A" for r in results)

    async def test_query_filters_by_feature_type_b(self, data_writer):
        results = data_writer.query(feature_type="B")
        assert len(results) == 1
        assert results[0]["feature_type"] == "B"

    async def test_query_filters_by_sensor_id(self, data_writer):
        results = data_writer.query(sensor_id="sensor-01")
        assert len(results) == 2
        assert all(r["sensor_id"] == "sensor-01" for r in results)

    async def test_query_filters_by_timestamp_start(self, data_writer):
        results = data_writer.query(start="2024-01-15T10:00:00+00:00")
        assert len(results) == 1
        assert results[0]["sensor_id"] == "sensor-02"

    async def test_query_filters_by_timestamp_end(self, data_writer):
        results = data_writer.query(end="2024-01-15T09:00:00+00:00")
        assert len(results) == 2

    async def test_query_combined_filters_narrow_results(self, data_writer):
        results = data_writer.query(feature_type="A", sensor_id="sensor-01")
        assert len(results) == 1

    async def test_query_returns_empty_list_for_no_matches(self, data_writer):
        assert data_writer.query(sensor_id="sensor-99") == []
