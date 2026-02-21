"""
Unit tests for InMemoryBroker utility methods.

Covers the broker API that is not exercised by the algorithm or data-writer tests:
  TestWorkQueueUtilities  — work_queue_depth on unknown queues; consume_work with timeout
  TestFanoutUtilities     — fanout_subscriber_count
  TestBrokerPurge         — purge_all clears all state
"""

import asyncio

import pytest

from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, InMemoryBroker
from tests.helpers import make_audio_message, make_feature_a_message


@pytest.mark.unit
class TestWorkQueueUtilities:
    """work_queue_depth and consume_work edge cases."""

    async def test_work_queue_depth_returns_zero_for_unknown_queue(self, broker):
        """Querying a queue that has never been written to must return 0, not raise."""
        assert broker.work_queue_depth("queue-that-does-not-exist") == 0

    async def test_consume_work_with_positive_timeout_returns_message(self, broker):
        """consume_work(timeout>0) must wait and return the message when it arrives."""
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        result = await broker.consume_work(AUDIO_STREAM, timeout=1.0)
        assert result is not None
        assert "message_id" in result

    async def test_consume_work_with_timeout_returns_none_when_queue_stays_empty(self, broker):
        """consume_work(timeout>0) must return None when no message arrives before timeout."""
        result = await broker.consume_work(AUDIO_STREAM, timeout=0.05)
        assert result is None


@pytest.mark.unit
class TestFanoutUtilities:
    """fanout_subscriber_count reflects the current subscriber list."""

    async def test_subscriber_count_is_zero_for_unknown_topic(self, broker):
        assert broker.fanout_subscriber_count("topic-nobody-subscribed-to") == 0

    async def test_subscriber_count_increments_after_each_subscribe(self, broker):
        assert broker.fanout_subscriber_count(FEATURES_A) == 0
        broker.subscribe_fanout(FEATURES_A)
        assert broker.fanout_subscriber_count(FEATURES_A) == 1
        broker.subscribe_fanout(FEATURES_A)
        assert broker.fanout_subscriber_count(FEATURES_A) == 2

    async def test_subscriber_count_is_independent_per_topic(self, broker):
        from mocks.rabbitmq import FEATURES_B
        broker.subscribe_fanout(FEATURES_A)
        broker.subscribe_fanout(FEATURES_A)
        broker.subscribe_fanout(FEATURES_B)
        assert broker.fanout_subscriber_count(FEATURES_A) == 2
        assert broker.fanout_subscriber_count(FEATURES_B) == 1


@pytest.mark.unit
class TestBrokerPurge:
    """purge_all resets all queues and subscriptions."""

    async def test_purge_all_empties_work_queues(self, broker):
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        assert broker.work_queue_depth(AUDIO_STREAM) == 1
        broker.purge_all()
        assert broker.work_queue_depth(AUDIO_STREAM) == 0

    async def test_purge_all_removes_fanout_subscriptions(self, broker):
        broker.subscribe_fanout(FEATURES_A)
        assert broker.fanout_subscriber_count(FEATURES_A) == 1
        broker.purge_all()
        assert broker.fanout_subscriber_count(FEATURES_A) == 0

    async def test_purge_all_allows_fresh_publishes_and_subscribes(self, broker):
        """The broker must be fully usable after purge_all."""
        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        broker.purge_all()

        await broker.publish_work(AUDIO_STREAM, make_audio_message())
        sub = broker.subscribe_fanout(FEATURES_A)
        await broker.publish_fanout(FEATURES_A, make_feature_a_message())

        assert broker.work_queue_depth(AUDIO_STREAM) == 1
        assert not sub.empty()
