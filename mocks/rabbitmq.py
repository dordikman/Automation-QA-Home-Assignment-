"""
In-memory RabbitMQ broker mock.

Supports two messaging patterns that mirror real RabbitMQ semantics:

  Work queue (competing consumers)
    Multiple consumers share a single queue. Each message is delivered to
    exactly one consumer. Used for the audio stream where multiple Algo A
    pods compete for work.

  Fanout (pub/sub)
    Each subscriber receives an independent copy of every message published
    to a topic. Used for feature streams where Algo B, DataWriter, and the
    REST API all need to receive the same feature messages simultaneously.
"""

import asyncio
import threading
from typing import Dict, List, Optional

# Named queue / topic constants
AUDIO_STREAM = "audio_stream"
FEATURES_A = "features_a"
FEATURES_B = "features_b"


class InMemoryBroker:
    """In-memory message broker simulating RabbitMQ semantics."""

    def __init__(self):
        # Work queues: queue_name -> shared asyncio.Queue (competing consumers)
        self._work_queues: Dict[str, asyncio.Queue] = {}
        # Fanout topics: topic_name -> list of per-subscriber asyncio.Queues
        self._fanout: Dict[str, List[asyncio.Queue]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Work queue API
    # ------------------------------------------------------------------

    async def publish_work(self, queue_name: str, message: dict) -> None:
        """Publish a message to a work queue (competing consumers)."""
        with self._lock:
            if queue_name not in self._work_queues:
                self._work_queues[queue_name] = asyncio.Queue()
        self._work_queues[queue_name].put_nowait(message)

    async def consume_work(self, queue_name: str, timeout: float = 0) -> Optional[dict]:
        """
        Consume one message from a work queue.
        Returns None immediately if the queue is empty (timeout=0),
        or after the given timeout expires.
        """
        with self._lock:
            if queue_name not in self._work_queues:
                self._work_queues[queue_name] = asyncio.Queue()
        try:
            if timeout > 0:
                return await asyncio.wait_for(
                    self._work_queues[queue_name].get(), timeout=timeout
                )
            return self._work_queues[queue_name].get_nowait()
        except (asyncio.QueueEmpty, asyncio.TimeoutError):
            return None

    def work_queue_depth(self, queue_name: str) -> int:
        """Return the number of pending messages in a work queue."""
        if queue_name not in self._work_queues:
            return 0
        return self._work_queues[queue_name].qsize()

    # ------------------------------------------------------------------
    # Fanout (pub/sub) API
    # ------------------------------------------------------------------

    def subscribe_fanout(self, topic: str) -> asyncio.Queue:
        """
        Register a new subscriber for a fanout topic.
        Returns a dedicated asyncio.Queue that will receive a copy of every
        message published to the topic after this call.
        """
        subscriber_queue: asyncio.Queue = asyncio.Queue()
        with self._lock:
            if topic not in self._fanout:
                self._fanout[topic] = []
            self._fanout[topic].append(subscriber_queue)
        return subscriber_queue

    async def publish_fanout(self, topic: str, message: dict) -> None:
        """Deliver a copy of message to every subscriber of the topic."""
        with self._lock:
            subscribers = list(self._fanout.get(topic, []))
        for q in subscribers:
            q.put_nowait(message)

    def fanout_subscriber_count(self, topic: str) -> int:
        """Return the number of active subscribers for a topic."""
        return len(self._fanout.get(topic, []))

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def purge_all(self) -> None:
        """Clear all queues and subscriptions. Useful for test isolation."""
        with self._lock:
            self._work_queues = {}
            self._fanout = {}
