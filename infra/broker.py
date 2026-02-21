"""
Real RabbitMQ broker adapter.

Implements the same interface as mocks.rabbitmq.InMemoryBroker so that
AlgorithmA, AlgorithmB, DataWriter, and Sensor can run against a live
RabbitMQ server without any code changes — only the broker instance differs.

Interface contract
------------------
  publish_work(queue_name, message)        → competing-consumer queue
  consume_work(queue_name, timeout) → dict  → consume one message or None
  work_queue_depth(queue_name) → int
  subscribe_fanout(topic) → FanoutQueue    → pub/sub; caller gets own copy
  publish_fanout(topic, message)
  fanout_subscriber_count(topic) → int
  purge_all()
"""

import asyncio
import json
import os

import pika
import pika.exceptions
from dotenv import load_dotenv

load_dotenv()

from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, FEATURES_B

# All durable queue names managed by this broker
_WORK_QUEUES = [AUDIO_STREAM]
_FANOUT_EXCHANGES = [FEATURES_A, FEATURES_B]


class FanoutQueue:
    """
    Wraps an exclusive RabbitMQ queue to match the asyncio.Queue interface.

    AlgorithmB.process_one() and DataWriter.flush() call get_nowait() on
    whatever subscribe_fanout() returns — this class makes that work with
    a real RabbitMQ exclusive queue, without changing the caller.
    """

    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel, queue_name: str):
        self._channel = channel
        self._queue_name = queue_name

    def get_nowait(self) -> dict:
        """Return the next message or raise asyncio.QueueEmpty."""
        method, _props, body = self._channel.basic_get(
            queue=self._queue_name, auto_ack=True
        )
        if method is None:
            raise asyncio.QueueEmpty()
        return json.loads(body)

    def empty(self) -> bool:
        """Return True if the queue has no pending messages."""
        try:
            result = self._channel.queue_declare(
                queue=self._queue_name, passive=True
            )
            return result.method.message_count == 0
        except pika.exceptions.ChannelClosedByBroker:
            return True


class RealBroker:
    """
    Real RabbitMQ broker implementing the same interface as InMemoryBroker.

    Uses pika BlockingConnection; all operations are synchronous and
    single-threaded (safe for test code that calls methods sequentially).
    The async def wrappers allow the mock components (AlgorithmA, AlgorithmB,
    Sensor) to await these methods just as they would with InMemoryBroker.
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
    ):
        self._params = pika.ConnectionParameters(
            host=host if host is not None else os.environ.get("RABBITMQ_HOST", "localhost"),
            port=port if port is not None else int(os.environ.get("RABBITMQ_PORT", "5672")),
            credentials=pika.PlainCredentials(
                user if user is not None else os.environ.get("RABBITMQ_USER", "guest"),
                password if password is not None else os.environ.get("RABBITMQ_PASSWORD", "guest"),
            ),
            heartbeat=0,          # disable heartbeat for test simplicity
            blocked_connection_timeout=5,
        )
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self._fanout_queues: list[str] = []   # track exclusive queue names for purge

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._declare_infrastructure()
        self._channel.confirm_delivery()  # block on publish until broker ACKs

    def _declare_infrastructure(self) -> None:
        """Declare all known queues and exchanges upfront."""
        for name in _WORK_QUEUES:
            self._channel.queue_declare(queue=name, durable=True)
        for name in _FANOUT_EXCHANGES:
            self._channel.exchange_declare(
                exchange=name, exchange_type="fanout", durable=True
            )

    def close(self) -> None:
        if self._connection and not self._connection.is_closed:
            self._connection.close()

    # ------------------------------------------------------------------
    # Work queue API  (competing consumers)
    # ------------------------------------------------------------------

    async def publish_work(self, queue_name: str, message: dict) -> None:
        self._channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )

    async def consume_work(self, queue_name: str, timeout: float = 0) -> dict | None:
        method, _props, body = self._channel.basic_get(
            queue=queue_name, auto_ack=True
        )
        if method:
            return json.loads(body)
        return None

    def work_queue_depth(self, queue_name: str) -> int:
        result = self._channel.queue_declare(queue=queue_name, durable=True)
        return result.method.message_count

    # ------------------------------------------------------------------
    # Fanout (pub/sub) API
    # ------------------------------------------------------------------

    def subscribe_fanout(self, topic: str) -> FanoutQueue:
        """
        Create an exclusive anonymous queue bound to the fanout exchange.
        Returns a FanoutQueue that delivers only to this subscriber.
        Exclusive queues auto-delete when the connection closes, giving
        each test a clean slate automatically.
        """
        result = self._channel.queue_declare(queue="", exclusive=True)
        q_name = result.method.queue
        self._channel.queue_bind(exchange=topic, queue=q_name)
        self._fanout_queues.append(q_name)
        return FanoutQueue(self._channel, q_name)

    async def publish_fanout(self, topic: str, message: dict) -> None:
        self._channel.basic_publish(
            exchange=topic,
            routing_key="",
            body=json.dumps(message),
        )

    def fanout_subscriber_count(self, topic: str) -> int:
        """Return the number of queues currently bound to the exchange."""
        # RabbitMQ management API would give this; via AMQP we approximate
        # by counting our own tracked exclusive queues for this topic.
        return len(self._fanout_queues)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def purge_all(self) -> None:
        """Purge all durable work queues. Exclusive queues auto-delete on close."""
        for name in _WORK_QUEUES:
            try:
                self._channel.queue_purge(name)
            except Exception:
                pass
