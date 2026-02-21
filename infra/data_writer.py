"""
Real DataWriter.

Subscribes to the features_a and features_b RabbitMQ fanout exchanges and
writes every received message to PostgreSQL via PostgreSQLDatabase.

Mirrors the flush() / interface of mocks.data_writer.DataWriter so the
real integration tests can call the same methods.
"""

import asyncio

from infra.broker import RealBroker
from infra.database import PostgreSQLDatabase
from mocks.rabbitmq import FEATURES_A, FEATURES_B


class RealDataWriter:
    """Consumes Feature A and B from RabbitMQ and persists them to PostgreSQL."""

    def __init__(self, broker: RealBroker, db: PostgreSQLDatabase):
        self.broker = broker
        self.db = db
        # Subscribe at construction time — same order requirement as the mock
        self._inbox_a = broker.subscribe_fanout(FEATURES_A)
        self._inbox_b = broker.subscribe_fanout(FEATURES_B)

    async def flush(self) -> int:
        """
        Drain both fanout inboxes and write all pending messages to the DB.
        Returns the number of new records written (duplicates skipped).
        """
        written = 0
        for inbox in (self._inbox_a, self._inbox_b):
            while True:
                try:
                    msg = inbox.get_nowait()
                    if self.db.write(msg):
                        written += 1
                except asyncio.QueueEmpty:
                    break
        return written
