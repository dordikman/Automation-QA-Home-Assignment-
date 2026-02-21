"""
DataWriter mock.

Subscribes to both the features_a and features_b fanout topics and writes
every received feature message asynchronously to an in-memory database
(a plain list of dicts).

Idempotency is enforced: a message with a message_id that already exists
in the database is silently skipped.
"""

import asyncio
import logging
import threading
from typing import List, Optional

from mocks.rabbitmq import FEATURES_A, FEATURES_B, InMemoryBroker

logger = logging.getLogger(__name__)


class DataWriter:
    """
    Consumes Feature A and Feature B messages and persists them to the DB.

    The in-memory DB is exposed via the `db` attribute so that tests and
    the REST API can query it directly.
    """

    def __init__(self, broker: InMemoryBroker):
        self.broker = broker
        self._inbox_a: asyncio.Queue = broker.subscribe_fanout(FEATURES_A)
        self._inbox_b: asyncio.Queue = broker.subscribe_fanout(FEATURES_B)
        self.db: List[dict] = []
        self._lock = threading.Lock()

    def _write(self, message: dict) -> bool:
        """
        Persist one feature message. Returns True if written, False if duplicate.
        """
        with self._lock:
            if any(r["message_id"] == message["message_id"] for r in self.db):
                logger.debug("Skipped duplicate message_id=%s", message["message_id"])
                return False
            self.db.append(dict(message))
            logger.debug(
                "Wrote Feature %s msg=%s sensor=%s",
                message.get("feature_type"), message["message_id"], message.get("sensor_id"),
            )
            return True

    async def flush(self) -> int:
        """
        Drain both inbox queues and write all pending messages to the DB.
        Returns the number of new records written.
        """
        written = 0
        for inbox in (self._inbox_a, self._inbox_b):
            while True:
                try:
                    msg = inbox.get_nowait()
                    if self._write(msg):
                        written += 1
                except asyncio.QueueEmpty:
                    break
        if written:
            logger.info("DataWriter flushed %d new record(s) to DB (total=%d)", written, len(self.db))
        return written

    def query(
        self,
        feature_type: Optional[str] = None,
        sensor_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[dict]:
        """
        Query the in-memory database with optional filters.

        Args:
            feature_type: Filter to 'A' or 'B'.
            sensor_id:    Filter to a specific sensor.
            start:        ISO-8601 lower bound (inclusive) on timestamp.
            end:          ISO-8601 upper bound (inclusive) on timestamp.
        """
        with self._lock:
            results = list(self.db)
        if feature_type:
            results = [r for r in results if r.get("feature_type") == feature_type]
        if sensor_id:
            results = [r for r in results if r.get("sensor_id") == sensor_id]
        if start:
            results = [r for r in results if r.get("timestamp", "") >= start]
        if end:
            results = [r for r in results if r.get("timestamp", "") <= end]
        return results
