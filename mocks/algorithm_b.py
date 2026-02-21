"""
Algorithm B mock.

Subscribes to the features_a fanout topic, derives higher-level features
(Feature Type B) from each Feature A message, and publishes the result to
the features_b fanout topic so that DataWriter and the REST API each receive
a copy.
"""

import asyncio
import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Optional

from mocks.rabbitmq import FEATURES_A, FEATURES_B, InMemoryBroker

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS = {"message_id", "sensor_id", "timestamp", "feature_type", "features"}
_CLASSIFICATIONS = ["speech", "music", "noise", "silence", "mixed"]


def _validate(message: dict) -> None:
    missing = _REQUIRED_FIELDS - set(message.keys())
    if missing:
        raise ValueError(f"Invalid Feature A message: missing fields {missing}")
    if message.get("feature_type") != "A":
        raise TypeError(
            f"Expected feature_type 'A', got '{message.get('feature_type')}'"
        )


def _derive_features(feature_a: dict) -> dict:
    """Simulate higher-level feature derivation from Feature A data."""
    mfcc = feature_a["features"].get("mfcc", [0])
    centroid = feature_a["features"].get("spectral_centroid", 440.0)
    energy = feature_a["features"].get("rms_energy", 0.1)
    mfcc_mean = sum(mfcc) / len(mfcc) if mfcc else 0
    class_index = int(abs(mfcc_mean * centroid)) % len(_CLASSIFICATIONS)
    return {
        "classification": _CLASSIFICATIONS[class_index],
        "confidence": round(min(0.99, abs(math.tanh(mfcc_mean)) + energy), 4),
        "derived_metrics": {
            "mfcc_mean": round(mfcc_mean, 4),
            "spectral_spread": round(centroid * 0.1, 2),
            "activity_score": round(energy * 10, 4),
        },
    }


class AlgorithmB:
    """
    Processes Feature Type A messages and produces Feature Type B.

    Subscribes to the features_a fanout topic at construction time so that
    it receives a dedicated copy of every Feature A message (independent of
    DataWriter and the REST API subscriptions).
    Publishes Feature B to the features_b fanout topic.
    """

    def __init__(self, broker: InMemoryBroker):
        self.broker = broker
        self._inbox: asyncio.Queue = broker.subscribe_fanout(FEATURES_A)
        self.processed_count = 0

    def process(self, message: dict) -> dict:
        """
        Process a Feature A dict and return a Feature B dict.
        Raises ValueError / TypeError for invalid input.
        """
        _validate(message)
        return {
            "message_id": str(uuid.uuid4()),
            "source_message_id": message["message_id"],
            "feature_type": "B",
            "sensor_id": message["sensor_id"],
            "timestamp": message["timestamp"],
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "features": _derive_features(message),
        }

    async def process_one(self) -> Optional[dict]:
        """
        Consume one Feature A message from the fanout inbox, process it,
        and publish Feature B.
        Returns the Feature B dict, or None if the inbox was empty.
        """
        try:
            message = self._inbox.get_nowait()
        except asyncio.QueueEmpty:
            return None
        feature_b = self.process(message)
        await self.broker.publish_fanout(FEATURES_B, feature_b)
        self.processed_count += 1
        logger.info(
            "AlgoB processed feature_a=%s → feature_b=%s classification=%s",
            message["message_id"], feature_b["message_id"],
            feature_b["features"]["classification"],
        )
        return feature_b

    async def process_all(self) -> int:
        """Drain all pending Feature A messages. Returns the number processed."""
        count = 0
        while await self.process_one() is not None:
            count += 1
        return count
