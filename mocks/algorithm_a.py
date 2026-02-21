"""
Algorithm A mock.

Consumes audio messages from the audio stream work queue, extracts a set
of synthetic audio features (Feature Type A), and publishes the result to
the features_a fanout topic so that Algo B, DataWriter, and the REST API
each receive a copy.
"""

import math
import base64
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from mocks.rabbitmq import AUDIO_STREAM, FEATURES_A, InMemoryBroker

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS = {"message_id", "sensor_id", "timestamp", "audio_data"}


def _validate(message: dict) -> None:
    missing = _REQUIRED_FIELDS - set(message.keys())
    if missing:
        raise ValueError(f"Invalid audio message: missing fields {missing}")
    if not message["audio_data"]:
        raise ValueError("audio_data cannot be empty")
    try:
        datetime.fromisoformat(message["timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid timestamp format: {message['timestamp']}")


def _extract_features(audio_data: str) -> dict:
    """
    Simulate audio feature extraction (MFCC, spectral centroid, etc.).
    Output is deterministic given the same input so tests can assert exact values.
    """
    raw = base64.b64decode(audio_data + "==")
    seed = sum(raw) % 1000
    return {
        "mfcc": [round(math.sin(seed + i) * 10, 4) for i in range(13)],
        "spectral_centroid": round(440.0 + seed, 2),
        "zero_crossing_rate": round(0.05 + (seed % 50) / 1000, 4),
        "rms_energy": round(0.1 + (seed % 100) / 1000, 4),
    }


class AlgorithmA:
    """
    Processes audio messages and produces Feature Type A.

    Consumes from the audio_stream work queue (competing with other Algo A pods).
    Publishes Feature A to the features_a fanout topic.
    """

    def __init__(self, broker: InMemoryBroker):
        self.broker = broker
        self.processed_count = 0

    def process(self, message: dict) -> dict:
        """
        Process a single audio message dict and return a Feature A dict.
        Raises ValueError if the message is invalid.
        """
        _validate(message)
        return {
            "message_id": str(uuid.uuid4()),
            "source_message_id": message["message_id"],
            "feature_type": "A",
            "sensor_id": message["sensor_id"],
            "timestamp": message["timestamp"],
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "features": _extract_features(message["audio_data"]),
        }

    async def process_one(self) -> Optional[dict]:
        """
        Consume one audio message, process it, and publish Feature A.
        Returns the Feature A dict, or None if the audio queue was empty.
        """
        message = await self.broker.consume_work(AUDIO_STREAM)
        if message is None:
            return None
        feature_a = self.process(message)
        await self.broker.publish_fanout(FEATURES_A, feature_a)
        self.processed_count += 1
        logger.info(
            "AlgoA processed audio=%s → feature_a=%s sensor=%s",
            message["message_id"], feature_a["message_id"], feature_a["sensor_id"],
        )
        return feature_a

    async def process_all(self) -> int:
        """Drain the audio queue completely. Returns the number of messages processed."""
        count = 0
        while await self.process_one() is not None:
            count += 1
        return count
