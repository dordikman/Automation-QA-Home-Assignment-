"""
Sensor mock.

Simulates a distributed audio sensor that encodes audio as base64 JSON
and transmits it to the audio stream work queue.
"""

import base64
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from mocks.rabbitmq import AUDIO_STREAM, InMemoryBroker

logger = logging.getLogger(__name__)


class Sensor:
    """
    Simulates a single distributed audio sensor.

    In the real system sensors transmit over a secured network channel;
    here they publish directly to the in-memory broker.
    """

    def __init__(self, broker: InMemoryBroker, sensor_id: Optional[str] = None):
        self.broker = broker
        self.sensor_id = sensor_id or f"sensor-{uuid.uuid4().hex[:8]}"

    async def publish_audio(
        self,
        audio_data: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> dict:
        """
        Publish one audio message to the audio stream queue.

        Args:
            audio_data: Base64-encoded audio content.
                        Generates deterministic synthetic data if not provided.
            timestamp:  ISO-8601 timestamp string.
                        Defaults to the current UTC time.

        Returns:
            The message dict that was published.
        """
        if audio_data is None:
            synthetic = b"SYNTHETIC_AUDIO_" + uuid.uuid4().bytes
            audio_data = base64.b64encode(synthetic).decode()

        message = {
            "message_id": str(uuid.uuid4()),
            "sensor_id": self.sensor_id,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
            "audio_data": audio_data,
        }
        await self.broker.publish_work(AUDIO_STREAM, message)
        logger.info("Published audio msg=%s sensor=%s", message["message_id"], self.sensor_id)
        return message
