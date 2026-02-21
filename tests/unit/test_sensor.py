"""
Unit tests for the Sensor mock.

Tests are split into two classes:
  TestSensorInit         — sensor_id assignment at construction time
  TestSensorPublishAudio — message schema, queue publishing, and default-value behaviour
"""

import base64
from datetime import datetime

import pytest

from mocks.rabbitmq import AUDIO_STREAM
from mocks.sensor import Sensor


@pytest.mark.unit
class TestSensorInit:
    """Verify sensor_id assignment on construction."""

    async def test_explicit_sensor_id_is_stored(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-99")
        assert sensor.sensor_id == "sensor-99"

    async def test_auto_generated_sensor_id_is_not_empty(self, broker):
        sensor = Sensor(broker)
        assert sensor.sensor_id
        assert len(sensor.sensor_id) > 0

    async def test_two_sensors_without_explicit_id_get_distinct_ids(self, broker):
        """Each Sensor instance must have a unique auto-generated ID."""
        sensor_1 = Sensor(broker)
        sensor_2 = Sensor(broker)
        assert sensor_1.sensor_id != sensor_2.sensor_id


@pytest.mark.unit
class TestSensorPublishAudio:
    """Verify publish_audio() produces correctly shaped messages and queues them."""

    async def test_publish_audio_returns_message_dict(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        result = await sensor.publish_audio()
        assert isinstance(result, dict)

    async def test_published_message_contains_all_required_fields(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        result = await sensor.publish_audio()
        assert {"message_id", "sensor_id", "timestamp", "audio_data"}.issubset(result.keys())

    async def test_published_message_carries_correct_sensor_id(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-check")
        result = await sensor.publish_audio()
        assert result["sensor_id"] == "sensor-check"

    async def test_explicit_audio_data_is_preserved(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        audio = base64.b64encode(b"custom-audio-bytes").decode()
        result = await sensor.publish_audio(audio_data=audio)
        assert result["audio_data"] == audio

    async def test_auto_generated_audio_data_is_valid_base64(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        result = await sensor.publish_audio()
        decoded = base64.b64decode(result["audio_data"])
        assert len(decoded) > 0

    async def test_explicit_timestamp_is_preserved(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        ts = "2024-06-01T10:00:00+00:00"
        result = await sensor.publish_audio(timestamp=ts)
        assert result["timestamp"] == ts

    async def test_default_timestamp_is_parseable_iso8601(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        result = await sensor.publish_audio()
        dt = datetime.fromisoformat(result["timestamp"])
        assert dt is not None

    async def test_message_is_enqueued_to_audio_stream(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        await sensor.publish_audio()
        assert broker.work_queue_depth(AUDIO_STREAM) == 1

    async def test_each_published_message_has_unique_message_id(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        result_1 = await sensor.publish_audio()
        result_2 = await sensor.publish_audio()
        assert result_1["message_id"] != result_2["message_id"]

    async def test_multiple_publishes_accumulate_in_queue(self, broker):
        sensor = Sensor(broker, sensor_id="sensor-01")
        for _ in range(4):
            await sensor.publish_audio()
        assert broker.work_queue_depth(AUDIO_STREAM) == 4
