"""
Factory functions for building valid test message dicts.

Using plain functions (not fixtures) keeps test data creation explicit and
easy to customise inline with **overrides. Every function returns a fresh
dict so tests cannot accidentally share mutable state.
"""

import base64
import uuid
from datetime import datetime, timezone


def make_audio_message(**overrides) -> dict:
    """Return a minimal valid audio message as produced by a Sensor."""
    msg = {
        "message_id": str(uuid.uuid4()),
        "sensor_id": "sensor-01",
        "timestamp": "2024-01-15T10:00:00+00:00",
        "audio_data": base64.b64encode(b"testaudiodata").decode(),
    }
    msg.update(overrides)
    return msg


def make_feature_a_message(**overrides) -> dict:
    """Return a minimal valid Feature Type A message as produced by Algorithm A."""
    msg = {
        "message_id": str(uuid.uuid4()),
        "source_message_id": str(uuid.uuid4()),
        "feature_type": "A",
        "sensor_id": "sensor-01",
        "timestamp": "2024-01-15T10:00:00+00:00",
        "processed_at": "2024-01-15T10:00:01+00:00",
        "features": {
            "mfcc": [round(i * 0.1, 1) for i in range(13)],
            "spectral_centroid": 540.0,
            "zero_crossing_rate": 0.055,
            "rms_energy": 0.11,
        },
    }
    msg.update(overrides)
    return msg


def make_feature_b_message(**overrides) -> dict:
    """Return a minimal valid Feature Type B message as produced by Algorithm B."""
    msg = {
        "message_id": str(uuid.uuid4()),
        "source_message_id": str(uuid.uuid4()),
        "feature_type": "B",
        "sensor_id": "sensor-01",
        "timestamp": "2024-01-15T10:00:00+00:00",
        "processed_at": "2024-01-15T10:00:02+00:00",
        "features": {
            "classification": "speech",
            "confidence": 0.92,
            "derived_metrics": {
                "mfcc_mean": 0.6,
                "spectral_spread": 54.0,
                "activity_score": 1.1,
            },
        },
    }
    msg.update(overrides)
    return msg


def utcnow_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
