"""
REST API mock.

A Flask application that exposes two endpoints to external clients:

  GET /features/realtime
    Returns features received in the last CACHE_MINUTES minutes.
    Data is sourced from the features_a and features_b fanout topics
    via a local in-memory cache that is populated on each request.

  GET /features/historical?start=<ISO-8601>&end=<ISO-8601>
    Returns features stored in the database for the given time window.
    Data is read from the DataWriter's in-memory DB.

Both endpoints require Bearer token authentication and enforce a simple
per-client rate limit.
"""

import asyncio
import logging
import os
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from dotenv import load_dotenv
from flask import Flask, jsonify, request

from mocks.rabbitmq import FEATURES_A, FEATURES_B, InMemoryBroker

load_dotenv()

logger = logging.getLogger(__name__)

# Valid Bearer tokens — set API_VALID_TOKENS env var (comma-separated) in production.
# Defaults to test-only values when the env var is absent.
_tokens_env = os.environ.get("API_VALID_TOKENS", "test-token,valid-token")
VALID_TOKENS = {t.strip() for t in _tokens_env.split(",") if t.strip()}

# How long feature messages stay in the real-time cache
CACHE_MINUTES = 5

# Simple rate limiting: max requests per client per window
RATE_LIMIT_MAX = 100
RATE_LIMIT_WINDOW_SECONDS = 60


def create_app(broker: InMemoryBroker, db: Optional[List[dict]] = None) -> Flask:
    """
    Create and configure the Flask application.

    Args:
        broker: The shared in-memory broker. The app subscribes to the
                features_a and features_b fanout topics at creation time.
        db:     Reference to the DataWriter's db list for historical queries.
                Pass None to disable the historical endpoint data.
    """
    app = Flask(__name__)

    # Subscribe to feature fanouts for the real-time cache
    _inbox_a: asyncio.Queue = broker.subscribe_fanout(FEATURES_A)
    _inbox_b: asyncio.Queue = broker.subscribe_fanout(FEATURES_B)

    # Real-time cache: deque of (message_dict, received_at) tuples
    _cache: deque = deque()

    # Rate limiter: client_ip -> list of request datetimes within the window
    _rate_tracker: dict = {}
    _rate_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _drain_and_refresh_cache() -> None:
        """Pull new messages from fanout inboxes and evict stale cache entries."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=CACHE_MINUTES)

        for inbox in (_inbox_a, _inbox_b):
            while True:
                try:
                    msg = inbox.get_nowait()
                    _cache.append((msg, now))
                except asyncio.QueueEmpty:
                    break

        while _cache and _cache[0][1] < cutoff:
            _cache.popleft()

    def _authenticate() -> tuple:
        """
        Validate the Bearer token.
        Returns (status_code, error_message_or_None).
        """
        auth = request.headers.get("Authorization", "")
        if not auth:
            logger.warning("Auth failed: missing header ip=%s", request.remote_addr)
            return 401, "Missing Authorization header"
        parts = auth.split(" ", 1)
        if len(parts) != 2 or parts[0] != "Bearer":
            logger.warning("Auth failed: malformed header ip=%s", request.remote_addr)
            return 401, "Invalid Authorization format; expected 'Bearer <token>'"
        if parts[1] not in VALID_TOKENS:
            logger.warning("Auth failed: invalid token ip=%s", request.remote_addr)
            return 403, "Invalid or expired token"
        return 200, None

    def _rate_limit_ok() -> bool:
        """Return True if the client is within the allowed request rate."""
        client = request.remote_addr or "unknown"
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(seconds=RATE_LIMIT_WINDOW_SECONDS)

        with _rate_lock:
            history = _rate_tracker.get(client, [])
            history = [t for t in history if t > window_start]

            if len(history) >= RATE_LIMIT_MAX:
                _rate_tracker[client] = history
                logger.warning("Rate limit exceeded ip=%s requests=%d", client, len(history))
                return False

            history.append(now)
            _rate_tracker[client] = history
            return True

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.route("/features/realtime", methods=["GET"])
    def realtime():
        status, err = _authenticate()
        if status != 200:
            return jsonify({"error": err}), status

        if not _rate_limit_ok():
            return jsonify({"error": "Rate limit exceeded"}), 429

        _drain_and_refresh_cache()
        features = [item[0] for item in _cache]
        return jsonify({"features": features, "count": len(features)}), 200

    @app.route("/features/historical", methods=["GET"])
    def historical():
        status, err = _authenticate()
        if status != 200:
            return jsonify({"error": err}), status

        if not _rate_limit_ok():
            return jsonify({"error": "Rate limit exceeded"}), 429

        start_str = request.args.get("start")
        end_str = request.args.get("end")

        if not start_str or not end_str:
            return jsonify({"error": "'start' and 'end' query parameters are required"}), 400

        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"error": "Invalid timestamp format; use ISO-8601"}), 400

        if start_dt >= end_dt:
            return jsonify({"error": "'start' must be earlier than 'end'"}), 400

        if db is None:
            return jsonify({"features": [], "count": 0}), 200

        results = []
        for record in db:
            try:
                ts = datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
                if start_dt <= ts <= end_dt:
                    results.append(record)
            except (ValueError, KeyError):
                continue

        return jsonify({"features": results, "count": len(results)}), 200

    return app
