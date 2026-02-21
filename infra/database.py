"""
Real PostgreSQL database adapter.

Implements the same query() interface as DataWriter.db (a plain list) but
backed by a real PostgreSQL table via psycopg2. The DataWriter.query()
dict-filter logic is replaced by proper parameterised SQL.

Schema
------
  features
    id           SERIAL PRIMARY KEY
    message_id   VARCHAR(36) UNIQUE   ← idempotency key
    feature_type CHAR(1)              ← 'A' or 'B'
    sensor_id    VARCHAR(128)
    timestamp    TIMESTAMPTZ
    features     JSONB
    created_at   TIMESTAMPTZ DEFAULT NOW()
"""

import json
import os
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


class PostgreSQLDatabase:
    """
    Thin adapter around psycopg2 for writing and querying feature records.

    The write() method enforces idempotency via an ON CONFLICT DO NOTHING
    clause — the same guarantee provided by DataWriter._write() in the mock.
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        dbname: str = None,
        user: str = None,
        password: str = None,
    ):
        self._conn_params = dict(
            host=host if host is not None else os.environ.get("POSTGRES_HOST", "localhost"),
            port=port if port is not None else int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=dbname if dbname is not None else os.environ.get("POSTGRES_DB", "features_db"),
            user=user if user is not None else os.environ.get("POSTGRES_USER", "qa_user"),
            password=password if password is not None else os.environ.get("POSTGRES_PASSWORD", "qa_password"),
            connect_timeout=5,
        )
        self._conn = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._conn = psycopg2.connect(**self._conn_params)
        self._ensure_schema()

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def _ensure_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS features (
                    id           SERIAL PRIMARY KEY,
                    message_id   VARCHAR(36)  UNIQUE NOT NULL,
                    feature_type CHAR(1)      NOT NULL,
                    sensor_id    VARCHAR(128) NOT NULL,
                    timestamp    TIMESTAMPTZ  NOT NULL,
                    features     JSONB        NOT NULL,
                    created_at   TIMESTAMPTZ  DEFAULT NOW()
                )
            """)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, message: dict) -> bool:
        """
        Persist one feature message.
        Returns True if written, False if the message_id already exists.
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO features
                        (message_id, feature_type, sensor_id, timestamp, features)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO NOTHING
                    RETURNING id
                """, (
                    message["message_id"],
                    message["feature_type"],
                    message["sensor_id"],
                    message["timestamp"],
                    json.dumps(message.get("features", {})),
                ))
                written = cur.fetchone() is not None
            self._conn.commit()
            return written
        except Exception:
            self._conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query(
        self,
        feature_type: Optional[str] = None,
        sensor_id: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[dict]:
        """Query features with optional filters — all using parameterised SQL."""
        conditions, params = [], []

        if feature_type:
            conditions.append("feature_type = %s")
            params.append(feature_type)
        if sensor_id:
            conditions.append("sensor_id = %s")
            params.append(sensor_id)
        if start:
            conditions.append("timestamp >= %s")
            params.append(start)
        if end:
            conditions.append("timestamp <= %s")
            params.append(end)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT message_id, feature_type, sensor_id, "
                f"       timestamp::text, features "
                f"FROM features {where} ORDER BY timestamp",
                params,
            )
            rows = cur.fetchall()

        return [
            {
                "message_id":   r["message_id"],
                "feature_type": r["feature_type"].strip(),
                "sensor_id":    r["sensor_id"],
                "timestamp":    r["timestamp"],
                "features":     r["features"],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Remove all rows — used between tests for isolation."""
        with self._conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE features")
        self._conn.commit()
