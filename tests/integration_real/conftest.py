"""
Fixtures for real-service integration tests.

All tests in this directory are automatically skipped when RabbitMQ or
PostgreSQL are not reachable, so the suite never fails on a machine that
hasn't run `docker-compose up -d`. When the services ARE running, the
tests execute against real infrastructure and verify actual AMQP/SQL
behaviour that the in-memory mocks cannot reproduce.
"""

import os
import types

import pytest
from dotenv import load_dotenv

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.sensor import Sensor

load_dotenv()

# infra imports require pika and psycopg2 — import lazily so collection
# does not break on machines where these packages are not installed.
try:
    from infra.broker import RealBroker
    from infra.data_writer import RealDataWriter
    from infra.database import PostgreSQLDatabase

    _HAS_DEPS = True
except ImportError:
    _HAS_DEPS = False


# ------------------------------------------------------------------
# Service availability — checked once at module import
# ------------------------------------------------------------------


def _rabbitmq_available() -> bool:
    if not _HAS_DEPS:
        return False
    try:
        import pika

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                os.environ.get("RABBITMQ_HOST", "localhost"),
                heartbeat=0,
                blocked_connection_timeout=2,
                credentials=pika.PlainCredentials(
                    os.environ.get("RABBITMQ_USER", "guest"),
                    os.environ.get("RABBITMQ_PASSWORD", "guest"),
                ),
            )
        )
        conn.close()
        return True
    except Exception:
        return False


def _postgres_available() -> bool:
    if not _HAS_DEPS:
        return False
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            dbname=os.environ.get("POSTGRES_DB", "features_db"),
            user=os.environ.get("POSTGRES_USER", "qa_user"),
            password=os.environ.get("POSTGRES_PASSWORD", "qa_password"),
            connect_timeout=2,
        )
        conn.close()
        return True
    except Exception:
        return False


_SERVICES_UP = _rabbitmq_available() and _postgres_available()


# ------------------------------------------------------------------
# Autouse skip — applied to every test in this directory
# ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def require_real_services():
    if not _SERVICES_UP:
        pytest.skip("Real services not available. " "Start them with: docker-compose up -d")


# ------------------------------------------------------------------
# Session-level cleanup — runs once at the start of the test session
# Guarantees a clean slate even if a previous session crashed mid-test
# ------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _session_cleanup():
    """Wipe all queues and DB rows once before the entire test session starts."""
    if not _SERVICES_UP:
        return

    broker = RealBroker()
    broker.connect()
    broker.purge_all()
    broker.close()

    db = PostgreSQLDatabase()
    db.connect()
    db.clear()
    db.close()


# ------------------------------------------------------------------
# Per-test fixtures — fresh, isolated state for each test
# ------------------------------------------------------------------


@pytest.fixture
def real_broker():
    """Connected RealBroker with all work queues purged before and after the test."""
    broker = RealBroker()
    broker.connect()
    broker.purge_all()
    yield broker
    broker.purge_all()
    broker.close()


@pytest.fixture
def real_db():
    """Connected PostgreSQLDatabase with the features table cleared before and after."""
    db = PostgreSQLDatabase()
    db.connect()
    db.clear()
    yield db
    db.clear()
    db.close()


@pytest.fixture
def real_pipeline(real_broker, real_db):
    """
    Fully wired real pipeline.

    Subscription order mirrors the in-memory pipeline fixture:
      1. algo_b      — subscribes to features_a fanout
      2. real_writer — subscribes to features_a and features_b fanouts
      3. algo_a, sensor — created last (publishers)
    """
    algo_b = AlgorithmB(real_broker)
    writer = RealDataWriter(real_broker, real_db)
    algo_a = AlgorithmA(real_broker)
    sensor = Sensor(real_broker, sensor_id="sensor-real-test")

    return types.SimpleNamespace(
        broker=real_broker,
        sensor=sensor,
        algo_a=algo_a,
        algo_b=algo_b,
        writer=writer,
        db=real_db,
    )
