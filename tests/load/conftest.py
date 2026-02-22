"""
Fixtures for the load test suite (tests/load/).

This conftest.py is picked up when tests are run explicitly:
    pytest tests/load/ -v -s

It is NOT auto-collected during a plain `pytest` run because pytest.ini
sets `norecursedirs = tests/load`.

Subscription order follows the same rule as tests/integration/conftest.py:
  consumers (writer, algo_b) subscribe before producers (algo_a, sensor)
  to guarantee no messages are lost on fanout queues.
"""

import types

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.data_writer import DataWriter
from mocks.rabbitmq import InMemoryBroker
from mocks.rest_api import create_app
from mocks.sensor import Sensor


@pytest.fixture
def broker():
    """Fresh InMemoryBroker for each test."""
    return InMemoryBroker()


@pytest.fixture
def pipeline(broker):
    """
    Fully wired in-memory pipeline: sensor → algo_a → algo_b → writer → REST API.

    Returns a SimpleNamespace with attributes:
        broker, sensor, algo_a, algo_b, writer, client
    """
    writer = DataWriter(broker)
    app = create_app(broker, writer.db)
    app.config["TESTING"] = True
    algo_b = AlgorithmB(broker)
    algo_a = AlgorithmA(broker)
    sensor = Sensor(broker, sensor_id="load-sensor")
    return types.SimpleNamespace(
        broker=broker,
        sensor=sensor,
        algo_a=algo_a,
        algo_b=algo_b,
        writer=writer,
        client=app.test_client(),
    )
