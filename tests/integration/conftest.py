"""
Integration-test fixture: a fully wired pipeline.

Construction order matters — fanout subscribers must be registered before
any component starts publishing, otherwise messages are lost.
The fixture takes advantage of pytest's fixture sharing: broker, data_writer,
and client are all created from the same InMemoryBroker instance.
"""

import types

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.data_writer import DataWriter
from mocks.rabbitmq import InMemoryBroker
from mocks.sensor import Sensor


@pytest.fixture
def pipeline(broker: InMemoryBroker, data_writer: DataWriter, client):
    """
    Fully wired pipeline: sensor → algo_a → algo_b → data_writer → REST API.

    Subscription order (before any message is published):
      1. data_writer  — subscribed to features_a, features_b (via its fixture)
      2. REST API     — subscribed to features_a, features_b (via create_app inside flask_app)
      3. algo_b       — subscribed to features_a (constructed here)

    Publishing components (algo_a, sensor) are created last.
    """
    algo_b = AlgorithmB(broker)
    algo_a = AlgorithmA(broker)
    sensor = Sensor(broker, sensor_id="sensor-test")

    return types.SimpleNamespace(
        broker=broker,
        sensor=sensor,
        algo_a=algo_a,
        algo_b=algo_b,
        writer=data_writer,
        client=client,
    )
