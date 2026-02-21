"""Unit-test fixtures for algorithm components."""

import pytest

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.rabbitmq import InMemoryBroker


@pytest.fixture
def algo_a(broker: InMemoryBroker) -> AlgorithmA:
    return AlgorithmA(broker)


@pytest.fixture
def algo_b(broker: InMemoryBroker) -> AlgorithmB:
    """AlgorithmB subscribes to the features_a fanout on construction."""
    return AlgorithmB(broker)
