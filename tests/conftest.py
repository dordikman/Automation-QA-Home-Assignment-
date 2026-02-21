"""
Root-level fixtures shared across all test categories.

Scope is function by default so every test starts with a fully isolated
in-memory broker, DataWriter, and Flask client — no shared state between tests.
"""

import pytest

from mocks.data_writer import DataWriter
from mocks.rabbitmq import InMemoryBroker
from mocks.rest_api import create_app


@pytest.fixture
def broker() -> InMemoryBroker:
    """Fresh in-memory broker for each test."""
    return InMemoryBroker()


@pytest.fixture
def data_writer(broker: InMemoryBroker) -> DataWriter:
    """DataWriter subscribed to both feature fanout topics."""
    return DataWriter(broker)


@pytest.fixture
def flask_app(broker: InMemoryBroker, data_writer: DataWriter):
    """Flask application wired to the shared broker and DataWriter DB."""
    app = create_app(broker, data_writer.db)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(flask_app):
    """Flask test client for the REST API."""
    return flask_app.test_client()
