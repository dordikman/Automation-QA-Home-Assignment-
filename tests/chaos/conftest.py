def pytest_configure(config):
    config.addinivalue_line(
        "markers", "chaos: Resilience and failure-mode tests"
    )
