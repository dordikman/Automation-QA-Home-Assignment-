def pytest_configure(config):
    config.addinivalue_line(
        "markers", "contract: Schema contract tests between pipeline components"
    )
