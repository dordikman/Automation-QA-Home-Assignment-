# Makefile — convenience targets for the Audio Processing QA test suite.
# Wraps common pytest, lint, format, and Docker commands so contributors
# can run everything with a single short command.
# Run `make help` to see all available targets.

.PHONY: install test test-unit test-integration test-security test-load test-real \
        coverage lint format format-check sast mock-server clean help

# ---------------------------------------------------------------------------
# Dependency management
# ---------------------------------------------------------------------------

install:
	pip install -r requirements.txt

# ---------------------------------------------------------------------------
# Test targets
# ---------------------------------------------------------------------------

test:
	pytest -m "unit or integration or security" -v

test-unit:
	pytest -m unit -v

test-integration:
	pytest -m integration -v

test-security:
	pytest -m security -v

test-load:
	pytest tests/load/test_pipeline_throughput.py -v -s

test-real:
	docker-compose up -d
	pytest tests/integration_real/ -v

coverage:
	pytest -m unit --cov=mocks --cov-report=html --cov-report=term-missing --cov-fail-under=80

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

lint:
	flake8 mocks/ tests/ --max-line-length=100 --extend-ignore=E203

format:
	black mocks/ tests/ --line-length=100

format-check:
	black --check mocks/ tests/ --line-length=100

sast:
	bandit -r mocks/ -ll

# ---------------------------------------------------------------------------
# Local development server
# ---------------------------------------------------------------------------

mock-server:
	python run_mock_server.py

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean:
	rm -rf .pytest_cache/ __pycache__/ coverage.xml htmlcov/ .coverage \
	       test-results.xml test.log
	find . -name "*.pyc" -delete
	find . -name "*.html" ! -path "./.git/*" -delete

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help:
	@echo ""
	@echo "Audio Processing QA — available make targets"
	@echo "============================================="
	@echo ""
	@echo "  install         Install all Python dependencies"
	@echo "  test            Run unit + integration + security tests"
	@echo "  test-unit       Run fast unit tests only (~0.4 s)"
	@echo "  test-integration Run in-memory pipeline integration tests"
	@echo "  test-security   Run security and input-validation tests"
	@echo "  test-load       Run throughput/latency load tests (explicit)"
	@echo "  test-real       Start Docker services then run real-service tests"
	@echo "  coverage        Unit tests with HTML coverage report (≥ 80% gate)"
	@echo "  lint            Run flake8 linter"
	@echo "  format          Auto-format source with black"
	@echo "  format-check    Check black formatting without modifying files"
	@echo "  sast            Static security analysis with Bandit"
	@echo "  mock-server     Start the mock pipeline HTTP server (port 5000)"
	@echo "  clean           Remove build artefacts, caches, and report files"
	@echo "  help            Show this help message"
	@echo ""
