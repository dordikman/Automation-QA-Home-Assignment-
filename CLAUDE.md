# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A QA & automation test suite for a distributed audio processing pipeline. The pipeline processes audio from sensors through two algorithm stages, persists features to a database, and exposes them via a REST API. **All tests run in-memory** — no Docker, RabbitMQ, or PostgreSQL required for the standard test suite.

## Commands

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run tests
```bash
pytest                                              # All tests
pytest -m unit                                      # Fast unit tests only (~0.4s)
pytest -m integration                               # In-memory pipeline tests
pytest -m security                                  # Security & validation tests
pytest -m "unit or integration or security"         # All except load tests
pytest tests/unit/test_algorithm_a.py               # Single test file
pytest tests/unit/test_algorithm_a.py::TestAlgorithmAProcess::test_name  # Single test
pytest -m unit --cov=mocks --cov-report=term-missing --cov-fail-under=80  # With coverage
pytest -v --html=report.html --self-contained-html  # With HTML report
```

### Lint and format (used in CI)
```bash
flake8 mocks/ tests/ --max-line-length=100 --extend-ignore=E203
black --check mocks/ tests/ --line-length=100
bandit -r mocks/ -f json -o bandit-report.json -ll
```

### Load tests (run explicitly — excluded from auto-discovery)
```bash
pytest tests/load/test_pipeline_throughput.py -v -s   # full suite
pytest tests/load/test_pipeline_throughput.py -v -s -k "Throughput"  # single class
```

### Real-service tests (optional, requires Docker)
```bash
docker-compose up -d          # Starts RabbitMQ 3.12 (:5672) + PostgreSQL 15 (:5432)
pytest tests/integration_real/ -v   # Auto-skips if Docker not running
```

## Architecture

### Pipeline Flow
```
Sensor → audio_stream (work queue) → AlgorithmA → features_a (fanout)
                                                         ├→ AlgorithmB → features_b (fanout) → DataWriter
                                                         └→ DataWriter
                                                              ↓
                                                        REST API (/features/realtime, /features/historical)
```

Two messaging patterns:
- **Work queue** (`audio_stream`): Each message delivered to exactly one AlgorithmA pod (competing consumers / load balancing)
- **Fanout** (`features_a`, `features_b`): Every subscriber gets an independent copy (pub/sub)

### Adapter Pattern (Key Design)
`InMemoryBroker` (`mocks/rabbitmq.py`) and `RealBroker` (`infra/broker.py`) implement the same interface. All components (Sensor, AlgorithmA/B, DataWriter) accept a broker instance — swap the broker and the same code runs against either in-memory queues or real RabbitMQ.

### Mock Components (`mocks/`)
| File | Role |
|------|------|
| `rabbitmq.py` | `InMemoryBroker` using `queue.Queue`; thread-safe; supports work queues + fanout |
| `sensor.py` | Publishes base64-encoded audio messages to `audio_stream` |
| `algorithm_a.py` | Consumes audio, extracts features (MFCC, spectral centroid, etc.), publishes to `features_a` |
| `algorithm_b.py` | Subscribes to `features_a`, derives classification/confidence, publishes to `features_b` |
| `data_writer.py` | Subscribes to `features_a`/`features_b`, persists to in-memory list with idempotency (dedup by `message_id`) |
| `rest_api.py` | Flask app: `GET /features/realtime` (last 5 min, cached) and `GET /features/historical?start=&end=`; Bearer token auth; per-client rate limiting (100 req/60s) |

### Test Structure
```
tests/
├── conftest.py          # Root fixtures: broker, data_writer, flask_app, client
├── helpers.py           # Factory functions: make_audio_message(), make_feature_a_message(), make_feature_b_message(), utcnow_iso()
├── unit/
│   ├── conftest.py      # algo_a, algo_b fixtures
│   └── test_*.py        # 108 tests total — pure logic, broker interaction, schema validation, sensor, broker utilities
├── integration/
│   ├── conftest.py      # pipeline fixture (SimpleNamespace with all components wired)
│   └── test_pipeline.py # 19 tests — stage-by-stage, fanout semantics, load balancing
├── integration_real/    # 21 tests — AMQP acks, PostgreSQL UNIQUE constraints, JSONB, parameterised SQL
├── security/
│   └── test_api_security.py  # 26 tests — auth, SQL injection, XSS, path traversal, rate limiting (both endpoints)
└── load/
    ├── conftest.py                  # broker + pipeline fixtures for load tests
    └── test_pipeline_throughput.py  # 16 pytest tests — throughput, latency, scalability, backpressure
```

### Fixture Dependency Chain
```
broker (InMemoryBroker)
  └→ data_writer (DataWriter subscribed to features_a + features_b)
       └→ flask_app (Flask app wired to broker + data_writer.db)
            └→ client (Flask test client)

pipeline (integration) = SimpleNamespace(broker, sensor, algo_a, algo_b, writer, client)
  Note: subscribers registered before publishers to prevent message loss
```

### CI/CD (`.github/workflows/ci.yml`)
Three stages:
1. **Fast checks** (every push/PR): flake8, black, bandit, unit tests with 80% coverage threshold
2. **Integration & security** (after stage 1): integration tests, security tests, HTML report artifact
3. **Load tests** (nightly 02:00 UTC): pytest pipeline throughput suite, SLA breach triggers Slack webhook alert

### Kubernetes (`k8s/`)
Namespace `audio-processing`. AlgorithmA HPA: 2–10 pods at 70% CPU. AlgorithmB HPA: 2–8 pods. `network-policy.yaml` enforces default-deny with explicit allow rules — DataWriter and REST API can reach both RabbitMQ and PostgreSQL; external traffic only enters via Ingress.

## Key Conventions
- **pytest markers**: `unit`, `integration`, `security`, `real`, `load` — always mark tests appropriately; load tests live in `tests/load/` and must be run explicitly (`pytest tests/load/test_pipeline_throughput.py`) since `norecursedirs = tests/load` prevents auto-collection
- **Message factory helpers**: Use `make_audio_message()`, `make_feature_a_message()`, `make_feature_b_message()` from `tests/helpers.py` rather than constructing dicts manually
- **Valid Bearer tokens** for REST API tests: `"test-token"` and `"valid-token"`
- **Line length**: 100 characters (flake8 + black configured)
- Real-service tests auto-skip when Docker services are unavailable — no special skip decorators needed; the `conftest.py` handles it
