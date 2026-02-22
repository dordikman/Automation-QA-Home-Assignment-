# Audio Processing System — QA & Automation Test Suite

A comprehensive test suite for a distributed audio processing pipeline deployed on Kubernetes. The repository covers the full QA lifecycle: test design, mock system implementation, unit/integration/security/load tests, CI/CD pipeline, and Kubernetes deployment manifests.

> **No external dependencies required.**
> All tests and scripts run entirely in-memory. There is no need to install Docker, RabbitMQ, or PostgreSQL to run any part of this solution. A standard Python environment is sufficient.

---

## Repository Structure

```
.
├── design.md                        # Part 1 — Full test plan with Mermaid diagrams
├── requirements.txt                 # Python dependencies
├── pytest.ini                       # pytest configuration and marker definitions
├── docker-compose.yml               # RabbitMQ + PostgreSQL for local/CI integration testing
├── run_mock_server.py               # Launches the mock pipeline for Locust load testing
│
├── mocks/                           # Simulated system components (in-memory)
│   ├── rabbitmq.py                  # In-memory broker (work queues + fanout pub/sub)
│   ├── sensor.py                    # Audio sensor — publishes to audio stream
│   ├── algorithm_a.py               # Algorithm A — audio → Feature Type A
│   ├── algorithm_b.py               # Algorithm B — Feature A → Feature Type B
│   ├── data_writer.py               # DataWriter — persists features to in-memory DB
│   └── rest_api.py                  # REST API — Flask app (real-time + historical)
│
├── tests/
│   ├── helpers.py                   # Message factory functions (make_audio_message, etc.)
│   ├── conftest.py                  # Root fixtures: broker, data_writer, flask_app, client
│   ├── unit/
│   │   ├── conftest.py              # Unit fixtures: algo_a, algo_b
│   │   ├── test_algorithm_a.py      # 23 tests — processing logic + broker interaction
│   │   ├── test_algorithm_b.py      # 23 tests — processing logic + fanout interaction
│   │   ├── test_data_writer.py      # 16 tests — flush() correctness + query() filtering
│   │   └── test_rest_api.py         # 22 tests — auth, endpoints, error handling
│   ├── integration/
│   │   ├── conftest.py              # Pipeline fixture (full wired system)
│   │   └── test_pipeline.py         # 19 tests — stage-by-stage + end-to-end + load balancing
│   ├── security/
│   │   └── test_api_security.py     # 25 tests — auth failures, injection, rate limiting
│   ├── load/
│   │   ├── conftest.py              # Load test fixtures (pipeline with load-sensor)
│   │   └── test_pipeline_throughput.py  # 16 SLA-gated pytest tests (throughput, latency, backpressure)
│   ├── contract/
│   │   └── test_message_contracts.py  # Schema contract tests between pipeline components
│   └── chaos/
│       └── test_resilience.py      # Resilience and failure-mode tests
│
├── k8s/                             # Kubernetes manifests for the real deployment
│   ├── rabbitmq-statefulset.yaml    # RabbitMQ StatefulSet + headless Service
│   ├── algorithm-a-deployment.yaml  # Algorithm A Deployment + HPA
│   ├── algorithm-b-deployment.yaml  # Algorithm B Deployment + HPA
│   ├── datawriter-deployment.yaml   # DataWriter Deployment
│   ├── restapi-deployment.yaml      # REST API Deployment + Service + Ingress (TLS)
│   └── network-policy.yaml          # NetworkPolicies enforcing least-privilege access
│
└── .github/
    └── workflows/
        └── ci.yml                   # GitHub Actions CI/CD pipeline (3 stages)
```

---

## System Architecture

```
Sensors ──► Audio Stream Queue ──► [Algo A pods × N] ──► Features A Fanout
                                                                   │
                                          ┌────────────────────────┤
                                          ▼                        ▼
                                   [Algo B pods × M]         DataWriter ──► DB
                                          │                        ▲
                                          └──► Features B Fanout ──┘
                                                        │
                                                   REST API ◄──── External Clients
                                                   (real-time cache + historical DB)
```

**Two messaging patterns are in play simultaneously:**

| Pattern | Used for | Behaviour |
|---|---|---|
| Work queue (competing consumers) | Audio Stream → Algo A pods | Each message delivered to exactly **one** pod |
| Fanout (pub/sub) | Features A/B → Algo B, DataWriter, REST API | Every subscriber receives an **independent copy** |

---

## Mock System vs. Real Kubernetes System

The Python mocks in `mocks/` faithfully simulate the system's messaging semantics without requiring real infrastructure:

| System component | Mock implementation | Real implementation |
|---|---|---|
| RabbitMQ | `InMemoryBroker` (Python `queue.Queue`) | `RealBroker` wrapping `pika` (AMQP) |
| Algorithm A/B pods | Python class instances | Same classes, different broker injected |
| Load balancing | Multiple `AlgorithmA` instances on one broker | K8s Deployment replicas + HPA auto-scaling |
| DataWriter | In-memory list with `flush()` | `RealDataWriter` writing to PostgreSQL |
| Database | Plain `list[dict]` | PostgreSQL 15 via `psycopg2` |
| REST API | Flask test client | Flask/FastAPI behind nginx Ingress with TLS |
| Network isolation | Not enforced in mocks | Kubernetes NetworkPolicies (see `k8s/`) |

The `InMemoryBroker` and `RealBroker` implement **the same interface**, so `AlgorithmA`, `AlgorithmB`, and `DataWriter` run against real RabbitMQ without any code changes — only the injected broker instance differs (adapter pattern).

---

## Prerequisites

- **Python 3.10+**
- **pip**

Optional (for load testing):
- **Locust** (`pip install locust`)

Optional (for K8s deployment):
- **kubectl** + a Kubernetes cluster
- **Docker** (to build component images)

---

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd audio-processing-qa

# Install all Python dependencies
pip install -r requirements.txt
```

---

## Running the Tests

### All tests

```bash
pytest
```

### By category (using markers)

```bash
# Unit tests only — fast, no infrastructure required (~0.4 s)
pytest -m unit

# Integration tests — full pipeline wired in-memory
pytest -m integration

# Security tests — auth, injection, rate limiting
pytest -m security

# Unit + integration + security (everything except load tests)
pytest -m "unit or integration or security"
```

### With coverage report

```bash
pytest -m unit --cov=mocks --cov-report=term-missing --cov-fail-under=80
```

### With verbose output and HTML report

```bash
pytest -v --html=report.html --self-contained-html
```

---

## Quick Start (Makefile)

A `Makefile` is provided for convenience. Run `make help` to see all available targets.
```bash
make install          # Install all dependencies
make test             # Run unit + integration + security tests
make test-unit        # Fast unit tests only (~0.4 s)
make coverage         # Unit tests with HTML coverage report
make lint             # Run flake8
make format           # Auto-format with black
make sast             # Static security analysis (Bandit)
```

### Additional Test Suites
```bash
# Contract tests — verify schema compatibility between pipeline components
pytest tests/contract/ -v

# Chaos / resilience tests — verify graceful degradation under failures
pytest tests/chaos/ -v

# All non-load tests including contract and chaos
pytest -m "unit or integration or security or contract or chaos" -v
```

---

## Running the Real-Service Integration Tests

These tests verify AMQP and SQL behaviour that the in-memory mocks cannot reproduce:
RabbitMQ acknowledgement semantics, JSONB round-trips, UNIQUE constraint enforcement, parameterised SQL injection resistance, and true fanout delivery to multiple subscribers.

**Step 1 — Start the services (requires Docker)**

```bash
docker-compose up -d
```

Wait for the health checks to pass (about 10–15 seconds).

**Step 2 — Run the real-service tests**

```bash
pytest tests/integration_real/ -v
```

When Docker is **not** running, every test in this directory is automatically skipped with the message *"Real services not available"* — the suite never fails due to missing infrastructure.

**What the real tests cover beyond the in-memory tests**

| Behaviour | In-memory test | Real test |
|---|:---:|:---:|
| AMQP acknowledgement prevents duplicate delivery | ✓ (simulated) | ✓ (enforced by broker) |
| Fanout exchange delivers independent copies per subscriber | ✓ | ✓ (real exchange bindings) |
| UNIQUE constraint rejects duplicate message_id | ✓ (app-level check) | ✓ (PostgreSQL constraint) |
| JSONB features column round-trips nested dicts | — | ✓ |
| Parameterised SQL resists injection at query-planner level | — | ✓ |
| Timestamp-range queries with real TIMESTAMPTZ semantics | — | ✓ |

---

## Running the In-Process Load Tests (pytest)

The primary load test suite runs entirely in-process against the in-memory broker — no running server, no Docker, no Locust required. This is what the CI nightly job executes.

```bash
pytest tests/load/test_pipeline_throughput.py -v -s
```

**What it covers (16 SLA-gated tests):**

| Class | What is measured | SLA threshold |
|---|---|---|
| `TestAudioQueueThroughput` | AlgorithmA single-pod drain rate | ≥ 100 msgs/sec |
| `TestMultiPodScalability` | 2- and 3-pod competing consumers, no loss/duplication | 0 dropped, 0 duplicates |
| `TestEndToEndPipelineLatency` | Sensor → DB wall-clock time | p99 ≤ 2 000 ms |
| `TestDataWriterThroughput` | `flush()` rate and idempotency at 1 000 features | ≥ 50 features/sec |
| `TestRestApiResponseTime` | REST API sequential latency incl. rate-limiter check | p99 ≤ 500 ms |
| `TestQueueBackpressure` | Burst of 1 000 messages — zero message loss | 0 dropped |

The job exits non-zero if any SLA threshold is breached, triggering a Slack alert in CI.

---

## Running the Mock Server (for external load testing)

`run_mock_server.py` launches the full mock pipeline as a live HTTP server for use with external load tools such as Locust or k6.

**Start the mock server**

```bash
python run_mock_server.py
```

This starts background threads simulating sensors, Algorithm A/B pods, and DataWriter, then launches Flask on `http://localhost:5000`.

Point any HTTP load tool at `http://localhost:5000` with a valid `Authorization: Bearer test-token` header to exercise the live endpoints.

---

## CI/CD Pipeline

The GitHub Actions workflow in `.github/workflows/ci.yml` runs three stages:

| Stage | Trigger | Jobs |
|---|---|---|
| **Fast checks** | Every push / PR | Lint (flake8 + black), SAST (Bandit), unit tests, coverage gate (≥ 80%) |
| **Integration & security** | After fast checks pass | Integration tests, security tests, HTML test report |
| **Load tests** | Nightly at 02:00 UTC | `pytest tests/load/test_pipeline_throughput.py` — 16 SLA-gated in-process tests (throughput, latency, backpressure); HTML report artifact; Slack alert on SLA breach |

---

## Kubernetes Deployment

The `k8s/` directory contains reference manifests for the real Kubernetes deployment.

> **Note:** These manifests reference placeholder Docker images (`your-registry/...`). Replace with your actual registry paths before applying.

### Apply all manifests

```bash
kubectl create namespace audio-processing
kubectl apply -f k8s/
```

### What each manifest defines

| File | Resource | Key detail |
|---|---|---|
| `rabbitmq-statefulset.yaml` | StatefulSet + headless Service | Persistent volume; not exposed outside cluster |
| `algorithm-a-deployment.yaml` | Deployment + HPA | Scales 2–10 pods at 70% CPU; competing consumers on `audio_stream` |
| `algorithm-b-deployment.yaml` | Deployment + HPA | Scales 2–8 pods; consumes from `features_a` exchange |
| `datawriter-deployment.yaml` | Deployment | 2 replicas; subscribes to both feature exchanges |
| `restapi-deployment.yaml` | Deployment + Service + Ingress | TLS termination via nginx-ingress; rate limiting at Ingress layer |
| `network-policy.yaml` | NetworkPolicy × 6 | Default-deny-all; explicit allow rules per component |

### NetworkPolicy access matrix

| Source | RabbitMQ (5672) | PostgreSQL (5432) | REST API (8000) |
|---|:---:|:---:|:---:|
| Algorithm A pods | ✓ | — | — |
| Algorithm B pods | ✓ | — | — |
| DataWriter | ✓ | ✓ | — |
| REST API | ✓ | ✓ | — |
| Ingress controller | — | — | ✓ |
| External internet | — | — | Via Ingress only |

---

## Design Document

See [design.md](design.md) for the full test plan, including:

- Test types, objectives, and expected outcomes
- Coverage matrix across all system components
- Automation strategy and example test scenarios
- CI/CD pipeline architecture (Mermaid diagram)
- Logging and observability stack (ELK, Prometheus, Grafana, Jaeger)
