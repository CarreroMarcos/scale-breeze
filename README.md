# ScaleBreeze Feed Service

A high-performance, production-ready FastAPI template designed for local development on Ubuntu/Linux.

## 🚀 Tech Stack & Industry Standards

### Backend Architecture
- **FastAPI**: Modern, async Python framework.
- **asyncpg**: Direct PostgreSQL interaction with a custom-tuned connection pool (min: 5, max: 50).
- **Redis 7**: Cache-aside implementation on `GET /posts` with `X-Cache` headers and 60s TTL.
- **Apache Kafka 3.7.0**: Configured in **KRaft mode** (no Zookeeper) for modern, simplified event streaming.

### Infrastructure & DevOps
- **uv**: The next-generation Python package manager for lightning-fast builds and reproducible environments via `uv.lock`.
- **Alembic**: Managed async database migrations, integrated into the container startup sequence.
- **Nginx Reverse Proxy**: Handles routing for both HTTP (80) and HTTPS (443) with X-Request-ID propagation.
- **Persistence**: Named Docker volumes for PostgreSQL, Redis, and Kafka to ensure data survival across restarts.
- **Structured Logging**: Every request emits a JSON log containing `timestamp`, `request_id`, `method`, `path`, `status_code`, `duration_ms`, and `client_ip`.

---

## 🛠 Getting Started

### Prerequisites
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) (optional, for local development)

### Running the Stack
```bash
docker compose up -d --build
```
- **API**: `http://localhost:8888`
- **Events**: `http://localhost:8888/events` (POST)
- **Postgres**: `localhost:5432`
- **Kafka**: `localhost:9092` (internal) / `localhost:9094` (external)

### Managing Migrations
Migrations run automatically on container startup. To create a new one:
```bash
uv run alembic revision -m "your description"
```

### Running Tests
#### Python (Feed Service)
```bash
export PYTHONPATH=.
uv run pytest tests/test_main.py
```

#### Go (Event Service)
```bash
cd event-service
go test -v .
```

---

## 🛡 Security & Best Practices (Implemented)
- **Non-Privileged Users**: All service containers (API, Events) run as non-root users (`scalebreeze`) for defense-in-depth isolation.
- **SSL/TLS & HSTS**: Enforced HTTPS with a 1-year HSTS policy and modern cipher suites.
- **Security Headers**: Nginx is configured with strict Content-Security-Policy (CSP), X-Frame-Options (DENY), and X-Content-Type-Options (nosniff).
- **CORS Policies**: Explicit cross-origin resource sharing allowed only for the gateway origin.
- **Rate Limiting**: IP-based throttling (100r/m) with burst handling.
- **Graceful Shutdown**: All services handle termination signals to prevent data loss.

---

## 📈 Future Improvements (Gap Analysis)
To move from "Production-Quality" to "Enterprise-Grade," consider these next steps:

1. **Observability (OpenTelemetry)**:
   - Integrate `opentelemetry-python` for distributed tracing and Prometheus metrics.
2. **Security (Secret Management)**:
   - Move from `.env` files to a secure provider like HashiCorp Vault or AWS Secrets Manager.
3. **Validation (Pydantic v2 Features)**:
   - Leverage Pydantic's `@field_validator` for more complex business logic checks beyond character length.
4. **Resiliency (Circuit Breakers)**:
   - Use libraries like `resilience4j` (or Python equivalents) for Kafka and DB interactions to handle transient failures.
5. **CI/CD (Pre-commit & Linting)**:
   - Add a `.pre-commit-config.yaml` with `ruff` and `mypy` for static analysis and formatting.
6. **Testing**:
   - Implement Integration tests using `pytest-asyncio` and `TestClient`.
ntegration tests using `pytest-asyncio` and `TestClient`.
