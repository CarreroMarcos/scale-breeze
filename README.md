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
- **Postgres**: `localhost:5432`
- **Kafka**: `localhost:9092` (internal) / `localhost:9094` (external)

### Managing Migrations
Migrations run automatically on container startup. To create a new one:
```bash
uv run alembic revision -m "your description"
```

---

## 🛡 Security & Best Practices (Implemented)
- **Non-Global Clients**: Database and Redis clients are managed via FastAPI's `lifespan` and injected via dependencies to prevent connection leaks.
- **Healthchecks**: Every service (DB, Redis, Kafka, API) has a Docker healthcheck defined.
- **Graceful Shutdown**: Lifecycle hooks ensure connection pools are closed cleanly.
- **Type Safety**: Pydantic models enforce strict schema validation for all inputs/outputs.

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
