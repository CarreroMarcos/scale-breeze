# ScaleBreeze Feed Service - Project Context

This document serves as the primary instructional context for Gemini CLI when working within the `scalebreeze` repository.

## Project Overview
ScaleBreeze Feed Service is a high-performance, asynchronous FastAPI application designed as a production-ready template. It features a modern tech stack focused on performance, scalability, and observability.

### Core Tech Stack
- **Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Python 3.11+)
- **Package Management**: [uv](https://github.com/astral-sh/uv)
- **Database**: PostgreSQL 16 (via `asyncpg` for raw performance)
- **Caching**: Redis 7 (via `redis-py`)
- **Messaging**: Apache Kafka 3.7.0 (KRaft mode, no Zookeeper)
- **Migrations**: Alembic (using `sqlalchemy[asyncio]`)
- **Proxy**: Nginx (handling HTTP 80 and HTTPS 443 placeholders)
- **Containerization**: Docker & Docker Compose

## Architecture & Conventions

### Database & Connections
- **Pool Management**: `asyncpg` connection pools are initialized in `main.py`'s `lifespan` context (min: 5, max: 50).
- **Dependency Injection**: Database and Redis connections are injected into routes via FastAPI `Depends`.
- **Statelessness**: Avoid global client instances; always use the provided dependencies.

### Caching Strategy
- **Pattern**: Cache-aside implementation for `GET /posts`.
- **Headers**: Returns `X-Cache: HIT` or `X-Cache: MISS`.
- **TTL**: 60 seconds for the posts list.
- **Serialization**: Custom `json_serial` helper handles `UUID` and `datetime` types.

### Observability & Logging
- **Structured Logging**: Middleware logs every request in JSON format to stdout.
- **Correlation IDs**: `X-Request-ID` is extracted from headers (or generated) and propagated in the response.
- **Healthchecks**: Integrated Docker healthchecks for all services (DB, Redis, Kafka, API).

### Code Style
- **Type Hints**: Mandatory for all function signatures and Pydantic models.
- **Linting**: [Ruff](https://github.com/astral-sh/ruff) is configured via `.pre-commit-config.yaml`.
- **Line Endings**: LF enforced for scripts and Dockerfiles; CRLF allowed for docs (configured in `.gitattributes`).

## Building and Running

### Prerequisites
- Docker & Docker Compose
- `uv` (recommended for local dev)

### Primary Commands
- **Start Stack**: `docker compose up -d --build`
- **Stop Stack**: `docker compose down`
- **View Logs**: `docker compose logs -f api`
- **Create Migration**: `uv run alembic revision -m "description"`
- **Apply Migrations**: Handled automatically by `run_migrations.sh` inside the container.

## Directory Structure
- `migrations/`: Alembic migration scripts.
- `nginx/`: Nginx configuration files.
- `main.py`: Core FastAPI application and logic.
- `Dockerfile.api`: Optimized build using `uv`.
- `docker-compose.yml`: Infrastructure orchestration.
- `pyproject.toml`: Dependency and tool configuration.
- `run_migrations.sh`: Container entrypoint script.
- `.pre-commit-config.yaml`: Quality control hooks.
