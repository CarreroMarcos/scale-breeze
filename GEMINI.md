# ScaleBreeze - Engineering Standards & AI Context

This document serves as the primary instructional context for Gemini CLI and other AI agents. It defines the architectural rules, coding conventions, and de facto contracts of the `scalebreeze` repository.

## 🏛 Architectural Rules

### 1. Multi-Language Service Strategy
- **Feed Service (Python)**: Use for complex domain logic, data transformation, and deep integration with the Python ecosystem.
- **Event Service (Go)**: Use for high-throughput I/O, messaging producers/consumers, and low-latency critical paths.

### 2. Infrastructure Hardening (Mandatory)
- **Non-Root**: All new services must run as a non-privileged user (convention: `scalebreeze`) in Docker.
- **Immutable Root**: Containers must be configured with `read_only: true` in `docker-compose.yml`, using `tmpfs` for ephemeral directories like `/tmp`.
- **Resource Constraints**: Always define CPU and Memory limits in the `deploy` section of Compose.

### 3. Database Least Privilege
- **No Superuser**: Applications must NOT connect using the `postgres` superuser.
- **App User**: Use the `sb_app` role (configured via `init-db/`) for all runtime application queries.

## API Design Standards (Contract First)

### Context Tracing
- **Request ID**: Nginx generates a unique `X-Request-ID` for every incoming request.
- **Propagation**: The ID is passed to upstreams via headers and embedded in Kafka event payloads (`request_id` field).
- **Observability**: All structured JSON logs must include the `request_id` to allow cross-service log correlation.

### Stateless Security
- **Authentication**: JWT-based stateless authentication is enforced on sensitive endpoints.
- **Verification**: Services verify tokens using a shared `JWT_SECRET` stored in environment variables.
- **Payload**: Tokens must contain a `sub` claim representing the `user_id`.

### Unified Error Format

All services must return errors in this JSON shape:
```json
{
  "error": {
    "code": "MACHINE_READABLE_CODE",
    "message": "Human readable description",
    "details": {}
  }
}
```

### Pagination & Resource Patterns
- **List Endpoints**: Must support `limit` (max 100) and `offset` query parameters.
- **Caching**: Segregate Redis cache keys by pagination parameters (e.g., `posts:limit:20:offset:0`).
- **Status Codes**: 
  - `201 Created` for resource persistence.
  - `202 Accepted` for async background tasks (e.g., Kafka publication).
  - `422 Unprocessable Entity` for validation failures.

## 💻 Coding Conventions

### Python (FastAPI)
- **Dependency Injection**: Use FastAPI `Depends` for all Database (`asyncpg`) and Redis connections. No global clients.
- **Type Safety**: Mandatory type hints for all function signatures and Pydantic models.
- **Lifespan**: Manage all connection pool lifecycles within the `asynccontextmanager` lifespan of the app.

### Go
- **Interfaces for Testing**: External dependencies (like Kafka writers) must be abstracted into interfaces to enable mocking.
- **Graceful Shutdown**: Implement signal handling (`SIGINT`, `SIGTERM`) to ensure all connections close cleanly without data loss.
- **Stdlib Preference**: Favor the Go standard library (`net/http`) for API development.

## 🧪 Testing Conventions

### Isolation Strategy
- **Python**: Patch `app.router.lifespan_context` during unit-level integration tests to bypass real infrastructure setup. Use `TestClient`.
- **Go**: Use `httptest.NewRecorder` for handler validation and `testify/assert` for expressive checks.

### Load Testing
- **Framework**: [Locust](https://locust.io/) for Python-based distributed load generation.
- **Traffic Shape**: Maintain a 4:1 ratio of reads (`GET /feed`) to writes (`POST /posts`) to simulate realistic usage patterns.
- **Authentication**: Virtual users must generate valid JWTs in `on_start` to pass gateway security boundaries.

## 🛠 Operational Overview
For build commands, port mappings, and local deployment instructions, refer to the **[README.md](./README.md)**.
