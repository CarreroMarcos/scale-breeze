# ScaleBreeze

A production-grade, multi-service architecture for high-performance feed management and event streaming. Built for local development on Ubuntu/Linux with a focus on security, observability, and scalability.

## 🚀 System Architecture

### 1. Feed Service (Python/FastAPI)
- **High-Performance Async**: Powered by `FastAPI` and `asyncpg` with a custom connection pool (min: 5, max: 50).
- **Cache-Aside Pattern**: Integrated with **Redis 7** for optimized data retrieval with `X-Cache` HIT/MISS tracking.
- **Automated Migrations**: Schema evolution managed by **Alembic** (Async template).

### 2. Event Service (Go)
- **Bidirectional Messaging**: High-performance event producer and consumer using `net/http` and `segmentio/kafka-go`.
- **Resilient Delivery**: Implements `connectWithRetry` with exponential backoff and **graceful shutdown** to prevent data loss.
- **Kafka integration**: Communicates via the `post-events` topic in a KRaft-mode cluster.

### 3. API Gateway (Nginx)
- **Security First**: Enforced SSL/TLS (HTTPS) with automated 301 redirection from HTTP.
- **Traffic Control**: IP-based **Rate Limiting** (100r/m) with burst protection.
- **Observability**: JSON-formatted analytics logs and custom JSON error responses (429, 502, 504).

---

## 🛠 Getting Started

### Prerequisites
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) (recommended for local Python dev)
- Go 1.23+ (optional for local Go dev)

### Launching the Stack
```bash
docker compose up -d --build
```
- **Gateway (HTTPS)**: `https://localhost:8889`
- **Gateway (HTTP Redirect)**: `http://localhost:8888`
- **PostgreSQL**: `localhost:5432` (User: `sb_app`)
- **Redis**: `localhost:6379`
- **Kafka**: `localhost:9094` (External Broker)

### Core Workflows
| Task | Command |
| :--- | :--- |
| **Python Tests** | `export PYTHONPATH=. && uv run pytest tests/test_main.py` |
| **Go Tests** | `cd event-service && go test -v .` |
| **Create Migration** | `uv run alembic revision -m "description"` |
| **View API Logs** | `docker compose logs -f api` |

---

## 🛡 Security & Hardening
ScaleBreeze adheres to strict production security standards:
- **Defense in Depth**: All service containers run as **non-privileged users** (`scalebreeze`) with **read-only root filesystems**.
- **Least Privilege**: Applications connect to PostgreSQL using a restricted `sb_app` user instead of the superuser.
- **Resource Insulation**: Strict CPU and Memory limits are applied to every container to mitigate local DoS.
- **Transport Security**: TLS 1.3 enforced with a 1-year HSTS policy and restrictive Content-Security-Policy (CSP).

---

## 📖 Documentation
- For internal architectural rules, coding standards, and AI-agent instructions, see **[GEMINI.md](./GEMINI.md)**.
- For database schema initialization logic, see **`init-db/`**.
