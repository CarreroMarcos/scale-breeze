# ScaleBreeze: A Distributed High-Performance Service Template

ScaleBreeze is a multi-service engineering showcase demonstrating how to architect, secure, and scale modern web applications. It serves as a robust foundation for building high-concurrency systems using distributed messaging, advanced caching, and a security-first infrastructure.

## 🏗 Key Engineering Highlights

### **Distributed Systems & Event-Driven Architecture**
- **High-Throughput I/O**: Developed a Go-based event service that manages both publishing and consuming messages via **Apache Kafka** (KRaft mode).
- **Resilient Messaging**: Implemented exponential backoff connection strategies and graceful shutdown handlers to ensure zero data loss during service lifecycles.
- **Asynchronous Processing**: Leveraged **FastAPI** and **asyncpg** for non-blocking database interactions, supporting high-concurrency feed management.

### **Performance Optimization**
- **Efficient Caching**: Implemented a **Cache-Aside pattern** using **Redis 7**, reducing database load through intelligent pagination-aware caching with `X-Cache` hit/miss visibility.
- **Connection Stewardship**: Tuned PostgreSQL performance with custom-sized connection pools (min: 5, max: 50) tailored for variable load.
- **Optimized Toolchain**: Utilized **uv** for ultra-fast, reproducible Python builds and **Go modules** for efficient dependency management.

### **Security-First Engineering**
- **Stateless Authentication**: Integrated **JWT-based authorization** across both Python and Go services, ensuring secure, decentralized user verification.
- **Infrastructure Hardening**: Architected Docker environments with **read-only root filesystems** and **non-privileged service users** to minimize the potential attack surface.
- **Least-Privilege Data Access**: Isolated application data by migrating from superuser access to a restricted database role (`sb_app`) with granular permissions.
- **Secure Transport**: Enforced **TLS 1.3** and high-grade cipher suites, protected by a 1-year **HSTS** policy and strict security headers (CSP, XFO, nosniff).

### **Observability & Operational Integrity**
- **Distributed Context Tracing**: Implemented end-to-end **Request ID propagation** (`X-Request-ID`) across Nginx, Python, Kafka, and Go for seamless log correlation.
- **Unified Structured Logging**: Standardized all backends to output structured JSON logs, enabling high-signal observability via ELK or Datadog.
- **Robust Health Monitoring**: Integrated Docker healthchecks across the entire stack (Postgres, Redis, Kafka, API) to ensure automated recovery and reliability.
- **Automated Evolutions**: Managed database schema versioning with **Alembic**, integrated directly into the container orchestration flow.

---

## 🚀 Core Technologies

- **Backends**: Python 3.11+ (FastAPI), Go 1.23+
- **Data & Messaging**: PostgreSQL 16, Redis 7, Apache Kafka 3.7.0
- **Gateway & Security**: Nginx, OpenSSL (Self-signed TLS)
- **DevOps**: Docker & Compose, uv, Alembic, Ruff

---

## 🛠 Getting Started

### Prerequisites
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) (for local development)

### Launching the Environment
```bash
docker compose up -d --build
```

### Verification & Testing
- **Python Suite**: `export PYTHONPATH=. && uv run pytest tests/test_main.py`
- **Go Suite**: `cd event-service && go test -v .`
- **Load Testing**: `uv run locust -f tests/load/locustfile.py --host https://localhost:8889`
- **Migrations**: `uv run alembic revision -m "your description"`

---

## 📖 Extended Documentation
- **[GEMINI.md](./GEMINI.md)**: Detailed coding standards, architectural contracts, and AI-agent instructions.
- **`init-db/`**: Scripts for secure database and user initialization.
