# ScaleBreeze

ScaleBreeze is a high-throughput, event-driven microservices architecture designed to model modern distributed systems patterns. Built with a Go event service, a Python/FastAPI application tier, Apache Kafka, Redis, and PostgreSQL, the system is engineered to handle intensive read/write streams under strict edge-level traffic and security boundaries.

## Development Methodology: Spec-Driven Orchestration

This system was engineered using a Spec-Driven Development (SDD) and Test-Driven Development (TDD) workflow, leveraging AI coding agents (Gemini, Claude Code) for rapid feature implementation. 

Rather than writing boilerplate handlers by hand, human-in-the-loop engineering focus was directed at defining strict system contracts, architectural rules, and runtime guardrails:
* **Multi-Language Strategy:** Segregating complex domain logic and data transformations into a Python/FastAPI service, while offloading high-throughput I/O and messaging consumers to a low-latency Go service.
* **Contract-First API Design:** Enforcing unified JSON error formats, mandatory context-tracing via `X-Request-ID` propagation across HTTP/Kafka boundaries, and stateless JWT authorization.
* **Infrastructure Hardening:** Restricting containers to non-privileged users (`scalebreeze`), configuring immutable root filesystems (`read_only: true`), and applying strict database least-privilege roles (`sb_app`).

## System Performance & Load Profiling

The stack was subjected to an incremental stress test simulating a burst of 100 concurrent virtual users to analyze edge mitigation and failure domains on a resource-constrained host environment (8GB RAM). 

| Metric | Target Performance | Status |
| :--- | :--- | :--- |
| **Peak Throughput** | ~43 Requests/second | Stabilized |
| **Median Latency (p50)** | 2.0 ms | Elite |
| **Tail Latency (p95)** | 130.0 ms | Healthy |
| **Edge Ingress Defense** | HTTP 429 (Rate Limited) | Enforced |
| **Nginx CPU Utilization** | ~3% during peak throttling | High Efficiency |

### Core Architectural Findings

* **Ingress Protection:** Nginx successfully shed ~93% of unauthorized burst traffic via the `limit_req` directive once the bucket capacity was breached, returning structured JSON error schemas to protect downstream services from compute starvation.
* **Database Insulation:** The Cache-Aside pattern via Redis maintained a flat 2ms p50 read latency on the pagination-segregated feed endpoints, entirely shielding the PostgreSQL engine from connection pool exhaustion under load.
* **Asynchronous Offloading:** Client write blocks were eliminated by publishing event tasks directly to Apache Kafka, where the Go worker tier consumed messages out-of-band to update user timelines.

## Future Development Roadmap

The next development phases focus on moving the validated local architecture into a cloud-native, production-ready state based on documented repository constraints:

1. **Infrastructure as Code (IaC):** Translate the local multi-container network topology into modular Terraform configurations targeting an AWS environment (VPC, private subnets, security groups, and ECS/EKS container infrastructure).
2. **Dynamic Tiered Ingress Limiting:** Enhance the Nginx gateway layer by implementing conditional variable mapping to dynamically scale rate limits based on auth posture (Anonymous: 100r/m, Authenticated: 1000r/m).
3. **Thundering Herd Mitigation:** Transition read-heavy endpoints from a strict Cache-Aside topology to an asynchronous Cache-Ahead model (background workers refreshing the cache) to prevent database spikes on TTL expiry.
4. **Serialization Optimization:** Benchmarking and replacing the native Python JSON parsing utilities in the FastAPI layer with `orjson` to reduce CPU overhead on high-throughput data validation paths.

## Getting Started

### Prerequisites
* Docker and Docker Compose
* [uv](https://github.com/astral-sh/uv) for local Python development
* Go 1.24+ for event-service verification

### Run the Stack
```bash
docker compose up -d --build
