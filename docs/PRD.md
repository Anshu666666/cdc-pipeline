# Product Requirements Document (PRD)

## Core Problem Statement & Architectural Goals
The objective is to build a robust, high-performance, real-time e-commerce order processing pipeline using Change Data Capture (CDC). This system will serve as a capstone project to demonstrate production-grade distributed systems engineering. The architecture decouples writes from reads, utilizes Avro for strict schema enforcement, and ensures system-wide graceful degradation without sacrificing strict observability.

## Core Features
1. **Idempotency & Deduplication**: Consumers must safely handle duplicate message deliveries (e.g., due to consumer crashes mid-batch). State sinks (ES, Redis) use UPSERT semantics. Non-idempotent side effects (like sending emails) use Redis-backed deduplication (`SETNX`).
2. **Schema Evolution (Avro)**: Events will be serialized in Avro using Confluent Schema Registry to demonstrate strict data contracts and binary serialization efficiency.
3. **Graceful Degradation with Strict Coding Standards**: The system supports conceptual fallbacks (e.g., the API falling back to PostgreSQL if Redis cache misses or is offline). However, the C++ code will strictly avoid generic "quirky" catch-all fallbacks. Expected fallbacks are explicitly engineered; unexpected errors will crash cleanly with full tracing logged to Logstash.
4. **Real Email Notifications**: The notification consumer will integrate with a real SMTP server to send actual emails (e.g., to a Gmail account) upon order creation/shipping.

## Non-Functional Requirements (NFRs)
- **Deployment Constraint**: Must fit within an Oracle VM Free Tier instance. The stack will be heavily memory-constrained (tuned JVM heaps for Kafka, ES, and Logstash) to ensure it runs stably on a single node.
- **Throughput**: Tuned to sustain a reasonable throughput that won't crash the free tier VM (target: 100-500 orders/sec).
- **Consistency Model**: Eventual Consistency. The system guarantees at-least-once delivery, meaning consumers may lag but will eventually process every order. Absolute strict ordering is guaranteed per `order_id` via Kafka partitioning.

## Explicit Out-of-Scope Boundaries
- **Exactly-once semantics**: At-least-once delivery with idempotent consumers is chosen; transactional outbox requires extra application logic, whereas CDC + Debezium offloads this cleanly for our use case.
- **Kubernetes**: The entire stack will be orchestrated via a single `docker-compose.yml` file for simple local/VM deployment.
- **Auth/authz on the API layer**: Assumed to be handled by a gateway out of scope.
