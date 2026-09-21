# Architecture Decision Records (ADRs)

## ADR 1: Delivery Guarantees - At-Least-Once vs. Exactly-Once
- **Decision**: We will use **At-Least-Once Delivery combined with Idempotent Consumers**.
- **Justification**: Exactly-once semantics in Kafka require complex transactional APIs. At-least-once means if a consumer processes an event but crashes before saving its offset, it will re-process it. We make this safe via **Idempotency** (using DB state/LSN as version vectors).

## ADR 2: Schema Management - Avro vs JSON
- **Decision**: Use **Avro** and **Confluent Schema Registry**.
- **Justification**: Avro provides a compact binary format, reducing network bandwidth and storage. The Schema Registry enforces strict schema compatibility rules (e.g., preventing a producer from deploying a change that breaks downstream consumers). This demonstrates high maturity in data engineering.

## ADR 3: System Resilience - Graceful Degradation & Explicit Engineering
- **Decision**: Architect for conceptual **Graceful Degradation** (e.g. falling back to DB if Redis is down), but strictly enforce explicit code-level error handling.
- **Justification**: We need the system to be resilient. If the cache layer goes offline, the API should survive by querying Postgres. However, this is an *engineered* fallback. We will explicitly avoid AI coding quirks like swallowing unhandled exceptions in generic catch blocks. If a non-engineered failure occurs, the component crashes fast and loudly with full Logstash tracing.

## ADR 4: Side-Effect Deduplication
- **Decision**: Use Redis `SETNX` (Set if Not Exists) for deduplicating non-idempotent actions like emails.
- **Justification**: Since we are using at-least-once delivery, the Notifier consumer might read an "Order Shipped" event twice if it crashes. Because we cannot "upsert" an email, we need a distributed lock/deduplication store. Before sending an email for order X, the consumer will execute `SETNX dedup:email_shipped:X 1` in Redis with a TTL of 7 days. If the command returns 1, the email is sent. If 0, it's a duplicate and is safely ignored.

## ADR 5: Deployment Environment - Oracle VM Optimization
- **Decision**: Heavily tune the `docker-compose` memory limits (JVM heaps) for Kafka, Elasticsearch, Zookeeper, and Logstash.
- **Justification**: The Oracle Free Tier (even the 24GB ARM instance) can buckle under unconstrained Java services. We must set explicit `ES_JAVA_OPTS` and `KAFKA_HEAP_OPTS` to ensure the entire CDC stack runs stably on a single VM.
