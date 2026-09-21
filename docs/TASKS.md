# Phased Implementation Roadmap

## Phase 1: Infrastructure Setup
- [x] Create `docker-compose.yml` defining PostgreSQL, Kafka, Debezium Connect, Redis, Elasticsearch, Logstash, and Kibana
- [x] Write SQL init scripts to bootstrap the Postgres `orders` table and configure WAL level to `logical`
- [x] Configure Debezium via REST payload script to register the Postgres connector targeting the `orders` table
- [x] Create Logstash `logstash.conf` to expose a TCP port and forward JSON to ES
- [x] **Definition of Done (DoD)**: Running `docker-compose up -d` successfully starts all containers.

## Phase 2: Ingestion & Core Data Flow
- [x] Setup CMake project with `cpp-httplib` and `libpqxx` (Postgres driver)
- [x] Implement `POST /orders` and `PATCH /orders/{id}/status` writing to Postgres
- [x] Implement continuous load generator firing requests
- [x] **Definition of Done (DoD)**: Load generator runs sustainably, API handles concurrent requests, changes confirm flowing into Kafka.

## Phase 3: Consumers & Log Shipping
- [x] Implement C++ Kafka Consumer base class (librdkafka)
- [x] Integrate JSON logging (spdlog) forwarding to Logstash
- [x] Implement ES Indexer Consumer (cpr)
- [x] Implement Redis Updater Consumer (hiredis)
- [x] Implement Notifier Consumer (simulator)
- [x] Verify end-to-end processing (DB -> Kafka -> Consumers -> ES/Redis/Logstash)

## Phase 4: Hardening (Idempotency, DLQ, Resilience)
- [x] Add logic to check LSN/timestamps to ensure Redis updates don't apply older events over newer ones (UPSERT logic)
- [x] Implement a DLQ routing mechanism in the Kafka consumer base class
- [x] Implement exponential backoff when ES or Redis connections fail, preserving Kafka offsets
- [x] **Definition of Done (DoD)**: Pipeline survives failure injection and network partitions without data loss.

## Phase 5: Load Testing & Benchmarking
- [x] Build Kibana dashboards visualizing throughput, latency, and consumer error rates
- [x] Run sustained soak test at 1,000 req/sec
- [x] **Definition of Done (DoD)**: Dashboards reflect 1,000 req/sec within target SLA latency.
