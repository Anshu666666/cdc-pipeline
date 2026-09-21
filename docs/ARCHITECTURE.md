# System Architecture & Component Design

## High-Level Data Flow

```mermaid
flowchart TD
    %% Business Data Path
    Client([Load Generator / Client]) -->|POST /orders| API[C++ REST API]
    API -->|Write/Fallback Read| DB[(PostgreSQL)]
    DB -->|WAL| Debezium[Debezium Connector]
    
    %% Schema Registry dependency
    Debezium <-->|Fetch/Register Schema| SR[Schema Registry]
    C1 <-->|Fetch Schema| SR
    C2 <-->|Fetch Schema| SR
    C3 <-->|Fetch Schema| SR

    Debezium -->|Avro Events| Kafka[Kafka (orders topic)]

    Kafka -->|Poll| C1[C++ ES Indexer]
    Kafka -->|Poll| C2[C++ Redis Updater]
    Kafka -->|Poll| C3[C++ Notifier]

    C1 -->|Index| ES[(Elasticsearch)]
    C2 -->|SET| Redis[(Redis)]
    C3 -->|SETNX (Dedup)| Redis
    C3 -->|SMTP Send| Ext[Gmail SMTP Server]
    
    %% Observability Path (Strict Logging)
    API ..->|TCP JSON Logs| Logstash[Logstash]
    C1 ..->|TCP JSON Logs| Logstash
    C2 ..->|TCP JSON Logs| Logstash
    C3 ..->|TCP JSON Logs| Logstash
    
    Logstash -->|Index| ESOps[(ES pipeline-logs-*)]
    ESOps --> Kibana[Kibana]
    ES --> Kibana
```

## Component Responsibilities

1. **Order Generator & API Layer (C++)**: Exposes endpoints (`POST /orders`, `GET /orders/{id}`). Queries Redis first; gracefully falls back to PostgreSQL on cache miss or Redis downtime.
2. **PostgreSQL**: The system of record.
3. **Debezium**: Extracts row-level changes from Postgres WAL and publishes them to Kafka serialized as Avro.
4. **Schema Registry**: Stores and serves Avro schemas.
5. **Kafka**: The central event bus acting as an immutable log.
6. **ES Indexer Consumer (C++)**: Deserializes Avro from Kafka, transforms it into a search document, and bulk-indexes into Elasticsearch.
7. **Redis Updater Consumer (C++)**: Deserializes Avro and maintains a point-read cache in Redis.
8. **Notifier Consumer (C++)**: Deserializes Avro. Uses Redis `SETNX` to prevent duplicate emails, then sends real emails via Gmail SMTP using `libcurl`.
9. **Logstash & Elasticsearch (Ops)**: Collects logs from all C++ services. Every engineered fallback or failure logs a detailed JSON trace.

## Failure Mode Analysis (Graceful Degradation & Strict Logging)

| Failure Point | Impact | Recovery & Mitigation |
| :--- | :--- | :--- |
| **Consumer Crashes Mid-Batch** | Offsets are not committed. | **Behavior**: Consumer restarts and re-polls. ES/Redis use UPSERT. The Notifier uses Redis `SETNX` deduplication keys to avoid sending duplicate emails. |
| **Redis Unavailable** | Cache point-reads fail. | **Behavior**: The API gracefully falls back to querying PostgreSQL directly for point-reads. It logs a specific warning to Logstash. The C++ code uses explicit error handling, not generic `catch(...)` blocks. |
| **Schema Registry Down** | Consumers/Producers cannot serialize/deserialize. | **Behavior**: Debezium halts. C++ Consumers log CRITICAL schema fetch errors and crash cleanly. |
