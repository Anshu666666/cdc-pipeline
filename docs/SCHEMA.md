# Data Models & Interface Contracts

## PostgreSQL DDL
```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL, 
    items_jsonb JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## Kafka Event Schema (Debezium Avro)
Instead of JSON, events are binary Avro. The Schema Registry will automatically store a schema resembling:
```json
{
  "type": "record",
  "name": "Envelope",
  "namespace": "dbserver1.public.orders",
  "fields": [
    {
      "name": "before",
      "type": ["null", "Value"]
    },
    {
      "name": "after",
      "type": ["null", "Value"]
    },
    {
      "name": "op",
      "type": "string"
    },
    {
      "name": "ts_ms",
      "type": ["null", "long"]
    }
  ]
}
```
*Note: The `Value` record contains the `id`, `user_id`, `total_amount`, etc.*

## Redis Key Schemas
1. **Data Cache Key**: `order:{id}`
   - **Value**: JSON string representing the order state.
   - **Usage**: Point-reads. If missing, the API explicitly logs a cache miss warning and queries Postgres.
2. **Deduplication Key**: `dedup:email_shipped:{id}`
   - **Value**: `"1"`
   - **Usage**: Used by the Notifier consumer with `SETNX` (and a 7-day TTL) to guarantee an email is only dispatched once, even if Kafka replays the message.

## Logstash Pipeline (`logstash.conf`)
```text
input {
  tcp { port => 5000, codec => json_lines }
}
filter {
  mutate { add_field => { "ingest_time" => "%{@timestamp}" } }
}
output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "pipeline-logs-%{+YYYY.MM.dd}"
  }
}
```
