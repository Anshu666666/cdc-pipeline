# REST API Specification

The Order API (written in C++) serves as the ingest point for the load generator and exposes point-reads for verification.

## Base URL
`http://localhost:8080/api/v1`

## 1. Create Order
**Endpoint**: `POST /orders`

**Description**: Creates a new order in PostgreSQL.

**Request Body**:
```json
{
  "user_id": "987e6543-e21b-12d3-a456-426614174000",
  "total_amount": 150.50,
  "items": [
    { "product_id": "abc", "qty": 2 }
  ]
}
```

**Response (201 Created)**:
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "CREATED"
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "Invalid payload format."
}
```

## 2. Get Order by ID
**Endpoint**: `GET /orders/{id}`

**Description**: Retrieves the current state of an order. The API will first attempt to fetch this from **Redis**. If it results in a cache miss, it will fall back to **PostgreSQL**.

**Response (200 OK)**:
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "user_id": "987e6543-e21b-12d3-a456-426614174000",
  "total_amount": 150.50,
  "status": "CREATED",
  "items": [
    { "product_id": "abc", "qty": 2 }
  ],
  "source": "redis" // or "postgres" to verify caching behavior
}
```

**Response (404 Not Found)**:
```json
{
  "error": "Order not found."
}
```

## 3. Update Order Status
**Endpoint**: `PATCH /orders/{id}/status`

**Description**: Simulates the order lifecycle (e.g., PAYMENT_PENDING -> SHIPPED). Used by the load generator to trigger UPDATE events in the CDC pipeline.

**Request Body**:
```json
{
  "status": "SHIPPED"
}
```

**Response (200 OK)**:
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "SHIPPED"
}
```
