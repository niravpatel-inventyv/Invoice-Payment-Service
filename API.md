# API Documentation

Base URL: `http://localhost:8080`

Authentication header:

- `Authorization: Bearer <api_key>`

## Error Format

Current implementation returns this canonical shape for most errors:

```json
{
  "error": "human readable message"
}
```

Validation errors use:

```json
{
  "error": "validation_error",
  "fields": [
    {"field": "name", "message": "name must not be empty"}
  ]
}
```

## Customers

### `POST /customers`

Request:

```json
{
  "name": "Alice",
  "email": "alice@example.com"
}
```

Response `201`:

```json
{
  "id": "uuid",
  "business_id": "uuid",
  "name": "Alice",
  "email": "alice@example.com",
  "created_at": "2026-05-05T12:00:00Z"
}
```

### `GET /customers/{id}`

Response `200`: same shape as create response.

Response `404`:

```json
{"error": "customer not found"}
```

### `GET /customers`

Response `200`: array of customer objects.

## Invoices

### `POST /invoices`

Request:

```json
{
  "customer_id": "uuid",
  "due_date": "2026-06-01",
  "line_items": [
    {"description": "Item A", "quantity": 2, "unit_amount_cents": 500}
  ]
}
```

Server computes totals using integer cents only.

Response `201`:

```json
{
  "id": "uuid",
  "business_id": "uuid",
  "customer_id": "uuid",
  "state": "open",
  "due_date": "2026-06-01",
  "total_amount_cents": 1000,
  "currency": "USD",
  "created_at": "2026-05-05T12:00:00Z",
  "updated_at": "2026-05-05T12:00:00Z",
  "line_items": [
    {
      "id": "uuid",
      "description": "Item A",
      "quantity": 2,
      "unit_amount_cents": 500,
      "line_total_cents": 1000,
      "created_at": "2026-05-05T12:00:00Z"
    }
  ]
}
```

### `GET /invoices/{id}`

Response `200`: same shape as create response.

### `GET /invoices?state=open`

Response `200`: array of invoice objects.

## Payments

### `POST /invoices/{id}/pay`

Required header:

- `Idempotency-Key: <opaque-key>`

Request:

```json
{
  "card_token": "tok_success"
}
```

Success response `200`:

```json
{
  "invoice_id": "uuid",
  "payment_attempt_id": "uuid",
  "status": "paid",
  "psp_ref": "uuid",
  "failure_code": null,
  "message": null
}
```

Failure response example (`tok_card_declined`, `402`):

```json
{
  "invoice_id": "uuid",
  "payment_attempt_id": "uuid",
  "status": "failed",
  "psp_ref": null,
  "failure_code": "card_declined",
  "message": null
}
```

Timeout/network uncertainty response (`202`):

```json
{
  "invoice_id": "uuid",
  "payment_attempt_id": "uuid",
  "status": "pending",
  "psp_ref": null,
  "failure_code": "psp_unreachable",
  "message": "payment status is pending; retry with same Idempotency-Key"
}
```

## Webhooks

### `POST /webhooks/endpoints`

Request:

```json
{
  "url": "https://example.com/webhooks/invoices"
}
```

Response `201`:

```json
{
  "id": "uuid",
  "business_id": "uuid",
  "url": "https://example.com/webhooks/invoices",
  "active": true,
  "created_at": "2026-05-05T12:00:00Z",
  "signing_secret": "whsec_..."
}
```

Emitted event types:

- `invoice.created`
- `invoice.paid`
- `invoice.payment_failed`

Delivery headers:

- `X-Webhook-Id`
- `X-Webhook-Event`
- `X-Webhook-Timestamp`
- `X-Webhook-Signature`
