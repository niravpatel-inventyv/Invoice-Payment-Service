# Invoice & Payment Service (Take-Home)

Rust/Axum implementation of the Dodo Payments backend take-home:

- API key-authenticated multi-tenant invoice API
- token-driven mock PSP service
- async webhook delivery worker with signed retries

## Quick Start

Run everything with one command:

```bash
docker compose up --build
```

What boots:

- `postgres` on `localhost:5432`
- `invoice-api` on `localhost:8080`
- `mock-psp` on `localhost:8081`

`invoice-api` also runs the background webhook/pending-payment worker loop in-process.

Migrations are applied automatically by the `migrate` service before API/worker startup.

Demo API key already seeded for local testing (tied to **Acme Retail Pvt Ltd**):

```
dpk_87c5b1db992d221ada071266_7eefd21e1357f8f6d62b24333933dfe8e6a24f4d6e80cd0c10e1f6f28e60d3a1
```

## Curl Examples (Required)

All four examples chain together — each step captures the UUID returned by the previous call. Copy and run them in order in one shell session.

```bash
API_KEY="dpk_87c5b1db992d221ada071266_7eefd21e1357f8f6d62b24333933dfe8e6a24f4d6e80cd0c10e1f6f28e60d3a1"
```

1. Create customer

```bash
CUSTOMER_RESP=$(curl -sS -X POST http://localhost:8080/customers \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","email":"alice@example.com"}')
echo "$CUSTOMER_RESP"
CUSTOMER_ID=$(echo "$CUSTOMER_RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
```

2. Create invoice (uses `$CUSTOMER_ID` from step 1)

```bash
INVOICE_RESP=$(curl -sS -X POST http://localhost:8080/invoices \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{
    \"customer_id\":\"$CUSTOMER_ID\",
    \"due_date\":\"2026-06-01\",
    \"line_items\":[
      {\"description\":\"Item A\",\"quantity\":2,\"unit_amount_cents\":500}
    ]
  }")
echo "$INVOICE_RESP"
INVOICE_ID=$(echo "$INVOICE_RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
```

3. Attempt successful payment (uses `$INVOICE_ID` from step 2)

```bash
curl -sS -X POST "http://localhost:8080/invoices/$INVOICE_ID/pay" \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Idempotency-Key: demo-pay-success-1' \
  -H 'Content-Type: application/json' \
  -d '{"card_token":"tok_success"}'
```

4. Attempt failed payment (creates a fresh invoice — the previous one is now `paid`)

```bash
INVOICE_RESP2=$(curl -sS -X POST http://localhost:8080/invoices \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{
    \"customer_id\":\"$CUSTOMER_ID\",
    \"due_date\":\"2026-06-01\",
    \"line_items\":[
      {\"description\":\"Item B\",\"quantity\":1,\"unit_amount_cents\":750}
    ]
  }")
INVOICE_ID2=$(echo "$INVOICE_RESP2" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
curl -sS -X POST "http://localhost:8080/invoices/$INVOICE_ID2/pay" \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Idempotency-Key: demo-pay-fail-1' \
  -H 'Content-Type: application/json' \
  -d '{"card_token":"tok_card_declined"}'
```

## Running Required Tests

```bash
cd services/invoice-api
TEST_DATABASE_URL=postgres://invoice_user:invoice_pass@127.0.0.1:5432/invoice_db cargo test --test required_tests
```
## Checking Webhook Deliveries

```bash
# connect to postgres and query
docker exec -it invoice-postgres psql -U invoice_user -d invoice_db \
  -c "SELECT event_type, status, attempt_count FROM webhook_deliveries ORDER BY created_at DESC LIMIT 5;"
```

