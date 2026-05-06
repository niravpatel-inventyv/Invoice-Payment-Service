#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-dpk_87c5b1db992d221ada071266_7eefd21e1357f8f6d62b24333933dfe8e6a24f4d6e80cd0c10e1f6f28e60d3a1}"

echo "1) Create customer"
 CUSTOMER_RESP=$(curl -sS -X POST "$BASE_URL/customers" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice","email":"alice@example.com"}')
echo "$CUSTOMER_RESP"
CUSTOMER_ID=$(echo "$CUSTOMER_RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "  -> customer_id: $CUSTOMER_ID"
echo

echo "2) Create invoice"
INVOICE_RESP=$(curl -sS -X POST "$BASE_URL/invoices" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"customer_id\":\"$CUSTOMER_ID\",
    \"due_date\":\"2026-06-01\",
    \"line_items\":[
      {\"description\":\"Item A\",\"quantity\":2,\"unit_amount_cents\":500}
    ]
  }")
echo "$INVOICE_RESP"
INVOICE_ID=$(echo "$INVOICE_RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "  -> invoice_id: $INVOICE_ID"
echo

echo "3) Attempt success payment"
curl -sS -X POST "$BASE_URL/invoices/$INVOICE_ID/pay" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: demo-success-1" \
  -H "Content-Type: application/json" \
  -d '{"card_token":"tok_success"}'
echo

echo "4) Attempt failed payment (new invoice needed — invoice is now paid)"
INVOICE_RESP2=$(curl -sS -X POST "$BASE_URL/invoices" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"customer_id\":\"$CUSTOMER_ID\",
    \"due_date\":\"2026-06-01\",
    \"line_items\":[
      {\"description\":\"Item B\",\"quantity\":1,\"unit_amount_cents\":750}
    ]
  }")
INVOICE_ID2=$(echo "$INVOICE_RESP2" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "  -> invoice_id: $INVOICE_ID2"
curl -sS -X POST "$BASE_URL/invoices/$INVOICE_ID2/pay" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: demo-fail-1" \
  -H "Content-Type: application/json" \
  -d '{"card_token":"tok_card_declined"}'
echo
