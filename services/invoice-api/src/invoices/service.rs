use axum::http::StatusCode;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

use super::model::{CreateInvoiceRequest, InvoiceLineItemResponse, InvoiceResponse, InvoiceState, PayInvoiceRequest, PayInvoiceResponse};
use crate::webhooks;

const MAX_PAYMENT_RETRY_ATTEMPTS: i64 = 3;

#[derive(Debug, Error)]
pub enum InvoiceError {
    #[error("invoice not found")]
    NotFound,
    #[error("customer not found for this business")]
    CustomerNotFound,
    #[error("invalid invoice state filter")]
    InvalidStateFilter,
    #[error("invalid state transition: {from} -> {to}")]
    InvalidStateTransition { from: String, to: String },
    #[error("money arithmetic overflow")]
    MoneyOverflow,
    #[error("invoice is not in payable state: {state}")]
    InvoiceNotPayable { state: String },
    #[error("idempotency key reuse with different payload")]
    IdempotencyKeyMismatch,
    #[error("invalid cached idempotent response payload")]
    InvalidCachedResponse,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

struct InvoiceRow {
    id: Uuid,
    business_id: Uuid,
    customer_id: Uuid,
    state: String,
    due_date: NaiveDate,
    total_amount_cents: i64,
    currency: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct LockedInvoice {
    state: String,
    total_amount_cents: i64,
    currency: String,
}

struct IdempotencyRow {
    id: Uuid,
    request_fingerprint: String,
    response_status: Option<i32>,
    response_body: Option<Value>,
}

/// Creates a new invoice for a business.
/// Steps: verify the customer belongs to the business, compute per-line totals and guard
/// against overflow, insert the invoice row in Draft state, insert all line items, enqueue
/// an `invoice.created` webhook, then commit the whole thing in one transaction.
pub async fn create(db: &PgPool, business_id: Uuid, req: CreateInvoiceRequest) -> Result<InvoiceResponse, InvoiceError> {
    // tracing::info!(business_id = %business_id, customer_id = %req.customer_id, line_item_count = req.line_items.len(), "invoice_service.create started");
    let mut tx: Transaction<'_, Postgres> = db.begin().await?;
    let due_date = req.due_date;
    let customer_id = req.customer_id;

    let customer = sqlx::query(
        r#"
		SELECT id
		FROM customers
		WHERE id = $1 AND business_id = $2
		"#,
    )
    .bind(req.customer_id)
    .bind(business_id)
    .fetch_optional(&mut *tx)
    .await?;

    if customer.is_none() {
        // tracing::info!(business_id = %business_id, customer_id = %req.customer_id, "invoice_service.create customer not found");
        tx.rollback().await?;
        return Err(InvoiceError::CustomerNotFound);
    }

    let mut total_amount_cents: i64 = 0;
    let mut computed_lines: Vec<(String, i32, i64, i64)> = Vec::new();
    let mut line_items_payload: Vec<Value> = Vec::new();

    for line in &req.line_items {
        let line_total = line.unit_amount_cents.checked_mul(i64::from(line.quantity)).ok_or(InvoiceError::MoneyOverflow)?;

        total_amount_cents = total_amount_cents.checked_add(line_total).ok_or(InvoiceError::MoneyOverflow)?;

        computed_lines.push((line.description.clone(), line.quantity, line.unit_amount_cents, line_total));
        line_items_payload.push(serde_json::json!({
            "description": line.description,
            "quantity": line.quantity,
            "unit_amount_cents": line.unit_amount_cents,
            "line_total_cents": line_total,
        }));
    }

    // tracing::info!(business_id = %business_id, customer_id = %req.customer_id, total_amount_cents = total_amount_cents, "invoice_service.create computed totals");

    let inserted = sqlx::query(
        r#"
		INSERT INTO invoices (
			business_id,
			customer_id,
			state,
			due_date,
			total_amount_cents,
			currency
		)
		VALUES ($1, $2, $3, $4, $5, 'USD')
		RETURNING id
		"#,
    )
    .bind(business_id)
    .bind(req.customer_id)
    .bind(InvoiceState::Draft.as_str())
    .bind(req.due_date)
    .bind(total_amount_cents)
    .fetch_one(&mut *tx)
    .await?;

    let invoice_id: Uuid = inserted.try_get("id")?;
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, "invoice_service.create inserted invoice row");

    for (description, quantity, unit_amount_cents, line_total_cents) in computed_lines {
        sqlx::query(
            r#"
			INSERT INTO invoice_line_items (
				invoice_id,
				description,
				quantity,
				unit_amount_cents,
				line_total_cents
			)
			VALUES ($1, $2, $3, $4, $5)
			"#,
        )
        .bind(invoice_id)
        .bind(description)
        .bind(quantity)
        .bind(unit_amount_cents)
        .bind(line_total_cents)
        .execute(&mut *tx)
        .await?;
    }

    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, line_item_count = line_items_payload.len(), "invoice_service.create inserted line items");

    let webhook_payload = serde_json::json!({
        "event_type": "invoice.created",
        "business_id": business_id,
        "invoice": {
            "id": invoice_id,
            "customer_id": customer_id,
            "state": InvoiceState::Draft.as_str(),
            "due_date": due_date,
            "total_amount_cents": total_amount_cents,
            "currency": "USD"
        },
        "line_items": line_items_payload,
        "occurred_at": Utc::now(),
    });

    webhooks::enqueue_event_for_business(&mut tx, business_id, invoice_id, "invoice.created", webhook_payload).await?;
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, "invoice_service.create queued invoice.created webhook deliveries");

    tx.commit().await?;

    get_by_id(db, business_id, invoice_id).await
}

/// Fetches a single invoice plus its line items, scoped to the given business.
/// Returns `NotFound` if the invoice doesn't exist or belongs to a different business.
pub async fn get_by_id(db: &PgPool, business_id: Uuid, invoice_id: Uuid) -> Result<InvoiceResponse, InvoiceError> {
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, "invoice_service.get_by_id started");
    let invoice = query_invoice_row_by_id(db, business_id, invoice_id).await?;
    let line_items = query_invoice_line_items(db, invoice.id).await?;
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, line_item_count = line_items.len(), "invoice_service.get_by_id fetched invoice");

    Ok(InvoiceResponse {
        id: invoice.id,
        business_id: invoice.business_id,
        customer_id: invoice.customer_id,
        state: invoice.state,
        due_date: invoice.due_date,
        total_amount_cents: invoice.total_amount_cents,
        currency: invoice.currency,
        created_at: invoice.created_at,
        updated_at: invoice.updated_at,
        line_items,
    })
}

/// Returns all invoices for a business, optionally filtered to a specific state string.
/// Validates the state value before querying; rejects unknown states with `InvalidStateFilter`.
/// Fetches line items for every invoice in the result set.
pub async fn list(db: &PgPool, business_id: Uuid, state: Option<String>) -> Result<Vec<InvoiceResponse>, InvoiceError> {
    // tracing::info!(business_id = %business_id, state_filter = ?state, "invoice_service.list started");
    let invoice_rows: Vec<InvoiceRow> = match state {
        Some(raw_state) => {
            if InvoiceState::parse(raw_state.as_str()).is_none() {
                return Err(InvoiceError::InvalidStateFilter);
            }

            let rows = sqlx::query(
                r#"
				SELECT id, business_id, customer_id, state, due_date, total_amount_cents, currency, created_at, updated_at
				FROM invoices
				WHERE business_id = $1 AND state = $2
				ORDER BY created_at DESC
				"#,
            )
            .bind(business_id)
            .bind(raw_state)
            .fetch_all(db)
            .await?;

            rows.into_iter().map(map_invoice_row).collect::<Result<Vec<_>, _>>()?
        }
        None => {
            let rows = sqlx::query(
                r#"
				SELECT id, business_id, customer_id, state, due_date, total_amount_cents, currency, created_at, updated_at
				FROM invoices
				WHERE business_id = $1
				ORDER BY created_at DESC
				"#,
            )
            .bind(business_id)
            .fetch_all(db)
            .await?;

            rows.into_iter().map(map_invoice_row).collect::<Result<Vec<_>, _>>()?
        }
    };

    let mut invoices: Vec<InvoiceResponse> = Vec::new();
    for invoice in invoice_rows {
        let line_items = query_invoice_line_items(db, invoice.id).await?;
        invoices.push(InvoiceResponse {
            id: invoice.id,
            business_id: invoice.business_id,
            customer_id: invoice.customer_id,
            state: invoice.state,
            due_date: invoice.due_date,
            total_amount_cents: invoice.total_amount_cents,
            currency: invoice.currency,
            created_at: invoice.created_at,
            updated_at: invoice.updated_at,
            line_items,
        });
    }

    // tracing::info!(business_id = %business_id, invoice_count = invoices.len(), "invoice_service.list returning invoices");

    Ok(invoices)
}

/// Moves an invoice to `next` state inside an existing transaction.
/// Steps: lock the row with `FOR UPDATE`, read the current state, check it is a valid
/// transition via `can_transition_to()`, then write the new state. Rejects invalid
/// transitions with `InvalidStateTransition` so callers never need to guard separately.
pub async fn transition_state(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, invoice_id: Uuid, next: InvoiceState) -> Result<(), InvoiceError> {
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, next_state = %next.as_str(), "invoice_service.transition_state started");
    let current = sqlx::query(
        r#"
		SELECT state
		FROM invoices
		WHERE id = $1 AND business_id = $2
		FOR UPDATE
		"#,
    )
    .bind(invoice_id)
    .bind(business_id)
    .fetch_optional(&mut **tx)
    .await?;

    let current = match current {
        Some(row) => row.try_get::<String, _>("state")?,
        None => return Err(InvoiceError::NotFound),
    };

    let current_state = match InvoiceState::parse(current.as_str()) {
        Some(state) => state,
        None => return Err(InvoiceError::InvalidStateTransition { from: current, to: next.as_str().to_string() }),
    };

    if !current_state.can_transition_to(next) {
        // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, current_state = %current_state.as_str(), requested_state = %next.as_str(), "invoice_service.transition_state rejected invalid transition");
        return Err(InvoiceError::InvalidStateTransition { from: current_state.as_str().to_string(), to: next.as_str().to_string() });
    }

    sqlx::query(
        r#"
		UPDATE invoices
		SET state = $1, updated_at = NOW()
		WHERE id = $2 AND business_id = $3
		"#,
    )
    .bind(next.as_str())
    .bind(invoice_id)
    .bind(business_id)
    .execute(&mut **tx)
    .await?;

    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, new_state = %next.as_str(), "invoice_service.transition_state updated invoice state");

    Ok(())
}

/// Attempts to charge the invoice through the PSP. The full flow inside one transaction:
/// 1. Check if the idempotency key already exists — if yes, replay the cached response.
/// 2. Lock the invoice row (`FOR UPDATE`) to block concurrent pay attempts.
/// 3. If the invoice is still Draft, auto-promote it to Open in the same transaction.
/// 4. Reject with `InvoiceNotPayable` if the invoice is in any other non-Open state.
/// 5. Insert the idempotency key row and a `pending` payment attempt record.
/// 6. Call the PSP; on network error or unparseable response, persist `pending` and return 202.
/// 7. On PSP success: mark attempt `succeeded`, transition invoice to Paid, enqueue `invoice.paid` webhook, return 200.
/// 8. On PSP failure: mark attempt `failed`, check if retry limit reached and move invoice to
///    Uncollectible if so, enqueue `invoice.payment_failed` webhook, return the PSP status code.
pub async fn pay(db: &PgPool, business_id: Uuid, invoice_id: Uuid, idempotency_key: String, req: PayInvoiceRequest, psp_base_url: &str, http_client: &reqwest::Client) -> Result<(StatusCode, PayInvoiceResponse), InvoiceError> {
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, idempotency_key = %idempotency_key, "invoice_service.pay started");
    let request_fingerprint = format!("POST:/invoices/{invoice_id}/pay:{}", req.card_token.trim());
    let mut tx: Transaction<'_, Postgres> = db.begin().await?;

    let existing = find_idempotency_key(&mut tx, business_id, idempotency_key.as_str()).await?;
    if let Some(row) = existing {
        // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, idempotency_key = %idempotency_key, "invoice_service.pay found existing idempotency record");
        return return_existing_idempotent_response(tx, invoice_id, request_fingerprint.as_str(), row).await;
    }

    let locked_invoice = lock_invoice_for_payment(&mut tx, business_id, invoice_id).await?;
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, locked_state = %locked_invoice.state, total_amount_cents = locked_invoice.total_amount_cents, "invoice_service.pay locked invoice row");

    if locked_invoice.state == InvoiceState::Draft.as_str() {
        transition_state(&mut tx, business_id, invoice_id, InvoiceState::Open).await?;
        // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, "invoice_service.pay transitioned invoice from draft to open");
    } else if locked_invoice.state != InvoiceState::Open.as_str() {
        let raced_existing = find_idempotency_key(&mut tx, business_id, idempotency_key.as_str()).await?;
        if let Some(row) = raced_existing {
            return return_existing_idempotent_response(tx, invoice_id, request_fingerprint.as_str(), row).await;
        }

        tx.rollback().await?;
        return Err(InvoiceError::InvoiceNotPayable { state: locked_invoice.state });
    }

    let idempotency_id = match insert_idempotency_key(&mut tx, business_id, idempotency_key.as_str(), request_fingerprint.as_str()).await? {
        Some(value) => value,
        None => {
            let raced_existing = find_idempotency_key(&mut tx, business_id, idempotency_key.as_str()).await?;
            match raced_existing {
                Some(row) => return return_existing_idempotent_response(tx, invoice_id, request_fingerprint.as_str(), row).await,
                None => {
                    tx.rollback().await?;
                    return Err(InvoiceError::InvalidCachedResponse);
                }
            }
        }
    };

    let payment_attempt_id = insert_pending_payment_attempt(&mut tx, invoice_id, idempotency_id).await?;
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, idempotency_id = %idempotency_id, payment_attempt_id = %payment_attempt_id, "invoice_service.pay inserted pending payment attempt");

    let psp_url = format!("{}/payments", psp_base_url.trim_end_matches('/'));
    let psp_payload = serde_json::json!({
        "card_token": req.card_token,
        "amount_cents": locked_invoice.total_amount_cents,
        "currency": locked_invoice.currency,
        "merchant_reference": payment_attempt_id.to_string(),
    });

    let psp_result = http_client.post(psp_url).json(&psp_payload).send().await;
    // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, "invoice_service.pay sent request to PSP");

    match psp_result {
        // tok_timeout: reqwest fires a timeout error after 3s (client timeout in main.rs).
        //   The mock PSP sleeps 30s for this token; the connection is dropped before any
        //   response arrives. send().await returns Err — lands here.
        Err(err) => {
            tracing::error!(
                error = %err,
                invoice_id = %invoice_id,
                payment_attempt_id = %payment_attempt_id,
                "psp request failed; leaving invoice state open"
            );

            let body = PayInvoiceResponse { invoice_id, payment_attempt_id, status: "pending".to_string(), psp_ref: None, failure_code: Some("psp_unreachable".to_string()), message: Some("payment status is pending; retry with same Idempotency-Key".to_string()) };

            persist_pay_result(&mut tx, idempotency_id, payment_attempt_id, "pending", None, Some("psp_unreachable"), StatusCode::ACCEPTED, &body).await?;
            tx.commit().await?;

            tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, "invoice_service.pay persisted pending result due to unreachable PSP");

            Ok((StatusCode::ACCEPTED, body))
        }
        // tok_network_error: mock PSP returns HTTP 500 with { status: "failed", code: "network_error" }.
        //   reqwest receives a complete HTTP response so send().await is Ok. Lands here.
        //   http_status.is_success() is false → falls through to the failure branch.
        //
        Ok(response) => {
            let http_status = response.status();
            // tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, psp_http_status = %http_status, "invoice_service.pay received response from PSP");
            let payload: Value = match response.json().await {
                Ok(json) => json,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        invoice_id = %invoice_id,
                        payment_attempt_id = %payment_attempt_id,
                        "failed to parse psp response; leaving invoice state open"
                    );

                    let body = PayInvoiceResponse {
                        invoice_id,
                        payment_attempt_id,
                        status: "pending".to_string(),
                        psp_ref: None,
                        failure_code: Some("psp_invalid_response".to_string()),
                        message: Some("payment status is pending; retry with same Idempotency-Key".to_string()),
                    };

                    persist_pay_result(&mut tx, idempotency_id, payment_attempt_id, "pending", None, Some("psp_invalid_response"), StatusCode::ACCEPTED, &body).await?;
                    tx.commit().await?;

                    tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, "invoice_service.pay persisted pending result due to invalid PSP response payload");

                    return Ok((StatusCode::ACCEPTED, body));
                }
            };

            let psp_status = payload.get("status").and_then(|value| value.as_str()).unwrap_or("unknown");
            // if success
            if http_status.is_success() && psp_status == "succeeded" {
                let psp_ref = payload.get("psp_ref").and_then(|value| value.as_str()).map(|value| value.to_string());

                if psp_ref.is_none() {
                    let body = PayInvoiceResponse {
                        invoice_id,
                        payment_attempt_id,
                        status: "pending".to_string(),
                        psp_ref: None,
                        failure_code: Some("psp_missing_reference".to_string()),
                        message: Some("payment status is pending; retry with same Idempotency-Key".to_string()),
                    };

                    persist_pay_result(&mut tx, idempotency_id, payment_attempt_id, "pending", None, Some("psp_missing_reference"), StatusCode::ACCEPTED, &body).await?;
                    tx.commit().await?;

                    tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, "invoice_service.pay persisted pending result due to missing PSP reference");

                    return Ok((StatusCode::ACCEPTED, body));
                }

                persist_pay_success(&mut tx, business_id, invoice_id, idempotency_id, payment_attempt_id, psp_ref.clone()).await?;

                let body = PayInvoiceResponse { invoice_id, payment_attempt_id, status: "paid".to_string(), psp_ref, failure_code: None, message: None };

                persist_cached_response(&mut tx, idempotency_id, StatusCode::OK, &body).await?;
                tx.commit().await?;
                tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, "invoice_service.pay succeeded and marked invoice paid");
                return Ok((StatusCode::OK, body));
            }

            // tok_network_error lands here: http_status = 500, psp_status = "failed",
            //   failure_code = "network_error". Attempt marked failed, invoice stays open
            //   (or moves to uncollectible if retries exhausted), invoice.payment_failed fires.
            let failure_code = payload.get("code").and_then(|value| value.as_str()).unwrap_or("payment_failed").to_string();

            let body = PayInvoiceResponse { invoice_id, payment_attempt_id, status: "failed".to_string(), psp_ref: None, failure_code: Some(failure_code.clone()), message: None };

            persist_pay_result(&mut tx, idempotency_id, payment_attempt_id, "failed", None, Some(failure_code.as_str()), http_status, &body).await?;

            let became_uncollectible = mark_invoice_uncollectible_if_retries_exhausted(&mut tx, business_id, invoice_id).await?;
            let invoice_state = if became_uncollectible {
                InvoiceState::Uncollectible.as_str()
            } else {
                InvoiceState::Open.as_str()
            };

            let webhook_payload = serde_json::json!({
                "event_type": "invoice.payment_failed",
                "invoice": {
                    "id": invoice_id,
                    "state": invoice_state,
                },
                "payment_attempt": {
                    "id": payment_attempt_id,
                    "status": "failed",
                    "psp_ref": Value::Null,
                    "failure_code": failure_code,
                },
                "occurred_at": Utc::now(),
            });

            webhooks::enqueue_event_for_business(&mut tx, business_id, invoice_id, "invoice.payment_failed", webhook_payload).await?;
            tx.commit().await?;

            tracing::info!(business_id = %business_id, invoice_id = %invoice_id, payment_attempt_id = %payment_attempt_id, failure_code = %body.failure_code.clone().unwrap_or_else(|| "unknown".to_string()), invoice_state = %invoice_state, "invoice_service.pay persisted failed payment result");

            Ok((http_status, body))
        }
    }
}

/// Fetches a raw invoice row from the DB, scoped to the business. Returns `NotFound` if missing.
async fn query_invoice_row_by_id(db: &PgPool, business_id: Uuid, invoice_id: Uuid) -> Result<InvoiceRow, InvoiceError> {
    let row = sqlx::query(
        r#"
		SELECT id, business_id, customer_id, state, due_date, total_amount_cents, currency, created_at, updated_at
		FROM invoices
		WHERE id = $1 AND business_id = $2
		"#,
    )
    .bind(invoice_id)
    .bind(business_id)
    .fetch_optional(db)
    .await?;

    match row {
        Some(row) => map_invoice_row(row),
        None => Err(InvoiceError::NotFound),
    }
}

/// Maps a raw Postgres row into the typed `InvoiceRow` struct.
fn map_invoice_row(row: sqlx::postgres::PgRow) -> Result<InvoiceRow, InvoiceError> {
    Ok(InvoiceRow {
        id: row.try_get("id")?,
        business_id: row.try_get("business_id")?,
        customer_id: row.try_get("customer_id")?,
        state: row.try_get("state")?,
        due_date: row.try_get("due_date")?,
        total_amount_cents: row.try_get("total_amount_cents")?,
        currency: row.try_get("currency")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Loads all line items for an invoice ordered by insertion time.
async fn query_invoice_line_items(db: &PgPool, invoice_id: Uuid) -> Result<Vec<InvoiceLineItemResponse>, InvoiceError> {
    let rows = sqlx::query(
        r#"
		SELECT id, description, quantity, unit_amount_cents, line_total_cents, created_at
		FROM invoice_line_items
		WHERE invoice_id = $1
		ORDER BY created_at ASC
		"#,
    )
    .bind(invoice_id)
    .fetch_all(db)
    .await?;

    let mut line_items: Vec<InvoiceLineItemResponse> = Vec::new();
    for row in rows {
        line_items.push(InvoiceLineItemResponse {
            id: row.try_get("id")?,
            description: row.try_get("description")?,
            quantity: row.try_get("quantity")?,
            unit_amount_cents: row.try_get("unit_amount_cents")?,
            line_total_cents: row.try_get("line_total_cents")?,
            created_at: row.try_get("created_at")?,
        });
    }

    // tracing::info!(invoice_id = %invoice_id, row_count = line_items.len(), "invoice_service.query_invoice_line_items fetched rows");

    Ok(line_items)
}

/// Acquires a pessimistic row lock on the invoice using `SELECT ... FOR UPDATE`.
/// Any concurrent pay attempt blocks here until this transaction commits or rolls back.
async fn lock_invoice_for_payment(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, invoice_id: Uuid) -> Result<LockedInvoice, InvoiceError> {
    let row = sqlx::query(
        r#"
		SELECT state, total_amount_cents, currency
		FROM invoices
		WHERE id = $1 AND business_id = $2
		FOR UPDATE
		"#,
    )
    .bind(invoice_id)
    .bind(business_id)
    .fetch_optional(&mut **tx)
    .await?;

    match row {
        Some(row) => Ok(LockedInvoice { state: row.try_get("state")?, total_amount_cents: row.try_get("total_amount_cents")?, currency: row.try_get("currency")? }),
        None => Err(InvoiceError::NotFound),
    }
}

/// Looks up an existing idempotency record for the business+key pair, locking the row.
/// Returns `None` if this is the first time the key is seen.
async fn find_idempotency_key(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, key: &str) -> Result<Option<IdempotencyRow>, InvoiceError> {
    let row = sqlx::query(
        r#"
		SELECT id, request_fingerprint, response_status, response_body
		FROM idempotency_keys
		WHERE business_id = $1 AND key = $2
		FOR UPDATE
		"#,
    )
    .bind(business_id)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;

    match row {
        Some(row) => {
            let parsed = IdempotencyRow { id: row.try_get("id")?, request_fingerprint: row.try_get("request_fingerprint")?, response_status: row.try_get("response_status")?, response_body: row.try_get("response_body")? };
            // tracing::info!(business_id = %business_id, idempotency_key = %key, idempotency_id = %parsed.id, "invoice_service.find_idempotency_key hit");
            Ok(Some(parsed))
        }
        None => {
            // tracing::info!(business_id = %business_id, idempotency_key = %key, "invoice_service.find_idempotency_key miss");
            Ok(None)
        }
    }
}

/// Inserts a new idempotency key record. Uses `ON CONFLICT DO NOTHING` so a concurrent
/// request that wins the race inserts first; returns `None` if the row already existed.
async fn insert_idempotency_key(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, key: &str, fingerprint: &str) -> Result<Option<Uuid>, InvoiceError> {
    let row = sqlx::query(
        r#"
		INSERT INTO idempotency_keys (business_id, key, request_fingerprint)
		VALUES ($1, $2, $3)
		ON CONFLICT (business_id, key) DO NOTHING
		RETURNING id
		"#,
    )
    .bind(business_id)
    .bind(key)
    .bind(fingerprint)
    .fetch_optional(&mut **tx)
    .await?;

    match row {
        Some(value) => {
            let inserted_id: Uuid = value.try_get("id")?;
            // tracing::info!(business_id = %business_id, idempotency_key = %key, idempotency_id = %inserted_id, "invoice_service.insert_idempotency_key inserted");
            Ok(Some(inserted_id))
        }
        None => {
            // tracing::info!(business_id = %business_id, idempotency_key = %key, "invoice_service.insert_idempotency_key conflict on existing key");
            Ok(None)
        }
    }
}

/// Creates a new payment attempt row in `pending` status, linked to the invoice and
/// idempotency key. The returned UUID is used as `merchant_reference` in the PSP call.
async fn insert_pending_payment_attempt(tx: &mut Transaction<'_, Postgres>, invoice_id: Uuid, idempotency_key_id: Uuid) -> Result<Uuid, InvoiceError> {
    let row = sqlx::query(
        r#"
		INSERT INTO payment_attempts (invoice_id, idempotency_key_id, status)
		VALUES ($1, $2, 'pending')
		RETURNING id
		"#,
    )
    .bind(invoice_id)
    .bind(idempotency_key_id)
    .fetch_one(&mut **tx)
    .await?;

    let attempt_id: Uuid = row.try_get("id")?;
    // tracing::info!(invoice_id = %invoice_id, idempotency_id = %idempotency_key_id, payment_attempt_id = %attempt_id, "invoice_service.insert_pending_payment_attempt inserted");
    Ok(attempt_id)
}

/// Finds the most recent payment attempt linked to an idempotency key.
/// Used when replaying a still-pending request that has no cached response yet.
async fn find_payment_attempt_id(tx: &mut Transaction<'_, Postgres>, idempotency_key_id: Uuid) -> Result<Option<Uuid>, InvoiceError> {
    let row = sqlx::query(
        r#"
		SELECT id
		FROM payment_attempts
		WHERE idempotency_key_id = $1
		ORDER BY created_at DESC
		LIMIT 1
		"#,
    )
    .bind(idempotency_key_id)
    .fetch_optional(&mut **tx)
    .await?;

    match row {
        Some(row) => Ok(Some(row.try_get("id")?)),
        None => Ok(None),
    }
}

/// Finalises a successful payment: marks the attempt `succeeded`, transitions the invoice
/// to `Paid`, and enqueues an `invoice.paid` webhook — all inside the caller's transaction.
async fn persist_pay_success(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, invoice_id: Uuid, idempotency_key_id: Uuid, payment_attempt_id: Uuid, psp_ref: Option<String>) -> Result<(), InvoiceError> {
    sqlx::query(
        r#"
		UPDATE payment_attempts
		SET status = 'succeeded', psp_ref = $1, failure_code = NULL, finalized_at = NOW()
		WHERE id = $2
		"#,
    )
    .bind(psp_ref.as_ref())
    .bind(payment_attempt_id)
    .execute(&mut **tx)
    .await?;

    transition_state(tx, business_id, invoice_id, InvoiceState::Paid).await?;

    let webhook_payload = serde_json::json!({
        "event_type": "invoice.paid",
        "invoice": {
            "id": invoice_id,
            "state": InvoiceState::Paid.as_str(),
        },
        "payment_attempt": {
            "id": payment_attempt_id,
            "status": "succeeded",
            "psp_ref": psp_ref,
        },
        "occurred_at": Utc::now(),
    });

    webhooks::enqueue_event_for_business(tx, business_id, invoice_id, "invoice.paid", webhook_payload).await?;

    sqlx::query(
        r#"
		UPDATE idempotency_keys
		SET response_status = NULL, response_body = NULL
		WHERE id = $1
		"#,
    )
    .bind(idempotency_key_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Updates the payment attempt to the given status and caches the HTTP response body on
/// the idempotency key row so identical retries get the exact same response replayed.
async fn persist_pay_result(tx: &mut Transaction<'_, Postgres>, idempotency_key_id: Uuid, payment_attempt_id: Uuid, status: &str, psp_ref: Option<String>, failure_code: Option<&str>, http_status: StatusCode, body: &PayInvoiceResponse) -> Result<(), InvoiceError> {
    let finalized_at_sql = if status == "pending" { "NULL" } else { "NOW()" };
    let payment_update_sql = format!("UPDATE payment_attempts SET status = $1, psp_ref = $2, failure_code = $3, finalized_at = {} WHERE id = $4", finalized_at_sql);

    sqlx::query(payment_update_sql.as_str()).bind(status).bind(psp_ref.as_ref()).bind(failure_code).bind(payment_attempt_id).execute(&mut **tx).await?;

    sqlx::query(
        r#"
		UPDATE idempotency_keys
		SET response_status = $1, response_body = $2
		WHERE id = $3
		"#,
    )
    .bind(i32::from(http_status.as_u16()))
    .bind(serde_json::to_value(body).map_err(|_| InvoiceError::InvalidCachedResponse)?)
    .bind(idempotency_key_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Writes the final HTTP status and response body onto the idempotency key row so any
/// future retry with the same key gets the identical response without re-processing.
async fn persist_cached_response(tx: &mut Transaction<'_, Postgres>, idempotency_key_id: Uuid, http_status: StatusCode, body: &PayInvoiceResponse) -> Result<(), InvoiceError> {
    sqlx::query(
        r#"
		UPDATE idempotency_keys
		SET response_status = $1, response_body = $2
		WHERE id = $3
		"#,
    )
    .bind(i32::from(http_status.as_u16()))
    .bind(serde_json::to_value(body).map_err(|_| InvoiceError::InvalidCachedResponse)?)
    .bind(idempotency_key_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// Counts failed payment attempts for the invoice. If the count has reached
/// `MAX_PAYMENT_RETRY_ATTEMPTS`, transitions the invoice to `Uncollectible` and returns `true`;
/// otherwise leaves the invoice in `Open` and returns `false`.
async fn mark_invoice_uncollectible_if_retries_exhausted(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, invoice_id: Uuid) -> Result<bool, InvoiceError> {
    let failed_attempt_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM payment_attempts
        WHERE invoice_id = $1
          AND status = 'failed'
        "#,
    )
    .bind(invoice_id)
    .fetch_one(&mut **tx)
    .await?;

    if failed_attempt_count < MAX_PAYMENT_RETRY_ATTEMPTS {
        return Ok(false);
    }

    transition_state(tx, business_id, invoice_id, InvoiceState::Uncollectible).await?;
    tracing::info!(
        business_id = %business_id,
        invoice_id = %invoice_id,
        failed_attempt_count = failed_attempt_count,
        max_payment_retry_attempts = MAX_PAYMENT_RETRY_ATTEMPTS,
        "invoice_service.mark_invoice_uncollectible_if_retries_exhausted moved invoice to uncollectible"
    );
    Ok(true)
}

/// Replays a previously seen request. First checks the fingerprint matches — if it doesn't,
/// the caller is reusing the key with a different payload, so return `IdempotencyKeyMismatch`.
/// If a cached response exists, return it directly. If the request is still in-flight
/// (no cached response yet), return 202 with a `processing` status so the caller retries.
async fn return_existing_idempotent_response(mut tx: Transaction<'_, Postgres>, invoice_id: Uuid, request_fingerprint: &str, row: IdempotencyRow) -> Result<(StatusCode, PayInvoiceResponse), InvoiceError> {
    if row.request_fingerprint != request_fingerprint {
        tx.rollback().await?;
        return Err(InvoiceError::IdempotencyKeyMismatch);
    }

    if let (Some(raw_status), Some(raw_body)) = (row.response_status, row.response_body) {
        let status = match u16::try_from(raw_status).ok().and_then(|code| StatusCode::from_u16(code).ok()) {
            Some(value) => value,
            None => {
                tx.rollback().await?;
                return Err(InvoiceError::InvalidCachedResponse);
            }
        };

        let body: PayInvoiceResponse = match serde_json::from_value(raw_body) {
            Ok(parsed) => parsed,
            Err(_) => {
                tx.rollback().await?;
                return Err(InvoiceError::InvalidCachedResponse);
            }
        };

        tx.rollback().await?;
        // tracing::info!(invoice_id = %invoice_id, idempotency_id = %row.id, returned_status = %status, "invoice_service.return_existing_idempotent_response returned cached response");
        return Ok((status, body));
    }

    let payment_attempt_id = match find_payment_attempt_id(&mut tx, row.id).await? {
        Some(found) => found,
        None => {
            tx.rollback().await?;
            return Err(InvoiceError::InvalidCachedResponse);
        }
    };

    let body = PayInvoiceResponse {
        invoice_id,
        payment_attempt_id,
        status: "pending".to_string(),
        psp_ref: None,
        failure_code: Some("processing".to_string()),
        message: Some("payment request is still processing; retry with same Idempotency-Key".to_string()),
    };

    tx.rollback().await?;
    // tracing::info!(invoice_id = %invoice_id, idempotency_id = %row.id, payment_attempt_id = %payment_attempt_id, "invoice_service.return_existing_idempotent_response returned pending processing response");
    Ok((StatusCode::ACCEPTED, body))
}
