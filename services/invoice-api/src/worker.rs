use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use uuid::Uuid;

const DELIVERY_LEASE_SECONDS: i64 = 30;
const WEBHOOK_RETRY_DELAYS_SECONDS: [i64; 6] = [30, 120, 600, 1800, 3600, 7200];
const MAX_DELIVERY_ATTEMPTS: i32 = WEBHOOK_RETRY_DELAYS_SECONDS.len() as i32 + 1;

type HmacSha256 = Hmac<sha2::Sha256>;

#[derive(Debug, Clone)]
pub struct WorkerSettings {
    pub psp_base_url: String,
    pub poll_interval_ms: u64,
    pub pending_payment_timeout_seconds: i64,
    pub max_payment_retry_attempts: i64,
}

impl WorkerSettings {
    pub fn from_env() -> Self {
        let psp_base_url = std::env::var("PSP_BASE_URL").unwrap_or_else(|_| "http://localhost:8081".to_string());

        let poll_interval_ms = match std::env::var("WORKER_POLL_INTERVAL_MS") {
            Ok(raw) => match raw.parse::<u64>() {
                Ok(parsed) => parsed,
                Err(err) => {
                    warn!(error = %err, raw = %raw, "invalid WORKER_POLL_INTERVAL_MS; defaulting to 2000");
                    2000
                }
            },
            Err(_) => 2000,
        };

        let pending_payment_timeout_seconds = match std::env::var("WORKER_PENDING_PAYMENT_TIMEOUT_SECONDS") {
            Ok(raw) => match raw.parse::<i64>() {
                Ok(parsed) if parsed > 0 => parsed,
                Ok(_) => {
                    warn!(raw = %raw, "WORKER_PENDING_PAYMENT_TIMEOUT_SECONDS must be > 0; defaulting to 35");
                    35
                }
                Err(err) => {
                    warn!(error = %err, raw = %raw, "invalid WORKER_PENDING_PAYMENT_TIMEOUT_SECONDS; defaulting to 35");
                    35
                }
            },
            Err(_) => 35,
        };

        let max_payment_retry_attempts = match std::env::var("WORKER_MAX_PAYMENT_RETRY_ATTEMPTS") {
            Ok(raw) => match raw.parse::<i64>() {
                Ok(parsed) if parsed > 0 => parsed,
                Ok(_) => {
                    warn!(raw = %raw, "WORKER_MAX_PAYMENT_RETRY_ATTEMPTS must be > 0; defaulting to 3");
                    3
                }
                Err(err) => {
                    warn!(error = %err, raw = %raw, "invalid WORKER_MAX_PAYMENT_RETRY_ATTEMPTS; defaulting to 3");
                    3
                }
            },
            Err(_) => 3,
        };

        Self {
            psp_base_url,
            poll_interval_ms,
            pending_payment_timeout_seconds,
            max_payment_retry_attempts,
        }
    }
}

struct ClaimedDelivery {
    id: Uuid,
    invoice_id: Uuid,
    endpoint_url: String,
    signing_secret: String,
    event_type: String,
    payload_json: Value,
    attempt_count: i32,
}

struct ClaimedPendingPayment {
    payment_attempt_id: Uuid,
    invoice_id: Uuid,
    business_id: Uuid,
    idempotency_key_id: Option<Uuid>,
}

pub async fn run(pool: sqlx::PgPool, settings: WorkerSettings) -> anyhow::Result<()> {
    let interval = Duration::from_millis(settings.poll_interval_ms);
    let http_client = reqwest::Client::builder().timeout(StdDuration::from_secs(10)).build()?;

    info!(
        psp_base_url = %settings.psp_base_url,
        poll_ms = settings.poll_interval_ms,
        pending_payment_timeout_seconds = settings.pending_payment_timeout_seconds,
        max_payment_retry_attempts = settings.max_payment_retry_attempts,
        "worker loop started"
    );

    loop {
        match reconcile_stale_pending_payment(
            &pool,
            settings.pending_payment_timeout_seconds,
            settings.max_payment_retry_attempts,
        )
        .await
        {
            Ok(true) => {
                continue;
            }
            Ok(false) => {}
            Err(err) => {
                warn!(error = %err, "failed to reconcile stale pending payment");
            }
        }

        match claim_due_delivery(&pool).await {
            Ok(Some(delivery)) => {
                info!(
                    delivery_id = %delivery.id,
                    invoice_id = %delivery.invoice_id,
                    event_type = %delivery.event_type,
                    attempt_count = delivery.attempt_count,
                    "worker picked pending webhook job"
                );

                if let Err(err) = deliver_webhook(&pool, &http_client, delivery).await {
                    warn!(error = %err, "webhook delivery processing failed");
                }
                continue;
            }
            Ok(None) => {}
            Err(err) => {
                warn!(error = %err, "failed to claim pending webhook delivery");
            }
        }

        sleep(interval).await;
    }
}

async fn reconcile_stale_pending_payment(
    pool: &sqlx::PgPool,
    timeout_seconds: i64,
    max_payment_retry_attempts: i64,
) -> Result<bool, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    let maybe_payment = claim_stale_pending_payment(&mut tx, timeout_seconds).await?;
    let payment = match maybe_payment {
        Some(value) => value,
        None => {
            tx.rollback().await?;
            return Ok(false);
        }
    };

    let affected_rows = sqlx::query(
        r#"
        UPDATE payment_attempts
        SET status = 'failed',
            failure_code = 'processing_timeout',
            psp_ref = NULL,
            finalized_at = NOW()
        WHERE id = $1 AND status = 'pending'
        "#,
    )
    .bind(payment.payment_attempt_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if affected_rows == 0 {
        tx.rollback().await?;
        return Ok(false);
    }

    if let Some(idempotency_key_id) = payment.idempotency_key_id {
        let body = json!({
            "invoice_id": payment.invoice_id,
            "payment_attempt_id": payment.payment_attempt_id,
            "status": "failed",
            "psp_ref": Value::Null,
            "failure_code": "processing_timeout",
            "message": "payment processing timed out and was finalized as failed"
        });

        sqlx::query(
            r#"
            UPDATE idempotency_keys
            SET response_status = $1,
                response_body = $2
            WHERE id = $3
            "#,
        )
        .bind(402_i32)
        .bind(body)
        .bind(idempotency_key_id)
        .execute(&mut *tx)
        .await?;
    }

    let became_uncollectible = mark_invoice_uncollectible_if_retries_exhausted(
        &mut tx,
        payment.business_id,
        payment.invoice_id,
        max_payment_retry_attempts,
    )
    .await?;

    let invoice_state = if became_uncollectible { "uncollectible" } else { "open" };

    let webhook_payload = json!({
        "event_type": "invoice.payment_failed",
        "invoice": {
            "id": payment.invoice_id,
            "state": invoice_state
        },
        "payment_attempt": {
            "id": payment.payment_attempt_id,
            "status": "failed",
            "psp_ref": Value::Null,
            "failure_code": "processing_timeout"
        },
        "occurred_at": Utc::now(),
    });

    enqueue_event_for_business(
        &mut tx,
        payment.business_id,
        payment.invoice_id,
        "invoice.payment_failed",
        webhook_payload,
    )
    .await?;

    tx.commit().await?;

    info!(
        payment_attempt_id = %payment.payment_attempt_id,
        invoice_id = %payment.invoice_id,
        business_id = %payment.business_id,
        failure_code = "processing_timeout",
        invoice_state = %invoice_state,
        "worker marked stale pending payment as failed"
    );

    Ok(true)
}

async fn mark_invoice_uncollectible_if_retries_exhausted(
    tx: &mut Transaction<'_, Postgres>,
    business_id: Uuid,
    invoice_id: Uuid,
    max_payment_retry_attempts: i64,
) -> Result<bool, sqlx::Error> {
    let failed_count: i64 = sqlx::query_scalar(
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

    if failed_count < max_payment_retry_attempts {
        return Ok(false);
    }

    let rows_affected = sqlx::query(
        r#"
        UPDATE invoices
        SET state = 'uncollectible',
            updated_at = NOW()
        WHERE id = $1
          AND business_id = $2
          AND state = 'open'
        "#,
    )
    .bind(invoice_id)
    .bind(business_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    if rows_affected > 0 {
        info!(
            business_id = %business_id,
            invoice_id = %invoice_id,
            failed_attempt_count = failed_count,
            max_payment_retry_attempts = max_payment_retry_attempts,
            "worker moved invoice to uncollectible after max payment retries"
        );
    }

    Ok(rows_affected > 0)
}

async fn claim_stale_pending_payment(tx: &mut Transaction<'_, Postgres>, timeout_seconds: i64) -> Result<Option<ClaimedPendingPayment>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT pa.id AS payment_attempt_id,
               pa.invoice_id,
               pa.idempotency_key_id,
               i.business_id
        FROM payment_attempts pa
        JOIN invoices i ON i.id = pa.invoice_id
        WHERE pa.status = 'pending'
          AND pa.created_at <= NOW() - ($1 || ' seconds')::interval
        ORDER BY pa.created_at ASC
        FOR UPDATE OF pa SKIP LOCKED
        LIMIT 1
        "#,
    )
    .bind(timeout_seconds)
    .fetch_optional(&mut **tx)
    .await?;

    match row {
        Some(row) => Ok(Some(ClaimedPendingPayment {
            payment_attempt_id: row.try_get::<Uuid, _>("payment_attempt_id")?,
            invoice_id: row.try_get::<Uuid, _>("invoice_id")?,
            business_id: row.try_get::<Uuid, _>("business_id")?,
            idempotency_key_id: row.try_get::<Option<Uuid>, _>("idempotency_key_id")?,
        })),
        None => Ok(None),
    }
}

async fn enqueue_event_for_business(
    tx: &mut Transaction<'_, Postgres>,
    business_id: Uuid,
    invoice_id: Uuid,
    event_type: &str,
    payload_json: Value,
) -> Result<(), sqlx::Error> {
    let endpoint_rows = sqlx::query(
        r#"
        SELECT id
        FROM webhook_endpoints
        WHERE business_id = $1
          AND active = TRUE
        "#,
    )
    .bind(business_id)
    .fetch_all(&mut **tx)
    .await?;

    for endpoint_row in endpoint_rows {
        let endpoint_id = endpoint_row.try_get::<Uuid, _>("id")?;
        sqlx::query(
            r#"
            INSERT INTO webhook_deliveries (endpoint_id, invoice_id, event_type, payload_json, status, attempt_count, next_attempt_at)
            VALUES ($1, $2, $3, $4, 'pending', 0, NOW())
            "#,
        )
        .bind(endpoint_id)
        .bind(invoice_id)
        .bind(event_type)
        .bind(payload_json.clone())
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn claim_due_delivery(pool: &sqlx::PgPool) -> Result<Option<ClaimedDelivery>, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    let row = sqlx::query(
        r#"
        SELECT d.id, d.invoice_id, d.event_type, d.payload_json, d.attempt_count, e.url, e.signing_secret
        FROM webhook_deliveries d
        JOIN webhook_endpoints e ON e.id = d.endpoint_id
        WHERE d.status = 'pending'
          AND e.active = TRUE
          AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= NOW())
        ORDER BY d.created_at ASC
        FOR UPDATE OF d SKIP LOCKED
        LIMIT 1
        "#,
    )
    .fetch_optional(&mut *tx)
    .await?;

    let row = match row {
        Some(value) => value,
        None => {
            tx.rollback().await?;
            return Ok(None);
        }
    };

    let delivery_id = row.try_get::<Uuid, _>("id")?;

    sqlx::query(
        r#"
        UPDATE webhook_deliveries
        SET next_attempt_at = NOW() + ($2 || ' seconds')::interval
        WHERE id = $1
        "#,
    )
    .bind(delivery_id)
    .bind(DELIVERY_LEASE_SECONDS)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(Some(ClaimedDelivery {
        id: delivery_id,
        invoice_id: row.try_get::<Uuid, _>("invoice_id")?,
        endpoint_url: row.try_get::<String, _>("url")?,
        signing_secret: row.try_get::<String, _>("signing_secret")?,
        event_type: row.try_get::<String, _>("event_type")?,
        payload_json: row.try_get::<Value, _>("payload_json")?,
        attempt_count: row.try_get::<i32, _>("attempt_count")?,
    }))
}

async fn deliver_webhook(pool: &sqlx::PgPool, http_client: &reqwest::Client, delivery: ClaimedDelivery) -> Result<(), sqlx::Error> {
    info!(
        delivery_id = %delivery.id,
        invoice_id = %delivery.invoice_id,
        event_type = %delivery.event_type,
        next_attempt_number = delivery.attempt_count + 1,
        endpoint_url = %delivery.endpoint_url,
        "webhook delivery attempt started"
    );

    let body = match serde_json::to_vec(&delivery.payload_json) {
        Ok(value) => value,
        Err(err) => {
            let message = format!("payload_serialization_error: {err}");
            finalize_failure(pool, &delivery, message.as_str()).await?;
            return Ok(());
        }
    };

    let timestamp = current_unix_timestamp();
    let signature = sign_payload(delivery.signing_secret.as_str(), timestamp, body.as_slice());

    let request = http_client
        .post(delivery.endpoint_url.as_str())
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .header(HeaderName::from_static("x-webhook-id"), header_value_from_string(delivery.id.to_string()))
        .header(HeaderName::from_static("x-webhook-timestamp"), header_value_from_string(timestamp.to_string()))
        .header(HeaderName::from_static("x-webhook-signature"), header_value_from_string(signature))
        .header(HeaderName::from_static("x-webhook-event"), header_value_from_string(delivery.event_type.clone()))
        .body(body);

    let response = request.send().await;

    match response {
        Ok(result) if result.status().is_success() => {
            sqlx::query(
                r#"
                UPDATE webhook_deliveries
                SET status = 'delivered',
                    attempt_count = $2,
                    next_attempt_at = NULL,
                    last_error = NULL,
                    delivered_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(delivery.id)
            .bind(delivery.attempt_count + 1)
            .execute(pool)
            .await?;

            info!(
                delivery_id = %delivery.id,
                invoice_id = %delivery.invoice_id,
                event_type = %delivery.event_type,
                new_status = "delivered",
                updated_attempt_count = delivery.attempt_count + 1,
                "webhook job status updated"
            );
        }
        Ok(result) => {
            info!(
                delivery_id = %delivery.id,
                invoice_id = %delivery.invoice_id,
                event_type = %delivery.event_type,
                receiver_status = %result.status().as_u16(),
                "webhook receiver returned non-success status"
            );
            let message = format!("receiver_http_status:{}", result.status().as_u16());
            finalize_failure(pool, &delivery, message.as_str()).await?;
        }
        Err(err) => {
            info!(
                delivery_id = %delivery.id,
                invoice_id = %delivery.invoice_id,
                event_type = %delivery.event_type,
                error = %err,
                "webhook delivery request failed before receiver response"
            );
            let message = format!("delivery_error:{err}");
            finalize_failure(pool, &delivery, message.as_str()).await?;
        }
    }

    Ok(())
}

async fn finalize_failure(pool: &sqlx::PgPool, delivery: &ClaimedDelivery, error_message: &str) -> Result<(), sqlx::Error> {
    let updated_attempt_count = delivery.attempt_count + 1;

    let retry_delay_seconds = match retry_delay_seconds_after_attempt(updated_attempt_count) {
        Some(value) => value,
        None => {
            sqlx::query(
                r#"
                UPDATE webhook_deliveries
                SET status = 'dead',
                    attempt_count = $2,
                    next_attempt_at = NULL,
                    last_error = $3
                WHERE id = $1
                "#,
            )
            .bind(delivery.id)
            .bind(updated_attempt_count)
            .bind(error_message)
            .execute(pool)
            .await?;

            info!(
                delivery_id = %delivery.id,
                invoice_id = %delivery.invoice_id,
                event_type = %delivery.event_type,
                new_status = "dead",
                updated_attempt_count = updated_attempt_count,
                max_delivery_attempts = MAX_DELIVERY_ATTEMPTS,
                last_error = %error_message,
                "webhook job status updated"
            );

            warn!(
                delivery_id = %delivery.id,
                invoice_id = %delivery.invoice_id,
                event_type = %delivery.event_type,
                attempts = updated_attempt_count,
                max_delivery_attempts = MAX_DELIVERY_ATTEMPTS,
                last_error = %error_message,
                "webhook delivery exhausted retry budget"
            );
            return Ok(());
        }
    };
    sqlx::query(
        r#"
        UPDATE webhook_deliveries
        SET status = 'pending',
            attempt_count = $2,
            next_attempt_at = NOW() + ($3 || ' seconds')::interval,
            last_error = $4
        WHERE id = $1
        "#,
    )
    .bind(delivery.id)
    .bind(updated_attempt_count)
    .bind(retry_delay_seconds)
    .bind(error_message)
    .execute(pool)
    .await?;

    info!(
        delivery_id = %delivery.id,
        invoice_id = %delivery.invoice_id,
        event_type = %delivery.event_type,
        new_status = "pending",
        updated_attempt_count = updated_attempt_count,
        retry_delay_seconds = retry_delay_seconds,
        last_error = %error_message,
        "webhook job status updated"
    );

    warn!(delivery_id = %delivery.id, invoice_id = %delivery.invoice_id, event_type = %delivery.event_type, attempts = updated_attempt_count, retry_delay_seconds = retry_delay_seconds, last_error = %error_message, "webhook delivery scheduled for retry");
    Ok(())
}

fn retry_delay_seconds_after_attempt(updated_attempt_count: i32) -> Option<i64> {
    if updated_attempt_count <= 0 {
        return None;
    }

    WEBHOOK_RETRY_DELAYS_SECONDS.get((updated_attempt_count - 1) as usize).copied()
}

fn sign_payload(signing_secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac = match HmacSha256::new_from_slice(signing_secret.as_bytes()) {
        Ok(value) => value,
        Err(_) => {
            return String::new();
        }
    };

    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

fn header_value_from_string(value: String) -> HeaderValue {
    match HeaderValue::from_str(value.as_str()) {
        Ok(header) => header,
        Err(_) => HeaderValue::from_static("invalid"),
    }
}

fn current_unix_timestamp() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_DELIVERY_ATTEMPTS, retry_delay_seconds_after_attempt};

    #[test]
    fn webhook_retry_schedule_covers_about_four_hours() {
        assert_eq!(MAX_DELIVERY_ATTEMPTS, 7);
        assert_eq!(retry_delay_seconds_after_attempt(1), Some(30));
        assert_eq!(retry_delay_seconds_after_attempt(2), Some(120));
        assert_eq!(retry_delay_seconds_after_attempt(3), Some(600));
        assert_eq!(retry_delay_seconds_after_attempt(4), Some(1800));
        assert_eq!(retry_delay_seconds_after_attempt(5), Some(3600));
        assert_eq!(retry_delay_seconds_after_attempt(6), Some(7200));
        assert_eq!(retry_delay_seconds_after_attempt(7), None);
    }
}
