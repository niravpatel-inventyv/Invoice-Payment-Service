use axum::{
    Json,
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use crate::middleware::{AppState, AuthenticatedBusiness};

#[derive(Debug, Deserialize)]
pub struct CreateWebhookEndpointRequest {
    pub url: String,
}

#[derive(Debug, Serialize)]
pub struct WebhookEndpointResponse {
    pub id: Uuid,
    pub business_id: Uuid,
    pub url: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub signing_secret: String,
}

pub async fn create_endpoint(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Json(body): Json<CreateWebhookEndpointRequest>) -> Response {
    let normalized_url = body.url.trim().to_string();

    if normalized_url.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "url must not be empty"}))).into_response();
    }

    match reqwest::Url::parse(normalized_url.as_str()) {
        Ok(parsed) => {
            let scheme = parsed.scheme();
            if scheme != "http" && scheme != "https" {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": "url must start with http:// or https://"}))).into_response();
            }
        }
        Err(_) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "url must be a valid absolute URL"}))).into_response();
        }
    }

    let signing_secret = generate_signing_secret();
    let secret_hash = crate::auth::hash_api_key(signing_secret.as_str(), state.api_key_pepper.as_deref());

    let inserted = sqlx::query(
        r#"
        INSERT INTO webhook_endpoints (business_id, url, secret_hash, signing_secret, active)
        VALUES ($1, $2, $3, $4, TRUE)
        RETURNING id, business_id, url, active, created_at
        "#,
    )
    .bind(auth.business_id)
    .bind(normalized_url)
    .bind(secret_hash)
    .bind(signing_secret.as_str())
    .fetch_one(&state.db)
    .await;

    match inserted {
        Ok(row) => {
            let id = match row.try_get::<Uuid, _>("id") {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(error = %err, business_id = %auth.business_id, "failed to decode webhook endpoint id");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response();
                }
            };
            let business_id = match row.try_get::<Uuid, _>("business_id") {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(error = %err, business_id = %auth.business_id, endpoint_id = %id, "failed to decode webhook endpoint business_id");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response();
                }
            };
            let url = match row.try_get::<String, _>("url") {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(error = %err, business_id = %auth.business_id, endpoint_id = %id, "failed to decode webhook endpoint url");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response();
                }
            };
            let active = match row.try_get::<bool, _>("active") {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(error = %err, business_id = %auth.business_id, endpoint_id = %id, "failed to decode webhook endpoint active flag");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response();
                }
            };
            let created_at = match row.try_get::<DateTime<Utc>, _>("created_at") {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(error = %err, business_id = %auth.business_id, endpoint_id = %id, "failed to decode webhook endpoint created_at");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response();
                }
            };

            (
                StatusCode::CREATED,
                Json(WebhookEndpointResponse {
                    id,
                    business_id,
                    url,
                    active,
                    created_at,
                    signing_secret,
                }),
            )
                .into_response()
        }
        Err(err) => {
            tracing::error!(error = %err, business_id = %auth.business_id, "create_webhook_endpoint failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}

pub async fn enqueue_event_for_business(tx: &mut Transaction<'_, Postgres>, business_id: Uuid, invoice_id: Uuid, event_type: &str, payload_json: Value) -> Result<(), sqlx::Error> {
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

fn generate_signing_secret() -> String {
    let mut secret_bytes = [0_u8; 32];
    let mut rng = rand::rngs::OsRng;
    rng.fill_bytes(&mut secret_bytes);
    format!("whsec_{}", hex::encode(secret_bytes))
}