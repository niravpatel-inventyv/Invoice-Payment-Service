use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    responses_by_reference: Arc<Mutex<HashMap<String, StoredResponse>>>,
}

#[derive(Clone)]
struct StoredResponse {
    status: StatusCode,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct PaymentRequest {
    card_token: String,
    amount_cents: i64,
    currency: String,
    merchant_reference: Option<String>,
}

#[derive(Debug, Serialize)]
struct PaymentSuccess {
    status: &'static str,
    psp_ref: String,
}

#[derive(Debug, Serialize)]
struct PaymentFailure {
    status: &'static str,
    code: &'static str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
      dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let bind_addr = match std::env::var("PSP_BIND_ADDR") {
        Ok(addr) => addr,
        Err(_) => "0.0.0.0:8081".to_string(),
    };
    let addr: SocketAddr = bind_addr.parse()?;

    let state = AppState {
        responses_by_reference: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/payments", post(create_payment))
        .with_state(state);

    info!(%addr, "starting mock-psp");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"service": "mock-psp", "status": "ok"}))
}

async fn create_payment(
    State(state): State<AppState>,
    Json(payload): Json<PaymentRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let _ = (payload.amount_cents, payload.currency);

    if let Some(reference) = payload.merchant_reference.as_ref() {
        let cached = {
            match state.responses_by_reference.lock() {
                Ok(map) => map.get(reference).cloned(),
                Err(_) => None,
            }
        };

        if let Some(stored) = cached {
            return (stored.status, Json(stored.payload));
        }
    }

    let (status, response_body) = match payload.card_token.as_str() {
        "tok_success" => {
            sleep(Duration::from_millis(100)).await;
            let body = serde_json::json!(PaymentSuccess {
                status: "succeeded",
                psp_ref: Uuid::new_v4().to_string(),
            });
            (StatusCode::OK, body)
        }
        "tok_insufficient_funds" => {
            sleep(Duration::from_millis(100)).await;
            let body = serde_json::json!(PaymentFailure {
                status: "failed",
                code: "insufficient_funds",
            });
            (StatusCode::PAYMENT_REQUIRED, body)
        }
        "tok_card_declined" => {
            sleep(Duration::from_millis(100)).await;
            let body = serde_json::json!(PaymentFailure {
                status: "failed",
                code: "card_declined",
            });
            (StatusCode::PAYMENT_REQUIRED, body)
        }
        "tok_timeout" => {
            sleep(Duration::from_secs(30)).await;
            let body = serde_json::json!(PaymentSuccess {
                status: "succeeded",
                psp_ref: Uuid::new_v4().to_string(),
            });
            (StatusCode::OK, body)
        }
        "tok_network_error" => (
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({
                "status": "failed",
                "code": "network_error"
            }),
        ),
        _ => (
            StatusCode::BAD_REQUEST,
            serde_json::json!({
                "status": "failed",
                "code": "invalid_token"
            }),
        ),
    };

    if let Some(reference) = payload.merchant_reference {
        if let Ok(mut map) = state.responses_by_reference.lock() {
            map.insert(
                reference,
                StoredResponse {
                    status,
                    payload: response_body.clone(),
                },
            );
        }
    }

    (status, Json(response_body))
}
