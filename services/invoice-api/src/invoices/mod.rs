pub mod model;
pub mod service;

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::HeaderMap,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use uuid::Uuid;

use crate::middleware::{AppState, AuthenticatedBusiness};
use model::{CreateInvoiceRequest, ListInvoicesQuery, PayInvoiceRequest};
use service::InvoiceError;

pub async fn create(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Json(body): Json<CreateInvoiceRequest>) -> Response {
    let normalized = body.normalize();

    if let Err(validation) = normalized.validate() {
        tracing::warn!(
            business_id = %auth.business_id,
            field_errors = ?validation.fields,
            "create_invoice rejected due to invalid request payload"
        );
        return (StatusCode::BAD_REQUEST, Json(json!(validation))).into_response();
    }

    match service::create(&state.db, auth.business_id, normalized).await {
        Ok(invoice) => (StatusCode::CREATED, Json(invoice)).into_response(),
        Err(InvoiceError::CustomerNotFound) => (StatusCode::NOT_FOUND, Json(json!({"error": "customer not found for this business"}))).into_response(),
        Err(InvoiceError::MoneyOverflow) => (StatusCode::BAD_REQUEST, Json(json!({"error": "invoice total overflow"}))).into_response(),
        Err(err) => {
            tracing::error!(error = %err, business_id = %auth.business_id, "create_invoice failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}

pub async fn get_by_id(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Path(invoice_id): Path<Uuid>) -> Response {
    match service::get_by_id(&state.db, auth.business_id, invoice_id).await {
        Ok(invoice) => Json(invoice).into_response(),
        Err(InvoiceError::NotFound) => (StatusCode::NOT_FOUND, Json(json!({"error": "invoice not found"}))).into_response(),
        Err(err) => {
            tracing::error!(error = %err, business_id = %auth.business_id, invoice_id = %invoice_id, "get_invoice failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}

pub async fn list(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Query(query): Query<ListInvoicesQuery>) -> Response {
    match service::list(&state.db, auth.business_id, query.state).await {
        Ok(invoices) => Json(invoices).into_response(),
        Err(InvoiceError::InvalidStateFilter) => (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid invoice state filter"}))).into_response(),
        Err(err) => {
            tracing::error!(error = %err, business_id = %auth.business_id, "list_invoices failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}

pub async fn pay(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Path(invoice_id): Path<Uuid>, headers: HeaderMap, Json(body): Json<PayInvoiceRequest>) -> Response {
    let idempotency_key = headers.get("Idempotency-Key").and_then(|value| value.to_str().ok()).map(|value| value.trim().to_string());

    let idempotency_key = match idempotency_key {
        Some(key) if !key.is_empty() => key,
        _ => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "Idempotency-Key header is required"}))).into_response();
        }
    };

    if body.card_token.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "card_token must not be empty"}))).into_response();
    }

    match service::pay(&state.db, auth.business_id, invoice_id, idempotency_key, body, state.psp_base_url.as_str(), &state.http_client).await {
        Ok((status, payload)) => (status, Json(payload)).into_response(),
        Err(InvoiceError::NotFound) => (StatusCode::NOT_FOUND, Json(json!({"error": "invoice not found"}))).into_response(),
        Err(InvoiceError::InvoiceNotPayable { state }) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "invoice is not payable",
                "state": state
            })),
        )
            .into_response(),
        Err(InvoiceError::IdempotencyKeyMismatch) => (StatusCode::CONFLICT, Json(json!({"error": "idempotency key reused with different payload"}))).into_response(),
        Err(err) => {
            tracing::error!(
                error = %err,
                business_id = %auth.business_id,
                invoice_id = %invoice_id,
                "pay_invoice failed"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}
