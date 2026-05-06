pub mod model;
pub mod service;

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use uuid::Uuid;

use crate::middleware::{AppState, AuthenticatedBusiness};
use model::CreateCustomerRequest;
use service::CustomerError;

/// `POST /customers`
pub async fn create(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Json(body): Json<CreateCustomerRequest>) -> Response {
    // tracing::info!(business_id = %auth.business_id, "create_customer handler started");
    let normalized = body.normalize();

    if let Err(validation) = normalized.validate() {
        tracing::warn!(
            business_id = %auth.business_id,
            field_errors = ?validation.fields,
            "create_customer rejected due to invalid request payload"
        );
        return (StatusCode::BAD_REQUEST, Json(json!(validation))).into_response();
    }

    match service::create(&state.db, auth.business_id, normalized).await {
        Ok(customer) => {
            // tracing::info!(business_id = %auth.business_id, customer_id = %customer.id, "create_customer handler succeeded");
            (StatusCode::CREATED, Json(customer)).into_response()
        }
        Err(CustomerError::DuplicateEmail) => {
            // tracing::info!(business_id = %auth.business_id, "create_customer handler duplicate email conflict");
            (StatusCode::CONFLICT, Json(json!({"error": "a customer with this email already exists"}))).into_response()
        }
        Err(e) => {
            tracing::error!("create_customer error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}

/// `GET /customers/:id`
pub async fn get_by_id(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>, Path(id): Path<Uuid>) -> Response {
    // tracing::info!(business_id = %auth.business_id, customer_id = %id, "get_customer handler started");
    match service::get_by_id(&state.db, auth.business_id, id).await {
        Ok(customer) => {
            // tracing::info!(business_id = %auth.business_id, customer_id = %customer.id, "get_customer handler succeeded");
            Json(customer).into_response()
        }
        Err(CustomerError::NotFound) => {
            // tracing::info!(business_id = %auth.business_id, customer_id = %id, "get_customer handler not found");
            (StatusCode::NOT_FOUND, Json(json!({"error": "customer not found"}))).into_response()
        }
        Err(e) => {
            tracing::error!("get_customer error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}

/// `GET /customers`
pub async fn list(State(state): State<AppState>, Extension(auth): Extension<AuthenticatedBusiness>) -> Response {
    // tracing::info!(business_id = %auth.business_id, "list_customers handler started");
    match service::list(&state.db, auth.business_id).await {
        Ok(customers) => {
            // tracing::info!(business_id = %auth.business_id, customer_count = customers.len(), "list_customers handler succeeded");
            Json(customers).into_response()
        }
        Err(e) => {
            tracing::error!("list_customers error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal server error"}))).into_response()
        }
    }
}
