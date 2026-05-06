use axum::{
    Json, Router, middleware,
    routing::{get, post},
};
use serde_json::json;

use crate::customer;
use crate::invoices;
use crate::middleware::{AppState, auth_middleware};
use crate::webhooks;

pub fn routes(state: AppState) -> Router {
    // Public routes — no auth required.
    let public = Router::new().route("/healthCheck", get(health_check));

    // Protected routes — require a valid API key.
    // AuthenticatedBusiness is injected into extensions by auth_middleware
    // and extracted by every handler via Extension<AuthenticatedBusiness>.
    let protected = Router::new()
        .route("/customers", post(customer::create).get(customer::list))
        .route("/customers/:id", get(customer::get_by_id))
        .route("/invoices", post(invoices::create).get(invoices::list))
        .route("/invoices/:id", get(invoices::get_by_id))
        .route("/invoices/:id/pay", post(invoices::pay))
        .route("/webhooks/endpoints", post(webhooks::create_endpoint))
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    Router::new().merge(public).merge(protected).with_state(state)
}

async fn health_check() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "invoice-api"
    }))
}
