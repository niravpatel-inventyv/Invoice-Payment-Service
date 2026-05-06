use axum::{
    Json,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::{self, AuthError};

/// Injected into request extensions after successful authentication.
/// All downstream handlers extract this to scope queries to one business.
#[derive(Clone, Debug)]
pub struct AuthenticatedBusiness {
    pub business_id: Uuid,
}

/// Shared application state threaded through axum via `State<AppState>`.
#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub psp_base_url: String,
    pub http_client: reqwest::Client,
    pub api_key_pepper: Option<String>,
}

/// API-key authentication middleware.
///
/// Expects:  `Authorization: Bearer <key>`
/// Key format: `<prefix>_<secret>` where `<prefix>` uniquely identifies the
/// row in `api_keys` and the full key is hashed with SHA-256 for comparison.
///
/// Rejects with 401 when:
///  - Header is absent or not a Bearer token.
///  - Key is malformed (no `_` separator).
///  - No matching prefix found in the database.
///  - Stored hash does not match the supplied key.
///  - The key's `revoked_at` is non-NULL.
pub async fn auth_middleware(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    // tracing::info!(method = %req.method(), path = %req.uri().path(), "auth_middleware started");

    let raw_key = match auth::extract_bearer(req.headers()) {
        Some(k) => k.to_owned(),
        None => {
            // tracing::info!(method = %req.method(), path = %req.uri().path(), "auth_middleware rejected request: missing bearer token");
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": "missing or invalid Authorization header"}))).into_response();
        }
    };

    match auth::authenticate_api_key(&state.db, raw_key.as_str(), state.api_key_pepper.as_deref()).await {
        Ok(authenticated) => {
            // tracing::info!(
            //     method = %req.method(),
            //     path = %req.uri().path(),
            //     business_id = %authenticated.business_id,
            //     key_prefix = %authenticated.key_prefix,
            //     "auth_middleware succeeded"
            // );
            req.extensions_mut().insert(AuthenticatedBusiness { business_id: authenticated.business_id });
            next.run(req).await
        }
        Err(AuthError::MalformedApiKey) => {
            // tracing::info!(method = %req.method(), path = %req.uri().path(), "auth_middleware rejected request: malformed API key");
            (StatusCode::UNAUTHORIZED, Json(json!({"error": "malformed API key"}))).into_response()
        }
        Err(AuthError::InvalidApiKey) => {
            // tracing::info!(method = %req.method(), path = %req.uri().path(), "auth_middleware rejected request: invalid API key");
            (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid API key"}))).into_response()
        }
        Err(AuthError::RevokedApiKey) => {
            // tracing::info!(method = %req.method(), path = %req.uri().path(), "auth_middleware rejected request: revoked API key");
            (StatusCode::UNAUTHORIZED, Json(json!({"error": "API key has been revoked"}))).into_response()
        }
        Err(AuthError::MissingOrInvalidAuthorization) => {
            // tracing::info!(method = %req.method(), path = %req.uri().path(), "auth_middleware rejected request: missing or invalid authorization header");
            (StatusCode::UNAUTHORIZED, Json(json!({"error": "missing or invalid Authorization header"}))).into_response()
        }
        Err(err) => {
            tracing::error!(error = %err, "authentication service unavailable");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "authentication service unavailable"}))).into_response()
        }
    }
}
