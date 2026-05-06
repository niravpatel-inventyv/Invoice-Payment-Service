use chrono::{DateTime, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AuthenticatedKey {
    pub business_id: Uuid,
    pub key_prefix: String,
}

#[derive(Debug, Clone)]
pub struct CreateApiKeyResult {
    pub plaintext_key: String,
    pub key_prefix: String,
}

#[derive(Debug)]
pub struct ApiKeyRecord {
    pub id: Uuid,
    pub business_id: Uuid,
    pub key_prefix: String,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing or invalid Authorization header")]
    MissingOrInvalidAuthorization,
    #[error("malformed API key")]
    MalformedApiKey,
    #[error("invalid API key")]
    InvalidApiKey,
    #[error("API key has been revoked")]
    RevokedApiKey,
    #[error("database decode error: {0}")]
    Decode(String),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

pub fn generate_api_key() -> CreateApiKeyResult {
    let mut prefix_bytes = [0_u8; 12];
    let mut secret_bytes = [0_u8; 32];

    let mut rng = rand::rngs::OsRng;
    rng.fill_bytes(&mut prefix_bytes);
    rng.fill_bytes(&mut secret_bytes);

    let key_prefix = format!("dpk_{}", hex::encode(prefix_bytes));
    let secret = hex::encode(secret_bytes);
    let plaintext_key = format!("{}_{}", key_prefix, secret);

    CreateApiKeyResult { plaintext_key, key_prefix }
}

pub fn extract_bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers.get(axum::http::header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "))
}

pub fn hash_api_key(raw_key: &str, pepper: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    if let Some(pepper) = pepper {
        hasher.update(b"::");
        hasher.update(pepper.as_bytes());
    }
    hex::encode(hasher.finalize())
}

pub fn parse_key_prefix(raw_key: &str) -> Option<String> {
    raw_key.rsplit_once('_').map(|(prefix, _)| prefix.to_string())
}

pub async fn authenticate_api_key(db: &PgPool, raw_key: &str, pepper: Option<&str>) -> Result<AuthenticatedKey, AuthError> {
    let key_prefix = parse_key_prefix(raw_key).ok_or(AuthError::MalformedApiKey)?;
    let key_hash = hash_api_key(raw_key, pepper);

    let row = sqlx::query(
        r#"
        SELECT business_id, key_prefix, revoked_at
        FROM api_keys
        WHERE key_prefix = $1
          AND key_hash = $2
        "#,
    )
    .bind(key_prefix)
    .bind(key_hash)
    .fetch_optional(db)
    .await?;

    let row = row.ok_or(AuthError::InvalidApiKey)?;

    let business_id = row.try_get::<Uuid, _>("business_id").map_err(|e| AuthError::Decode(format!("business_id: {e}")))?;

    let key_prefix = row.try_get::<String, _>("key_prefix").map_err(|e| AuthError::Decode(format!("key_prefix: {e}")))?;

    let revoked_at = row.try_get::<Option<DateTime<Utc>>, _>("revoked_at").map_err(|e| AuthError::Decode(format!("revoked_at: {e}")))?;

    if revoked_at.is_some() {
        return Err(AuthError::RevokedApiKey);
    }

    Ok(AuthenticatedKey { business_id, key_prefix })
}

pub async fn create_api_key_for_business(db: &PgPool, business_id: Uuid, pepper: Option<&str>) -> Result<CreateApiKeyResult, AuthError> {
    let generated = generate_api_key();
    let key_hash = hash_api_key(generated.plaintext_key.as_str(), pepper);

    sqlx::query(
        r#"
        INSERT INTO api_keys (business_id, key_prefix, key_hash)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(business_id)
    .bind(generated.key_prefix.as_str())
    .bind(key_hash)
    .execute(db)
    .await?;

    Ok(generated)
}

pub async fn revoke_api_key(db: &PgPool, business_id: Uuid, key_prefix: &str) -> Result<bool, AuthError> {
    let result = sqlx::query(
        r#"
        UPDATE api_keys
        SET revoked_at = NOW()
        WHERE business_id = $1
          AND key_prefix = $2
          AND revoked_at IS NULL
        "#,
    )
    .bind(business_id)
    .bind(key_prefix)
    .execute(db)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn rotate_api_key(db: &PgPool, business_id: Uuid, pepper: Option<&str>) -> Result<CreateApiKeyResult, AuthError> {
    let mut tx = db.begin().await?;

    sqlx::query(
        r#"
        UPDATE api_keys
        SET revoked_at = NOW()
        WHERE business_id = $1
          AND revoked_at IS NULL
        "#,
    )
    .bind(business_id)
    .execute(&mut *tx)
    .await?;

    let generated = generate_api_key();
    let key_hash = hash_api_key(generated.plaintext_key.as_str(), pepper);

    sqlx::query(
        r#"
        INSERT INTO api_keys (business_id, key_prefix, key_hash)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(business_id)
    .bind(generated.key_prefix.as_str())
    .bind(key_hash)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(generated)
}

pub async fn list_api_keys_for_business(db: &PgPool, business_id: Uuid) -> Result<Vec<ApiKeyRecord>, AuthError> {
    let rows = sqlx::query(
        r#"
        SELECT id, business_id, key_prefix, revoked_at, created_at
        FROM api_keys
        WHERE business_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(business_id)
    .fetch_all(db)
    .await?;

    let mut records: Vec<ApiKeyRecord> = Vec::new();
    for row in rows {
        let id = row.try_get::<Uuid, _>("id").map_err(|e| AuthError::Decode(format!("id: {e}")))?;
        let business_id = row.try_get::<Uuid, _>("business_id").map_err(|e| AuthError::Decode(format!("business_id: {e}")))?;
        let key_prefix = row.try_get::<String, _>("key_prefix").map_err(|e| AuthError::Decode(format!("key_prefix: {e}")))?;
        let revoked_at = row.try_get::<Option<DateTime<Utc>>, _>("revoked_at").map_err(|e| AuthError::Decode(format!("revoked_at: {e}")))?;
        let created_at = row.try_get::<DateTime<Utc>, _>("created_at").map_err(|e| AuthError::Decode(format!("created_at: {e}")))?;

        records.push(ApiKeyRecord { id, business_id, key_prefix, revoked_at, created_at });
    }

    Ok(records)
}
