use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A customer row as stored in the database.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Customer {
    pub id: Uuid,
    pub business_id: Uuid,
    pub name: String,
    pub email: String,
    pub created_at: DateTime<Utc>,
}

/// Request body for `POST /customers`.
#[derive(Debug, Deserialize)]
pub struct CreateCustomerRequest {
    pub name: String,
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct ValidationErrorResponse {
    pub error: &'static str,
    pub fields: Vec<ValidationFieldError>,
}

#[derive(Debug, Serialize)]
pub struct ValidationFieldError {
    pub field: &'static str,
    pub message: &'static str,
}

impl CreateCustomerRequest {
    pub fn normalize(mut self) -> Self {
        self.name = self.name.trim().to_string();
        self.email = self.email.trim().to_lowercase();
        self
    }

    pub fn validate(&self) -> Result<(), ValidationErrorResponse> {
        let mut fields: Vec<ValidationFieldError> = Vec::new();

        if self.name.is_empty() {
            fields.push(ValidationFieldError { field: "name", message: "name must not be empty" });
        }

        if self.name.len() > 120 {
            fields.push(ValidationFieldError { field: "name", message: "name is too long (max 120 characters)" });
        }

        if !looks_like_email(&self.email) {
            fields.push(ValidationFieldError { field: "email", message: "email must be a valid address" });
        }

        if fields.is_empty() { Ok(()) } else { Err(ValidationErrorResponse { error: "validation_error", fields }) }
    }
}

fn looks_like_email(email: &str) -> bool {
    if email.is_empty() || email.len() > 254 {
        return false;
    }

    let mut parts = email.split('@');
    let local = parts.next();
    let domain = parts.next();
    let extra = parts.next();

    if extra.is_some() {
        return false;
    }

    let local = match local {
        Some(value) if !value.is_empty() => value,
        _ => return false,
    };

    let domain = match domain {
        Some(value) if !value.is_empty() => value,
        _ => return false,
    };

    if local.starts_with('.') || local.ends_with('.') {
        return false;
    }

    if domain.starts_with('.') || domain.ends_with('.') {
        return false;
    }

    if !domain.contains('.') {
        return false;
    }

    true
}
