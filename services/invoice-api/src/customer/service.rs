use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

use super::model::{CreateCustomerRequest, Customer};

#[derive(Debug, Error)]
pub enum CustomerError {
    #[error("customer not found")]
    NotFound,
    #[error("a customer with this email already exists for this business")]
    DuplicateEmail,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// Creates a new customer for the business.
/// Inserts a single row; if the email already exists for this business the DB unique
/// constraint fires and the error is mapped to `DuplicateEmail`.
pub async fn create(db: &PgPool, business_id: Uuid, req: CreateCustomerRequest) -> Result<Customer, CustomerError> {
    // tracing::info!(business_id = %business_id, email = %req.email, "customer_service.create started");

    let inserted = sqlx::query_as::<_, Customer>(
        r#"
        INSERT INTO customers (business_id, name, email)
        VALUES ($1, $2, $3)
        RETURNING id, business_id, name, email, created_at
        "#,
    )
    .bind(business_id)
    .bind(req.name)
    .bind(req.email)
    .fetch_one(db)
    .await;

    inserted.map(|customer| {
        // tracing::info!(business_id = %business_id, customer_id = %customer.id, "customer_service.create inserted customer");
        customer
    })
    .map_err(|e| {
        if let sqlx::Error::Database(ref dbe) = e {
            if dbe.constraint() == Some("customers_business_email_unique") {
                // tracing::info!(business_id = %business_id, "customer_service.create duplicate email detected");
                return CustomerError::DuplicateEmail;
            }
        }
        CustomerError::Db(e)
    })
}

/// Fetches a single customer by ID, scoped to the business.
/// Returns `NotFound` if the customer doesn't exist or belongs to a different business.
pub async fn get_by_id(db: &PgPool, business_id: Uuid, customer_id: Uuid) -> Result<Customer, CustomerError> {
    // tracing::info!(business_id = %business_id, customer_id = %customer_id, "customer_service.get_by_id started");

    let customer = sqlx::query_as::<_, Customer>(
        r#"
        SELECT id, business_id, name, email, created_at
        FROM   customers
        WHERE  id = $1 AND business_id = $2
        "#,
    )
    .bind(customer_id)
    .bind(business_id)
    .fetch_optional(db)
    .await?;

    match customer {
        Some(value) => {
            // tracing::info!(business_id = %business_id, customer_id = %value.id, "customer_service.get_by_id found customer");
            Ok(value)
        }
        None => {
            // tracing::info!(business_id = %business_id, customer_id = %customer_id, "customer_service.get_by_id customer not found");
            Err(CustomerError::NotFound)
        }
    }
}

/// Returns all customers for the business, ordered by newest first.
pub async fn list(db: &PgPool, business_id: Uuid) -> Result<Vec<Customer>, CustomerError> {
    // tracing::info!(business_id = %business_id, "customer_service.list started");

    let customers = sqlx::query_as::<_, Customer>(
        r#"
        SELECT id, business_id, name, email, created_at
        FROM   customers
        WHERE  business_id = $1
        ORDER  BY created_at DESC
        "#,
    )
    .bind(business_id)
    .fetch_all(db)
    .await
    .map_err(CustomerError::Db)?;

    // tracing::info!(business_id = %business_id, row_count = customers.len(), "customer_service.list fetched customers");
    Ok(customers)
}
