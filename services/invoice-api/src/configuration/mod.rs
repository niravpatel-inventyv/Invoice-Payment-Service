use sqlx::{PgPool, postgres::PgPoolOptions};

/// Build and return a Postgres connection pool.
/// Reads `DATABASE_URL` from the environment (set in docker-compose or .env).
pub async fn db_pool() -> anyhow::Result<PgPool> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new().max_connections(10).connect(&url).await?;

    Ok(pool)
}
