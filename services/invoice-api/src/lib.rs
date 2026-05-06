pub mod auth;
pub mod configuration;
pub mod customer;
pub mod invoices;
pub mod middleware;
pub mod routes;
pub mod webhooks;
pub mod worker;

pub fn build_app(state: middleware::AppState) -> axum::Router {
    routes::routes(state)
}