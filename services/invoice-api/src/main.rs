use invoice_api::{build_app, configuration, middleware, worker};
use std::net::SocketAddr;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db = match configuration::db_pool().await {
        Ok(pool) => pool,
        Err(err) => {
            error!(error = %err, "failed to initialize database pool");
            return;
        }
    };

    let psp_base_url = match std::env::var("PSP_BASE_URL") {
        Ok(url) => url,
        Err(_) => "http://localhost:8081".to_string(),
    };

    let http_client = match reqwest::Client::builder().timeout(std::time::Duration::from_secs(3)).build() {
        Ok(client) => client,
        Err(err) => {
            error!(error = %err, "failed to build HTTP client");
            return;
        }
    };

    let api_key_pepper = std::env::var("API_KEY_PEPPER").ok().and_then(|value| {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() { None } else { Some(trimmed) }
    });

    let bind_addr = match std::env::var("API_BIND_ADDR") {
        Ok(addr) => addr,
        Err(_) => match std::env::var("PORT") {
            Ok(port) => format!("0.0.0.0:{port}"),
            Err(_) => "0.0.0.0:8080".to_string(),
        },
    };

    let addr: SocketAddr = match bind_addr.parse() {
        Ok(parsed) => parsed,
        Err(err) => {
            error!(error = %err, bind_addr = %bind_addr, "invalid API bind address");
            return;
        }
    };

    let app_state = middleware::AppState {
        db: db.clone(),
        psp_base_url,
        http_client,
        api_key_pepper,
    };
    let app = build_app(app_state);

    let worker_settings = worker::WorkerSettings::from_env();

    let mut api_task = tokio::spawn(async move { run_api_server(app, addr).await });
    let mut worker_task = tokio::spawn(async move { worker::run(db, worker_settings).await });

    tokio::select! {
        api_result = &mut api_task => {
            match api_result {
                Ok(Ok(())) => info!("api task exited"),
                Ok(Err(err)) => error!(error = %err, "api task returned error"),
                Err(err) => error!(error = %err, "api task join error"),
            }

            worker_task.abort();
            let _ = worker_task.await;
        }
        worker_result = &mut worker_task => {
            match worker_result {
                Ok(Ok(())) => info!("worker task exited"),
                Ok(Err(err)) => error!(error = %err, "worker task returned error"),
                Err(err) => error!(error = %err, "worker task join error"),
            }

            api_task.abort();
            let _ = api_task.await;
        }
    }
}

async fn run_api_server(app: axum::Router, addr: SocketAddr) -> anyhow::Result<()> {
    info!(bind_addr = %addr, "api server starting");

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            return Err(anyhow::anyhow!("failed to bind TCP listener: {err}"));
        }
    };

    if let Err(err) = axum::serve(listener, app).await {
        return Err(anyhow::anyhow!("server terminated with error: {err}"));
    }

    Ok(())
}
