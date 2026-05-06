use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::post,
};
use chrono::NaiveDate;
use invoice_api::{auth, build_app, middleware::AppState};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tokio::{net::TcpListener, task::JoinHandle, time::sleep};
use uuid::Uuid;

#[derive(Clone)]
struct MockPspState {
    call_count: Arc<AtomicUsize>,
}

#[derive(Debug, Deserialize)]
struct MockPspRequest {
    card_token: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreatedInvoice {
    id: Uuid,
}

struct TestContext {
    base_url: String,
    psp_call_count: Arc<AtomicUsize>,
    db: PgPool,
    business_id: Uuid,
    customer_id: Uuid,
    api_key: String,
    app_handle: JoinHandle<()>,
    psp_handle: JoinHandle<()>,
}

impl TestContext {
    async fn new() -> Self {
        let db = PgPoolOptions::new()
            .max_connections(10)
            .connect(test_database_url().as_str())
            .await
            .expect("failed to connect to postgres for tests");

        let psp_call_count = Arc::new(AtomicUsize::new(0));
        let psp_base_url = start_mock_psp_server(psp_call_count.clone()).await;
        let base_url = start_invoice_api_server(db.clone(), psp_base_url).await;

        let fixture = insert_business_fixture(&db).await;

        Self {
            base_url,
            psp_call_count,
            db,
            business_id: fixture.business_id,
            customer_id: fixture.customer_id,
            api_key: fixture.api_key,
            app_handle: fixture.app_handle,
            psp_handle: fixture.psp_handle,
        }
    }

    async fn create_invoice(&self, amount_cents: i64) -> Uuid {
        let client = Client::new();
        let response = client
            .post(format!("{}/invoices", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&json!({
                "customer_id": self.customer_id,
                "due_date": NaiveDate::from_ymd_opt(2026, 6, 30).expect("valid date"),
                "line_items": [
                    {
                        "description": "Required test item",
                        "quantity": 1,
                        "unit_amount_cents": amount_cents
                    }
                ]
            }))
            .send()
            .await
            .expect("create invoice request failed");

        assert_eq!(response.status(), StatusCode::CREATED);
        let payload: CreatedInvoice = response.json().await.expect("invoice creation response was invalid");
        payload.id
    }

    async fn invoice_state(&self, invoice_id: Uuid) -> String {
        let row = sqlx::query(
            r#"
            SELECT state
            FROM invoices
            WHERE id = $1 AND business_id = $2
            "#,
        )
        .bind(invoice_id)
        .bind(self.business_id)
        .fetch_one(&self.db)
        .await
        .expect("failed to query invoice state");

        row.try_get::<String, _>("state").expect("failed to decode invoice state")
    }

    async fn successful_payment_count(&self, invoice_id: Uuid) -> i64 {
        let row = sqlx::query(
            r#"
            SELECT count(*)::bigint AS count
            FROM payment_attempts
            WHERE invoice_id = $1
              AND status = 'succeeded'
            "#,
        )
        .bind(invoice_id)
        .fetch_one(&self.db)
        .await
        .expect("failed to query payment_attempts count");

        row.try_get::<i64, _>("count").expect("failed to decode payment_attempts count")
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.app_handle.abort();
        self.psp_handle.abort();
    }
}

struct FixtureSeed {
    business_id: Uuid,
    customer_id: Uuid,
    api_key: String,
    app_handle: JoinHandle<()>,
    psp_handle: JoinHandle<()>,
}

#[tokio::test]
async fn concurrent_pay_requests_allow_only_one_success() {
    let context = TestContext::new().await;
    let invoice_id = context.create_invoice(700).await;
    let client = Client::new();

    let mut tasks = Vec::new();
    for request_index in 0..8 {
        let client = client.clone();
        let base_url = context.base_url.clone();
        let api_key = context.api_key.clone();
        let idempotency_key = format!("concurrency-{}-{}", invoice_id, request_index);

        tasks.push(tokio::spawn(async move {
            let response = client
                .post(format!("{}/invoices/{}/pay", base_url, invoice_id))
                .header("Authorization", format!("Bearer {}", api_key))
                .header("Idempotency-Key", idempotency_key)
                .header("Content-Type", "application/json")
                .json(&json!({"card_token": "tok_success"}))
                .send()
                .await
                .expect("concurrent pay request failed");

            let status = response.status();
            let body: Value = response.json().await.expect("concurrent pay response body was invalid");
            (status, body)
        }));
    }

    let mut success_count = 0_usize;
    for task in tasks {
        let (status, body) = task.await.expect("concurrent pay task panicked");
        match status {
            StatusCode::OK => {
                success_count += 1;
                assert_eq!(body.get("status").and_then(Value::as_str), Some("paid"));
            }
            StatusCode::CONFLICT => {
                assert_eq!(body.get("error").and_then(Value::as_str), Some("invoice is not payable"));
            }
            other => panic!("unexpected status from concurrent pay request: {other}"),
        }
    }

    assert_eq!(success_count, 1, "expected exactly one successful payment");
    assert_eq!(context.successful_payment_count(invoice_id).await, 1, "expected exactly one succeeded payment_attempt row");
    assert_eq!(context.invoice_state(invoice_id).await, "paid", "expected invoice to end in paid state");
}

#[tokio::test]
async fn idempotent_retry_returns_same_response_without_second_psp_call() {
    let context = TestContext::new().await;
    let invoice_id = context.create_invoice(800).await;
    let client = Client::new();
    let endpoint = format!("{}/invoices/{}/pay", context.base_url, invoice_id);
    let idempotency_key = format!("idempotency-{}", invoice_id);

    let first_response = client
        .post(endpoint.as_str())
        .header("Authorization", format!("Bearer {}", context.api_key))
        .header("Idempotency-Key", idempotency_key.as_str())
        .header("Content-Type", "application/json")
        .json(&json!({"card_token": "tok_success"}))
        .send()
        .await
        .expect("first idempotent pay request failed");

    assert_eq!(first_response.status(), StatusCode::OK);
    let first_body: Value = first_response.json().await.expect("first idempotent response body was invalid");

    let second_response = client
        .post(endpoint.as_str())
        .header("Authorization", format!("Bearer {}", context.api_key))
        .header("Idempotency-Key", idempotency_key.as_str())
        .header("Content-Type", "application/json")
        .json(&json!({"card_token": "tok_success"}))
        .send()
        .await
        .expect("second idempotent pay request failed");

    assert_eq!(second_response.status(), StatusCode::OK);
    let second_body: Value = second_response.json().await.expect("second idempotent response body was invalid");

    assert_eq!(first_body, second_body, "expected same key replay to return the exact cached response");
    assert_eq!(context.psp_call_count.load(Ordering::SeqCst), 1, "expected only one PSP call for idempotent replay");
}

#[tokio::test]
async fn psp_failure_leaves_invoice_open() {
    let context = TestContext::new().await;
    let invoice_id = context.create_invoice(900).await;
    let client = Client::new();

    let response = client
        .post(format!("{}/invoices/{}/pay", context.base_url, invoice_id))
        .header("Authorization", format!("Bearer {}", context.api_key))
        .header("Idempotency-Key", format!("psp-failure-{}", invoice_id))
        .header("Content-Type", "application/json")
        .json(&json!({"card_token": "tok_network_error"}))
        .send()
        .await
        .expect("psp failure pay request failed");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = response.json().await.expect("psp failure response body was invalid");
    assert_eq!(body.get("status").and_then(Value::as_str), Some("failed"));
    assert_eq!(body.get("failure_code").and_then(Value::as_str), Some("network_error"));
    assert_eq!(context.invoice_state(invoice_id).await, "open", "expected invoice to remain open after PSP failure");
}

async fn start_mock_psp_server(call_count: Arc<AtomicUsize>) -> String {
    let state = MockPspState { call_count };
    let app = Router::new().route("/payments", post(mock_psp_payment)).with_state(state);
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await.expect("failed to bind mock PSP listener");
    let address = listener.local_addr().expect("failed to read mock PSP address");

    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            panic!("mock PSP server exited unexpectedly: {err}");
        }
    });

    let url = format!("http://{}", address);
    MOCK_PSP_HANDLE.with(|slot| {
        *slot.borrow_mut() = Some(handle);
    });
    url
}

async fn start_invoice_api_server(db: PgPool, psp_base_url: String) -> String {
    let http_client = Client::builder().timeout(Duration::from_secs(3)).build().expect("failed to build test HTTP client");
    let state = AppState {
        db,
        psp_base_url,
        http_client,
        api_key_pepper: None,
    };

    let app = build_app(state);
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await.expect("failed to bind invoice API listener");
    let address = listener.local_addr().expect("failed to read invoice API address");

    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            panic!("invoice API server exited unexpectedly: {err}");
        }
    });

    APP_HANDLE.with(|slot| {
        *slot.borrow_mut() = Some(handle);
    });

    format!("http://{}", address)
}

async fn insert_business_fixture(db: &PgPool) -> FixtureSeed {
    let business_id = Uuid::new_v4();
    let customer_id = Uuid::new_v4();
    let key_prefix = format!("test{}", Uuid::new_v4().simple());
    let api_key = format!("{}_secret", key_prefix);
    let key_hash = auth::hash_api_key(api_key.as_str(), None);

    sqlx::query(
        r#"
        INSERT INTO businesses (id, name)
        VALUES ($1, $2)
        "#,
    )
    .bind(business_id)
    .bind(format!("Required Tests {}", business_id))
    .execute(db)
    .await
    .expect("failed to insert test business");

    sqlx::query(
        r#"
        INSERT INTO api_keys (business_id, key_prefix, key_hash)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(business_id)
    .bind(key_prefix)
    .bind(key_hash)
    .execute(db)
    .await
    .expect("failed to insert test API key");

    sqlx::query(
        r#"
        INSERT INTO customers (id, business_id, name, email)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(customer_id)
    .bind(business_id)
    .bind("Required Test Customer")
    .bind(format!("{}@example.com", customer_id))
    .execute(db)
    .await
    .expect("failed to insert test customer");

    let app_handle = APP_HANDLE.with(|slot| slot.borrow_mut().take()).expect("missing invoice API handle");
    let psp_handle = MOCK_PSP_HANDLE.with(|slot| slot.borrow_mut().take()).expect("missing mock PSP handle");

    FixtureSeed {
        business_id,
        customer_id,
        api_key,
        app_handle,
        psp_handle,
    }
}

async fn mock_psp_payment(State(state): State<MockPspState>, Json(payload): Json<MockPspRequest>) -> (StatusCode, Json<Value>) {
    state.call_count.fetch_add(1, Ordering::SeqCst);

    match payload.card_token.as_str() {
        "tok_success" => {
            sleep(Duration::from_millis(100)).await;
            (
                StatusCode::OK,
                Json(json!({
                    "status": "succeeded",
                    "psp_ref": Uuid::new_v4().to_string()
                })),
            )
        }
        "tok_network_error" => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "status": "failed",
                "code": "network_error"
            })),
        ),
        other => panic!("unexpected mock PSP token in required tests: {other}"),
    }
}

fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://invoice_user:invoice_pass@127.0.0.1:5432/invoice_db".to_string())
}

thread_local! {
    static APP_HANDLE: std::cell::RefCell<Option<JoinHandle<()>>> = const { std::cell::RefCell::new(None) };
    static MOCK_PSP_HANDLE: std::cell::RefCell<Option<JoinHandle<()>>> = const { std::cell::RefCell::new(None) };
}