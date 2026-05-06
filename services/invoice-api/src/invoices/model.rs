use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvoiceState {
    Draft,
    Open,
    Paid,
    Void,
    Uncollectible,
}

impl InvoiceState {
    pub fn as_str(self) -> &'static str {
        match self {
            InvoiceState::Draft => "draft",
            InvoiceState::Open => "open",
            InvoiceState::Paid => "paid",
            InvoiceState::Void => "void",
            InvoiceState::Uncollectible => "uncollectible",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "draft" => Some(InvoiceState::Draft),
            "open" => Some(InvoiceState::Open),
            "paid" => Some(InvoiceState::Paid),
            "void" => Some(InvoiceState::Void),
            "uncollectible" => Some(InvoiceState::Uncollectible),
            _ => None,
        }
    }

    pub fn can_transition_to(self, next: InvoiceState) -> bool {
        matches!((self, next), (InvoiceState::Draft, InvoiceState::Open) | (InvoiceState::Draft, InvoiceState::Void) | (InvoiceState::Open, InvoiceState::Paid) | (InvoiceState::Open, InvoiceState::Void) | (InvoiceState::Open, InvoiceState::Uncollectible))
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateInvoiceLineItemRequest {
    pub description: String,
    pub quantity: i32,
    pub unit_amount_cents: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateInvoiceRequest {
    pub customer_id: Uuid,
    pub due_date: NaiveDate,
    pub line_items: Vec<CreateInvoiceLineItemRequest>,
}

#[derive(Debug, Deserialize)]
pub struct ListInvoicesQuery {
    pub state: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PayInvoiceRequest {
    pub card_token: String,
}

#[derive(Debug, Serialize)]
pub struct InvoiceLineItemResponse {
    pub id: Uuid,
    pub description: String,
    pub quantity: i32,
    pub unit_amount_cents: i64,
    pub line_total_cents: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct InvoiceResponse {
    pub id: Uuid,
    pub business_id: Uuid,
    pub customer_id: Uuid,
    pub state: String,
    pub due_date: NaiveDate,
    pub total_amount_cents: i64,
    pub currency: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub line_items: Vec<InvoiceLineItemResponse>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PayInvoiceResponse {
    pub invoice_id: Uuid,
    pub payment_attempt_id: Uuid,
    pub status: String,
    pub psp_ref: Option<String>,
    pub failure_code: Option<String>,
    pub message: Option<String>,
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

impl CreateInvoiceRequest {
    pub fn normalize(mut self) -> Self {
        for line in &mut self.line_items {
            line.description = line.description.trim().to_string();
        }
        self
    }

    pub fn validate(&self) -> Result<(), ValidationErrorResponse> {
        let mut fields: Vec<ValidationFieldError> = Vec::new();

        if self.line_items.is_empty() {
            fields.push(ValidationFieldError { field: "line_items", message: "at least one line item is required" });
        }

        for line in &self.line_items {
            if line.description.is_empty() {
                fields.push(ValidationFieldError { field: "line_items.description", message: "line item description must not be empty" });
            }

            if line.quantity <= 0 {
                fields.push(ValidationFieldError { field: "line_items.quantity", message: "line item quantity must be greater than 0" });
            }

            if line.unit_amount_cents < 0 {
                fields.push(ValidationFieldError { field: "line_items.unit_amount_cents", message: "line item unit_amount_cents must be >= 0" });
            }
        }

        if fields.is_empty() { Ok(()) } else { Err(ValidationErrorResponse { error: "validation_error", fields }) }
    }
}
