# AI Usage

This project used AI as a coding assistant, not as an auto-pilot.

## 1) Tools Used and Where

- GitHub Copilot Chat : for generating route handler boilerplate (Axum extractors, response types, error mapping), which I revised because the generated handlers did not enforce the required idempotency-key header or guard against invalid state transitions.
- GitHub Copilot Chat : to draft the initial SQL queries for invoice creation and status updates, which I revised because the drafts used two separate statements rather than a single locked transaction, leaving a race window that the assignment explicitly forbids.
- GitHub Copilot Chat : to cross-check my implementation against the assignment requirements checklist; I used the output as a review prompt and corrected gaps myself rather than accepting its edits directly.

## 2) Three Decisions I Made Myself

1. Payment Concurrency Strategy
   - AI proposed: either an optimistic-update approach with a version counter or a split read-then-write flow (read current state, call PSP, then write result).
   - I chose: database row locking with `SELECT ... FOR UPDATE` combined with idempotency-key validation inside a single transaction at pay finalization.
   - Why: the split flow lets two concurrent requests both pass the initial read before either write commits, which directly violates the "at most one success" requirement. A row lock prevents that. The trade-off is higher lock contention, which I accepted because correctness is graded explicitly and throughput is not.

2. Webhook Retry Logic
   - AI proposed: sending the webhook inline during the API request, or using fixed-interval retries without a bound.
   - I chose: async background delivery with bounded retries and exponential backoff.
   - Why: inline delivery blocks the API response on an external call and fails permanently on any transient error. Unbounded retries risk infinite loops. Bounded async delivery keeps the API responsive and handles transient PSP or customer-endpoint failures gracefully, which is what the assignment retry scenarios test.

3. Invoice State Machine Design
   - AI proposed: a minimal boolean paid/unpaid model, or loose status updates that allow any transition at any time.
   - I chose: explicit invoice and payment status enums with guarded transitions and terminal-state protection.
   - Why: the loose model allows invalid mutations (e.g. re-paying an already-paid invoice) that the required tests explicitly check against. Explicit guarded transitions make every invalid path a hard error and make the concurrent-pay and idempotent-replay scenarios straightforward to reason about.

## 3) One Thing the AI Got Wrong

When I asked Copilot to help scaffold the payment flow, it confidently produced code that split the operation across two database interactions: first read the invoice state, then later write the result. I initially copied this because it looked clean and readable. It was only when I started thinking through the concurrent-pay scenario by hand — two requests arriving at the same millisecond — that I realised both would pass the initial read before either committed a write. That's a direct double-charge risk.

I scrapped that structure and rewrote it as a single transaction: lock the row up front with `SELECT FOR UPDATE`, check idempotency inside the same transaction, and commit everything atomically. The AI's version would have passed basic happy-path tests; it would have failed the race condition test. That gap between "looks fine" and "is actually correct under concurrency" is exactly the kind of thing I couldn't delegate.