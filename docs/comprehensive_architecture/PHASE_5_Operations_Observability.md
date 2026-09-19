# PHASE 5: Booking Microservice - Observability & Operations

## 1. Failure Scenario Catalogue

| Scenario | Trigger | Immediate System Response | Recovery / Compensation |
| :--- | :--- | :--- | :--- |
| **Kafka Broker Outage** | Fast-path `trySinglePublish` throws `TimeoutException` | Method catches exception, logs warning, and exits. DB is already committed. | `OutgoingOutboxPublisherJob` sweeps the table later and retries publishing once Kafka is healthy. |
| **Payment Service Timeout** | Payment Service fails to respond after `PaymentRequestedEvent` | Booking stays in `PENDING` state indefinitely. | A dedicated reconciliation job (or external Payment Service timeout event) must eventually transition it to `FAILED`. *(See Phase 6 Tech Debt)* |
| **Poison Inbound Message** | `BookingRequestEvent` has malformed JSON causing a NullPointerException in mapper | Thread fails, transaction rolls back. Inbox remains `PENDING`. Quartz retries 5 times. | After 5 retries, `OutboxDeadLetterHandler` transitions inbox to `DEAD`. **Crucially, it synthesizes a `BookingFailedEvent`** to ensure Platform releases the locked inventory. |
| **Simultaneous Success/Fail** | Payment Success and Payment Timeout events hit API at exact same ms | First thread updates `Booking` status. Second thread gets `ObjectOptimisticLockingFailureException`. | Second thread rolls back. When it retries, it sees the status is already terminal (`CONFIRMED`) and safely ignores the failure event. |
| **Database Connection Exhaustion** | Spike in traffic | HikariCP throws connection acquisition timeout. Kafka listener threads block. | Kafka listeners pause polling. Backpressure is naturally applied. Once DB recovers, polling resumes. No data loss. |

### The "Capacity-Leak" Scenario
**What happens if Booking Service fails to publish the `BookingFailedEvent`?**
If a booking fails, the Booking Service *must* publish the `BookingFailedEvent` so Platform can decrement the inventory counter. 
If the database transaction creating the `BookingFailedEvent` outbox record crashes, the inventory in Platform will be held permanently, creating a "Capacity Leak" (revenue loss, as the seat cannot be resold). 
**Protection:** The Booking Service guarantees this via `InboundOutboxRetryService` and `OutboxDeadLetterHandler`. If the initial processing fails completely, the Dead Letter Handler explicitly constructs and forces a `BookingFailedEvent` into the outgoing queue. As long as the `inbound_outbox` recorded the initial hit, the capacity release is guaranteed to eventually fire.

## 2. Database Reference

### `bookings` Table
- `booking_id`: `VARCHAR(60)` Primary Key. Example: `MFB-1735000000000-A3F2`.
- `user_id`: `BIGINT`. Indexed for user-profile lookups.
- `status`: `VARCHAR`. Indexed. Valid states: `PENDING`, `CONFIRMED`, `FAILED`, `CANCELLED`.
- `version`: `BIGINT`. Crucial for JPA `@Version` Optimistic Locking.
- `experience_id`: `BIGINT`. Indexed for catalog relation.

### `inbound_outbox` Table
- `id`: `BIGINT` PK Auto-Increment.
- `booking_ref_id`: `VARCHAR`. Connects the event to the Booking Aggregate.
- `event_type`: `VARCHAR`. E.g., `BOOKING_REQUESTED`.
- `payload`: `TEXT`. Raw JSON for Quartz retries.
- `status`: `VARCHAR`. Default `PENDING`.

### `outgoing_outbox` / `outbox_record`
- `id`: UUID.
- `event_type`: `VARCHAR`. E.g., `PAYMENT_REQUESTED`.
- `payload`: `TEXT`.
- `status`: `VARCHAR`. `PENDING`, `SENT`, `FAILED`.

## 3. Error-Handling and Retry Classification

1. **Transient Errors (Retriable)**
   - Database Connection timeouts.
   - Kafka Broker disconnects.
   - `ObjectOptimisticLockingFailureException` (Concurrent updates).
   - *Action:* Allow exception to bubble up, trigger DB rollback. Quartz will naturally retry later.
2. **Permanent Errors (Non-Retriable)**
   - `ConstraintViolationException` (Duplicate keys).
   - Invalid JSON deserialization (`JsonMappingException`).
   - *Action:* If unrecoverable after 5 retries, move to `DEAD` state via `OutboxDeadLetterHandler`. Trigger compensation events.

## 4. Observability and Operations Guide

**Key Metrics to Monitor (Datadog/Prometheus):**
1. **Inbox Retry Count:** Alert if `SELECT count(*) FROM inbound_outbox WHERE status = 'PENDING'` > 100 for more than 5 minutes. Indicates Kafka consumers are processing but business logic is crashing constantly.
2. **Outbox Send Failures:** Alert if `outgoing_outbox` contains rows in `FAILED` state. Indicates Kafka cluster is unreachable.
3. **Dead Letters:** Alert immediately if `inbound_outbox` has rows in `DEAD`. This requires human intervention.
4. **Deadlocks/Optimistic Locks:** Monitor the frequency of `ObjectOptimisticLockingFailureException`. A small number is normal; a high number indicates an architectural design flaw or DDOS attack.

## 5. Troubleshooting Runbook

### Alert: "Incoming outbox record moved to DEAD after max retries"
- **Symptom:** A Kafka event failed processing 5 times.
- **Immediate Action:** The system automatically sent a `BookingFailedEvent` to Platform to release inventory (via `OutboxDeadLetterHandler`).
- **Investigation:** 
  1. Retrieve the payload from `inbound_outbox` using the `id` from the alert.
  2. Attempt to parse the JSON manually. Look for missing required fields (e.g., null `bookingId`).
  3. Check application logs for the exact exception stack trace that triggered during the 5 retries.

### Alert: Bookings stuck in `PENDING`
- **Symptom:** Users complain they paid, but their booking says "Pending".
- **Investigation:**
  1. Check if the Payment Service ever fired a `PaymentProcessedEvent` for that `booking_id`.
  2. If Yes: Check the `inbound_outbox` in the Booking DB. Did it fail? Is it `DEAD`?
  3. If No: Check the `outgoing_outbox` in the Booking DB. Did we ever send the `PaymentRequestedEvent`? If it's stuck in `PENDING` there, check Kafka connectivity.

### Alert: Platform shows 0 capacity, but no confirmed bookings exist
- **Symptom:** Capacity leak.
- **Investigation:**
  1. This means Platform sent a `BookingRequestEvent`, but Booking Service never sent back a `BookingFailedEvent`.
  2. Check `inbound_outbox` for the `booking_id`. If it's completely missing, Kafka dropped the original event (or Platform never sent it).
  3. If present but stuck, check Quartz logs. Is `OutboxRetriesJob` running?