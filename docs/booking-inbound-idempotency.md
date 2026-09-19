# Booking inbound idempotency: architecture case study

## Purpose and context

The Booking service deliberately acknowledges Kafka messages early and relies on a
database-backed inbound record plus Quartz to recover application-processing
failures. That pattern can work, but only if every accepted business event can first
be persisted as a distinct inbox record.

This case study documents a mismatch between the Booking application's idempotency
lookup and its database uniqueness rule. It explains the immediate correction and
the longer-term event identity contract required across Booking, Payment, and
Platform.

The central distinction is:

- a **Kafka redelivery** is another delivery of the same event and must not repeat
  business effects;
- a **new business event** is a distinct fact, even when it has the same booking ID
  and event type as an earlier fact, and must be independently recorded and
  evaluated.

Booking currently cannot represent that distinction reliably.

## Current processing model

All three Booking consumers delegate to the same inbound processor:

- `BOOKING_REQUESTED`:
  `src/main/java/com/forvmom/MomentForeverBooking/consumer/BookingRequestConsumer.java:19-22`
- `PAYMENT_PROCESSED`:
  `src/main/java/com/forvmom/MomentForeverBooking/consumer/PaymentProcessedConsumer.java:23-26`
- `PAYMENT_FAILED`:
  `src/main/java/com/forvmom/MomentForeverBooking/consumer/PaymentFailedConsumer.java:32-35`

The Kafka container disables auto-commit and uses `MANUAL_IMMEDIATE`
acknowledgement:
`src/main/java/com/forvmom/MomentForeverBooking/config/BookingKafkaConfig.java:117-137`.

For each message, `InboundEventProcessorService`:

1. calls `findOrCreateForEvent`;
2. ignores an existing `PROCESSED` record as a duplicate;
3. acknowledges Kafka;
4. runs the business processor;
5. marks the inbound record `PROCESSED` or `FAILED`.

See
`src/main/java/com/forvmom/MomentForeverBooking/service/inbound/InboundEventProcessorService.java:31-61`.
The important ordering is explicit at lines 37 and 44-45: inbox persistence is
attempted before the early Kafka acknowledgement.

`findOrCreateForEvent` is transactional. It looks up an inbound record by
`(bookingReferenceId, eventType)` and inserts a `PENDING` record when none exists:
`src/main/java/com/forvmom/MomentForeverBooking/service/InboundOutboxService.java:29-47`.
The transaction commits when this method returns, before the caller acknowledges
Kafka.

If later processing fails, Quartz selects old `PENDING` and `FAILED` rows and retries
them:
`src/main/java/com/forvmom/MomentForeverBooking/service/retries_cleanup/InboundOutboxRetryService.java:108-154`.
The retry mapping explicitly covers `BOOKING_REQUESTED`, `PAYMENT_PROCESSED`, and
`PAYMENT_FAILED` at lines 61-73.

## Discovered key mismatch

The application treats the logical inbound key as:

```text
(booking_ref_id, event_type)
```

Evidence:

- `InboundOutboxDao.findByBookingReferenceIdAndEventType`:
  `src/main/java/com/forvmom/MomentForeverBooking/repository/InboundOutboxDao.java:24`
- invocation with `event.getBookingId()` and `event.getEventType()`:
  `src/main/java/com/forvmom/MomentForeverBooking/service/InboundOutboxService.java:29-36`

The entity, however, makes `booking_ref_id` unique by itself in two places:

- unique index `idx_inbound_outbox_ref`:
  `src/main/java/com/forvmom/MomentForeverBooking/domain/entity/InboundOutbox.java:8-12`
- `@Column(... unique = true)`:
  `src/main/java/com/forvmom/MomentForeverBooking/domain/entity/InboundOutbox.java:19-21`

Therefore the implemented database key is:

```text
(booking_ref_id)
```

The service can ask, "Has this booking and event type been seen?", receive "no", and
then be forbidden by the database from inserting the record because another event
type for the same booking already exists.

There are no versioned schema migrations in the repository. Hibernate is configured
with `ddl-auto: update` at `src/main/resources/application.yml:79-83`, so the exact
deployed constraint must be inspected before rollout. The entity nevertheless
defines the intended and likely generated uniqueness rule.

## Concrete failure: booking B-100

Assume the inbox is initially empty.

### Step 1: `BOOKING_REQUESTED`

```text
bookingId = B-100
eventType = BOOKING_REQUESTED
```

The lookup for `(B-100, BOOKING_REQUESTED)` finds no row. Booking inserts:

| booking_ref_id | event_type | status |
|---|---|---|
| B-100 | BOOKING_REQUESTED | PENDING |

That transaction commits. Kafka is acknowledged. Normal processing can then create
the booking and eventually mark the row `PROCESSED`.

### Step 2a: `PAYMENT_PROCESSED`

```text
bookingId = B-100
eventType = PAYMENT_PROCESSED
```

The lookup for `(B-100, PAYMENT_PROCESSED)` correctly finds no matching row.
Booking attempts to insert it. The database rejects the insert because `B-100`
already occupies the globally unique `booking_ref_id`.

The payment-success processor is never reached, so the transition implemented at
`src/main/java/com/forvmom/MomentForeverBooking/service/inbound/PaymentProcessedProcessor.java:38-66`
cannot confirm the booking or create its outgoing event.

### Step 2b: `PAYMENT_FAILED`

The same failure occurs for:

```text
bookingId = B-100
eventType = PAYMENT_FAILED
```

The lookup for `(B-100, PAYMENT_FAILED)` finds no row, but insertion conflicts with
the existing `BOOKING_REQUESTED` row. The failure processor at
`src/main/java/com/forvmom/MomentForeverBooking/service/inbound/PaymentFailedProcessor.java:31-56`
is not invoked.

This key defect is independent of other issues inside the payment processors. Even a
perfect processor cannot run when inbox insertion fails first.

## Why early acknowledgement and Quartz do not recover this failure

`findOrCreateForEvent` completes before `ack.acknowledge()`:
`src/main/java/com/forvmom/MomentForeverBooking/service/inbound/InboundEventProcessorService.java:36-45`.
The unique-constraint violation occurs while the transactional inbox insertion is
committing. Control does not reach the acknowledgement statement.

The Kafka listener's `DefaultErrorHandler` may redeliver before eventually sending
the record to a dead-letter topic, but every redelivery repeats the same impossible
insert against the same constraint. This is not a transient application-processing
failure.

Quartz cannot repair it because Quartz only discovers rows already present in
`inbound_outbox`. Its query selects old records with `PENDING` or `FAILED` status:
`src/main/java/com/forvmom/MomentForeverBooking/service/retries_cleanup/InboundOutboxRetryService.java:112-128`.
The rejected payment event has no row, payload, status, or retry count. It is
invisible to Quartz.

This produces an important operational rule:

> Early acknowledgement plus Quartz is safe only after the service can durably
> represent every distinct accepted event before acknowledgement.

## Business impact

For an affected booking:

- a valid payment result may never update Booking state;
- a paid booking may remain `PENDING`, delaying confirmation and downstream
  fulfillment;
- a failed payment may leave inventory or a Platform reservation held;
- `BOOKING_CONFIRMED` or `BOOKING_FAILED` may never be created;
- repeated Kafka delivery consumes retries but cannot heal the data;
- the dead-letter record and the Booking database disagree about whether the event
  was durably accepted;
- customer support may see payment and booking systems report contradictory states.

Because `booking_id` is the Booking primary key
(`src/main/java/com/forvmom/MomentForeverBooking/domain/entity/Booking.java:28-32`),
using it as the sole inbox uniqueness key conflates aggregate identity with event
identity.

## Immediate resolution

### Change the constraint to `(booking_ref_id, event_type)`

The shortest corrective migration is:

```sql
ALTER TABLE inbound_outbox
    DROP CONSTRAINT IF EXISTS <booking_ref_id_unique_constraint>;

DROP INDEX IF EXISTS idx_inbound_outbox_ref;

CREATE UNIQUE INDEX uq_inbound_outbox_booking_event_type
    ON inbound_outbox (booking_ref_id, event_type);
```

Constraint and index names must be confirmed against the deployed database before
applying the migration. The entity should likewise declare a table-level unique
constraint or unique composite index and remove `unique = true` from
`booking_ref_id`.

This aligns database behavior with the current DAO lookup and allows B-100 to have
one row each for `BOOKING_REQUESTED`, `PAYMENT_PROCESSED`, and `PAYMENT_FAILED`.

### Treat duplicate-key insertion as a concurrency race

The existing read-then-insert sequence is not atomic:

```text
worker 1: SELECT -> absent
worker 2: SELECT -> absent
worker 1: INSERT -> succeeds
worker 2: INSERT -> unique violation
```

The second insert is expected under concurrent Kafka deliveries. Handle it narrowly:

1. attempt to insert the composite key;
2. on the specific unique-key conflict, start or continue in a clean transaction;
3. re-read exactly `(booking_ref_id, event_type)`;
4. return the winner's row;
5. propagate every other persistence error.

PostgreSQL `INSERT ... ON CONFLICT ...` is preferable when repository conventions
allow it. Do not broadly catch database exceptions or treat an unverified failure as
a duplicate.

The transaction must not continue in a rollback-only state after a constraint
violation. Either use a native upsert or isolate insertion so the subsequent read
uses a valid transaction.

## Limitation of the immediate composite key

`UNIQUE(booking_ref_id,event_type)` distinguishes event types, but it still assumes
that a booking can have only one legitimate event of each type. Payment retries
invalidate that assumption.

Consider two payment attempts for B-100:

```text
E-1: eventId=E-1, paymentAttemptId=A-1,
     bookingId=B-100, eventType=PAYMENT_FAILED

E-2: eventId=E-2, paymentAttemptId=A-2,
     bookingId=B-100, eventType=PAYMENT_FAILED
```

E-1 and E-2 are not Kafka duplicates. They are separate business facts:

- A-1 may fail because of an issuer decline.
- The customer retries with A-2.
- A-2 may fail for a different reason, at a different time, with different
  causation and operational consequences.

The composite key accepts E-1 and rejects E-2 because both map to
`(B-100, PAYMENT_FAILED)`. Treating E-2 as a duplicate loses history and may suppress
required state-machine or compensation decisions.

Similarly, multiple same-type events can arise from:

- multiple payment attempts;
- a payment provider's new event after manual review;
- authorization, capture, and asynchronous reversal workflows represented under an
  overly broad event type;
- replay from a corrected upstream source where a new event supersedes an earlier
  fact;
- cancellation or reservation-release attempts initiated by distinct commands;
- producer reprocessing that intentionally emits a new event with new causation;
- split or partial payments;
- an administrative retry after an earlier terminal outcome was reversed.

Business rules may reject some events based on aggregate state, but the inbox must
first identify them correctly. Deduplication must not silently convert a new event
into a redelivery.

## Long-term design: producer-scoped event identity

The durable inbox key should be:

```text
UNIQUE (producer, event_id)
```

`eventId` is generated once by the producing service, persisted with its outgoing
outbox row, and reused unchanged for every Kafka retry or republish. `producer`
namespaces IDs and protects against accidental collisions between services.

Booking should insert the inbox event and later processing status using this key.
When the same `(producer,eventId)` arrives again, it is a Kafka redelivery regardless
of topic partition, offset, or delivery count. A different event ID is a new event
that must be recorded, even if booking ID and event type match an older row.

Recommended database shape:

```sql
CREATE TABLE inbound_event (
    id                  BIGSERIAL PRIMARY KEY,
    producer            VARCHAR(100) NOT NULL,
    event_id            VARCHAR(100) NOT NULL,
    event_type          VARCHAR(100) NOT NULL,
    schema_version      INTEGER NOT NULL,
    booking_id          VARCHAR(60),
    payment_attempt_id  VARCHAR(100),
    payload             JSONB NOT NULL,
    status              VARCHAR(30) NOT NULL,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    UNIQUE (producer, event_id)
);
```

Indexes on `(status,updated_at)`, `booking_id`, and
`(booking_id,event_type)` remain useful for polling and diagnostics, but they are not
idempotency constraints.

## Recommended event envelope

Every cross-service event should carry a stable envelope:

```json
{
  "eventId": "E-2",
  "eventType": "PAYMENT_FAILED",
  "schemaVersion": 1,
  "producer": "payment-service",
  "occurredAt": "2026-08-22T07:15:30.123Z",
  "bookingId": "B-100",
  "reservationId": "R-100",
  "paymentAttemptId": "A-2",
  "causationId": "CMD-PAY-A-2",
  "correlationId": "B-100",
  "reason": {
    "code": "ISSUER_DECLINED",
    "message": "Issuer declined the payment attempt"
  }
}
```

Field semantics:

| Field | Purpose |
|---|---|
| `eventId` | Immutable identity of this business event; reused on delivery retries |
| `eventType` | Routing and schema discriminator |
| `schemaVersion` | Explicit payload evolution contract |
| `producer` | Event-ID namespace and operational owner |
| `occurredAt` | Time the business fact occurred, not consumer receipt time |
| `bookingId` | Booking aggregate correlation |
| `reservationId` | Stable Platform reservation affected by compensation |
| `paymentAttemptId` | Distinguishes A-1 from A-2 and later attempts |
| `causationId` | Command or preceding event that directly caused this event |
| `correlationId` | End-to-end workflow or saga identifier |
| `reason` | Structured machine-readable code plus diagnostic description |

The current `InboundEvent` exposes only `bookingId` and `eventType`:
`src/main/java/com/forvmom/MomentForeverBooking/events/InboundEvent.java:3-5`.
`PaymentFailedEvent` has a failure reason and error code but no event or attempt
identity:
`src/main/java/com/forvmom/MomentForeverBooking/events/PaymentFailedEvent.java:13-19`.
`PaymentProcessedEvent` has a transaction ID but no common event ID:
`src/main/java/com/forvmom/MomentForeverBooking/events/PaymentProcessedEvent.java:14-21`.

## API Gateway versus service ownership

The API Gateway is the policy enforcement point for inbound technical trace and
request-correlation context, but it cannot own durable workflow identity or event
idempotency on behalf of downstream services. The service that commits a business
change must also commit the identifiers needed to recover and explain that change.
Those responsibilities cross different lifetimes:

- an HTTP request and its OpenTelemetry spans are transient;
- a booking workflow may continue asynchronously for minutes or days;
- Kafka and Quartz may process work after the original trace has ended;
- a service can restart between inbox persistence and retry;
- dead-letter processing may occur under a new operational trace;
- delayed or replayed events may no longer have useful in-memory tracing context.

The architecture should therefore carry two complementary forms of observability:

1. **OpenTelemetry with W3C Trace Context** for technical request, producer, consumer,
   and job-execution spans;
2. **durable business correlation and causation** persisted in commands, inboxes,
   outboxes, and event envelopes.

They should be searchable together, but they are not interchangeable.

### Gateway responsibilities

At the public HTTP boundary, the Gateway should:

1. accept and validate a W3C `traceparent` header or create a new trace context, then
   propagate the Gateway span's outbound W3C context rather than blindly copying
   untrusted input;
2. accept a valid `X-Correlation-ID` or generate a new opaque correlation ID;
3. validate both headers for syntax, length, and allowed characters rather than
   forwarding unbounded client input;
4. forward `traceparent`, optional `tracestate`, and `X-Correlation-ID` to Platform;
5. return `X-Correlation-ID` in the HTTP response so callers and support teams can
   reference the workflow;
6. accept, validate, and forward an `Idempotency-Key` when the endpoint supports
   command deduplication.

The Gateway may reject malformed values or replace them according to a documented
policy. It must not reuse an HTTP idempotency key as a trace ID, and it must not
assume that correlation alone makes a command idempotent. Gateway validation is
syntactic and protective; Platform remains authoritative for caller scoping, request
fingerprinting, duplicate detection, retention, and replaying the stored result.

### `Idempotency-Key` is a command concern

`Idempotency-Key` protects an HTTP command from repeated execution, for example when
a client retries `POST /bookings` after a timeout. Platform owns the durable mapping
between the authenticated caller, operation, idempotency key, request fingerprint,
and stored response or command result.

Two requests with the same key and same request fingerprint should resolve to the
same command result. Reusing a key with a materially different request should be
rejected. The key's retention period and caller scope must be explicit.

This key is distinct from:

- `traceparent`, which identifies a technical distributed trace;
- `correlationId`, which groups the end-to-end business workflow;
- `eventId`, which identifies one immutable event;
- `causationId`, which links an event to its direct command or parent event.

When a new booking command has an `Idempotency-Key`, Platform may represent the
durable command identity as a namespaced value such as
`http-command:customer-42:POST-bookings:IK-900`. That command identity can be the
first event's `causationId`. It should not copy an unscoped raw client key into a
global identity namespace.

### Platform responsibilities

Platform is the first business service and owns the durable handoff from HTTP to
Kafka. In one local database transaction it must:

1. claim or re-read the HTTP `Idempotency-Key`;
2. create or update the reservation/booking command state;
3. persist `correlationId` with that business state;
4. generate the `BOOKING_REQUESTED` `eventId` exactly once;
5. persist the complete `BOOKING_REQUESTED` envelope and payload in its outbox.

Kafka publication retries must reuse the persisted event ID, correlation ID,
causation ID, and payload. A retry must not generate another event identity.
Committing only the business row and adding identifiers later is insufficient:
a crash between those steps would leave state that cannot be reliably joined to its
outbox event or support trace.

For a gateway-originated request, Platform uses the validated forwarded correlation
ID. For internal tools, tests, scheduled work, or any non-gateway caller that omits
one, Platform generates the fallback correlation ID at the service boundary and
persists it before creating the outbox event. Platform must not require the Gateway
to be present for correctness.

For the first `BOOKING_REQUESTED` event:

- `eventId` is a new producer-owned ID;
- `correlationId` is the durable workflow ID;
- `causationId` is the durable HTTP command identity, derived from the scoped
  idempotency key when one exists, or from a generated command ID otherwise.

### Booking and Payment responsibilities

Booking persists the incoming `correlationId`, `eventId`, and `causationId` with its
inbox row before early Kafka acknowledgement. Any outgoing event derived from that
inbound event:

- keeps the same `correlationId`;
- receives a new event ID generated and persisted once with the outgoing outbox;
- sets `causationId` to the direct parent inbound `eventId`.

Payment follows the same rule. For example, `PAYMENT_REQUESTED` causes either a
`PAYMENT_PROCESSED` or `PAYMENT_FAILED` event:

```text
PAYMENT_PROCESSED.causationId = PAYMENT_REQUESTED.eventId
PAYMENT_FAILED.causationId    = PAYMENT_REQUESTED.eventId
```

If Payment creates a new attempt after a separate retry command, that command or
event is the direct cause. The new result must carry a new event ID and its actual
parent ID rather than pointing to an older attempt merely because the booking and
correlation IDs match.

Quartz retries reuse the stored envelope. Quartz must never create a new
`correlationId`, `eventId`, or causation link for the same logical event. A Quartz
execution can create a new technical span linked to the persisted trace metadata,
but the durable business lineage remains unchanged.

### Why gateway-only tracing is insufficient

A Gateway log proves that an HTTP request entered the system. It does not prove
which asynchronous events resulted from it or what happened after the request
returned.

Gateway-only tracing fails to provide durable lineage when:

- Kafka delivers after the HTTP trace has completed;
- a producer retries an outbox row after restart;
- Booking acknowledges early and Quartz later resumes a `FAILED` inbox row;
- the original process and OpenTelemetry span context no longer exist;
- a record reaches a DLT and is replayed under a different trace;
- delayed processing starts a new consumer or scheduler trace;
- the same booking has multiple payment attempts and same-type events;
- sampling removes some technical spans.

W3C context should still be propagated through instrumented Kafka headers where
possible, and OpenTelemetry should create producer, consumer, and Quartz spans.
However, correctness and auditability must rely on persisted `correlationId`,
`causationId`, and `eventId`, not on the availability of a trace backend.
After a restart, delay, DLT transfer, or replay, the new execution may continue valid
W3C context when retained, or start a new trace and use an OpenTelemetry span link to
the prior context. Neither choice changes the persisted business identifiers.

### Identifier responsibility table

| Identifier | Created or accepted by | Persisted by | Propagation and purpose |
|---|---|---|---|
| `traceparent` | Gateway accepts a valid W3C value or creates one; each service/consumer/job creates child spans | Observability backend; optionally selected trace metadata in durable records for diagnostics | Technical distributed tracing. Propagate in HTTP and Kafka headers when possible; a later Quartz or DLT execution may start a new trace with a link |
| `correlationId` | Gateway accepts/creates it; Platform creates a fallback for non-gateway callers | Platform business state and outbox; every service inbox/outbox and event envelope | Stable end-to-end business workflow identity. Returned as `X-Correlation-ID` and propagated unchanged |
| `causationId` | The service creating a command/event sets it to the direct parent command ID or event ID | Every outgoing outbox and event envelope; preferably inbox for audit | Reconstructs the immediate causal chain. Changes at each derived event |
| `eventId` | The event-producing service generates it exactly once in the same transaction as its outbox row | Producer outbox and consumer inboxes | Immutable identity of one business event. Reused for every publication retry and deduplicated with `producer` |
| `Idempotency-Key` | HTTP client supplies it; Gateway validates/forwards it; Platform scopes and claims it | Platform command-idempotency store with request fingerprint and result | Prevents duplicate execution of an HTTP command. It is not a trace, correlation, or event ID |

### End-to-end example

Assume a client submits a booking command:

```http
POST /bookings
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
X-Correlation-ID: CORR-700
Idempotency-Key: IK-900
```

1. **Gateway** validates the headers, forwards them to Platform, and returns
   `X-Correlation-ID: CORR-700`. Its outbound `traceparent` represents a child span
   in the same technical trace; it need not be byte-for-byte identical to the
   client's inbound header.
2. **Platform** scopes `IK-900` to the caller and operation. In one transaction it
   creates reservation R-100, persists `CORR-700`, and inserts:

   ```json
   {
     "eventId": "EV-BR-1",
     "eventType": "BOOKING_REQUESTED",
     "producer": "platform-service",
     "correlationId": "CORR-700",
     "causationId": "http-command:customer-42:POST-bookings:IK-900",
     "bookingId": "B-100",
     "reservationId": "R-100"
   }
   ```

3. **Booking** inserts `(platform-service, EV-BR-1)` into its inbox before early
   acknowledgement. OpenTelemetry creates a Kafka consumer span from the propagated
   messaging context, while the durable envelope remains the source of business
   lineage. Booking's `PAYMENT_REQUESTED` outbox event is:

   ```json
   {
     "eventId": "EV-PR-1",
     "eventType": "PAYMENT_REQUESTED",
     "producer": "booking-service",
     "correlationId": "CORR-700",
     "causationId": "EV-BR-1",
     "bookingId": "B-100",
     "reservationId": "R-100",
     "paymentAttemptId": "A-1"
   }
   ```

4. **Payment** persists EV-PR-1 in its inbox. An issuer decline creates:

   ```json
   {
     "eventId": "EV-PF-1",
     "eventType": "PAYMENT_FAILED",
     "producer": "payment-service",
     "correlationId": "CORR-700",
     "causationId": "EV-PR-1",
     "bookingId": "B-100",
     "reservationId": "R-100",
     "paymentAttemptId": "A-1"
   }
   ```

5. **Booking** consumes EV-PF-1 and emits `BOOKING_FAILED` with a new event ID,
   unchanged `CORR-700`, and `causationId: EV-PF-1`.
6. **Platform** deduplicates that event by `(booking-service,eventId)` and applies
   R-100's guarded `RESERVED -> RELEASED` effect once.

If Booking crashes and Quartz resumes processing, it reuses the same persisted IDs.
The Quartz execution has a new technical span and may link to stored or propagated
trace context, but the event lineage remains:

```text
HTTP command IK-900
  -> EV-BR-1 BOOKING_REQUESTED
  -> EV-PR-1 PAYMENT_REQUESTED
  -> EV-PF-1 PAYMENT_FAILED
  -> EV-BF-1 BOOKING_FAILED

correlationId = CORR-700 throughout
```

## Temporary fallback: Kafka coordinates

If producers cannot add stable event IDs immediately, Booking can temporarily use:

```text
UNIQUE (topic, partition, offset)
```

This accurately deduplicates repeated delivery of one Kafka record because its
coordinates remain stable on redelivery. The listener must persist topic, partition,
and offset from Kafka headers in the same pre-ack inbox transaction.

This is only a bridge:

- producer retries may create a new Kafka record with a new offset for the same
  logical event;
- republishing to another topic or cluster changes identity;
- retention and replay procedures complicate long-lived auditing;
- coordinates do not travel naturally across service boundaries;
- they cannot express causation or distinguish event identity from transport
  identity.

Do not derive a permanent event ID from mutable payload serialization.

## Platform requires dual idempotency

Event-level deduplication alone does not guarantee business-effect idempotency.
Platform should use two guards when consuming `BOOKING_FAILED`.

### 1. Inbox event deduplication

Insert `(producer,eventId)` into a Platform inbox with a unique constraint. Apply the
event only if that insert wins. The inbox insert and business changes must commit in
one local database transaction.

This prevents Kafka redelivery of E-1 from executing the handler twice.

### 2. Reservation-release effect idempotency

Apply a conditional state transition:

```text
BookingReservation: RESERVED -> RELEASED
```

Only the transaction that observes `RESERVED` may release capacity. A duplicate or
later event observing `RELEASED` is a successful no-op, not another decrement or
release.

Also persist a unique business-effect key, for example:

```text
UNIQUE (reservation_id, effect_type)
where effect_type = RESERVATION_RELEASE
```

or a dedicated release record whose `reservation_id` is unique. This protects the
capacity effect even if two distinct events, E-1/A-1 and E-2/A-2, both legitimately
reach Platform for the same reservation.

The two layers address different failure modes:

- inbox uniqueness stops the same event from being handled twice;
- reservation state plus effect uniqueness stops different events from applying the
  same irreversible business effect twice.

## Decision table

| Scenario | Same business event? | Immediate composite key | Long-term `(producer,eventId)` | Required business behavior |
|---|---:|---|---|---|
| Same Kafka record redelivered | Yes | Dedupes if booking/type unchanged | Dedupes correctly | Return existing inbox result; no repeated effect |
| Producer republishes E-1 to a new offset | Yes | Usually dedupes, but by coincidence | Dedupes correctly | No repeated effect |
| B-100 `BOOKING_REQUESTED`, then `PAYMENT_PROCESSED` | No | Accepts after composite fix | Accepts | Process confirmation once |
| B-100 `BOOKING_REQUESTED`, then `PAYMENT_FAILED` | No | Accepts after composite fix | Accepts | Process failure once |
| E-1/A-1 and E-2/A-2 are both `PAYMENT_FAILED` | No | Incorrectly rejects E-2 | Accepts both | Record both; state machine decides effect |
| Two consumers race to insert E-1 | Yes | One insert wins; loser must re-read | One insert wins; loser must re-read | No error-shaped duplicate handling |
| E-1 and E-2 both request release of R-100 | No | Inbox key is insufficient | Both events are accepted | Platform releases R-100 only once |
| Producer ID collision across services | Usually no | Cannot distinguish | `producer` namespace distinguishes | Record independently |
| Legacy event has no `eventId` | Unknown | Composite key is lossy | Use Kafka coordinates temporarily | Migrate producer envelope |

## Recommended tests

### Booking persistence and acknowledgement

1. Insert `BOOKING_REQUESTED` for B-100, then insert `PAYMENT_PROCESSED`; assert both
   inbox rows exist under the immediate composite constraint.
2. Repeat with `PAYMENT_FAILED`.
3. Deliver the same event twice; assert one inbox identity and one business effect.
4. Race two inserts of the same identity; assert one wins and the other re-reads the
   row without surfacing a generic persistence success.
5. Inject an inbox commit failure; assert Kafka is not acknowledged.
6. Persist an inbox row, fail processing after acknowledgement, run Quartz, and
   assert processing resumes from the stored payload.
7. Assert Quartz cannot select an event absent from the inbox, documenting why
   pre-ack persistence is mandatory.

### Event identity and payment attempts

8. Deliver E-1/A-1 `PAYMENT_FAILED`, then redeliver E-1; assert only one inbox event.
9. Deliver E-2/A-2 `PAYMENT_FAILED` for the same B-100; assert a second inbox event is
   stored even if the booking state machine performs no additional transition.
10. Republish E-1 at a new Kafka offset; assert stable `eventId` dedupes it.
11. Deliver identical event IDs from two producer namespaces; assert they remain
    distinct.
12. Validate required envelope fields and reject unsupported `schemaVersion` values
    before acknowledgement, with an explicit dead-letter policy.

### Temporary Kafka-coordinate identity

13. Redeliver the same topic/partition/offset; assert one inbox row.
14. Deliver a new offset with the same payload; assert it is not falsely claimed to
    be the same transport record.

### Platform dual idempotency

15. Deliver E-1 twice; assert one Platform inbox row and one
    `RESERVED -> RELEASED` transition.
16. Deliver E-1/A-1 and E-2/A-2 for the same reservation; assert two inbox rows but
    one reservation-release effect.
17. Race two Platform handlers; assert the conditional state update and unique
    release-effect constraint prevent double capacity release.
18. Crash after the release transaction commits but before Kafka acknowledgement;
    redeliver and assert both idempotency layers make the retry a no-op.

## Recommendation

Apply `UNIQUE(booking_ref_id,event_type)` as the immediate production repair because
it aligns the schema with current code and unblocks the normal booking-to-payment
event sequence. Handle concurrent duplicate insertion through an atomic upsert or a
narrow unique-conflict re-read.

Treat that repair as transitional. Adopt a producer-owned immutable event envelope
and `UNIQUE(producer,event_id)` across service inboxes. On Platform, combine event
deduplication with the guarded `RESERVED -> RELEASED` transition and a unique
reservation-release effect. This preserves Booking's early-ack plus Quartz model
while making Kafka redelivery harmless and retaining distinct payment-attempt
history.
