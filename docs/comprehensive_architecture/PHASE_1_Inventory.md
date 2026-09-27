# PHASE 1: Booking Microservice - Repository Evidence Inventory

## 1. Repository Evidence Inventory
**Location:** `C:\work_new\forever-moment-booking-exp`
**Build Tool:** Maven (`pom.xml`)
**Framework:** Spring Boot 3.1.5, Java 17
**Key Dependencies:** `spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-boot-starter-quartz`, `spring-kafka`, `postgresql`
**Application Entry Point:** `MomentForeverBookingApplication.java`

**Core Implementation Files Discovered:**
- **Entities:** `Booking.java`, `BookingAddon.java`, `InboundOutbox.java`, `OutgoingOutboxRecord.java`, `OutboxRecord.java`
- **Enums:** `BookingStatus`, `PricingLevel`
- **Consumers:** `BookingRequestConsumer`, `PaymentFailedConsumer`, `PaymentProcessedConsumer`
- **Producers:** `BookingEventProducer`
- **Schedulers:** `OutboxCleanupJob`, `OutboxRetriesJob`, `OutgoingOutboxCleanupJob`, `OutgoingOutboxPublisherJob`
- **Services:** `BookingService`, `BookingStatusTransitionService`, `InboundEventProcessingTransactionService`, `InboundOutboxRetryService`, `OutgoingOutboxPublisher`
- **DAOs:** `BookingRepository`, `InboundOutboxDao`, `OutgoingOutboxDao`

## 2. Package and Component Catalogue
**Root Package:** `com.forvmom.MomentForeverBooking`
- `.api`: REST Controllers (e.g., `BookingAdminController`) and Exception Handlers.
- `.commons`: Shared constants (`EventConstants`) and utilities (`OutboundEventGenerator`).
- `.config`: Configuration for Kafka (`BookingKafkaConfig`), Quartz (`QuartzConfig`), and Swagger.
- `.consumer`: Kafka listeners intercepting cross-service domain events.
- `.domain.entity`: Aggregate roots (e.g. `Booking`) and transactional outbox/inbox persistence models.
- `.domain.enums`: Domain states like `BookingStatus`.
- `.dto.events`: The actual Kafka payload definitions (`BaseEvent`, `BookingRequestEvent`, `PaymentRequestedEvent`, etc.).
- `.mapper`: Bean mapping utilities (`BookingMapper`).
- `.producer`: Kafka dispatchers (`BookingEventProducer`).
- `.repository`: Spring Data JPA DAOs for database interaction.
- `.scheduler`: Quartz `Job` implementations for asynchronous processing and recovery.
- `.service`: Business logic, transactional boundaries, and state-machine transitions.

## 3. Terminology Glossary
- **Booking Aggregate (`Booking.java`):** The primary domain model owned by this service tracking a user's purchase intent lifecycle.
- **Inbound Outbox (Inbox):** A database table (`inbound_outbox`) used for event deduplication to ensure exactly-once processing of consumed Kafka messages.
- **Outgoing Outbox:** A transactional outbox table (`outgoing_outbox` / `outbox_record`) ensuring atomicity between database state changes and Kafka event publishing.
- **BookingStatus:** The state machine for the aggregate (`INITIATED`, `CONFIRMED`, `FAILED`, etc.).
- **Compensation:** The act of reverting a prior successful action (e.g., releasing inventory in Platform via a `BookingFailedEvent`).

## 4. Events Inventory
**Inbound (Consumed) Events:**
- `BookingRequestEvent` (Consumed by `BookingRequestConsumer`)
- `PaymentProcessedEvent` (Consumed by `PaymentProcessedConsumer`)
- `PaymentFailedEvent` (Consumed by `PaymentFailedConsumer`)

**Outbound (Produced) Events:**
- `PaymentRequestedEvent` (Instructs Payment Service)
- `BookingConfirmedEvent` (Final success state broadcast)
- `BookingFailedEvent` (Instructs Platform to release capacity / triggers compensation)

## 5. Tables Inventory
*(Inferred directly from JPA Entities)*
- `bookings`: Central aggregate state. Contains an `@Version` column for optimistic locking and indexes on `user_id`, `status`, and `experience_id`.
- `booking_addons`: One-to-many relationship with bookings.
- `inbound_outbox`: Dedicated to idempotency. Unique constraints on `(producer, event_id)` and `(booking_ref_id, event_type)`.
- `outgoing_outbox` / `outbox_record`: Stores payloads waiting to be pushed to Kafka.

## 6. Scheduler Inventory
All schedulers are Quartz-based (`org.quartz.Job`):
- `OutgoingOutboxPublisherJob`: Polls the outgoing outbox for PENDING messages and attempts Kafka delivery.
- `OutgoingOutboxCleanupJob`: Purges old/completed outgoing events to prevent table bloat.
- `OutboxRetriesJob`: Handles retry logic for stuck or failed outgoing payloads.
- `OutboxCleanupJob`: Handles cleanup of inbound outbox processing logs.

## 7. Initial Service Ownership Assessment
**Owned by Booking Service:**
- The `Booking` aggregate and its state machine lifecycle.
- Coordination of the payment flow (dispatching `PaymentRequestedEvent`).
- Safely deduplicating incoming events (`InboundOutbox`).
- Reliably publishing outgoing lifecycle events (`OutgoingOutboxRecord`).
- Deciding terminal states (Confirmed vs Failed).

**NOT Owned by Booking Service (Delegated):**
- **Inventory & Capacity:** Reserved by Platform prior to the `BookingRequestEvent`, released by Platform upon receiving a `BookingFailedEvent`.
- **Payment Processing:** Delegated entirely to Payment Service.

## 8. List of Unresolved Cross-Repository Dependencies
*(Awaiting Phase 2-6 deep dives or multi-repo verification)*
1. **Topic Alignments:** Verify that the topics configured in `BookingKafkaConfig` perfectly match those produced/consumed by Platform and Payment services.
2. **Payment Service Contract:** Does Payment Service explicitly consume `PaymentRequestedEvent` and produce `PaymentProcessedEvent`/`PaymentFailedEvent` in the exact schema `Booking` expects?
3. **Platform Compensation:** Does Platform actively listen for `BookingFailedEvent` to execute the capacity release logic? (Assumed yes, but must be verified cross-repo).
4. **Event Identity Continuity:** Are `event_id` and `correlation_id` passed continuously across all 3 services for tracing?