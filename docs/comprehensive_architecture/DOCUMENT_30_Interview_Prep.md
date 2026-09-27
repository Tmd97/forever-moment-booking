# DOCUMENT 30: SYSTEM DESIGN INTERVIEW PREPARATION

*Role: FAANG Senior Engineer & System Design Interviewer*
*Context: Derived from the Forever Moment Booking Service Architecture*

---

## PART A - BEGINNER QUESTIONS

**Q1: How would you design a simple booking service?**
*Ideal Answer:* I would separate the system into at least three components: Catalog/Inventory, Booking, and Payment. The Booking Service would act as a state machine. The database would have a `Bookings` table with states like `PENDING`, `CONFIRMED`, and `FAILED`. A client initiates a booking, we reserve the seat, the booking enters `PENDING`, we request payment, and upon success, we transition to `CONFIRMED`.

**Q2: How would you prevent duplicate bookings?**
*Ideal Answer:* By implementing Idempotency Keys. When the frontend sends a booking request, it includes a unique UUID. The backend stores this UUID with a unique constraint in the database. If the user double-clicks the button, the second request fails the database unique constraint, preventing the system from processing the same intent twice.

**Q3: How would you handle payment failures?**
*Ideal Answer:* I would ensure the Booking Service listens for Payment Failure events. When received, the service updates the booking status to `FAILED`. Critically, it must then trigger a compensation action to tell the Inventory service to release the reserved seat so someone else can buy it.

**Q4: How would you maintain booking states?**
*Ideal Answer:* By using a strictly defined State Machine in the code (e.g., `PENDING` -> `CONFIRMED` or `FAILED`). Transitions must be protected by database transactions. I would not allow invalid jumps (like going from `FAILED` back to `CONFIRMED`).

---

## PART B - MID LEVEL QUESTIONS

**Q1: Design an event-driven booking architecture. How do the services communicate?**
*Ideal Answer:* I would use an asynchronous, event-driven model using a broker like Kafka. 
1. The Platform Service reserves inventory and publishes a `BookingRequestEvent`.
2. The Booking Service consumes it, creates a `PENDING` booking, and publishes a `PaymentRequestedEvent`.
3. The Payment Service processes the charge and publishes a `PaymentProcessedEvent`.
4. The Booking Service consumes it and updates the state to `CONFIRMED`.
This choreography minimizes synchronous HTTP coupling, increasing overall system availability.

**Q2: How do you handle retries safely when a downstream service fails?**
*Ideal Answer:* Retries must be bounded and delayed. I would implement a retry counter and a backoff strategy. Crucially, the receiving endpoint *must* be idempotent. If the Booking Service retries sending a payment request, the Payment Service must recognize the same `bookingId` or `eventId` and not charge the credit card twice.

**Q3: Design a compensation workflow.**
*Ideal Answer:* Distributed systems lack 2-Phase Commit (2PC). If step 3 (Payment) fails, we must undo step 1 (Inventory). 
When Payment fails, the Booking Service sets status to `FAILED` and publishes a `BookingFailedEvent`. The Platform Service consumes this event and executes a compensating transaction (adding +1 back to available inventory). If the compensation fails, it must be retried until it succeeds, or pushed to a Dead Letter Queue for manual intervention to prevent a capacity leak.

---

## PART C - SENIOR ENGINEER QUESTIONS

**Q1: How would you guarantee booking consistency across services? (Explain Inbox vs Outbox)**
*Detailed Answer:* To achieve eventual consistency, I would use the Transactional Outbox and Inbox patterns. 
- **Outbox:** When the Booking Service updates state to `PENDING`, it cannot synchronously publish to Kafka. Instead, in the same PostgreSQL transaction, it writes the `PaymentRequestedEvent` to an `outgoing_outbox` table. A background worker (Quartz) polls this table and pushes to Kafka. This solves the "Dual-Write" problem.
- **Inbox:** When receiving `PaymentProcessedEvent` from Kafka, the Booking Service writes the `event_id` to an `inbound_outbox` table. If Kafka re-delivers the message, the unique constraint on the Inbox table blocks the duplicate. This guarantees Exactly-Once *Processing* semantics over At-Least-Once *Delivery* infrastructure.

**Q2: Explain Worker Fencing and Lease-Based Ownership.**
*Detailed Answer:* When multiple Quartz background workers poll the `outgoing_outbox` table, they might select the same `PENDING` row and send it to Kafka twice. 
To prevent this, we use Lease-Based Ownership. A worker executes an atomic SQL update: 
`UPDATE outbox SET owner_node = 'pod-1', lease_expires = NOW() + 30s WHERE status = 'PENDING' AND owner_node IS NULL`. 
The worker has "fenced" this row for 30 seconds. If `pod-1` crashes (OOM), the lease expires. Another pod can safely claim it after 30 seconds.

**Q3: Explain why exactly-once delivery is mostly a myth.**
*Detailed Answer & Tradeoffs:* Distributed networks drop ACKs. If a producer sends a message to Kafka, Kafka writes it, but the ACK is lost in the network, the producer *must* retry, resulting in a duplicate. Kafka's idempotent producer feature solves this at the broker level, but at the *application* level (Service A to Service B), you must design for At-Least-Once delivery. The true goal is Exactly-Once *Processing*, achieved via idempotency keys (Inbox pattern).

**Q4: Explain State Machine protection.**
*Detailed Answer:* If a `PaymentProcessed` and `PaymentFailed` event arrive at the exact same microsecond due to upstream weirdness, they race to update the database. We protect the state machine using JPA Optimistic Locking (`@Version`). The first thread transitions to `CONFIRMED` and increments the version. The second thread attempts to update to `FAILED`, sees the version changed, throws an `OptimisticLockingFailureException`, and rolls back safely.

---

## PART D - STAFF / PRINCIPAL QUESTIONS

**Q1: How would you scale this architecture to 1 million bookings per minute?**
*Architecture Answer:* A single PostgreSQL database handling Inbox/Outbox polling via Quartz will melt under 1M TPS due to severe lock contention and IOPS limits. 
- **Eliminate Quartz:** I would replace Quartz polling with Debezium/Kafka Connect using Change Data Capture (CDC). Debezium tails the Postgres Write-Ahead Log (WAL) and streams outbox inserts directly to Kafka with sub-millisecond latency and zero polling overhead.
- **Partitioning:** I would shard the Postgres database by `bookingId` hash. 
- **Redis Dedup:** Move the Inbox deduplication check to a Redis Cluster using `SETNX` with a TTL, falling back to DB only if Redis misses.

**Q2: How would you handle active-active multi-region failover?**
*Architecture Answer:* Active-Active requires conflict resolution. If US-East and EU-West both accept a booking for the same inventory slot, we have a split-brain. 
To fix this, I would pin Inventory ownership to a single region per experience (e.g., Paris tours are mastered in EU-West). 
If EU-West goes down, US-East can accept bookings but places them in a "Deferred" state. When EU-West recovers, it processes the backlog. Kafka topics would be mirrored via MirrorMaker2.

**Q3: How would you design auditability?**
*Architecture Answer:* Instead of just updating `status` and `@Version`, I would introduce **Event Sourcing** for the Booking Aggregate. Every state change (`INITIATED`, `PAYMENT_REQUESTED`, `CONFIRMED`) is stored as an immutable event in an Append-Only Event Store. The current state is just a projection (fold) of those events. This provides a mathematically perfect audit trail for customer disputes.

---

## PART E - DEEP DIVE QUESTIONS SPECIFIC TO THIS REPOSITORY

**Q1: Why was `producer` included in deduplication alongside `event_id`?**
*Implementation Answer:* In `InboundOutbox.java`, the unique constraint is `(producer, event_id)`. Event IDs (like UUIDs) are only guaranteed unique within the context of the service that generated them. By combining the producer name (e.g., `PAYMENT_SERVICE`) with the `event_id`, we create a globally unique deduplication key, preventing accidental collisions if two separate teams use bad UUID generators.

**Q2: Why does `InboundOutboxRetryService` use a 2-minute Grace Period?**
*Implementation Answer:* To prevent active worker interference. If a Kafka listener thread is currently executing the business transaction, inserting the Outbox row, and committing, that process might take 1 second. If the Quartz Retry job sweeps instantly, it might grab a row that is actively being processed but hasn't fully committed yet, causing DB deadlocks or duplicate work. The 2-minute grace period ensures Quartz only touches truly stuck/dead transactions.

**Q3: Why is the `inbound_outbox` insert in a separate, isolated transaction?**
*Implementation Answer:* In `InboundOutboxService.findOrCreateForEvent`, the transaction propagation is `REQUIRED`, but it is called *before* the main business logic. It commits immediately to lock the identity in the DB. If we combined the inbox insert and the business logic into one massive transaction, and the business logic threw an exception, the entire transaction (including the inbox insert) would roll back. Kafka would retry the message, and we'd execute the failing business logic in an infinite loop. By committing the inbox early, we trap the message and allow Quartz to handle retries cleanly.

---

## PART F - COMMON INTERVIEW MISTAKES

### Topic: The Outbox Pattern
**Question:** How do you reliably send an email after a database update?
**Incorrect Answer:** I update the database, commit the transaction, and then call the Email API (or `kafkaTemplate.send()`).
**Why Incorrect:** If the JVM crashes after the DB commit but before the Email API call, the email is lost forever.
**Correct Answer:** Write the DB update and a new record to an `Outbox` table in the *same* database transaction. A separate process reads the Outbox and sends the email.
**Tradeoffs:** Introduces latency (the email isn't sent instantly) and requires background polling infrastructure.

### Topic: Compensation (Saga Pattern)
**Question:** How do you handle a failed step in a distributed transaction?
**Incorrect Answer:** I use 2-Phase Commit (2PC) or XA Transactions across my microservices.
**Why Incorrect:** 2PC locks resources across network boundaries. If one service is slow, all services lock up. It scales terribly in modern microservice architectures.
**Correct Answer:** Use the Saga Pattern. Each local transaction publishes an event. If a downstream transaction fails, publish a compensation event that triggers upstream services to execute a "rollback" action (e.g., releasing inventory).
**Tradeoffs:** Eventual consistency. There is a window where the system is in an inconsistent state (inventory is held, but booking is failing) until the compensation completes.

### Topic: Retries
**Question:** A downstream REST API is returning 500s. How do you handle retries?
**Incorrect Answer:** I put it in a `while(true)` loop and retry until it succeeds.
**Why Incorrect:** This creates a cascading failure (Retry Storm). You will DDOS the failing service and bring it down completely.
**Correct Answer:** Use Exponential Backoff with Jitter. Start with a 1-second delay, then 2, 4, 8, etc. Add random jitter so all instances don't retry at the exact same millisecond. Implement a Circuit Breaker so if it fails 5 times, you stop trying and fail fast.