package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.service.BookingService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import com.forvmom.MomentForeverBooking.service.alerts.AlertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Handles Kafka publishing for outgoing outbox records.
 *
 * <p>
 * Two entry points:
 * <ol>
 * <li>{@link #trySinglePublish(OutgoingOutboxRecord)} — called by consumers
 * immediately after the DB transaction commits. No transaction is opened
 * here; if the send fails the record stays PENDING for Quartz to retry.</li>
 * <li>{@link #publishPendingEvents()} — called by Quartz every minute to retry
 * any PENDING or FAILED records that missed the immediate publish.</li>
 * </ol>
 *
 * <p>
 * Both paths share the same {@link #sendToKafka} routing logic so there is
 * no duplication.
 */
@Service
public class OutgoingOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxPublisher.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 1;

    private final OutgoingOutboxDao outgoingOutboxDao;
    private final BookingEventProducer eventProducer;
    private final ObjectMapper objectMapper;
    private final AlertService alertService;
    private final OutgoingOutboxService outgoingOutboxService;
    private final OutgoingOutboxDeadLetterHandler outgoingOutboxDeadLetterHandler;


    @Autowired
    private BookingService bookingService;

    public OutgoingOutboxPublisher(OutgoingOutboxDao outgoingOutboxDao,
                                   BookingEventProducer eventProducer,
                                   ObjectMapper objectMapper,
                                   AlertService alertService,
                                   OutgoingOutboxService outgoingOutboxService,
                                      OutgoingOutboxDeadLetterHandler outgoingOutboxDeadLetterHandler
    ) {
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
        this.alertService = alertService;
        this.outgoingOutboxService = outgoingOutboxService;
        this.outgoingOutboxDeadLetterHandler = outgoingOutboxDeadLetterHandler;
    }

    // ── Immediate publish (called by consumers, outside TX) ───────────────────

    /**
     * Attempts a single Kafka publish for the given outbox record.
     * <p>
     * <b>No transaction is opened.</b> The caller must ensure the record is
     * already committed in DB before calling this.
     * <ul>
     * <li>Success → marks SENT (small independent TX via
     * {@link OutgoingOutboxService})</li>
     * <li>Failure → logs warning; record stays PENDING for Quartz retry</li>
     * </ul>
     *
     * @param record the outgoing outbox record to publish (must be non-null)
     */
    public void trySinglePublish(OutgoingOutboxRecord record) {
        if (record == null)
            return; // idempotent caller may pass null on duplicate
        try {
            sendToKafka(record);
            outgoingOutboxService.markAsSent(record);
            log.info("Immediate publish succeeded: outboxId={}, type={}, bookingId={}",
                    record.getId(), record.getEventType(), record.getBookingId());
        } catch (Exception e) {
            log.warn("Immediate publish failed — record id={} stays PENDING for Quartz retry: {}",
                    record.getId(), e.getMessage());
            outgoingOutboxService.markAsFailed(record);
            // Do NOT rethrow — DB state is the source of truth, Quartz will retry
        }
    }

    /**
     * Convenience overload: looks up an existing record by (bookingId, eventType)
     * and calls {@link #trySinglePublish(OutgoingOutboxRecord)}.
     * Used by {@link InboundOutboxRetryService} for the common retry case where the record
     * already exists — avoiding re-opening a full transaction.
     */
    public void trySinglePublishByBookingAndType(String bookingId, String eventType) {
        Optional<OutgoingOutboxRecord> opt = outgoingOutboxDao
                .findByBookingIdAndEventType(bookingId, eventType);
        if (opt.isEmpty()) {
            log.warn("trySinglePublishByBookingAndType: no record found for bookingId={}, type={}", bookingId,
                    eventType);
            return;
        }
        OutgoingOutboxRecord record = opt.get();
        if (OutgoingOutboxRecord.STATUS_SENT.equals(record.getStatus())) {
            log.debug("Record id={} already SENT — skipping", record.getId());
            return;
        }
        trySinglePublish(record);
    }

    // ── Quartz batch retry (every minute) ─────────────────────────────────────

    /**
     * Called by Quartz every minute to retry PENDING / FAILED outgoing records.
     * Picks up records updated more than {@value GRACE_PERIOD_MINUTES} minute(s)
     * ago
     * to avoid racing the immediate-publish path.
     */
    public void publishPendingEvents() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);
        List<OutgoingOutboxRecord> pending = outgoingOutboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(OutgoingOutboxRecord.STATUS_PENDING, OutgoingOutboxRecord.STATUS_FAILED),
                cutoff);

        if (!pending.isEmpty()) {
            log.info("Quartz outbox poller found {} record(s) to retry", pending.size());
        }

        for (OutgoingOutboxRecord record : pending) {
            if (record.getRetryCount() >= MAX_RETRIES) {
                outgoingOutboxDeadLetterHandler.handleDeadRecord(record);
                continue;
            }
            try {
                outgoingOutboxService.incrementRetry(record);
                sendToKafka(record);
                outgoingOutboxService.markAsSent(record);
                log.info("Quartz retry succeeded: id={}, type={}, bookingId={}",
                        record.getId(), record.getEventType(), record.getBookingId());
            } catch (Exception e) {
                log.error("Quartz retry failed: id={}, type={}, bookingId={}: {}",
                        record.getId(), record.getEventType(), record.getBookingId(), e.getMessage());
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void sendToKafka(OutgoingOutboxRecord record) throws Exception {
        String type = record.getEventType();
        String json = record.getPayload();

        switch (type) {

            case EventConstants.PAYMENT_REQUESTED -> {
                PaymentRequestedEvent event = objectMapper.readValue(json, PaymentRequestedEvent.class);
                eventProducer.sendPaymentRequestedEvent(event);
            }
            case EventConstants.BOOKING_CONFIRMED -> {
                BookingConfirmedEvent event = objectMapper.readValue(json, BookingConfirmedEvent.class);
                eventProducer.sendBookingConfirmedEvent(event);
            }
            case EventConstants.BOOKING_FAILED -> {
                BookingFailedEvent event = objectMapper.readValue(json, BookingFailedEvent.class);
                eventProducer.sendBookingFailedEvent(event);
            }
            default -> log.warn("Unknown outgoing event type: {}", type);
        }
    }
}