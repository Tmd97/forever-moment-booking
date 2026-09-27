package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import com.forvmom.MomentForeverBooking.service.alerts.AlertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OutgoingOutboxDeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxDeadLetterHandler.class);

    private final OutgoingOutboxService outgoingOutboxService;
    private final OutgoingDeadLetterCompensationService compensationService;
    private final AlertService alertService;
    private final ObjectMapper objectMapper;

    public OutgoingOutboxDeadLetterHandler(OutgoingOutboxService outgoingOutboxService,
                                           OutgoingDeadLetterCompensationService compensationService,
                                           AlertService alertService,
                                           ObjectMapper objectMapper) {
        this.outgoingOutboxService = outgoingOutboxService;
        this.compensationService = compensationService;
        this.alertService = alertService;
        this.objectMapper = objectMapper;
    }

    public void handleDeadRecord(OutgoingOutboxRecord record) {
        try {
            OutgoingOutboxRecord compensationRecord = compensate(record);
            String msg = String.format(
                    "Outgoing outbox record id=%d type=%s bookingId=%s moved to DEAD after max retries",
                    record.getId(), record.getEventType(), record.getBookingId());
            log.error("DEAD: {}", msg);
            alertService.sendAlert(msg);
            if (compensationRecord != null) {
                log.info("BOOKING_FAILED compensation queued: outboxId={}, bookingId={}",
                        compensationRecord.getId(), compensationRecord.getBookingId());
            }
        } catch (Exception e) {
            log.error("Compensation failed for outgoing dead record id={}", record.getId(), e);
            alertService.sendAlert(String.format(
                    "COMPENSATION FAILED for outgoing dead record id=%d - MANUAL INTERVENTION REQUIRED",
                    record.getId()
            ));
        }
    }

    private OutgoingOutboxRecord compensate(OutgoingOutboxRecord record) throws Exception {
        String eventType = record.getEventType();
        String bookingId = record.getBookingId();
        String payload = record.getPayload();

        switch (eventType) {
            case EventConstants.PAYMENT_REQUESTED:
                return compensatePaymentRequested(record);
            case EventConstants.BOOKING_CONFIRMED:
                outgoingOutboxService.markAsDead(record);
                compensateBookingConfirmed(bookingId, payload);
                break;
            case EventConstants.BOOKING_FAILED:
                outgoingOutboxService.markAsDead(record);
                compensateBookingFailed(bookingId, payload);
                break;
            default:
                outgoingOutboxService.markAsDead(record);
                log.warn("No compensation handler for outgoing dead event type: {}", eventType);
        }
        return null;
    }

    private OutgoingOutboxRecord compensatePaymentRequested(OutgoingOutboxRecord record) {
        log.warn("PAYMENT_REQUESTED dead letter for bookingId={} – failing booking and releasing inventory",
                record.getBookingId());
        return compensationService.compensateDeadPaymentRequest(record).orElse(null);
    }

    private void compensateBookingConfirmed(String bookingId, String payload) throws Exception {
        log.warn("BOOKING_CONFIRMED dead letter for bookingId={} – external services not notified", bookingId);
        BookingConfirmedEvent event = objectMapper.readValue(payload, BookingConfirmedEvent.class);

        // Option: Create a manual retry record (the record itself is dead, so we create a new one)
        // OutgoingOutboxRecord retryRecord = outgoingOutboxService.createRecord(bookingId, EventConstants.BOOKING_CONFIRMED, event);
        // log.info("Created manual retry record id={} for dead BOOKING_CONFIRMED", retryRecord.getId());

        alertService.sendAlert(String.format(
                "⚠️ BOOKING_CONFIRMED dead letter: booking %s is confirmed but external systems not notified. Manual intervention may be needed.",
                bookingId
        ));
    }

    private void compensateBookingFailed(String bookingId, String payload) throws Exception {
        log.warn("BOOKING_FAILED dead letter for bookingId={} – external services not notified", bookingId);
        BookingFailedEvent event = objectMapper.readValue(payload, BookingFailedEvent.class);

        // Less critical; just alert
        alertService.sendAlert(String.format(
                "BOOKING_FAILED dead letter: booking %s is failed. Manual check recommended.",
                bookingId
        ));
    }
}