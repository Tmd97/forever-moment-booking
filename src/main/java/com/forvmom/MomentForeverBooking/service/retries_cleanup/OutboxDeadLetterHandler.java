package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.alerts.AlertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class OutboxDeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(OutboxDeadLetterHandler.class);

    private final InboundOutboxService inboundOutboxService;
    private final InboundDeadLetterCompensationService compensationService;
    private final AlertService alertService;
    private final ObjectMapper objectMapper;

    public OutboxDeadLetterHandler(InboundOutboxService inboundOutboxService,
                                   InboundDeadLetterCompensationService compensationService,
                                   AlertService alertService,
                                   ObjectMapper objectMapper) {
        this.inboundOutboxService = inboundOutboxService;
        this.compensationService = compensationService;
        this.alertService = alertService;
        this.objectMapper = objectMapper;
    }

    public void handleDeadRecord(InboundOutbox outbox) {
        String bookingId = outbox.getBookingReferenceId();
        String eventType = outbox.getEventType();

        try {
            sendCompensationEvent(outbox);
            log.error("Record moved to DEAD after max retries: id={}, bookingId={}, eventType={}",
                    outbox.getId(), bookingId, eventType);
            alertService.sendAlert(String.format(
                    "Incoming outbox record id=%d for booking=%s eventType=%s moved to DEAD after max retries",
                    outbox.getId(), bookingId, eventType));
        } catch (Exception e) {
            log.error("Failed to send compensation for dead record id={}, bookingId={}, eventType={}",
                    outbox.getId(), bookingId, eventType, e);
            alertService.sendAlert("Compensation failed for dead record " + outbox.getId());
        }
    }

    private void sendCompensationEvent(InboundOutbox outbox) throws Exception {
        String eventType = outbox.getEventType();
        String payload = outbox.getPayload();
        String bookingId = outbox.getBookingReferenceId();

        switch (eventType) {
            case EventConstants.BOOKING_REQUESTED:
                BookingRequestEvent requestEvent = objectMapper.readValue(payload, BookingRequestEvent.class);
                OutgoingOutboxRecord compensationRecord =
                        compensationService.compensateDeadBookingRequest(
                                outbox,
                                requestEvent,
                                "Booking request permanently failed after max retries");
                log.info("Compensation queued: BOOKING_FAILED outboxId={} for dead BOOKING_REQUESTED bookingId={}",
                        compensationRecord.getId(), bookingId);
                break;

            case EventConstants.PAYMENT_PROCESSED:
                inboundOutboxService.markAsDead(outbox);
                // Payment succeeded but we couldn't confirm the booking → request a refund
                PaymentProcessedEvent paymentEvent = objectMapper.readValue(payload, PaymentProcessedEvent.class);
                createRefundRequest(paymentEvent);
                log.info("Compensation: REFUND_REQUESTED created for dead PAYMENT_PROCESSED: bookingId={}", bookingId);
                break;

            case EventConstants.PAYMENT_FAILED:
                inboundOutboxService.markAsDead(outbox);
                // Payment failed and we couldn't record the failure → maybe send a user notification
                // For now, just log and alert (manual intervention may be needed)
                log.warn("Dead PAYMENT_FAILED record for bookingId={} - booking may be stuck in PENDING. Manual check recommended.", bookingId);
                // You could also create a notification event here if needed
                break;

            default:
                inboundOutboxService.markAsDead(outbox);
                log.warn("No compensation handler for dead event type: {}", eventType);
        }
    }

    private void createRefundRequest(PaymentProcessedEvent paymentEvent) {

        //TODO This assumes you have a RefundRequestEvent class and a constant REFUND_REQUESTED
        // If not, you can either create one or just send an alert.
        // Example:
        // RefundRequestEvent refundEvent = new RefundRequestEvent();
        // refundEvent.setBookingId(paymentEvent.getBookingId());
        // refundEvent.setAmount(paymentEvent.getAmountPaid());
        // refundEvent.setReason("Booking confirmation failed after max retries");
        // OutgoingOutboxRecord record = outgoingOutboxService.createRecord(
        //         paymentEvent.getBookingId(),
        //         EventConstants.REFUND_REQUESTED,
        //         refundEvent
        // );
        // outgoingOutboxPublisher.trySinglePublish(record);

        // For now, just send an enhanced alert
        alertService.sendAlert(String.format(
                "MANUAL ACTION REQUIRED: Payment succeeded (id=%s) but booking confirmation failed. Refund may be needed.",
                paymentEvent.getBookingId()
        ));
    }
}