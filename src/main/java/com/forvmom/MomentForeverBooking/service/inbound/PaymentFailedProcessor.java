package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.events.PaymentFailedEvent;
import com.forvmom.MomentForeverBooking.service.BookingStatusTransitionService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedProcessor implements InboundBookingEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedProcessor.class);

    private static final String DEFAULT_FAILURE_REASON = "Payment failed";

    private final BookingStatusTransitionService bookingStatusTransitionService;
    private final OutgoingOutboxService outgoingOutboxService;

    public PaymentFailedProcessor(
            BookingStatusTransitionService bookingStatusTransitionService,
            OutgoingOutboxService outgoingOutboxService) {
        this.bookingStatusTransitionService = bookingStatusTransitionService;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    @Override
    public OutgoingOutboxRecord process(InboundEvent inboundEvent) {
        String failureReason = resolveFailureReason((PaymentFailedEvent) inboundEvent);
        Booking booking = bookingStatusTransitionService
                .transitionPendingBookingToFailed(inboundEvent.getBookingId(), failureReason)
                .orElse(null);
        if (booking == null) {
            return null;
        }

        BookingFailedEvent bookingFailedEvent = (BookingFailedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        booking, EventConstants.BOOKING_FAILED, inboundEvent);
        bookingFailedEvent.setFailureReason(failureReason);

        OutgoingOutboxRecord outgoingOutboxRecord = outgoingOutboxService.createRecord(
                inboundEvent.getBookingId(), EventConstants.BOOKING_FAILED, bookingFailedEvent);
        log.warn("Booking failure outbox created: bookingId={}, eventType={}, failureReason={}",
                inboundEvent.getBookingId(), EventConstants.BOOKING_FAILED, failureReason);
        return outgoingOutboxRecord;
    }

    private String resolveFailureReason(PaymentFailedEvent paymentFailedEvent) {
        if (hasText(paymentFailedEvent.getFailureReason())) {
            return paymentFailedEvent.getFailureReason().trim();
        }
        if (hasText(paymentFailedEvent.getErrorCode())) {
            return DEFAULT_FAILURE_REASON + " (" + paymentFailedEvent.getErrorCode().trim() + ")";
        }
        return DEFAULT_FAILURE_REASON;
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public String getSupportedEventType() {
        return EventConstants.PAYMENT_FAILED;
    }
}
