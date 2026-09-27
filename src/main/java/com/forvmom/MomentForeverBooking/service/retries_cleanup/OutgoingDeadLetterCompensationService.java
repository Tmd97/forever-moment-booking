package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.service.BookingStatusTransitionService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class OutgoingDeadLetterCompensationService {

    static final String PAYMENT_REQUEST_FAILURE_REASON =
            "Payment request could not be delivered after max retries";

    private final BookingStatusTransitionService bookingStatusTransitionService;
    private final OutgoingOutboxService outgoingOutboxService;

    public OutgoingDeadLetterCompensationService(
            BookingStatusTransitionService bookingStatusTransitionService,
            OutgoingOutboxService outgoingOutboxService) {
        this.bookingStatusTransitionService = bookingStatusTransitionService;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    @Transactional
    public Optional<OutgoingOutboxRecord> compensateDeadPaymentRequest(
            OutgoingOutboxRecord paymentRequestRecord) {
        outgoingOutboxService.markAsDead(paymentRequestRecord);

        Optional<Booking> failedBooking =
                bookingStatusTransitionService.transitionPendingBookingToFailed(
                        paymentRequestRecord.getBookingId(),
                        PAYMENT_REQUEST_FAILURE_REASON);
        if (failedBooking.isEmpty()) {
            return Optional.empty();
        }

        BookingFailedEvent failureEvent =
                OutboundEventGenerator.buildDeadPaymentRequestFailure(
                        failedBooking.get(),
                        PAYMENT_REQUEST_FAILURE_REASON,
                        "outgoing-outbox:" + paymentRequestRecord.getId());
        return Optional.of(outgoingOutboxService.createRecord(
                paymentRequestRecord.getBookingId(),
                EventConstants.BOOKING_FAILED,
                failureEvent));
    }
}
