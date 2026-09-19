package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.service.BookingStatusTransitionService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PaymentProcessedProcessor  implements InboundBookingEventProcessor  {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedProcessor.class);


    private final BookingStatusTransitionService bookingStatusTransitionService;
    private final OutgoingOutboxService outgoingOutboxService;

    public PaymentProcessedProcessor(
            BookingStatusTransitionService bookingStatusTransitionService,
            OutgoingOutboxService outgoingOutboxService) {
        this.bookingStatusTransitionService = bookingStatusTransitionService;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    @Override
    public OutgoingOutboxRecord process(InboundEvent inboundEvent) {

        Booking booking = bookingStatusTransitionService
                .transitionPendingBookingToConfirmed(inboundEvent.getBookingId())
                .orElse(null);
        if (booking == null) {
            return null;
        }

        BookingConfirmedEvent bookingConfirmedEvent = (BookingConfirmedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        booking, EventConstants.BOOKING_CONFIRMED, inboundEvent);

        OutgoingOutboxRecord outgoingOutboxRecord = outgoingOutboxService.createRecord(
                inboundEvent.getBookingId(), EventConstants.BOOKING_CONFIRMED, bookingConfirmedEvent);
        log.info("Booking confirmation outbox created: bookingId={}, eventType={}",
                inboundEvent.getBookingId(), EventConstants.BOOKING_CONFIRMED);
        return outgoingOutboxRecord;
    }

    @Override
    public String getSupportedEventType() {
        return EventConstants.PAYMENT_PROCESSED;
    }
}
