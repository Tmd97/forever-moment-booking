package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InboundDeadLetterCompensationService {

    private final InboundOutboxService inboundOutboxService;
    private final OutgoingOutboxService outgoingOutboxService;

    public InboundDeadLetterCompensationService(
            InboundOutboxService inboundOutboxService,
            OutgoingOutboxService outgoingOutboxService) {
        this.inboundOutboxService = inboundOutboxService;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    @Transactional
    public OutgoingOutboxRecord compensateDeadBookingRequest(
            InboundOutbox inboundOutbox,
            BookingRequestEvent requestEvent,
            String failureReason) {
        inboundOutboxService.markAsDead(inboundOutbox);
        BookingFailedEvent failureEvent = (BookingFailedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        null, EventConstants.BOOKING_FAILED, requestEvent);
        failureEvent.setFailureReason(failureReason);
        return outgoingOutboxService.createRecord(
                requestEvent.getBookingId(),
                EventConstants.BOOKING_FAILED,
                failureEvent);
    }
}
