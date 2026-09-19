package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class InboundEventProcessorService {

    private static final Logger log = LoggerFactory.getLogger(InboundEventProcessorService.class);
    private final InboundOutboxService inboundOutboxService;
    private final InboundEventProcessingTransactionService inboundEventProcessingTransactionService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;

    public InboundEventProcessorService(
            InboundOutboxService inboundOutboxService,
            InboundEventProcessingTransactionService inboundEventProcessingTransactionService,
            OutgoingOutboxPublisher outgoingOutboxPublisher) {
        this.inboundOutboxService = inboundOutboxService;
        this.inboundEventProcessingTransactionService = inboundEventProcessingTransactionService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
    }

    public void processEvent(InboundEvent inboundEvent, Acknowledgment acknowledgment) {
        String bookingId = inboundEvent.getBookingId();
        String eventType = inboundEvent.getEventType();
        log.info("Processing inbound event: {} for bookingId={}, producer={}, eventId={}, correlationId={}",
                eventType, bookingId, inboundEvent.getProducer(), inboundEvent.getEventId(),
                inboundEvent.getCorrelationId());

        // Idempotency guard
        InboundOutbox inboundOutbox = inboundOutboxService.findOrCreateForEvent(inboundEvent);
        if (EventConstants.PROCESSED.equals(inboundOutbox.getStatus())) {
            log.info("Duplicate event ignored (already PROCESSED): {} for bookingId={}", eventType, bookingId);
            acknowledgment.acknowledge();
            return;
        }

        // ACK immediately – we'll rely on retry mechanism if processing fails
        acknowledgment.acknowledge();

        try {
            inboundOutboxService.markAsProcessing(inboundOutbox);
            OutgoingOutboxRecord outgoingOutboxRecord =
                    inboundEventProcessingTransactionService.processInboundEventAtomically(
                            inboundOutbox, inboundEvent);

            // Publish outside the transaction (no DB connection held)
            if (outgoingOutboxRecord != null) {
                outgoingOutboxPublisher.trySinglePublish(outgoingOutboxRecord);
            }
            log.info("Successfully processed inbound event: {} for bookingId={}, producer={}, eventId={}, correlationId={}",
                    eventType, bookingId, inboundEvent.getProducer(), inboundEvent.getEventId(),
                    inboundEvent.getCorrelationId());
        } catch (Exception processingException) {
            log.error("Failed to process inbound event: {} for bookingId={}",
                    eventType, bookingId, processingException);
            inboundOutboxService.markAsFailed(inboundOutbox);
        }
    }
}