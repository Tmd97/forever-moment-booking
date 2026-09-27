package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.*;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessingTransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Retries failed inbound records by completing committed work or atomically
 * reprocessing rolled-back work.
 */
@Service
public class InboundOutboxRetryService {

    private static final Logger log = LoggerFactory.getLogger(InboundOutboxRetryService.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 2;


    // Mapping from inbound event type to the expected outgoing event type
    private static final Map<String, String> OUTGOING_EVENT_TYPE_MAP = Map.of(
            EventConstants.BOOKING_REQUESTED, EventConstants.PAYMENT_REQUESTED,
            EventConstants.PAYMENT_PROCESSED, EventConstants.BOOKING_CONFIRMED,
            EventConstants.PAYMENT_FAILED, EventConstants.BOOKING_FAILED
    );

    // Mapping from inbound event type to the corresponding event class for deserialization
    private static final Map<String, Class<? extends InboundEvent>> EVENT_CLASS_MAP = Map.of(
            EventConstants.BOOKING_REQUESTED, BookingRequestEvent.class,
            EventConstants.PAYMENT_PROCESSED, PaymentProcessedEvent.class,
            EventConstants.PAYMENT_FAILED, PaymentFailedEvent.class
    );

    private final InboundOutboxDao inboundOutboxDao;
    private final InboundOutboxService inboundOutboxService;
    private final OutboxDeadLetterHandler deadLetterHandler;
    private final OutgoingOutboxDao outgoingOutboxDao;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;
    private final ObjectMapper objectMapper;
    private final InboundEventProcessingTransactionService inboundEventProcessingTransactionService;

    public InboundOutboxRetryService(InboundOutboxDao inboundOutboxDao,
                                     InboundOutboxService inboundOutboxService,
                                     OutboxDeadLetterHandler deadLetterHandler,
                                     OutgoingOutboxDao outgoingOutboxDao,
                                     OutgoingOutboxPublisher outgoingOutboxPublisher,
                                     ObjectMapper objectMapper,
                                     InboundEventProcessingTransactionService
                                             inboundEventProcessingTransactionService) {

        this.inboundOutboxDao = inboundOutboxDao;
        this.inboundOutboxService = inboundOutboxService;
        this.deadLetterHandler = deadLetterHandler;
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
        this.objectMapper = objectMapper;
        this.inboundEventProcessingTransactionService = inboundEventProcessingTransactionService;
    }

    /**
     * Called by Quartz every 2 minutes.
     * Finds incoming inboundOutbox records stuck in PROCESSING or FAILED and retries them.
     */
    public void retryStuckAndFailedRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);

        // check if it is incoming or outgoing record stuck, if incoming, we have to
        // re-run the enrichment and publish flow, if outgoing, we only need to
        // re-publish, no enrichment needed as it is already enriched before

        List<InboundOutbox> stuck = inboundOutboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(EventConstants.PENDING, EventConstants.FAILED, EventConstants.PROCESSING),
                cutoff);

        if (!stuck.isEmpty()) {
            log.info("Incoming inboundOutbox poller found {} stuck record(s) to retry", stuck.size());
        }

        for (InboundOutbox inboundOutbox : stuck) {
            if (inboundOutbox.getRetryCount() >= MAX_RETRIES) {
                deadLetterHandler.handleDeadRecord(inboundOutbox);
                continue;
            }
            inboundOutboxService.incrementRetry(inboundOutbox);
            String bookingId = inboundOutbox.getBookingReferenceId();
            String eventType = inboundOutbox.getEventType();

            // Determine expected outgoing event type
            String outgoingEventType = OUTGOING_EVENT_TYPE_MAP.get(eventType);
            if (outgoingEventType == null) {
                log.warn("No outgoing event mapping for event type: {}", eventType);
                inboundOutboxService.markAsFailed(inboundOutbox);
                continue;
            }
            try {
                OutgoingOutboxRecord outgoingOutboxRecord = handleIncomingEventRetry(inboundOutbox, bookingId, eventType, outgoingEventType);
                if (outgoingOutboxRecord != null &&
                        !OutgoingOutboxRecord.STATUS_SENT.equals(outgoingOutboxRecord.getStatus())) {
                    outgoingOutboxPublisher.trySinglePublish(outgoingOutboxRecord);
                }
            } catch (Exception e) {
                log.warn("Retry failed for bookingId={}, eventType={}, error: {}", bookingId, eventType, e.getMessage());
                inboundOutboxService.markAsFailed(inboundOutbox);
            }
        }
    }

    private OutgoingOutboxRecord handleIncomingEventRetry(InboundOutbox inboundOutbox, String bookingId, String eventType, String outgoingEventType) throws JsonProcessingException {
        // Branch A: Check if outgoing record already exists
        Optional<OutgoingOutboxRecord> existingOutgoing = outgoingOutboxDao.findByBookingIdAndEventType(
                bookingId, outgoingEventType);
        if (existingOutgoing.isPresent()) {
            inboundEventProcessingTransactionService.markInboundEventAsProcessed(inboundOutbox);
            return existingOutgoing.get();
        }

        // Branch B: No outgoing record, re-run the full process
        log.info("[Branch-B] No outgoing record for bookingId={} — re-running inbound processing", bookingId);
        Class<? extends InboundEvent> inboundEventClass = EVENT_CLASS_MAP.get(eventType);
        if (inboundEventClass == null) {
            throw new IllegalArgumentException("No inbound event class mapping for event type: " + eventType);
        }
        InboundEvent inboundEvent = objectMapper.readValue(inboundOutbox.getPayload(), inboundEventClass);
        return inboundEventProcessingTransactionService.processInboundEventAtomically(
                inboundOutbox, inboundEvent);
    }
}