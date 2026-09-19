package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessingTransactionService;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InboundOutboxRetryServiceTest {

    @Test
    void missingOutgoingRecordUsesAtomicProcessingService() throws Exception {
        RetryFixture fixture = retryFixture();
        InboundOutbox inboundOutbox = failedInboundOutbox(fixture.objectMapper);
        OutgoingOutboxRecord outgoingOutboxRecord = outgoingRecord(OutgoingOutboxRecord.STATUS_PENDING);
        when(fixture.inboundOutboxDao.findByStatusInAndUpdatedAtBefore(
                any(), any(LocalDateTime.class))).thenReturn(List.of(inboundOutbox));
        when(fixture.outgoingOutboxDao.findByBookingIdAndEventType(
                "B-100", EventConstants.BOOKING_CONFIRMED)).thenReturn(Optional.empty());
        when(fixture.transactionService.processInboundEventAtomically(
                org.mockito.ArgumentMatchers.eq(inboundOutbox),
                any(PaymentProcessedEvent.class))).thenReturn(outgoingOutboxRecord);

        fixture.retryService.retryStuckAndFailedRecords();

        verify(fixture.transactionService).processInboundEventAtomically(
                org.mockito.ArgumentMatchers.eq(inboundOutbox),
                any(PaymentProcessedEvent.class));
        verify(fixture.outgoingOutboxPublisher).trySinglePublish(outgoingOutboxRecord);
    }

    @Test
    void existingSentOutgoingMarksInboundProcessedAndSkipsPublish() throws Exception {
        RetryFixture fixture = retryFixture();
        InboundOutbox inboundOutbox = failedInboundOutbox(fixture.objectMapper);
        OutgoingOutboxRecord outgoingOutboxRecord = outgoingRecord(OutgoingOutboxRecord.STATUS_SENT);
        when(fixture.inboundOutboxDao.findByStatusInAndUpdatedAtBefore(
                any(), any(LocalDateTime.class))).thenReturn(List.of(inboundOutbox));
        when(fixture.outgoingOutboxDao.findByBookingIdAndEventType(
                "B-100", EventConstants.BOOKING_CONFIRMED))
                .thenReturn(Optional.of(outgoingOutboxRecord));

        fixture.retryService.retryStuckAndFailedRecords();

        verify(fixture.transactionService).markInboundEventAsProcessed(inboundOutbox);
        verify(fixture.transactionService, never()).processInboundEventAtomically(
                org.mockito.ArgumentMatchers.any(InboundOutbox.class),
                org.mockito.ArgumentMatchers.any());
        verify(fixture.outgoingOutboxPublisher, never()).trySinglePublish(outgoingOutboxRecord);
    }

    private RetryFixture retryFixture() {
        InboundOutboxDao inboundOutboxDao = mock(InboundOutboxDao.class);
        InboundOutboxService inboundOutboxService = mock(InboundOutboxService.class);
        OutboxDeadLetterHandler deadLetterHandler = mock(OutboxDeadLetterHandler.class);
        OutgoingOutboxDao outgoingOutboxDao = mock(OutgoingOutboxDao.class);
        OutgoingOutboxPublisher outgoingOutboxPublisher = mock(OutgoingOutboxPublisher.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        InboundEventProcessingTransactionService transactionService =
                mock(InboundEventProcessingTransactionService.class);
        InboundOutboxRetryService retryService = new InboundOutboxRetryService(
                inboundOutboxDao,
                inboundOutboxService,
                deadLetterHandler,
                outgoingOutboxDao,
                outgoingOutboxPublisher,
                objectMapper,
                transactionService);
        return new RetryFixture(
                inboundOutboxDao,
                outgoingOutboxDao,
                outgoingOutboxPublisher,
                objectMapper,
                transactionService,
                retryService);
    }

    private InboundOutbox failedInboundOutbox(ObjectMapper objectMapper) throws Exception {
        PaymentProcessedEvent event = new PaymentProcessedEvent();
        event.setBookingId("B-100");
        event.setEventType(EventConstants.PAYMENT_PROCESSED);
        InboundOutbox inboundOutbox = new InboundOutbox();
        inboundOutbox.setBookingReferenceId("B-100");
        inboundOutbox.setEventType(EventConstants.PAYMENT_PROCESSED);
        inboundOutbox.setPayload(objectMapper.writeValueAsString(event));
        inboundOutbox.setStatus(EventConstants.FAILED);
        return inboundOutbox;
    }

    private OutgoingOutboxRecord outgoingRecord(String status) {
        OutgoingOutboxRecord outgoingOutboxRecord = new OutgoingOutboxRecord();
        outgoingOutboxRecord.setBookingId("B-100");
        outgoingOutboxRecord.setEventType(EventConstants.BOOKING_CONFIRMED);
        outgoingOutboxRecord.setStatus(status);
        return outgoingOutboxRecord;
    }
}
