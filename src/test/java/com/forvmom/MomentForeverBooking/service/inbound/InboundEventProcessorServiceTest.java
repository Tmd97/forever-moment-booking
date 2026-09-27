package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InboundEventProcessorServiceTest {

    @Test
    void durableInboxAndAcknowledgmentPrecedeAtomicProcessingAndPublishingFollowsIt() {
        InboundOutboxService inboundOutboxService = mock(InboundOutboxService.class);
        InboundEventProcessingTransactionService transactionService =
                mock(InboundEventProcessingTransactionService.class);
        OutgoingOutboxPublisher outgoingOutboxPublisher = mock(OutgoingOutboxPublisher.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        InboundEventProcessorService processorService = new InboundEventProcessorService(
                inboundOutboxService, transactionService, outgoingOutboxPublisher);
        PaymentProcessedEvent inboundEvent = paymentProcessedEvent();
        InboundOutbox inboundOutbox = new InboundOutbox();
        OutgoingOutboxRecord outgoingOutboxRecord = new OutgoingOutboxRecord();
        when(inboundOutboxService.findOrCreateForEvent(inboundEvent)).thenReturn(inboundOutbox);
        when(transactionService.processInboundEventAtomically(inboundOutbox, inboundEvent))
                .thenReturn(outgoingOutboxRecord);

        processorService.processEvent(inboundEvent, acknowledgment);

        InOrder processingOrder = inOrder(
                inboundOutboxService, acknowledgment, transactionService, outgoingOutboxPublisher);
        processingOrder.verify(inboundOutboxService).findOrCreateForEvent(inboundEvent);
        processingOrder.verify(acknowledgment).acknowledge();
        processingOrder.verify(inboundOutboxService).markAsProcessing(inboundOutbox);
        processingOrder.verify(transactionService)
                .processInboundEventAtomically(inboundOutbox, inboundEvent);
        processingOrder.verify(outgoingOutboxPublisher).trySinglePublish(outgoingOutboxRecord);
    }

    @Test
    void transactionFailureMarksInboundFailedAndDoesNotPublish() {
        InboundOutboxService inboundOutboxService = mock(InboundOutboxService.class);
        InboundEventProcessingTransactionService transactionService =
                mock(InboundEventProcessingTransactionService.class);
        OutgoingOutboxPublisher outgoingOutboxPublisher = mock(OutgoingOutboxPublisher.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        InboundEventProcessorService processorService = new InboundEventProcessorService(
                inboundOutboxService, transactionService, outgoingOutboxPublisher);
        PaymentProcessedEvent inboundEvent = paymentProcessedEvent();
        InboundOutbox inboundOutbox = new InboundOutbox();
        when(inboundOutboxService.findOrCreateForEvent(inboundEvent)).thenReturn(inboundOutbox);
        when(transactionService.processInboundEventAtomically(inboundOutbox, inboundEvent))
                .thenThrow(new RuntimeException("transaction failed"));

        processorService.processEvent(inboundEvent, acknowledgment);

        InOrder failureOrder = inOrder(transactionService, inboundOutboxService);
        failureOrder.verify(transactionService)
                .processInboundEventAtomically(inboundOutbox, inboundEvent);
        failureOrder.verify(inboundOutboxService).markAsFailed(inboundOutbox);
        verify(outgoingOutboxPublisher, never()).trySinglePublish(
                org.mockito.ArgumentMatchers.any(OutgoingOutboxRecord.class));
    }

    private PaymentProcessedEvent paymentProcessedEvent() {
        PaymentProcessedEvent event = new PaymentProcessedEvent();
        event.setBookingId("B-100");
        event.setEventType(EventConstants.PAYMENT_PROCESSED);
        event.setProducer("payment-service");
        event.setEventId("payment-1");
        return event;
    }
}
