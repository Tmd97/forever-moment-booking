package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InboundEventProcessingTransactionServiceTest {

    @Test
    void processingSuccessReturnsOutgoingRecordThenMarksExactInboundRecordProcessed() {
        InboundEventProcessorRegistry processorRegistry = mock(InboundEventProcessorRegistry.class);
        InboundOutboxService inboundOutboxService = mock(InboundOutboxService.class);
        InboundBookingEventProcessor processor = mock(InboundBookingEventProcessor.class);
        InboundEventProcessingTransactionService transactionService =
                new InboundEventProcessingTransactionService(processorRegistry, inboundOutboxService);
        InboundOutbox inboundOutbox = new InboundOutbox();
        PaymentProcessedEvent inboundEvent = paymentProcessedEvent();
        OutgoingOutboxRecord outgoingOutboxRecord = new OutgoingOutboxRecord();
        when(processorRegistry.getProcessor(EventConstants.PAYMENT_PROCESSED)).thenReturn(processor);
        when(processor.process(inboundEvent)).thenReturn(outgoingOutboxRecord);

        OutgoingOutboxRecord result =
                transactionService.processInboundEventAtomically(inboundOutbox, inboundEvent);

        assertSame(outgoingOutboxRecord, result);
        InOrder processingOrder = inOrder(processor, inboundOutboxService);
        processingOrder.verify(processor).process(inboundEvent);
        processingOrder.verify(inboundOutboxService).markAsProcessed(inboundOutbox);
    }

    @Test
    void processorFailurePropagatesWithoutMarkingInboundRecordProcessed() {
        InboundEventProcessorRegistry processorRegistry = mock(InboundEventProcessorRegistry.class);
        InboundOutboxService inboundOutboxService = mock(InboundOutboxService.class);
        InboundBookingEventProcessor processor = mock(InboundBookingEventProcessor.class);
        InboundEventProcessingTransactionService transactionService =
                new InboundEventProcessingTransactionService(processorRegistry, inboundOutboxService);
        InboundOutbox inboundOutbox = new InboundOutbox();
        PaymentProcessedEvent inboundEvent = paymentProcessedEvent();
        RuntimeException processingFailure = new RuntimeException("outgoing outbox save failed");
        when(processorRegistry.getProcessor(EventConstants.PAYMENT_PROCESSED)).thenReturn(processor);
        when(processor.process(inboundEvent)).thenThrow(processingFailure);

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> transactionService.processInboundEventAtomically(inboundOutbox, inboundEvent));

        assertSame(processingFailure, thrown);
        verify(inboundOutboxService, never()).markAsProcessed(inboundOutbox);
    }

    private PaymentProcessedEvent paymentProcessedEvent() {
        PaymentProcessedEvent event = new PaymentProcessedEvent();
        event.setBookingId("B-100");
        event.setEventType(EventConstants.PAYMENT_PROCESSED);
        return event;
    }
}
