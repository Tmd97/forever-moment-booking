package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import com.forvmom.MomentForeverBooking.service.alerts.AlertService;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OutgoingOutboxPublisherFailureTest {

    @Test
    void failedImmediateSendIsMarkedFailedAndNeverOverwrittenSent() throws Exception {
        BookingEventProducer eventProducer = mock(BookingEventProducer.class);
        OutgoingOutboxService outgoingOutboxService = mock(OutgoingOutboxService.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        OutgoingOutboxPublisher publisher = new OutgoingOutboxPublisher(
                mock(OutgoingOutboxDao.class),
                eventProducer,
                objectMapper,
                mock(AlertService.class),
                outgoingOutboxService,
                mock(OutgoingOutboxDeadLetterHandler.class));
        PaymentRequestedEvent event = new PaymentRequestedEvent();
        event.setBookingId("B-100");
        OutgoingOutboxRecord outgoingOutboxRecord = new OutgoingOutboxRecord();
        outgoingOutboxRecord.setBookingId("B-100");
        outgoingOutboxRecord.setEventType(EventConstants.PAYMENT_REQUESTED);
        outgoingOutboxRecord.setStatus(OutgoingOutboxRecord.STATUS_PENDING);
        outgoingOutboxRecord.setPayload(objectMapper.writeValueAsString(event));
        doThrow(new RuntimeException("Kafka unavailable"))
                .when(eventProducer).sendPaymentRequestedEvent(any(PaymentRequestedEvent.class));

        publisher.trySinglePublish(outgoingOutboxRecord);

        verify(outgoingOutboxService).markAsFailed(outgoingOutboxRecord);
        verify(outgoingOutboxService, never()).markAsSent(outgoingOutboxRecord);
    }
}
