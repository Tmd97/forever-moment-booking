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
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutgoingIdentityRetryTest {

    @Test
    void persistedPayloadReusesIdentityAcrossPublishAttempts() throws Exception {
        BookingEventProducer producer = mock(BookingEventProducer.class);
        OutgoingOutboxDao outboxDao = mock(OutgoingOutboxDao.class);
        when(outboxDao.save(any(OutgoingOutboxRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        OutgoingOutboxService outboxService = new OutgoingOutboxService(outboxDao);
        OutgoingOutboxPublisher publisher = new OutgoingOutboxPublisher(
                outboxDao,
                producer,
                new ObjectMapper().findAndRegisterModules(),
                mock(AlertService.class),
                outboxService,
                mock(OutgoingOutboxDeadLetterHandler.class));

        PaymentRequestedEvent event = new PaymentRequestedEvent();
        event.setBookingId("B-100");
        event.setEventId("booking-event-1");
        event.setProducer("booking-service");
        event.setSchemaVersion(1);
        event.setOccurredAt(Instant.parse("2026-08-22T09:00:00Z"));
        event.setCorrelationId("booking-flow-1");
        event.setCausationId("platform-event-1");
        event.setEventType(EventConstants.PAYMENT_REQUESTED);

        OutgoingOutboxRecord record = outboxService.createRecord(
                "B-100", EventConstants.PAYMENT_REQUESTED, event);

        publisher.trySinglePublish(record);
        publisher.trySinglePublish(record);

        ArgumentCaptor<PaymentRequestedEvent> eventCaptor =
                ArgumentCaptor.forClass(PaymentRequestedEvent.class);
        verify(producer, times(2)).sendPaymentRequestedEvent(eventCaptor.capture());
        assertEquals("booking-event-1", eventCaptor.getAllValues().get(0).getEventId());
        assertEquals("booking-event-1", eventCaptor.getAllValues().get(1).getEventId());
        assertEquals("platform-event-1", eventCaptor.getAllValues().get(1).getCausationId());
        verify(outboxDao, times(3)).save(record);
    }
}
