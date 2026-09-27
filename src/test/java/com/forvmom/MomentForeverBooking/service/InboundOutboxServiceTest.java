package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InboundOutboxServiceTest {

    private InboundOutboxDao dao;
    private InboundOutboxService service;

    @BeforeEach
    void setUp() {
        dao = mock(InboundOutboxDao.class);
        service = new InboundOutboxService(dao, Collections.emptyList());
        when(dao.save(any(InboundOutbox.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void sameProducerAndEventIdReturnsExistingInboxRecord() {
        BookingRequestEvent event = event("B-100", "producer-a", "event-1");
        InboundOutbox existing = new InboundOutbox();
        when(dao.findByProducerAndEventId("producer-a", "event-1"))
                .thenReturn(Optional.of(existing));

        InboundOutbox result = service.findOrCreateForEvent(event);

        assertSame(existing, result);
        verify(dao, never()).save(any(InboundOutbox.class));
    }

    @Test
    void differentEventIdsForSameBookingCreateDistinctInboxRecords() {
        BookingRequestEvent first = event("B-100", "producer-a", "event-1");
        BookingRequestEvent second = event("B-100", "producer-a", "event-2");

        InboundOutbox firstRecord = service.findOrCreateForEvent(first);
        InboundOutbox secondRecord = service.findOrCreateForEvent(second);

        assertNotSame(firstRecord, secondRecord);
        assertEquals("event-1", firstRecord.getEventId());
        assertEquals("event-2", secondRecord.getEventId());
        verify(dao).findByProducerAndEventId("producer-a", "event-1");
        verify(dao).findByProducerAndEventId("producer-a", "event-2");
    }

    @Test
    void sameEventIdFromDifferentProducersCreatesDistinctInboxRecords() {
        InboundOutbox first = service.findOrCreateForEvent(
                event("B-100", "producer-a", "event-1"));
        InboundOutbox second = service.findOrCreateForEvent(
                event("B-100", "producer-b", "event-1"));

        assertNotSame(first, second);
        assertEquals("producer-a", first.getProducer());
        assertEquals("producer-b", second.getProducer());
    }

    @Test
    void legacyEventFallsBackToBookingAndEventType() {
        BookingRequestEvent event = event("B-legacy", null, null);

        service.findOrCreateForEvent(event);

        verify(dao).findByBookingReferenceIdAndEventType(
                "B-legacy", "BOOKING_REQUESTED");
        verify(dao, never()).findByProducerAndEventId(any(), any());
    }

    @Test
    void partialIdentityIsRejected() {
        BookingRequestEvent event = event("B-100", "producer-a", null);

        assertThrows(
                IllegalArgumentException.class,
                () -> service.findOrCreateForEvent(event));
        verify(dao, never()).save(any(InboundOutbox.class));
    }

    @Test
    void blankIdentityIsStoredAsLegacyNulls() {
        BookingRequestEvent event = event("B-legacy", " ", "");

        InboundOutbox result = service.findOrCreateForEvent(event);

        assertNull(result.getProducer());
        assertNull(result.getEventId());
        verify(dao).findByBookingReferenceIdAndEventType(
                "B-legacy", "BOOKING_REQUESTED");
    }

    private BookingRequestEvent event(
            String bookingId,
            String producer,
            String eventId
    ) {
        BookingRequestEvent event = new BookingRequestEvent();
        event.setBookingId(bookingId);
        event.setEventType("BOOKING_REQUESTED");
        event.setProducer(producer);
        event.setEventId(eventId);
        return event;
    }
}
