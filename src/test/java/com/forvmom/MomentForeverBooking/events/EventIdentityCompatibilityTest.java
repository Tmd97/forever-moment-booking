package com.forvmom.MomentForeverBooking.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class EventIdentityCompatibilityTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void deserializesPlatformEnvelopeAndTimestampAlias() throws Exception {
        String json = "{"
                + "\"bookingId\":\"B-100\","
                + "\"eventId\":\"platform-event-1\","
                + "\"producer\":\"moment-forever-core\","
                + "\"schemaVersion\":1,"
                + "\"timestamp\":\"2026-08-22T09:00:00Z\","
                + "\"correlationId\":\"booking-flow-1\","
                + "\"causationId\":\"command-1\","
                + "\"eventType\":\"BOOKING_REQUESTED\""
                + "}";

        BookingRequestEvent event = objectMapper.readValue(json, BookingRequestEvent.class);

        assertEquals("platform-event-1", event.getEventId());
        assertEquals("moment-forever-core", event.getProducer());
        assertEquals(Integer.valueOf(1), event.getSchemaVersion());
        assertEquals(Instant.parse("2026-08-22T09:00:00Z"), event.getOccurredAt());
        assertEquals(event.getOccurredAt(), event.getTimestamp());
        assertEquals("booking-flow-1", event.getCorrelationId());
        assertEquals("command-1", event.getCausationId());
        assertEquals("BOOKING_REQUESTED", event.getEventType());
    }

    @Test
    void legacyInboundEventDoesNotInventIdentity() throws Exception {
        BookingRequestEvent event = objectMapper.readValue(
                "{\"bookingId\":\"B-legacy\",\"eventType\":\"BOOKING_REQUESTED\"}",
                BookingRequestEvent.class);

        assertNull(event.getEventId());
        assertNull(event.getProducer());
        assertNull(event.getSchemaVersion());
        assertNull(event.getOccurredAt());
        assertNull(event.getCorrelationId());
        assertNull(event.getCausationId());
    }

    @Test
    void derivedEventGetsOwnedIdentityAndPropagatesContext() {
        BookingRequestEvent inbound = new BookingRequestEvent();
        inbound.setBookingId("B-100");
        inbound.setEventId("platform-event-1");
        inbound.setCorrelationId("booking-flow-1");

        PaymentRequestedEvent derived = (PaymentRequestedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        booking("B-100"), EventConstants.PAYMENT_REQUESTED, inbound);

        assertNotNull(derived.getEventId());
        assertNotEquals(inbound.getEventId(), derived.getEventId());
        assertEquals("booking-service", derived.getProducer());
        assertEquals(Integer.valueOf(1), derived.getSchemaVersion());
        assertNotNull(derived.getOccurredAt());
        assertEquals(derived.getOccurredAt(), derived.getTimestamp());
        assertEquals("booking-flow-1", derived.getCorrelationId());
        assertEquals("platform-event-1", derived.getCausationId());
        assertEquals(EventConstants.PAYMENT_REQUESTED, derived.getEventType());
    }

    @Test
    void legacyDerivedEventKeepsCausationNull() {
        BookingRequestEvent inbound = new BookingRequestEvent();
        inbound.setBookingId("B-legacy");

        PaymentRequestedEvent derived = (PaymentRequestedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        booking("B-legacy"), EventConstants.PAYMENT_REQUESTED, inbound);

        assertNull(derived.getCausationId());
        assertEquals("B-legacy", derived.getCorrelationId());
    }

    @Test
    void bookingFailureCarriesPersistedBookingDate() {
        BookingRequestEvent inbound = new BookingRequestEvent();
        inbound.setBookingId("B-100");
        Booking booking = booking("B-100");
        booking.setBookingDate(LocalDate.of(2026, 8, 22).atStartOfDay());

        BookingFailedEvent derived = (BookingFailedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        booking, EventConstants.BOOKING_FAILED, inbound);

        assertEquals(LocalDate.of(2026, 8, 22), derived.getBookingDate());
    }

    @Test
    void compensationFailureCarriesInboundBookingDate() {
        BookingRequestEvent inbound = new BookingRequestEvent();
        inbound.setBookingId("B-100");
        inbound.setBookingDate(LocalDate.of(2026, 8, 22));
        inbound.setTimeSlotMapperId(300L);
        inbound.setGuestCount(2);

        BookingFailedEvent derived = (BookingFailedEvent)
                OutboundEventGenerator.buildOutboundEvent(
                        null, EventConstants.BOOKING_FAILED, inbound);

        assertEquals(LocalDate.of(2026, 8, 22), derived.getBookingDate());
        assertEquals(300L, derived.getTimeSlotMapperId());
        assertEquals(2, derived.getGuestCount());
    }

    private Booking booking(String bookingId) {
        Booking booking = new Booking();
        booking.setBookingId(bookingId);
        booking.setGuestCount(1);
        return booking;
    }
}
