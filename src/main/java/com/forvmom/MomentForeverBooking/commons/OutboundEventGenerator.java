package com.forvmom.MomentForeverBooking.commons;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.events.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

public class OutboundEventGenerator {

    private static final String PRODUCER = "booking-service";
    private static final int SCHEMA_VERSION = 1;

    public static OutboundEvent buildOutboundEvent(Booking booking, String eventType, InboundEvent inboundEvent) {

        if (eventType != null) {
            if (eventType.equals(EventConstants.BOOKING_FAILED)) {
                return buildBookingFailedEvent(booking, inboundEvent);
            }

            if (eventType.equals(EventConstants.BOOKING_CONFIRMED)) {
                return buildBookingConfirmedEvent(booking, inboundEvent);
            }

            if (eventType.equals(EventConstants.PAYMENT_REQUESTED)) {
                return buildPaymentRequestedEvent(booking, inboundEvent);
            }
        }
        throw new IllegalArgumentException("Unsupported event type: " + eventType);
    }

    private static OutboundEvent buildBookingConfirmedEvent(Booking booking, InboundEvent inboundEvent) {
        BookingConfirmedEvent e = new BookingConfirmedEvent();
        applyIdentity(e, EventConstants.BOOKING_CONFIRMED, booking.getBookingId(), inboundEvent);
        e.setBookingId(booking.getBookingId());
        e.setConfirmedAt(LocalDateTime.now());
        e.setUserId(booking.getUserId());
        e.setUserEmail(booking.getUserEmail());
        e.setExperienceId(booking.getExperienceId());
        e.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        e.setGuestCount(booking.getGuestCount());
        return e;
    }

    private static PaymentRequestedEvent buildPaymentRequestedEvent(Booking booking, InboundEvent inboundEvent) {
        PaymentRequestedEvent e = new PaymentRequestedEvent();
        applyIdentity(e, EventConstants.PAYMENT_REQUESTED, booking.getBookingId(), inboundEvent);
        e.setBookingId(booking.getBookingId());
        e.setRequestedAt(LocalDateTime.now());
        e.setGrandTotal(booking.getGrandTotal());
        e.setUserId(booking.getUserId());
        e.setUserEmail(booking.getUserEmail());
        e.setExperienceId(booking.getExperienceId());
        e.setExperienceName(booking.getExperienceName());
        e.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        e.setGuestCount(booking.getGuestCount());
        e.setCurrency("INR");
        return e;
    }

    private static BookingFailedEvent buildBookingFailedEvent(Booking booking, InboundEvent inboundEvent) {

        if(booking==null){
            BookingFailedEvent bookingFailedEvent = new BookingFailedEvent();
            applyIdentity(bookingFailedEvent, EventConstants.BOOKING_FAILED,
                    inboundEvent.getBookingId(), inboundEvent);
            bookingFailedEvent.setBookingId(inboundEvent.getBookingId());
            if (inboundEvent instanceof BookingRequestEvent requestEvent) {
                bookingFailedEvent.setUserId(requestEvent.getUserId());
                bookingFailedEvent.setUserEmail(requestEvent.getUserEmail());
                bookingFailedEvent.setExperienceId(requestEvent.getExperienceId());
                bookingFailedEvent.setTimeSlotMapperId(requestEvent.getTimeSlotMapperId());
                bookingFailedEvent.setGuestCount(requestEvent.getGuestCount());
                bookingFailedEvent.setBookingDate(requestEvent.getBookingDate());
            }
            bookingFailedEvent.setFailedAt(LocalDateTime.now());
            bookingFailedEvent.setFailureReason("Booking not found for id: " + inboundEvent.getBookingId());
            return bookingFailedEvent;
        }
        BookingFailedEvent bookingFailedEvent = new BookingFailedEvent();
        applyIdentity(bookingFailedEvent, EventConstants.BOOKING_FAILED, booking.getBookingId(), inboundEvent);
        bookingFailedEvent.setBookingId(booking.getBookingId());
        bookingFailedEvent.setFailedAt(LocalDateTime.now());
        bookingFailedEvent.setFailureReason("TO BE FILLED BY CALLER");
        bookingFailedEvent.setUserId(booking.getUserId());
        bookingFailedEvent.setUserEmail(booking.getUserEmail());
        bookingFailedEvent.setExperienceId(booking.getExperienceId());
        bookingFailedEvent.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        bookingFailedEvent.setGuestCount(booking.getGuestCount());
        if (booking.getBookingDate() != null) {
            bookingFailedEvent.setBookingDate(booking.getBookingDate().toLocalDate());
        }
        return bookingFailedEvent;
    }

    public static BookingFailedEvent buildDeadPaymentRequestFailure(
            Booking booking,
            String failureReason,
            String causationId) {
        BookingFailedEvent event = new BookingFailedEvent();
        applyIdentity(event, EventConstants.BOOKING_FAILED, booking.getBookingId(),
                booking.getBookingId(), causationId);
        event.setBookingId(booking.getBookingId());
        event.setFailedAt(LocalDateTime.now());
        event.setFailureReason(failureReason);
        event.setUserId(booking.getUserId());
        event.setUserEmail(booking.getUserEmail());
        event.setExperienceId(booking.getExperienceId());
        event.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        event.setGuestCount(booking.getGuestCount());
        if (booking.getBookingDate() != null) {
            event.setBookingDate(booking.getBookingDate().toLocalDate());
        }
        return event;
    }

    private static void applyIdentity(BaseEvent event, String eventType, String bookingId,
                                      InboundEvent inboundEvent) {
        applyIdentity(event, eventType, bookingId,
                inboundEvent.getCorrelationId(), inboundEvent.getEventId());
    }

    private static void applyIdentity(BaseEvent event, String eventType, String bookingId,
                                      String correlationId, String causationId) {
        Instant now = Instant.now();
        event.setEventId(UUID.randomUUID().toString());
        event.setProducer(PRODUCER);
        event.setSchemaVersion(SCHEMA_VERSION);
        event.setOccurredAt(now);
        event.setEventType(eventType);
        event.setCorrelationId(correlationId != null
                ? correlationId
                : bookingId);
        event.setCausationId(causationId);
    }
}
