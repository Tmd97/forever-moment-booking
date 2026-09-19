package com.forvmom.MomentForeverBooking.events;

public interface InboundEvent {
    String getBookingId();
    String getEventType();
    String getEventId();
    String getProducer();
    String getCorrelationId();
}