package com.forvmom.MomentForeverBooking.events;

public interface OutboundEvent {
    String getEventId();
    String getCorrelationId();
    String getCausationId();
    String getEventType();
}
