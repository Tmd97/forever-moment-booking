package com.forvmom.MomentForeverBooking.events;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Inbound event from the Payment service indicating payment failed.
 * Consumed by
 * {@link com.forvmom.MomentForeverBooking.consumer.PaymentFailedConsumer}.
 */

public class PaymentFailedEvent extends BaseEvent implements InboundEvent {

    private String bookingId;
    private String failureReason;
    private String errorCode;
    private LocalDateTime failedAt;

    public PaymentFailedEvent() {
    }

    public String getBookingId() {
        return bookingId;
    }

    public void setBookingId(String bookingId) {
        this.bookingId = bookingId;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public LocalDateTime getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(LocalDateTime failedAt) {
        this.failedAt = failedAt;
    }

}
