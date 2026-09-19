package com.forvmom.MomentForeverBooking.events;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Inbound event from the Payment service indicating payment succeeded.
 * Consumed by
 * {@link com.forvmom.MomentForeverBooking.consumer.PaymentProcessedConsumer}.
 */

public class PaymentProcessedEvent extends BaseEvent implements InboundEvent {

    private String bookingId;
    private String transactionId;
    private BigDecimal amountPaid;
    private String currency;
    private LocalDateTime paidAt;

    public PaymentProcessedEvent() {
    }

    public String getBookingId() {
        return bookingId;
    }

    public void setBookingId(String bookingId) {
        this.bookingId = bookingId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public BigDecimal getAmountPaid() {
        return amountPaid;
    }

    public void setAmountPaid(BigDecimal amountPaid) {
        this.amountPaid = amountPaid;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public LocalDateTime getPaidAt() {
        return paidAt;
    }

    public void setPaidAt(LocalDateTime paidAt) {
        this.paidAt = paidAt;
    }

}
