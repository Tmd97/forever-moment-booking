package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.events.PaymentFailedEvent;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.service.BookingStatusTransitionService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentProcessorFailurePropagationTest {

    @Test
    void paymentProcessedOutgoingOutboxFailurePropagates() {
        BookingStatusTransitionService transitionService = mock(BookingStatusTransitionService.class);
        OutgoingOutboxService outgoingOutboxService = mock(OutgoingOutboxService.class);
        PaymentProcessedProcessor processor =
                new PaymentProcessedProcessor(transitionService, outgoingOutboxService);
        PaymentProcessedEvent event = new PaymentProcessedEvent();
        event.setBookingId("B-100");
        Booking booking = pendingBooking();
        RuntimeException outboxFailure = new RuntimeException("outbox unavailable");
        when(transitionService.transitionPendingBookingToConfirmed("B-100"))
                .thenReturn(Optional.of(booking));
        when(outgoingOutboxService.createRecord(anyString(), anyString(), any()))
                .thenThrow(outboxFailure);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> processor.process(event));

        assertSame(outboxFailure, thrown);
        verify(outgoingOutboxService, never()).markAsFailed(any());
    }

    @Test
    void paymentFailedOutgoingOutboxFailurePropagates() {
        BookingStatusTransitionService transitionService = mock(BookingStatusTransitionService.class);
        OutgoingOutboxService outgoingOutboxService = mock(OutgoingOutboxService.class);
        PaymentFailedProcessor processor =
                new PaymentFailedProcessor(transitionService, outgoingOutboxService);
        PaymentFailedEvent event = new PaymentFailedEvent();
        event.setBookingId("B-100");
        Booking booking = pendingBooking();
        RuntimeException outboxFailure = new RuntimeException("outbox unavailable");
        when(transitionService.transitionPendingBookingToFailed("B-100", "Payment failed"))
                .thenReturn(Optional.of(booking));
        when(outgoingOutboxService.createRecord(anyString(), anyString(), any()))
                .thenThrow(outboxFailure);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> processor.process(event));

        assertSame(outboxFailure, thrown);
        verify(outgoingOutboxService, never()).markAsFailed(any());
    }

    private Booking pendingBooking() {
        Booking booking = new Booking();
        booking.setBookingId("B-100");
        booking.setStatus(BookingStatus.PENDING);
        return booking;
    }
}
