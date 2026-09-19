package com.forvmom.MomentForeverBooking.mapper;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BookingMapperTest {

    @Test
    void persistsBookingDateFromRequestEvent() {
        BookingRequestEvent event = new BookingRequestEvent();
        event.setBookingId("B-100");
        event.setBookingDate(LocalDate.of(2026, 8, 22));
        event.setPricingLevel("BASE");
        event.setResolvedPricePerPerson(BigDecimal.TEN);
        event.setTotalAmount(BigDecimal.TEN);
        event.setGrandTotal(BigDecimal.TEN);

        Booking booking = BookingMapper.fromBookingRequestEvent(event);

        assertEquals(LocalDate.of(2026, 8, 22), booking.getBookingDate().toLocalDate());
    }
}
