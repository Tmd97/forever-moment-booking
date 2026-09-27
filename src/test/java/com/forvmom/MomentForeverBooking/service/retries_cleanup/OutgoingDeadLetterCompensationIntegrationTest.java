package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.domain.enums.PricingLevel;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

@SpringBootTest
@ActiveProfiles("test")
class OutgoingDeadLetterCompensationIntegrationTest {

    @Autowired
    private OutgoingOutboxDeadLetterHandler deadLetterHandler;

    @Autowired
    private BookingRepository bookingRepository;

    @SpyBean
    private OutgoingOutboxDao outgoingOutboxDao;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void cleanDatabase() {
        reset(outgoingOutboxDao);
        outgoingOutboxDao.deleteAllInBatch();
        bookingRepository.deleteAllInBatch();
    }

    @Test
    void deadPaymentRequestFailsBookingAndCreatesCompleteFailureEventOnce() throws Exception {
        Booking booking = booking("B-DEAD-100");
        bookingRepository.saveAndFlush(booking);
        OutgoingOutboxRecord paymentRequest = paymentRequest(booking.getBookingId());
        outgoingOutboxDao.saveAndFlush(paymentRequest);

        deadLetterHandler.handleDeadRecord(paymentRequest);
        deadLetterHandler.handleDeadRecord(paymentRequest);

        assertEquals(
                BookingStatus.FAILED,
                bookingRepository.findById(booking.getBookingId()).orElseThrow().getStatus());

        List<OutgoingOutboxRecord> records = outgoingOutboxDao.findAll();
        assertEquals(2, records.size());
        OutgoingOutboxRecord deadPaymentRequest = records.stream()
                .filter(record -> EventConstants.PAYMENT_REQUESTED.equals(record.getEventType()))
                .findFirst()
                .orElseThrow();
        OutgoingOutboxRecord bookingFailure = records.stream()
                .filter(record -> EventConstants.BOOKING_FAILED.equals(record.getEventType()))
                .findFirst()
                .orElseThrow();
        assertEquals(OutgoingOutboxRecord.STATUS_DEAD, deadPaymentRequest.getStatus());
        assertEquals(OutgoingOutboxRecord.STATUS_PENDING, bookingFailure.getStatus());

        BookingFailedEvent event =
                objectMapper.readValue(bookingFailure.getPayload(), BookingFailedEvent.class);
        assertEquals(booking.getBookingId(), event.getBookingId());
        assertEquals(booking.getTimeSlotMapperId(), event.getTimeSlotMapperId());
        assertEquals(LocalDate.of(2030, 1, 15), event.getBookingDate());
        assertEquals(booking.getGuestCount(), event.getGuestCount());
        assertEquals("booking-service", event.getProducer());
        assertNotNull(event.getEventId());
        assertEquals(
                OutgoingDeadLetterCompensationService.PAYMENT_REQUEST_FAILURE_REASON,
                event.getFailureReason());
    }

    @Test
    void compensationPersistenceFailureRollsBackDeadStatusAndBookingTransition() {
        Booking booking = booking("B-DEAD-ROLLBACK");
        bookingRepository.saveAndFlush(booking);
        OutgoingOutboxRecord paymentRequest = paymentRequest(booking.getBookingId());
        outgoingOutboxDao.saveAndFlush(paymentRequest);
        doThrow(new RuntimeException("compensation outbox unavailable"))
                .when(outgoingOutboxDao)
                .save(argThat(record ->
                        EventConstants.BOOKING_FAILED.equals(record.getEventType())));

        deadLetterHandler.handleDeadRecord(paymentRequest);

        assertEquals(
                BookingStatus.PENDING,
                bookingRepository.findById(booking.getBookingId()).orElseThrow().getStatus());
        assertEquals(
                OutgoingOutboxRecord.STATUS_FAILED,
                outgoingOutboxDao.findById(paymentRequest.getId()).orElseThrow().getStatus());
        assertEquals(1, outgoingOutboxDao.count());
    }

    private OutgoingOutboxRecord paymentRequest(String bookingId) {
        OutgoingOutboxRecord record = new OutgoingOutboxRecord();
        record.setBookingId(bookingId);
        record.setEventType(EventConstants.PAYMENT_REQUESTED);
        record.setPayload("{}");
        record.setStatus(OutgoingOutboxRecord.STATUS_FAILED);
        record.setRetryCount(5);
        return record;
    }

    private Booking booking(String bookingId) {
        Booking booking = new Booking();
        booking.setBookingId(bookingId);
        booking.setBookingDate(LocalDate.of(2030, 1, 15).atStartOfDay());
        booking.setUserId(42L);
        booking.setUserEmail("guest@example.com");
        booking.setUserFullName("Integration Guest");
        booking.setExperienceId(100L);
        booking.setExperienceName("Integration Experience");
        booking.setLocationId(200L);
        booking.setLocationName("Integration Location");
        booking.setTimeSlotMapperId(300L);
        booking.setTimeSlotId(301L);
        booking.setGuestCount(2);
        booking.setStatus(BookingStatus.PENDING);
        booking.setResolvedPricePerPerson(new BigDecimal("100.00"));
        booking.setPricingLevel(PricingLevel.BASE);
        booking.setTotalAmount(new BigDecimal("200.00"));
        booking.setAddonsTotal(BigDecimal.ZERO);
        booking.setGrandTotal(new BigDecimal("200.00"));
        return booking;
    }
}
