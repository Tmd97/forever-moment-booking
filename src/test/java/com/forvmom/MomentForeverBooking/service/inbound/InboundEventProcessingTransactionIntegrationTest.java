package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.domain.enums.PricingLevel;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

/**
 * Spring/JPA integration coverage for the inbound processing transaction.
 */
@SpringBootTest
@ActiveProfiles("test")
class InboundEventProcessingTransactionIntegrationTest {

    @Autowired
    private InboundEventProcessingTransactionService transactionService;

    @Autowired
    private InboundEventProcessorService processorService;

    @Autowired
    private InboundOutboxService inboundOutboxService;

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private InboundOutboxDao inboundOutboxDao;

    @SpyBean
    private OutgoingOutboxDao outgoingOutboxDao;

    @MockBean
    private OutgoingOutboxPublisher outgoingOutboxPublisher;

    @BeforeEach
    void cleanDatabase() {
        reset(outgoingOutboxDao, outgoingOutboxPublisher);
        outgoingOutboxDao.deleteAllInBatch();
        inboundOutboxDao.deleteAllInBatch();
        bookingRepository.deleteAll();

        assertTrue(AopUtils.isAopProxy(transactionService));
        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
    }

    @Test
    void bookingRequestedCommitsBookingOutgoingOutboxAndExactInboundCompletion() {
        BookingRequestEvent event = bookingRequestEvent("booking-success", "booking-event-success");
        InboundOutbox inbound = inboundOutboxService.findOrCreateForEvent(event);

        transactionService.processInboundEventAtomically(inbound, event);

        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
        Booking booking = bookingRepository.findById(event.getBookingId()).orElseThrow();
        assertEquals(BookingStatus.PENDING, booking.getStatus());

        List<OutgoingOutboxRecord> outgoingRecords = outgoingOutboxDao.findAll();
        assertEquals(1, outgoingRecords.size());
        assertEquals(EventConstants.PAYMENT_REQUESTED, outgoingRecords.get(0).getEventType());
        assertEquals(event.getBookingId(), outgoingRecords.get(0).getBookingId());

        InboundOutbox reloadedInbound = inboundOutboxDao.findById(inbound.getId()).orElseThrow();
        assertEquals(EventConstants.PROCESSED, reloadedInbound.getStatus());
    }

    @Test
    void bookingRequestedOutgoingPersistenceFailureRollsBackBookingAndInboundCompletion() {
        BookingRequestEvent event = bookingRequestEvent("booking-create-rollback", "booking-event-rollback");
        InboundOutbox inbound = inboundOutboxService.findOrCreateForEvent(event);
        RuntimeException persistenceFailure = failOutgoingPersistence();

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> transactionService.processInboundEventAtomically(inbound, event));

        assertEquals(persistenceFailure, thrown);
        assertFalse(bookingRepository.existsById(event.getBookingId()));
        assertEquals(0, outgoingOutboxDao.count());
        assertEquals(
                EventConstants.PENDING,
                inboundOutboxDao.findById(inbound.getId()).orElseThrow().getStatus());
    }

    @Test
    void paymentProcessedOutgoingPersistenceFailureRollsBackBookingStatusAndInboundCompletion() {
        String bookingId = "booking-status-rollback";
        bookingRepository.saveAndFlush(pendingBooking(bookingId));
        PaymentProcessedEvent event = paymentProcessedEvent(bookingId, "payment-event-rollback");
        InboundOutbox inbound = inboundOutboxService.findOrCreateForEvent(event);
        failOutgoingPersistence();

        assertThrows(
                RuntimeException.class,
                () -> transactionService.processInboundEventAtomically(inbound, event));

        assertEquals(
                BookingStatus.PENDING,
                bookingRepository.findById(bookingId).orElseThrow().getStatus());
        assertEquals(0, outgoingOutboxDao.count());
        assertEquals(
                EventConstants.PENDING,
                inboundOutboxDao.findById(inbound.getId()).orElseThrow().getStatus());
    }

    @Test
    void processorAcknowledgesThenRollsBackBusinessWritesAndPersistsFailedInboxInNewTransaction() {
        BookingRequestEvent event = bookingRequestEvent("booking-service-rollback", "booking-event-service");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        failOutgoingPersistence();

        processorService.processEvent(event, acknowledgment);

        verify(acknowledgment).acknowledge();
        verify(outgoingOutboxPublisher, never()).trySinglePublish(any(OutgoingOutboxRecord.class));
        assertFalse(bookingRepository.existsById(event.getBookingId()));
        assertEquals(0, outgoingOutboxDao.count());

        InboundOutbox inbound = inboundOutboxDao
                .findByProducerAndEventId(event.getProducer(), event.getEventId())
                .orElseThrow();
        assertEquals(EventConstants.FAILED, inbound.getStatus());
    }

    private RuntimeException failOutgoingPersistence() {
        RuntimeException failure = new RuntimeException("injected outgoing outbox persistence failure");
        doThrow(failure).when(outgoingOutboxDao).save(any(OutgoingOutboxRecord.class));
        return failure;
    }

    private BookingRequestEvent bookingRequestEvent(String bookingId, String eventId) {
        BookingRequestEvent event = new BookingRequestEvent();
        event.setBookingId(bookingId);
        event.setEventType(EventConstants.BOOKING_REQUESTED);
        event.setEventId(eventId);
        event.setProducer("booking-core");
        event.setCorrelationId(bookingId);
        event.setUserId(42L);
        event.setUserEmail("guest@example.com");
        event.setUserFullName("Integration Guest");
        event.setExperienceId(100L);
        event.setExperienceName("Integration Experience");
        event.setLocationId(200L);
        event.setLocationName("Integration Location");
        event.setTimeSlotMapperId(300L);
        event.setTimeSlotId(301L);
        event.setTimeSlotLabel("Morning");
        event.setStartTime("09:00");
        event.setEndTime("11:00");
        event.setBookingDate(LocalDate.of(2030, 1, 15));
        event.setGuestCount(2);
        event.setPincode("560001");
        event.setResolvedPricePerPerson(new BigDecimal("100.00"));
        event.setPricingLevel(PricingLevel.BASE.name());
        event.setTotalAmount(new BigDecimal("200.00"));
        event.setAddonsTotal(BigDecimal.ZERO);
        event.setGrandTotal(new BigDecimal("200.00"));
        event.setRequestedAt(LocalDateTime.of(2026, 8, 23, 12, 0));
        return event;
    }

    private PaymentProcessedEvent paymentProcessedEvent(String bookingId, String eventId) {
        PaymentProcessedEvent event = new PaymentProcessedEvent();
        event.setBookingId(bookingId);
        event.setEventType(EventConstants.PAYMENT_PROCESSED);
        event.setEventId(eventId);
        event.setProducer("payment-service");
        event.setCorrelationId(bookingId);
        event.setTransactionId("transaction-" + bookingId);
        event.setAmountPaid(new BigDecimal("200.00"));
        event.setCurrency("INR");
        event.setPaidAt(LocalDateTime.of(2026, 8, 23, 12, 5));
        return event;
    }

    private Booking pendingBooking(String bookingId) {
        Booking booking = new Booking();
        booking.setBookingId(bookingId);
        booking.setBookingDate(LocalDateTime.of(2030, 1, 15, 0, 0));
        booking.setUserId(42L);
        booking.setUserEmail("guest@example.com");
        booking.setUserFullName("Integration Guest");
        booking.setExperienceId(100L);
        booking.setExperienceName("Integration Experience");
        booking.setLocationId(200L);
        booking.setLocationName("Integration Location");
        booking.setTimeSlotMapperId(300L);
        booking.setTimeSlotId(301L);
        booking.setTimeSlotLabel("Morning");
        booking.setStartTime("09:00");
        booking.setEndTime("11:00");
        booking.setGuestCount(2);
        booking.setStatus(BookingStatus.PENDING);
        booking.setResolvedPricePerPerson(new BigDecimal("100.00"));
        booking.setPricingLevel(PricingLevel.BASE);
        booking.setTotalAmount(new BigDecimal("200.00"));
        booking.setAddonsTotal(BigDecimal.ZERO);
        booking.setGrandTotal(new BigDecimal("200.00"));
        booking.setPincode("560001");
        booking.setRequestedAt(LocalDateTime.of(2026, 8, 23, 12, 0));
        return booking;
    }
}
