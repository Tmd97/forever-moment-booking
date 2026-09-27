// package com.forvmom.MomentForeverBooking.service.inbound;

// import com.forvmom.MomentForeverBooking.commons.EventConstants;
// import com.forvmom.MomentForeverBooking.domain.entity.Booking;
// import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
// import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
// import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
// import com.forvmom.MomentForeverBooking.domain.enums.PricingLevel;
// import com.forvmom.MomentForeverBooking.events.PaymentFailedEvent;
// import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
// import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
// import
// com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
// import com.forvmom.MomentForeverBooking.repository.BookingRepository;
// import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
// import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
// import com.forvmom.MomentForeverBooking.service.BookingService;
// import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
// import
// com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxPublisher;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.springframework.aop.support.AopUtils;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.boot.test.mock.mockito.MockBean;
// import org.springframework.test.context.ActiveProfiles;
// import
// org.springframework.transaction.support.TransactionSynchronizationManager;

// import java.math.BigDecimal;
// import java.time.LocalDateTime;
// import java.util.List;
// import java.util.concurrent.Callable;
// import java.util.concurrent.CyclicBarrier;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.Future;
// import java.util.concurrent.TimeUnit;

// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertFalse;
// import static org.junit.jupiter.api.Assertions.assertInstanceOf;
// import static org.junit.jupiter.api.Assertions.assertNull;
// import static org.junit.jupiter.api.Assertions.assertTrue;

// @SpringBootTest
// @ActiveProfiles("test")
// class BookingStatusConcurrencyIntegrationTest {

// @Autowired
// private InboundEventProcessingTransactionService transactionService;

// @Autowired
// private InboundOutboxService inboundOutboxService;

// @Autowired
// private BookingRepository bookingRepository;

// @Autowired
// private InboundOutboxDao inboundOutboxDao;

// @Autowired
// private OutgoingOutboxDao outgoingOutboxDao;

// @Autowired
// private BookingService bookingService;

// @MockBean
// private OutgoingOutboxPublisher outgoingOutboxPublisher;

// private ExecutorService executorService;

// @BeforeEach
// void cleanDatabase() {
// outgoingOutboxDao.deleteAllInBatch();
// inboundOutboxDao.deleteAllInBatch();
// bookingRepository.deleteAllInBatch();
// executorService = Executors.newFixedThreadPool(2);

// assertTrue(AopUtils.isAopProxy(transactionService));
// assertTrue(AopUtils.isAopProxy(bookingService));
// assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
// }

// @AfterEach
// void stopExecutor() throws InterruptedException {
// executorService.shutdownNow();
// assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
// }

// @Test
// void
// concurrentPaymentProcessedAndFailedAllowExactlyOneMatchingTerminalOutcome()
// throws Exception {
// String bookingId = "booking-payment-race";
// bookingRepository.saveAndFlush(pendingBooking(bookingId));
// PaymentProcessedEvent processedEvent = paymentProcessedEvent(bookingId,
// "processed-race");
// PaymentFailedEvent failedEvent = paymentFailedEvent(
// bookingId, "failed-race", "Issuer declined the payment");
// InboundOutbox processedInbound =
// inboundOutboxService.findOrCreateForEvent(processedEvent);
// InboundOutbox failedInbound =
// inboundOutboxService.findOrCreateForEvent(failedEvent);
// CyclicBarrier startBarrier = new CyclicBarrier(3);

// Future<OutgoingOutboxRecord> processedResult = submitAtBarrier(
// startBarrier,
// () -> transactionService.processInboundEventAtomically(
// processedInbound, processedEvent));
// Future<OutgoingOutboxRecord> failedResult = submitAtBarrier(
// startBarrier,
// () -> transactionService.processInboundEventAtomically(
// failedInbound, failedEvent));
// startBarrier.await(5, TimeUnit.SECONDS);

// OutgoingOutboxRecord processedRecord = processedResult.get(10,
// TimeUnit.SECONDS);
// OutgoingOutboxRecord failedRecord = failedResult.get(10, TimeUnit.SECONDS);
// Booking booking = bookingRepository.findById(bookingId).orElseThrow();
// List<OutgoingOutboxRecord> outgoingRecords = outgoingOutboxDao.findAll();

// assertTrue(booking.getStatus() == BookingStatus.CONFIRMED
// || booking.getStatus() == BookingStatus.FAILED);
// assertEquals(1, outgoingRecords.size());
// if (booking.getStatus() == BookingStatus.CONFIRMED) {
// assertEquals(EventConstants.BOOKING_CONFIRMED,
// outgoingRecords.get(0).getEventType());
// assertTrue(processedRecord != null);
// assertNull(failedRecord);
// } else {
// assertEquals(EventConstants.BOOKING_FAILED,
// outgoingRecords.get(0).getEventType());
// assertEquals("Issuer declined the payment", booking.getFailureReason());
// assertNull(processedRecord);
// assertTrue(failedRecord != null);
// }
// assertInboxProcessed(processedInbound);
// assertInboxProcessed(failedInbound);
// }

// @Test
// void concurrentDuplicatePaymentProcessedCreatesOneConfirmationOutbox() throws
// Exception {
// String bookingId = "booking-duplicate-race";
// bookingRepository.saveAndFlush(pendingBooking(bookingId));
// PaymentProcessedEvent firstEvent = paymentProcessedEvent(bookingId,
// "processed-duplicate-1");
// PaymentProcessedEvent secondEvent = paymentProcessedEvent(bookingId,
// "processed-duplicate-2");
// InboundOutbox firstInbound =
// inboundOutboxService.findOrCreateForEvent(firstEvent);
// InboundOutbox secondInbound =
// inboundOutboxService.findOrCreateForEvent(secondEvent);
// CyclicBarrier startBarrier = new CyclicBarrier(3);

// Future<OutgoingOutboxRecord> firstResult = submitAtBarrier(
// startBarrier,
// () -> transactionService.processInboundEventAtomically(firstInbound,
// firstEvent));
// Future<OutgoingOutboxRecord> secondResult = submitAtBarrier(
// startBarrier,
// () -> transactionService.processInboundEventAtomically(secondInbound,
// secondEvent));
// startBarrier.await(5, TimeUnit.SECONDS);

// OutgoingOutboxRecord firstRecord = firstResult.get(10, TimeUnit.SECONDS);
// OutgoingOutboxRecord secondRecord = secondResult.get(10, TimeUnit.SECONDS);

// assertEquals(BookingStatus.CONFIRMED,
// bookingRepository.findById(bookingId).orElseThrow().getStatus());
// assertEquals(1, countOutgoing(EventConstants.BOOKING_CONFIRMED));
// assertEquals(1, (firstRecord == null ? 0 : 1) + (secondRecord == null ? 0 :
// 1));
// assertInboxProcessed(firstInbound);
// assertInboxProcessed(secondInbound);
// }

// @Test
// void adminCancellationRacingPaymentConfirmationNeverOverwritesWinner() throws
// Exception {
// String bookingId = "booking-cancel-race";
// bookingRepository.saveAndFlush(pendingBooking(bookingId));
// PaymentProcessedEvent processedEvent = paymentProcessedEvent(bookingId,
// "processed-cancel-race");
// InboundOutbox inbound =
// inboundOutboxService.findOrCreateForEvent(processedEvent);
// CyclicBarrier startBarrier = new CyclicBarrier(3);

// Future<OutgoingOutboxRecord> paymentResult = submitAtBarrier(
// startBarrier,
// () -> transactionService.processInboundEventAtomically(inbound,
// processedEvent));
// Future<RuntimeException> cancellationResult = submitAtBarrier(startBarrier,
// () -> {
// try {
// bookingService.cancelBooking(bookingId);
// return null;
// } catch (RuntimeException cancellationFailure) {
// return cancellationFailure;
// }
// });
// startBarrier.await(5, TimeUnit.SECONDS);

// OutgoingOutboxRecord paymentRecord = paymentResult.get(10, TimeUnit.SECONDS);
// RuntimeException cancellationFailure = cancellationResult.get(10,
// TimeUnit.SECONDS);
// BookingStatus finalStatus =
// bookingRepository.findById(bookingId).orElseThrow().getStatus();

// if (finalStatus == BookingStatus.CANCELLED) {
// assertNull(cancellationFailure);
// assertNull(paymentRecord);
// assertEquals(0, outgoingOutboxDao.count());
// } else {
// assertEquals(BookingStatus.CONFIRMED, finalStatus);
// assertInstanceOf(BookingStatusConflictException.class, cancellationFailure);
// assertTrue(paymentRecord != null);
// assertEquals(1, countOutgoing(EventConstants.BOOKING_CONFIRMED));
// }
// assertInboxProcessed(inbound);
// }

// @Test
// void lateIdenticalAndContradictoryTerminalEventsAreProcessedNoOps() {
// String bookingId = "booking-late-events";
// bookingRepository.saveAndFlush(pendingBooking(bookingId));
// PaymentProcessedEvent winningEvent = paymentProcessedEvent(bookingId,
// "processed-winner");
// InboundOutbox winningInbound =
// inboundOutboxService.findOrCreateForEvent(winningEvent);
// transactionService.processInboundEventAtomically(winningInbound,
// winningEvent);

// PaymentProcessedEvent identicalEvent = paymentProcessedEvent(bookingId,
// "processed-late");
// PaymentFailedEvent contradictoryEvent = paymentFailedEvent(
// bookingId, "failed-late", "Late decline");
// InboundOutbox identicalInbound =
// inboundOutboxService.findOrCreateForEvent(identicalEvent);
// InboundOutbox contradictoryInbound =
// inboundOutboxService.findOrCreateForEvent(contradictoryEvent);

// assertNull(transactionService.processInboundEventAtomically(
// identicalInbound, identicalEvent));
// assertNull(transactionService.processInboundEventAtomically(
// contradictoryInbound, contradictoryEvent));

// assertEquals(BookingStatus.CONFIRMED,
// bookingRepository.findById(bookingId).orElseThrow().getStatus());
// assertEquals(1, outgoingOutboxDao.count());
// assertEquals(1, countOutgoing(EventConstants.BOOKING_CONFIRMED));
// assertEquals(0, countOutgoing(EventConstants.BOOKING_FAILED));
// assertInboxProcessed(winningInbound);
// assertInboxProcessed(identicalInbound);
// assertInboxProcessed(contradictoryInbound);
// }

// @Test
// void adminCancellationIsIdempotentAndRetainsTerminalConflicts() {
// Booking cancelled = pendingBooking("booking-cancelled");
// cancelled.setStatus(BookingStatus.CANCELLED);
// Booking confirmed = pendingBooking("booking-confirmed");
// confirmed.setStatus(BookingStatus.CONFIRMED);
// Booking failed = pendingBooking("booking-failed");
// failed.setStatus(BookingStatus.FAILED);
// bookingRepository.saveAllAndFlush(List.of(cancelled, confirmed, failed));

// bookingService.cancelBooking(cancelled.getBookingId());
// RuntimeException confirmedFailure =
// captureCancellationFailure(confirmed.getBookingId());
// RuntimeException failedFailure =
// captureCancellationFailure(failed.getBookingId());
// RuntimeException missingFailure =
// captureCancellationFailure("booking-missing");

// assertInstanceOf(BookingStatusConflictException.class, confirmedFailure);
// assertInstanceOf(BookingStatusConflictException.class, failedFailure);
// assertInstanceOf(BookingNotFoundException.class, missingFailure);
// assertEquals(BookingStatus.CANCELLED,
// bookingRepository.findById(cancelled.getBookingId()).orElseThrow().getStatus());
// assertEquals(BookingStatus.CONFIRMED,
// bookingRepository.findById(confirmed.getBookingId()).orElseThrow().getStatus());
// assertEquals(BookingStatus.FAILED,
// bookingRepository.findById(failed.getBookingId()).orElseThrow().getStatus());
// }

// private <T> Future<T> submitAtBarrier(CyclicBarrier startBarrier, Callable<T>
// operation) {
// return executorService.submit(() -> {
// startBarrier.await(5, TimeUnit.SECONDS);
// return operation.call();
// });
// }

// private void assertInboxProcessed(InboundOutbox inboundOutbox) {
// assertEquals(
// EventConstants.PROCESSED,
// inboundOutboxDao.findById(inboundOutbox.getId()).orElseThrow().getStatus());
// }

// private long countOutgoing(String eventType) {
// return outgoingOutboxDao.findAll().stream()
// .filter(record -> eventType.equals(record.getEventType()))
// .count();
// }

// private RuntimeException captureCancellationFailure(String bookingId) {
// try {
// bookingService.cancelBooking(bookingId);
// return null;
// } catch (RuntimeException cancellationFailure) {
// return cancellationFailure;
// }
// }

// private PaymentProcessedEvent paymentProcessedEvent(String bookingId, String
// eventId) {
// PaymentProcessedEvent event = new PaymentProcessedEvent();
// event.setBookingId(bookingId);
// event.setEventType(EventConstants.PAYMENT_PROCESSED);
// event.setEventId(eventId);
// event.setProducer("payment-service");
// event.setCorrelationId(bookingId);
// event.setTransactionId("transaction-" + eventId);
// event.setAmountPaid(new BigDecimal("200.00"));
// event.setCurrency("INR");
// event.setPaidAt(LocalDateTime.of(2026, 8, 23, 12, 5));
// return event;
// }

// private PaymentFailedEvent paymentFailedEvent(
// String bookingId,
// String eventId,
// String failureReason) {
// PaymentFailedEvent event = new PaymentFailedEvent();
// event.setBookingId(bookingId);
// event.setEventType(EventConstants.PAYMENT_FAILED);
// event.setEventId(eventId);
// event.setProducer("payment-service");
// event.setCorrelationId(bookingId);
// event.setFailureReason(failureReason);
// event.setErrorCode("PAYMENT_DECLINED");
// event.setFailedAt(LocalDateTime.of(2026, 8, 23, 12, 5));
// return event;
// }

// private Booking pendingBooking(String bookingId) {
// Booking booking = new Booking();
// booking.setBookingId(bookingId);
// booking.setBookingDate(LocalDateTime.of(2030, 1, 15, 0, 0));
// booking.setUserId(42L);
// booking.setUserEmail("guest@example.com");
// booking.setUserFullName("Integration Guest");
// booking.setExperienceId(100L);
// booking.setExperienceName("Integration Experience");
// booking.setLocationId(200L);
// booking.setLocationName("Integration Location");
// booking.setTimeSlotMapperId(300L);
// booking.setTimeSlotId(301L);
// booking.setTimeSlotLabel("Morning");
// booking.setStartTime("09:00");
// booking.setEndTime("11:00");
// booking.setGuestCount(2);
// booking.setStatus(BookingStatus.PENDING);
// booking.setResolvedPricePerPerson(new BigDecimal("100.00"));
// booking.setPricingLevel(PricingLevel.BASE);
// booking.setTotalAmount(new BigDecimal("200.00"));
// booking.setAddonsTotal(BigDecimal.ZERO);
// booking.setGrandTotal(new BigDecimal("200.00"));
// booking.setPincode("560001");
// booking.setRequestedAt(LocalDateTime.of(2026, 8, 23, 12, 0));
// return booking;
// }
// }
