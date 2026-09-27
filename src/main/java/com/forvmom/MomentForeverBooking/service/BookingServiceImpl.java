package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BookingServiceImpl implements BookingService {

    private static final Logger log = LoggerFactory.getLogger(BookingServiceImpl.class);

    private final BookingRepository bookingRepository;
    private final BookingStatusTransitionService bookingStatusTransitionService;
    private final InboundEventProcessorRegistry processorRegistry; // if you still need it

    public BookingServiceImpl(BookingRepository bookingRepository,
                              BookingStatusTransitionService bookingStatusTransitionService,
                              InboundEventProcessorRegistry processorRegistry) {
        this.bookingRepository = bookingRepository;
        this.bookingStatusTransitionService = bookingStatusTransitionService;
        this.processorRegistry = processorRegistry;
    }

    // ─── Inbound processing ─────────────────────────────────────────────
    @Override
    @Transactional
    public OutgoingOutboxRecord processBookingRequest(InboundEvent event) {
        // Delegate to the appropriate strategy via registry
        return processorRegistry.getProcessor(event.getEventType()).process(event);
    }

    // ─── Query methods ──────────────────────────────────────────────────
    @Override
    @Transactional(readOnly = true)
    public Booking getBookingByBookingId(String bookingId) {
        return bookingRepository.findByBookingId(bookingId)
                .orElseThrow(() -> new BookingNotFoundException("Booking not found: " + bookingId));
    }

    @Override
    @Transactional(readOnly = true)
    public Page<Booking> getBookingsByUser(Long userId, Pageable pageable) {
        return bookingRepository.findAllByUserId(userId, pageable);
    }

    // ─── Admin update methods ───────────────────────────────────────────
    @Override
    @Transactional
    public void cancelBooking(String bookingId) {
        if (bookingStatusTransitionService
                .transitionPendingBookingToCancelled(bookingId)
                .isPresent()) {
            log.info("Booking cancellation won: bookingId={}, currentStatus={}",
                    bookingId, BookingStatus.CANCELLED);
            return;
        }

        Booking booking = bookingRepository.findByBookingId(bookingId)
                .orElseThrow(() -> new BookingNotFoundException("Booking not found: " + bookingId));
        if (booking.getStatus() == BookingStatus.CANCELLED) {
            log.info("Booking cancellation skipped idempotently: bookingId={}, currentStatus={}",
                    bookingId, booking.getStatus());
            return;
        }
        if (booking.getStatus() == BookingStatus.CONFIRMED ||
                booking.getStatus() == BookingStatus.FAILED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "cancel");
        }
    }
}