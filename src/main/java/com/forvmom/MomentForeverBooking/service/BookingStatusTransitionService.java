package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
@Transactional
public class BookingStatusTransitionService {

    private static final Logger log = LoggerFactory.getLogger(BookingStatusTransitionService.class);

    private final BookingRepository bookingRepository;

    public BookingStatusTransitionService(BookingRepository bookingRepository) {
        this.bookingRepository = bookingRepository;
    }

    public Optional<Booking> transitionPendingBookingToConfirmed(String bookingId) {
        LocalDateTime transitionTime = LocalDateTime.now();
        int updatedBookings = bookingRepository.transitionPendingBookingToConfirmed(
                bookingId, transitionTime, transitionTime);
        return loadTransitionResult(bookingId, BookingStatus.CONFIRMED, updatedBookings);
    }

    public Optional<Booking> transitionPendingBookingToFailed(String bookingId, String failureReason) {
        LocalDateTime transitionTime = LocalDateTime.now();
        int updatedBookings = bookingRepository.transitionPendingBookingToFailed(
                bookingId, failureReason, transitionTime);
        return loadTransitionResult(bookingId, BookingStatus.FAILED, updatedBookings);
    }

    public Optional<Booking> transitionPendingBookingToCancelled(String bookingId) {
        int updatedBookings = bookingRepository.transitionPendingBookingToCancelled(
                bookingId, LocalDateTime.now());
        return loadTransitionResult(bookingId, BookingStatus.CANCELLED, updatedBookings);
    }

    private Optional<Booking> loadTransitionResult(
            String bookingId,
            BookingStatus requestedStatus,
            int updatedBookings) {
        Booking booking = bookingRepository.findByBookingId(bookingId)
                .orElseThrow(() -> new BookingNotFoundException(
                        "Booking not found for id: " + bookingId));

        if (updatedBookings == 1) {
            log.info(
                    "Booking status transition won: bookingId={}, requestedTransition=PENDING->{}, currentStatus={}",
                    bookingId, requestedStatus, booking.getStatus());
            return Optional.of(booking);
        }

        log.warn(
                "Booking status transition skipped: bookingId={}, requestedTransition=PENDING->{}, currentStatus={}",
                bookingId, requestedStatus, booking.getStatus());
        return Optional.empty();
    }
}
