package com.forvmom.MomentForeverBooking.repository;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface BookingRepository extends JpaRepository<Booking, String> {

    Optional<Booking> findByBookingId(String bookingId);

    boolean existsByBookingId(String bookingId);

    Page<Booking> findAllByUserId(Long userId, Pageable pageable);

    Page<Booking> findAllByStatus(BookingStatus status, Pageable pageable);

    // The status predicate makes the first committed terminal transition the only winner.
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE Booking booking
            SET booking.status = com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.CONFIRMED,
                booking.confirmedAt = :confirmedAt,
                booking.updatedAt = :updatedAt
            WHERE booking.bookingId = :bookingId
              AND booking.status = com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.PENDING
            """)
    int transitionPendingBookingToConfirmed(
            @Param("bookingId") String bookingId,
            @Param("confirmedAt") LocalDateTime confirmedAt,
            @Param("updatedAt") LocalDateTime updatedAt);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE Booking booking
            SET booking.status = com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.FAILED,
                booking.failureReason = :failureReason,
                booking.updatedAt = :updatedAt
            WHERE booking.bookingId = :bookingId
              AND booking.status = com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.PENDING
            """)
    int transitionPendingBookingToFailed(
            @Param("bookingId") String bookingId,
            @Param("failureReason") String failureReason,
            @Param("updatedAt") LocalDateTime updatedAt);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE Booking booking
            SET booking.status = com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.CANCELLED,
                booking.updatedAt = :updatedAt
            WHERE booking.bookingId = :bookingId
              AND booking.status = com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.PENDING
            """)
    int transitionPendingBookingToCancelled(
            @Param("bookingId") String bookingId,
            @Param("updatedAt") LocalDateTime updatedAt);
}
