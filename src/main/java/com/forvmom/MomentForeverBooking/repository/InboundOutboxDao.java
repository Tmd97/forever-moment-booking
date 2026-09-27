package com.forvmom.MomentForeverBooking.repository;

import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface InboundOutboxDao extends JpaRepository<InboundOutbox, Long> {
    Optional<InboundOutbox> findByBookingReferenceId(String bookingReferenceId);

    @Modifying
    @Query("DELETE FROM InboundOutbox b WHERE b.status = :status AND b.updatedAt < :cutoff")
    int deleteByStatusAndUpdatedAtBefore(@Param("status") String status,
                                         @Param("cutoff") LocalDateTime cutoff);

    List<InboundOutbox> findByStatusInAndUpdatedAtBefore(List<String> statuses,
                                                         LocalDateTime cutoff);

    Optional<InboundOutbox> findByBookingReferenceIdAndEventType(String bookingReferenceId, String eventType);

    Optional<InboundOutbox> findByProducerAndEventId(String producer, String eventId);
}