package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class InboundOutboxService {

    private final InboundOutboxDao inboundOutboxDao;
    private final Map<String, InboundEvent> creatorMap;

    public InboundOutboxService(InboundOutboxDao inboundOutboxDao, List<InboundEvent> inboundEventList) {
        this.inboundOutboxDao = inboundOutboxDao;
        this.creatorMap = inboundEventList.stream()
                .collect(Collectors.toMap(InboundEvent::getEventType, e -> e));
    }

    @Transactional
    public InboundOutbox findOrCreateForEvent(InboundEvent event) {
        validateIdentity(event);
        Optional<InboundOutbox> existing = hasEventIdentity(event)
                ? inboundOutboxDao.findByProducerAndEventId(
                        event.getProducer().trim(), event.getEventId().trim())
                : inboundOutboxDao.findByBookingReferenceIdAndEventType(
                        event.getBookingId(), event.getEventType());
        if (existing.isPresent()) {
            return existing.get();
        } else {
            return createNewOutbox(event);
        }

    }

    private InboundOutbox createNewOutbox(InboundEvent event) {
        InboundOutbox outbox = new InboundOutbox();
        outbox.setBookingReferenceId(event.getBookingId());
        if (hasEventIdentity(event)) {
            outbox.setProducer(event.getProducer().trim());
            outbox.setEventId(event.getEventId().trim());
        }
        outbox.setEventType(event.getEventType());
        outbox.setStatus(EventConstants.PENDING);
        outbox.setRetryCount(0);
        outbox.setPayload(JsonUtils.toJson(event));
        return inboundOutboxDao.save(outbox);
    }

    private void validateIdentity(InboundEvent event) {
        boolean hasProducer = hasText(event.getProducer());
        boolean hasEventId = hasText(event.getEventId());
        if (hasProducer != hasEventId) {
            throw new IllegalArgumentException(
                    "Inbound event producer and eventId must either both be present or both be absent");
        }
    }

    private boolean hasEventIdentity(InboundEvent event) {
        return hasText(event.getProducer()) && hasText(event.getEventId());
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Transactional
    public void markAsProcessing(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.PROCESSING);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void markAsProcessed(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.PROCESSED);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsFailed(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.FAILED);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void markAsDead(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.DEAD);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void incrementRetry(InboundOutbox outbox) {
        outbox.setRetryCount(outbox.getRetryCount() + 1);
        outbox.setStatus(EventConstants.PENDING);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }
}