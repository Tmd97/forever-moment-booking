package com.forvmom.MomentForeverBooking.producer;

import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class BookingEventProducer {

    private static final Logger log = LoggerFactory.getLogger(BookingEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.booking-confirmed}")
    private String bookingConfirmedTopic;

    @Value("${kafka.topics.booking-failed}")
    private String bookingFailedTopic;

    @Value("${kafka.topics.payment-requested}")
    private String paymentRequestedTopic;

    public BookingEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendBookingConfirmedEvent(BookingConfirmedEvent event) throws ExecutionException, InterruptedException, TimeoutException {
        kafkaTemplate.send(bookingConfirmedTopic, event.getBookingId(), event).get(5, TimeUnit.SECONDS);
        log.info("Sent BookingConfirmedEvent: bookingId={}, eventId={}, correlationId={}",
                event.getBookingId(), event.getEventId(), event.getCorrelationId());
    }

    public void sendBookingFailedEvent(BookingFailedEvent event) throws ExecutionException, InterruptedException, TimeoutException {
        kafkaTemplate.send(bookingFailedTopic, event.getBookingId(), event).get(5, TimeUnit.SECONDS);
        log.info("Sent BookingFailedEvent: bookingId={}, eventId={}, correlationId={}",
                event.getBookingId(), event.getEventId(), event.getCorrelationId());
    }

    public void sendPaymentRequestedEvent(PaymentRequestedEvent event) throws ExecutionException, InterruptedException, TimeoutException {
        kafkaTemplate.send(paymentRequestedTopic, event.getBookingId(), event).get(5, TimeUnit.SECONDS);
        log.info("Sent PaymentRequestedEvent: bookingId={}, eventId={}, correlationId={}",
                event.getBookingId(), event.getEventId(), event.getCorrelationId());
    }
}