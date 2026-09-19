package com.forvmom.MomentForeverBooking.producer;

import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BookingEventProducerTest {

    @SuppressWarnings("unchecked")
    @Test
    void keepsBookingIdAsKafkaKey() throws Exception {
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        PaymentRequestedEvent event = new PaymentRequestedEvent();
        event.setBookingId("B-100");
        BookingEventProducer producer = new BookingEventProducer(kafkaTemplate);
        ReflectionTestUtils.setField(producer, "paymentRequestedTopic", "payment-requested");
        when(kafkaTemplate.send("payment-requested", "B-100", event))
                .thenReturn(CompletableFuture.completedFuture(null));

        producer.sendPaymentRequestedEvent(event);

        verify(kafkaTemplate).send("payment-requested", "B-100", event);
    }
}
