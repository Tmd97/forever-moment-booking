package com.forvmom.MomentForeverBooking.config;

import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BookingKafkaConfigTest {

    @Test
    void bookingFailureUsesPlatformTypeId() {
        BookingKafkaConfig config = new BookingKafkaConfig();
        ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");
        Map<String, Object> properties = config.producerFactory().getConfigurationProperties();

        JsonSerializer<Object> serializer = new JsonSerializer<Object>();
        serializer.configure(properties, false);
        RecordHeaders headers = new RecordHeaders();
        serializer.serialize("booking-failed", headers, new BookingFailedEvent());

        assertEquals(JsonSerializer.class, properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertNotNull(headers.lastHeader("__TypeId__"));
        assertEquals(
                "com.forvmom.common.dto.events.BookingFailedEvent",
                new String(headers.lastHeader("__TypeId__").value(), StandardCharsets.UTF_8));
    }
}
