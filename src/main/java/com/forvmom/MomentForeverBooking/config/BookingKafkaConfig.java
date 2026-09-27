package com.forvmom.MomentForeverBooking.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class BookingKafkaConfig {

        @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
        private String bootstrapServers;

        @Value("${kafka.topics.booking-requested}")
        private String bookingRequestedTopic;

        @Value("${kafka.topics.booking-confirmed}")
        private String bookingConfirmedTopic;

        @Value("${kafka.topics.booking-failed}")
        private String bookingFailedTopic;

        // ==================== PRODUCER CONFIG ====================
        // Booking service produces:
        // - booking-confirmed (to Core)
        // - booking-failed (to Core)
        // - payment-requested (to Payment service)

        @Bean
        public ProducerFactory<String, Object> producerFactory() {
                Map<String, Object> config = new HashMap<>();

                config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
                config.put(JsonSerializer.TYPE_MAPPINGS,
                                "com.forvmom.common.dto.events.BookingConfirmedEvent:" +
                                                "com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent," +
                                                                "com.forvmom.common.dto.events.BookingFailedEvent:" +
                                                                "com.forvmom.MomentForeverBooking.events.BookingFailedEvent");

                // Reliability settings
                config.put(ProducerConfig.RETRIES_CONFIG, 3);
                config.put(ProducerConfig.ACKS_CONFIG, "all");

                return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate() {
                return new KafkaTemplate<>(producerFactory());
        }

        // ==================== CONSUMER CONFIG ====================
        // Booking service consumes:
        // - booking-requested (from Core)
        // - payment-processed (from Payment service)
        // - payment-failed (from Payment service)

        @Bean
        public ConsumerFactory<String, Object> consumerFactory() {
                Map<String, Object> config = new HashMap<>();

                config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                config.put(ConsumerConfig.GROUP_ID_CONFIG, "booking-group");

                // Wrap with ErrorHandlingDeserializer so deserialisation errors don't
                // crash the listener thread — they become ErrorHandlingDeserializer.Value
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.class);

                config.put(org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                                StringDeserializer.class);
                config.put(org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                                JsonDeserializer.class);

                // ── Type resolution ───────────────────────────────────────────────────
                // The platform (core) publishes with header __TypeId__ set to:
                // com.forvmom.common.dto.events.BookingRequestEvent
                //
                // This service has its own mirror copy of that class at:
                // com.forvmom.MomentForeverBooking.events.BookingRequestEvent
                //
                // TYPE_MAPPINGS tells SpringDoc: "when you see the platform's type
                // header value, deserialise into OUR local class instead".
                // This avoids a ClassNotFoundException and removes the coupling of
                // having to share the commons JAR with the booking service.
                config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
                config.put(JsonDeserializer.TYPE_MAPPINGS,
                                // Platform (core) → local booking event
                                "com.forvmom.common.dto.events.BookingRequestEvent:" +
                                                "com.forvmom.MomentForeverBooking.events.BookingRequestEvent," +
                                                // Payment service → local payment events
                                                // Adjust the left-hand side when payment service class names are known
                                                "com.forvmom.payment.events.PaymentProcessedEvent:" +
                                                "com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent," +
                                                "com.forvmom.payment.events.PaymentFailedEvent:" +
                                                "com.forvmom.MomentForeverBooking.events.PaymentFailedEvent");

                // Use the __TypeId__ header for type resolution (default true, but
                // being explicit to document intent)
                config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);

                config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

                return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                        ConsumerFactory<String, Object> consumerFactory,
                        KafkaTemplate<String, Object> kafkaTemplate) {

                ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

                factory.setConsumerFactory(consumerFactory);
                factory.setConcurrency(1);
                factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

                // Error handling: 3 retries then dead letter
                DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
                DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
                factory.setCommonErrorHandler(errorHandler);

                return factory;
        }

        // ==================== TOPICS ====================

        // Topics Booking CONSUMES
        @Bean
        public NewTopic bookingRequestedTopic() {
                return TopicBuilder.name(bookingRequestedTopic)
                                .partitions(1)
                                .replicas(1)
                                .build();
        }

        @Bean
        public NewTopic paymentProcessedTopic() {
                return TopicBuilder.name("payment-processed")
                                .partitions(1)
                                .replicas(1)
                                .build();
        }

        @Bean
        public NewTopic paymentFailedTopic() {
                return TopicBuilder.name("payment-failed")
                                .partitions(1)
                                .replicas(1)
                                .build();
        }

        // Topics Booking PRODUCES
        @Bean
        public NewTopic bookingConfirmedTopic() {
                return TopicBuilder.name(bookingConfirmedTopic)
                                .partitions(1)
                                .replicas(1)
                                .build();
        }

        @Bean
        public NewTopic bookingFailedTopic() {
                return TopicBuilder.name(bookingFailedTopic)
                                .partitions(1)
                                .replicas(1)
                                .build();
        }

        @Bean
        public NewTopic paymentRequestedTopic() {
                return TopicBuilder.name("payment-requested")
                                .partitions(1)
                                .replicas(1)
                                .build();
        }

        // Dead letter topic
        @Bean
        public NewTopic bookingDltTopic() {
                return TopicBuilder.name("booking-dlt")
                                .partitions(1)
                                .replicas(1)
                                .build();
        }
}
