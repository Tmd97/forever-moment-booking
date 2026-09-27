package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessingTransactionService;

class RetryFixture {

    final InboundOutboxDao inboundOutboxDao;
    final OutgoingOutboxDao outgoingOutboxDao;
    final OutgoingOutboxPublisher outgoingOutboxPublisher;
    final ObjectMapper objectMapper;
    final InboundEventProcessingTransactionService transactionService;
    final InboundOutboxRetryService retryService;

    RetryFixture(
            InboundOutboxDao inboundOutboxDao,
            OutgoingOutboxDao outgoingOutboxDao,
            OutgoingOutboxPublisher outgoingOutboxPublisher,
            ObjectMapper objectMapper,
            InboundEventProcessingTransactionService transactionService,
            InboundOutboxRetryService retryService) {
        this.inboundOutboxDao = inboundOutboxDao;
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
        this.objectMapper = objectMapper;
        this.transactionService = transactionService;
        this.retryService = retryService;
    }
}
