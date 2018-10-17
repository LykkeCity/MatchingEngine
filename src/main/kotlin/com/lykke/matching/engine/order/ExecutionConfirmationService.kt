package com.lykke.matching.engine.order

import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component

@Component
class ExecutionConfirmationService(private val executionPersistenceService: ExecutionPersistenceService,
                                   private val executionEventSender: ExecutionEventSender) {

    fun persistAndSendEvents(messageWrapper: MessageWrapper?, executionContext: ExecutionContext): Boolean {
        val sequenceNumbers = executionEventSender.generateSequenceNumbers(executionContext)

        val persisted = executionPersistenceService.persist(messageWrapper, executionContext, sequenceNumbers.sequenceNumber)
        if (persisted) {
            executionEventSender.sendEvents(executionContext, sequenceNumbers)
        }

        return persisted
    }
}

