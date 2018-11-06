package com.lykke.matching.engine.order

import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.transaction.ExecutionEventsSequenceNumbersGenerator
import org.springframework.stereotype.Component

@Component
class ExecutionDataApplyService(private val executionEventsSequenceNumbersGenerator: ExecutionEventsSequenceNumbersGenerator,
                                private val executionPersistenceService: ExecutionPersistenceService,
                                private val executionEventSender: ExecutionEventSender) {

    fun persistAndSendEvents(messageWrapper: MessageWrapper?, executionContext: ExecutionContext): Boolean {
        val sequenceNumbers = executionEventsSequenceNumbersGenerator.generateSequenceNumbers(executionContext)

        val persisted = executionPersistenceService.persist(messageWrapper, executionContext, sequenceNumbers.sequenceNumber)
        if (persisted) {
            executionEventSender.sendEvents(executionContext, sequenceNumbers)
        }

        return persisted
    }
}

