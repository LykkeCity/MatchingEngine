package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.transaction.ExecutionEventsSequenceNumbersGenerator
import com.lykke.matching.engine.outgoing.senders.OutgoingEventProcessor
import org.springframework.stereotype.Component

@Component
class ExecutionDataApplyService(private val executionEventsSequenceNumbersGenerator: ExecutionEventsSequenceNumbersGenerator,
                                private val executionPersistenceService: ExecutionPersistenceService,
                                private val outgoingEventProcessor: OutgoingEventProcessor) {

    fun persistAndSendEvents(messageWrapper: MessageWrapper?, executionContext: ExecutionContext): Boolean {
        val sequenceNumbers = executionEventsSequenceNumbersGenerator.generateSequenceNumbers(executionContext)

        val persisted = executionPersistenceService.persist(messageWrapper, executionContext, sequenceNumbers.sequenceNumber)
        if (persisted) {
            outgoingEventProcessor.submitExecutionEvent(ExecutionData(executionContext, sequenceNumbers))
        }

        return persisted
    }
}

