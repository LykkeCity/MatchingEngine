package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.LoggerNames
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class CashTransferInputQueueListener(cashTransferInputQueue: BlockingQueue<MessageWrapper>,
                                     cashTransferPreprocessor: CashTransferPreprocessor)
    : InputQueueListener(cashTransferInputQueue, cashTransferPreprocessor, LOGGER) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(String.format("%s.%s", CashTransferInputQueueListener::class.java.name, LoggerNames.CASH_TRANSFER))
    }
}