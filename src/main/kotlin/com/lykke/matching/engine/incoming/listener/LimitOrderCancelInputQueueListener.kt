package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.LoggerNames
import com.lykke.matching.engine.incoming.preprocessor.impl.LimitOrderCancelOperationPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderCancelInputQueueListener(limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
                                         limitOrderCancelOperationPreprocessor: LimitOrderCancelOperationPreprocessor)
    : InputQueueListener(limitOrderCancelInputQueue, limitOrderCancelOperationPreprocessor, LOGGER) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(String.format("%s.%s", LimitOrderCancelInputQueueListener::class.java.name, LoggerNames.LIMIT_ORDER_CANCEL))
    }
}