package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.impl.LimitOrderCancelOperationPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderCancelInputQueueListener(limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
                                         limitOrderCancelOperationPreprocessor: LimitOrderCancelOperationPreprocessor,
                                         @Qualifier("limitOrderCancelPreProcessingLogger")
                                         logger: ThrottlingLogger)
    : InputQueueListener(limitOrderCancelInputQueue, limitOrderCancelOperationPreprocessor, logger)