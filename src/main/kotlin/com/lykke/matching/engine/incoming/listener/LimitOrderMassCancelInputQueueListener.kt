package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.impl.LimitOrderMassCancelOperationPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderMassCancelInputQueueListener(limitOrderMassCancelInputQueue: BlockingQueue<MessageWrapper>,
                                             limitOrderMassCancelOperationPreprocessor: LimitOrderMassCancelOperationPreprocessor,
                                             @Qualifier("limitOrderMassCancelPreProcessingLogger")
                                             logger: ThrottlingLogger)
    : InputQueueListener(limitOrderMassCancelInputQueue, limitOrderMassCancelOperationPreprocessor, logger)