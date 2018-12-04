package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderInputQueueListener(limitOrderInputQueue: BlockingQueue<MessageWrapper>,
                                   singleLimitOrderPreprocessor: SingleLimitOrderPreprocessor,
                                   @Qualifier("singleLimitOrderPreProcessingLogger")
                                   logger: ThrottlingLogger)
    : InputQueueListener(limitOrderInputQueue, singleLimitOrderPreprocessor, logger)