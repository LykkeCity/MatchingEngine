package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.LoggerNames
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderInputQueueListener(limitOrderInputQueue: BlockingQueue<MessageWrapper>,
                                   singleLimitOrderPreprocessor: SingleLimitOrderPreprocessor)
    : InputQueueListener(limitOrderInputQueue, singleLimitOrderPreprocessor, ThrottlingLogger.getLogger(LoggerNames.SINGLE_LIMIT_ORDER))