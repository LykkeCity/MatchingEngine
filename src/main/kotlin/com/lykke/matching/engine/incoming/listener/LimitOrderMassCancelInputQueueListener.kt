package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.LoggerNames
import com.lykke.matching.engine.incoming.preprocessor.impl.LimitOrderMassCancelOperationPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderMassCancelInputQueueListener(limitOrderMassCancelInputQueue: BlockingQueue<MessageWrapper>,
                                             limitOrderMassCancelOperationPreprocessor: LimitOrderMassCancelOperationPreprocessor)
    : InputQueueListener(limitOrderMassCancelInputQueue, limitOrderMassCancelOperationPreprocessor, ThrottlingLogger.getLogger(LoggerNames.LIMIT_ORDER_MASS_CANCEL))