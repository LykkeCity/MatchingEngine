package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class CashTransferInputQueueListener(cashTransferInputQueue: BlockingQueue<MessageWrapper>,
                                     cashTransferPreprocessor: CashTransferPreprocessor,
                                     @Qualifier("cashTransferPreProcessingLogger")
                                     logger: ThrottlingLogger)
    : InputQueueListener(cashTransferInputQueue, cashTransferPreprocessor, logger)