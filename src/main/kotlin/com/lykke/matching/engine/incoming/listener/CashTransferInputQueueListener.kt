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
    : InputQueueListener(cashTransferInputQueue, cashTransferPreprocessor, ThrottlingLogger.getLogger(LoggerNames.CASH_TRANSFER))