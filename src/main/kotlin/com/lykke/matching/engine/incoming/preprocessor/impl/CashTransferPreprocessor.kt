package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class CashTransferPreprocessor(
        private val cashTransferQueue: BlockingQueue<MessageWrapper>,
        private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
        private val databaseAccessor: CashOperationIdDatabaseAccessor
): MessagePreprocessor, Thread(CashTransferPreprocessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashTransferPreprocessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = ProtocolMessages.CashTransferOperation.parseFrom(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.id
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        parseMessage(messageWrapper)
        if (databaseAccessor.isAlreadyProcessed(messageWrapper.type.toString(), messageWrapper.messageId!!)) {
            writeResponse(messageWrapper, DUPLICATE)
            LOGGER.info("Message already processed: ${messageWrapper.type}: ${messageWrapper.messageId!!}")
            METRICS_LOGGER.logError("Message already processed: ${messageWrapper.type}: ${messageWrapper.messageId!!}")
        } else {
            preProcessedMessageQueue.put(messageWrapper)
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

    override fun run() {
        while (true) {
            val message = cashTransferQueue.take()
            try {
                preProcess(message)
            } catch (exception: Exception) {
                LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message}", exception)
                METRICS_LOGGER.logError( "[${message.sourceIp}]: Got error during message preprocessing", exception)
                writeResponse(message, RUNTIME)
            }
        }
    }
}