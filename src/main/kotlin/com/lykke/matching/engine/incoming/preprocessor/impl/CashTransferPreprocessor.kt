package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue

class CashTransferPreprocessor(
        private val incomingQueue: BlockingQueue<MessageWrapper>,
        private val outgoingQueue: BlockingQueue<MessageWrapper>,
        private val databaseAccessor: CashOperationIdDatabaseAccessor,
        private val contextParser: CashTransferContextParser
): MessagePreprocessor, Thread(CashTransferPreprocessor::class.java.name) {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashTransferPreprocessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedMessageWrapper = contextParser.parse(messageWrapper)
        val context = parsedMessageWrapper.context as CashTransferContext

        if (databaseAccessor.isAlreadyProcessed(parsedMessageWrapper.type.toString(), context.messageId)) {
            writeResponse(parsedMessageWrapper, DUPLICATE)
            LOGGER.info("Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}")
            METRICS_LOGGER.logError("Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}")
        } else {
            outgoingQueue.put(parsedMessageWrapper)
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

    override fun run() {
        while (true) {
            val message = incomingQueue.take()
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