package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderMassCancelOperationContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class LimitOrderMassCancelOperationPreprocessor(val limitOrderMassCancelInputQueue: BlockingQueue<MessageWrapper>,
                                                val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                                val limitOrderMassCancelOperationContextParser: LimitOrderMassCancelOperationContextParser) :
        MessagePreprocessor, Thread(LimitOrderMassCancelOperationPreprocessor::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(LimitOrderMassCancelOperationPreprocessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = limitOrderMassCancelOperationContextParser.parse(messageWrapper)
        preProcessedMessageQueue.put(parsedData.messageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        val responseBuilder = ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type)

        message?.let { responseBuilder.setStatusReason(it) }
        messageWrapper.writeNewResponse(responseBuilder)
    }

    override fun run() {
        while (true) {
            val messageWrapper = limitOrderMassCancelInputQueue.take()
            try {
                messageWrapper.messagePreProcessorStartTimestamp = System.nanoTime()
                preProcess(messageWrapper)
                messageWrapper.messagePreProcessorEndTimestamp = System.nanoTime()
            } catch (e: Exception) {
                handlePreprocessingException(e, messageWrapper)
            }
        }
    }

    @PostConstruct
    fun init() {
        this.start()
    }

    private fun handlePreprocessingException(exception: Exception, message: MessageWrapper) {
        try {
            val context = message.context
            CashTransferPreprocessor.LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message} " +
                    if (context != null) "Error details: $context" else "", exception)

            CashTransferPreprocessor.METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
            writeResponse(message, MessageStatus.RUNTIME)
        } catch (e: Exception) {
            val errorMessage = "Got error during message preprocessing failure handling"
            e.addSuppressed(exception)
            CashTransferPreprocessor.LOGGER.error(errorMessage, e)
            CashTransferPreprocessor.METRICS_LOGGER.logError(errorMessage, e)
        }
    }
}
