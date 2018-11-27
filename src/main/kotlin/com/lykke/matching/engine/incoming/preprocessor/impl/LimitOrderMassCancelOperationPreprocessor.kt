package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderMassCancelOperationContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
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
                                                val limitOrderMassCancelOperationContextParser: LimitOrderMassCancelOperationContextParser,
                                                val messageProcessingStatusHolder: MessageProcessingStatusHolder) :
        MessagePreprocessor, Thread(LimitOrderMassCancelOperationPreprocessor::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(LimitOrderMassCancelOperationPreprocessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = limitOrderMassCancelOperationContextParser.parse(messageWrapper)
        val context = parsedData.messageWrapper.context as LimitOrderMassCancelOperationContext

        if (!messageProcessingStatusHolder.isMessageProcessingEnabled(DisabledFunctionalityRule(null, context.assetPairId, MessageType.LIMIT_ORDER_MASS_CANCEL))) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

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
                preProcess(messageWrapper)
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
