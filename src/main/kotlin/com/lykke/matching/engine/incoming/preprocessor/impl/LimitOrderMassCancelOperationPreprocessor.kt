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

        message.let { responseBuilder.setStatusReason(it) }
        messageWrapper.writeNewResponse(responseBuilder)
    }

    override fun run() {
        val messageWrapper = limitOrderMassCancelInputQueue.take()

        while (true) {
            try {
                preProcess(messageWrapper)
            } catch (e: Exception) {
                val context = messageWrapper.context
                LOGGER.error("[${messageWrapper.sourceIp}]: Got error during message preprocessing: ${e.message}" +
                        if(context != null) " context: $context" else "", e)
                METRICS_LOGGER.logError("[${messageWrapper.sourceIp}]: Got error during message preprocessing", e)
                writeResponse(messageWrapper, MessageStatus.RUNTIME)
            }
        }
    }

    @PostConstruct
    fun init() {
        this.start()
    }
}
