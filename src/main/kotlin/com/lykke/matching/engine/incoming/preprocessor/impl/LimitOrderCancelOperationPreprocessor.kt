package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.incoming.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.input.LimitOrderCancelOperationInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import javax.xml.bind.ValidationException

@Component
class LimitOrderCancelOperationPreprocessor(val limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData>,
                                            val limitOrderCancelOperationValidator: LimitOrderCancelOperationInputValidator,
                                            val limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
                                            val preProcessedMessageQueue: BlockingQueue<MessageWrapper>): MessagePreprocessor, Thread(LimitOrderCancelOperationPreprocessor::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(LimitOrderCancelOperationPreprocessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = limitOrderCancelOperationContextParser.parse(messageWrapper)
        val parsedMessageWrapper = parsedData.messageWrapper

        try {
            limitOrderCancelOperationValidator.performValidation(parsedData)
        } catch (e: ValidationException) {
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(e.errorCode), e.message)
            return
        }

        preProcessedMessageQueue.put(parsedMessageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
            return
        }

        val builder = ProtocolMessages.NewResponse.newBuilder().setStatus(status.type)

        message?.let {
            builder.statusReason = message
        }

        messageWrapper.writeNewResponse(builder)
    }

    override fun run() {
        while (true) {
            val messageWrapper = limitOrderCancelInputQueue.take()

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
    private fun init() {
        this.start()
    }
}