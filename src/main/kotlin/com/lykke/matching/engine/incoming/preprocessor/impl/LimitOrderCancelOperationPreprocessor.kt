package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderCancelOperationContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.services.validators.business.LimitOrderCancelOperationValidator
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import javax.xml.bind.ValidationException

@Component
class LimitOrderCancelOperationPreprocessor(val limitOrderCancelOperationContextParser: LimitOrderCancelOperationContextParser,
                                            val limitOrderCancelOperationValidator: LimitOrderCancelOperationValidator,
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
            writeResponse()
            return
        }

        preProcessedMessageQueue.put(parsedMessageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun run() {
        while (true) {
            val messageWrapper = limitOrderCancelInputQueue.take()

            try {
                preProcess(messageWrapper)
            } catch (e: Exception) {
                LOGGER.error("[${messageWrapper.sourceIp}]: Got error during message preprocessing: ${e.message}", e)
                METRICS_LOGGER.logError("[${messageWrapper.sourceIp}]: Got error during message preprocessing", e)

                val context = messageWrapper.context as LimitOrderCancelOperationContext?

                if (context != null) {
                    LOGGER.error("Error details: $context")
                }
            }
        }
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //nothing to do
    }

    @PostConstruct
    private fun init() {
        this.start()
    }
}