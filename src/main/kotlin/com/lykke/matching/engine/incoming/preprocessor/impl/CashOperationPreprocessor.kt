package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.input.CashOperationInputValidator
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import javax.xml.bind.ValidationException

@Component
class CashOperationPreprocessor(private val cashOperationInputQueue: BlockingQueue<MessageWrapper>,
                                private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                private val cashOperationParser: ContextParser<CashOperationParsedData>,
                                private val cashOperationInputValidator: CashOperationInputValidator): MessagePreprocessor, Thread(CashOperationPreprocessor::class.java.name) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(CashInOutPreprocessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun preProcess(messageWrapper: MessageWrapper) {
        val cashOperationParsedData = cashOperationParser.parse(messageWrapper)
        val cashOperationContext = cashOperationParsedData.messageWrapper.context as CashOperationContext

        try {
            cashOperationInputValidator.performValidation(cashOperationParsedData)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, cashOperationContext)
            return
        }

        preProcessedMessageQueue.put(cashOperationParsedData.messageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        //do nothing
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper, context: CashOperationContext?) {
        val newBuilder = ProtocolMessages.Response.newBuilder()

        if (context != null) {
            newBuilder.bussinesId = context.businessId
        }

        messageWrapper.writeResponse(newBuilder)
    }

    override fun run () {
        while(true) {
            val messageWrapper = cashOperationInputQueue.take()

            try {
                preProcess(messageWrapper)
            } catch (e: Exception) {
                //todo think what we are doing if failed to set uid in message wrapper
                LOGGER.error("[${messageWrapper.sourceIp}]: Got error during message preprocessing: ${e.message}", e)
                METRICS_LOGGER.logError("[${messageWrapper.sourceIp}]: Got error during message preprocessing", e)

                val context = messageWrapper.context as CashOperationContext?
                if (context != null) {
                    LOGGER.error("Error details: $context")
                }

                writeErrorResponse(messageWrapper, context)
            }
        }
    }

    @PostConstruct
    fun init() {
        this.start()
    }
}