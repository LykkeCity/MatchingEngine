package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.impl.OrderFatalValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class SingleLimitOrderPreprocessor(private val limitOrderInputQueue: BlockingQueue<MessageWrapper>,
                                   private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                   @Qualifier("singleLimitOrderContextPreprocessorLogger")
                                   private val LOGGER: ThrottlingLogger) : MessagePreprocessor, Thread(SingleLimitOrderPreprocessor::class.java.name) {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var singleLimitOrderContextParser: SingleLimitOrderContextParser

    @Autowired
    private lateinit var limitOrderInputValidator: LimitOrderInputValidator

    override fun preProcess(messageWrapper: MessageWrapper) {
        val singleLimitOrderParsedData = singleLimitOrderContextParser.parse(messageWrapper)
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitOrderContext

        val validationResult = getValidationResult(singleLimitOrderParsedData)

        //currently if order is not valid at all - can not be passed to the business thread - ignore it
        if (validationResult.isFatalInvalid) {
            LOGGER.error("Fatal validation error occurred, ${validationResult.message} " +
                    "Error details: $singleLimitContext")
            return
        }

        singleLimitContext.validationResult = validationResult
        preProcessedMessageQueue.put(singleLimitOrderParsedData.messageWrapper)
    }

    private fun getValidationResult(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitOrderContext

        try {
            when (singleLimitContext.limitOrder.type) {
                LimitOrderType.LIMIT -> limitOrderInputValidator.validateLimitOrder(singleLimitOrderParsedData)
                LimitOrderType.STOP_LIMIT -> limitOrderInputValidator.validateStopOrder(singleLimitOrderParsedData)
            }
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, false, e.message, e.orderStatus)
        } catch (e: OrderFatalValidationException) {
            return OrderValidationResult(false, true, e.message)
        }

        return OrderValidationResult(true)
    }

    override fun run() {
        while (true) {
            val messageWrapper = limitOrderInputQueue.take()
            try {
                messageWrapper.messagePreProcessorStartTimestamp = System.nanoTime()
                preProcess(messageWrapper)
                messageWrapper.messagePreProcessorEndTimestamp = System.nanoTime()
            } catch (exception: Exception) {
                handlePreprocessingException(exception, messageWrapper)
            }
        }
    }

    @PostConstruct
    fun init() {
        this.start()
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
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
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }
}