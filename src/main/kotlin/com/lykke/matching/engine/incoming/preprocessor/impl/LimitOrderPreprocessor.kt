package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.SingleLimitContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class LimitOrderPreprocessor(private val limitOrderInputQueue: BlockingQueue<MessageWrapper>,
                             private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>): MessagePreprocessor, Thread(LimitOrderPreprocessor::class.java.name) {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(CashTransferPreprocessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var singleLimitOrderContextParser: SingleLimitOrderContextParser

    @Autowired
    private lateinit var limitOrderInputValidator: LimitOrderInputValidator

    override fun preProcess(messageWrapper: MessageWrapper) {
        val singleLimitOrderParsedData = singleLimitOrderContextParser.parse(messageWrapper)
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        singleLimitContext.validationResult = isOrderValid(singleLimitOrderParsedData)

        preProcessedMessageQueue.put(singleLimitOrderParsedData.messageWrapper)
    }

    private fun isOrderValid(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitContext

        return if (singleLimitContext.limitOrder.type == LimitOrderType.LIMIT) {
            validateLimitOrder(singleLimitOrderParsedData)
        } else {
            validateStopOrder(singleLimitOrderParsedData)
        }
    }

    private fun validateLimitOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        try {
            limitOrderInputValidator.validateLimitOrder(singleLimitOrderParsedData)
        } catch (e: OrderValidationException) {
            OrderValidationResult(false, e.message, e.orderStatus)
        }
        return OrderValidationResult(true)
    }

    private fun validateStopOrder(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        try {
            limitOrderInputValidator.validateStopOrder(singleLimitOrderParsedData)
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, e.message, e.orderStatus)
        }
        return OrderValidationResult(true)
    }

    override fun run() {
        while (true) {
            val message = limitOrderInputQueue.take()
            try {
                preProcess(message)
            } catch (exception: Exception) {
                LOGGER.error("[${message.sourceIp}]: Got error during message preprocessing: ${exception.message}", exception)
                METRICS_LOGGER.logError("[${message.sourceIp}]: Got error during message preprocessing", exception)
                writeResponse(message, MessageStatus.RUNTIME)
            }
        }
    }

    @PostConstruct
    fun init() {
        this.start()
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }
}