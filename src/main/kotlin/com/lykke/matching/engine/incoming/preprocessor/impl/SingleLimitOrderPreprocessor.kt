package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.incoming.LoggerNames
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class SingleLimitOrderPreprocessor(private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>) : MessagePreprocessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(String.format("%s.%s", SingleLimitOrderPreprocessor::class.java.name, LoggerNames.SINGLE_LIMIT_ORDER))
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
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(validationResult.status!!), validationResult.message)
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
            return OrderValidationResult(false, isFatalInvalid(e), e.message, e.orderStatus)
        }

        return OrderValidationResult(true)
    }

    private fun isFatalInvalid(validationException: OrderValidationException): Boolean {
        return validationException.orderStatus == OrderStatus.UnknownAsset
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }
}