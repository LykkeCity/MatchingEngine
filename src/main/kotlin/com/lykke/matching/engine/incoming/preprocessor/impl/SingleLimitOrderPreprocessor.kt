package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class SingleLimitOrderPreprocessor(singleLimitOrderContextParser: SingleLimitOrderContextParser,
                                   preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                   private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                   @Qualifier("singleLimitOrderPreProcessingLogger")
                                   private val logger: ThrottlingLogger) :
        AbstractMessagePreprocessor<SingleLimitOrderParsedData>(singleLimitOrderContextParser,
                messageProcessingStatusHolder,
                preProcessedMessageQueue,
                logger) {

    @Autowired
    private lateinit var limitOrderInputValidator: LimitOrderInputValidator

    override fun preProcessParsedData(parsedData: SingleLimitOrderParsedData): Boolean {
        val singleLimitContext = parsedData.messageWrapper.context as SingleLimitOrderContext

        if (messageProcessingStatusHolder.isTradeDisabled(singleLimitContext.assetPair)) {
            writeResponse(parsedData.messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return false
        }

        val validationResult = getValidationResult(parsedData)

        //currently if order is not valid at all - can not be passed to the business thread - ignore it
        if (validationResult.isFatalInvalid) {
            logger.error("Fatal validation error occurred, ${validationResult.message} " +
                    "Error details: $singleLimitContext")
            writeResponse(parsedData.messageWrapper, MessageStatusUtils.toMessageStatus(validationResult.status!!), validationResult.message)
            return false
        }

        singleLimitContext.validationResult = validationResult
        return true
    }

    private fun getValidationResult(singleLimitOrderParsedData: SingleLimitOrderParsedData): OrderValidationResult {
        val singleLimitContext = singleLimitOrderParsedData.messageWrapper.context as SingleLimitOrderContext

        try {
            when (singleLimitContext.limitOrder.type) {
                LimitOrderType.LIMIT -> limitOrderInputValidator.validateLimitOrder(singleLimitOrderParsedData)
                LimitOrderType.STOP_LIMIT -> limitOrderInputValidator.validateStopOrder(singleLimitOrderParsedData)
            }
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, OrderValidationUtils.isFatalInvalid(e), e.message, e.orderStatus)
        }

        return OrderValidationResult(true)
    }
}