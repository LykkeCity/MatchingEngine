package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.MultilimitOrderContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.MultilimitOrderParsedData
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class MultilimitOrderPreprocessor(private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                  private val limitOrderInputValidator: LimitOrderInputValidator,
                                  multilimitOrderContextParser: ContextParser<MultilimitOrderParsedData>,
                                  preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                  @Qualifier("multiLimitOrderPreProcessingLogger")
                                  private val logger: ThrottlingLogger) : AbstractMessagePreprocessor<MultilimitOrderParsedData>(multilimitOrderContextParser,
        messageProcessingStatusHolder, preProcessedMessageQueue, logger) {
    override fun preProcessParsedData(parsedData: MultilimitOrderParsedData): Boolean {
        val context = parsedData.messageWrapper.context as MultilimitOrderContext
        if (messageProcessingStatusHolder.isTradeDisabled(context.assetPair)) {
            writeResponse(parsedData.messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return false
        }

        val validationResult = getValidationResult(context)
        context.inputValidationResultByOrderId = validationResult

        val fatallyInvalidValidationResult = validationResult.values.find { it.isFatalInvalid }
        if (fatallyInvalidValidationResult != null) {
            logger.error("Fatal validation error occurred, ${fatallyInvalidValidationResult.message} " +
                    "Error details: $context")
            writeResponse(parsedData.messageWrapper, MessageStatusUtils.toMessageStatus(fatallyInvalidValidationResult.status!!), fatallyInvalidValidationResult.message)
            return false
        }

        return true
    }

    private fun getValidationResult(context: MultilimitOrderContext): Map<String, OrderValidationResult> {
        val orderValidationResultByOrderId = HashMap<String, OrderValidationResult>()
        for (order in context.multiLimitOrder.orders) {
            try {
                when (order.type) {
                    LimitOrderType.LIMIT -> limitOrderInputValidator.validateLimitOrder(context.isTrustedClient,
                            order,
                            context.assetPair,
                            order.assetPairId,
                            context.baseAsset)
                    LimitOrderType.STOP_LIMIT -> limitOrderInputValidator.validateStopOrder(order, context.assetPair, order.assetPairId, context.baseAsset)
                }
            } catch (e: OrderValidationException) {
                val fatalInvalid = isFatalInvalid(e)
                orderValidationResultByOrderId[order.id] = OrderValidationResult(false, fatalInvalid, e.message, e.orderStatus)
            }
        }

        return orderValidationResultByOrderId
    }

    private fun isFatalInvalid(validationException: OrderValidationException): Boolean {
        return validationException.orderStatus == OrderStatus.UnknownAsset
    }
}