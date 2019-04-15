package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.MultilimitOrderContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.MultilimitOrderParsedData
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.MultilimitOrderValidationResult
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.services.validators.input.OrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.stream.Stream

@Component
class MultilimitOrderPreprocessor(private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                  private val limitOrderInputValidator: LimitOrderInputValidator,
                                  private val orderInputValidator: OrderInputValidator,
                                  multilimitOrderContextParser: ContextParser<MultilimitOrderParsedData>,
                                  preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                  @Qualifier("multiLimitOrderPreProcessingLogger")
                                  private val logger: ThrottlingLogger) : AbstractMessagePreprocessor<MultilimitOrderParsedData>(multilimitOrderContextParser,
        messageProcessingStatusHolder, preProcessedMessageQueue, logger) {
    override fun preProcessParsedData(parsedData: MultilimitOrderParsedData): Boolean {
        val context = parsedData.messageWrapper.context as MultilimitOrderContext
        if (messageProcessingStatusHolder.isTradeDisabled(context.assetPair)) {
            writeResponse(parsedData.messageWrapper,
                    context.multiLimitOrder.assetPairId,
                    MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return false
        }

         context.multilimitOrderValidationResult = getValidationResult(parsedData)

        val multilimitOrderValidationResult = context.multilimitOrderValidationResult
        val fatallyInvalidValidationResult = Stream.concat(Stream.of(multilimitOrderValidationResult!!.globalValidationResult),
                multilimitOrderValidationResult.inputValidationResultByOrderId?.values?.stream() ?: Stream.empty<OrderValidationResult>())
                .filter { it.isFatalInvalid }
                .findFirst()

        if (fatallyInvalidValidationResult.isPresent) {
            logger.error("Fatal validation error occurred, ${fatallyInvalidValidationResult.get().message} " +
                    "Error details: $context")
            writeResponse(parsedData.messageWrapper,
                    context.multiLimitOrder.assetPairId,
                    MessageStatusUtils.toMessageStatus(fatallyInvalidValidationResult.get().status!!),
                    fatallyInvalidValidationResult.get().message)
            return false
        }

        return true
    }

    fun writeResponse(messageWrapper: MessageWrapper, assetPairId: String, status: MessageStatus, message: String? = null) {
        messageWrapper.writeMultiLimitOrderResponse(ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                .setStatus(status.type)
                .setAssetPairId(assetPairId))
    }

    private fun getValidationResult(parsedData: MultilimitOrderParsedData): MultilimitOrderValidationResult {
        val context = parsedData.messageWrapper.context as MultilimitOrderContext
        val orderValidationResultByOrderId = HashMap<String, OrderValidationResult>()

        try {
            orderInputValidator.validateAsset(context.assetPair, parsedData.inputAssetPairId)
        }
        catch(e: OrderValidationException) {
            val fatalInvalid = OrderValidationUtils.isFatalInvalid(e)
            return MultilimitOrderValidationResult(OrderValidationResult(false, fatalInvalid, e.message, e.orderStatus))
        }


        for (order in context.multiLimitOrder.orders) {
            try {
                when (order.type) {
                    LimitOrderType.LIMIT -> limitOrderInputValidator.validateLimitOrder(context.isTrustedClient,
                            order,
                            context.assetPair,
                            null,
                            context.baseAsset)
                    LimitOrderType.STOP_LIMIT -> limitOrderInputValidator.validateStopOrder(order, context.assetPair, order.assetPairId, context.baseAsset)
                }
            } catch (e: OrderValidationException) {
                val fatalInvalid = OrderValidationUtils.isFatalInvalid(e)
                orderValidationResultByOrderId[order.id] = OrderValidationResult(false, fatalInvalid, e.message, e.orderStatus)
            }
        }

        return MultilimitOrderValidationResult(OrderValidationResult(true), orderValidationResultByOrderId)
    }
}