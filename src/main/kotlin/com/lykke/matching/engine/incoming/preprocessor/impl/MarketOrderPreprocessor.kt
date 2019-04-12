package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.MarketOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.MarkerOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.daos.context.MarketOrderContext
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.MarketOrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.BlockingQueue

@Component
class MarketOrderPreprocessor(marketOrderContextParser: MarkerOrderContextParser,
                              private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                              preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                              @Qualifier("marketOrderPreProcessingLogger")
                              private val logger: ThrottlingLogger) :
        AbstractMessagePreprocessor<MarketOrderParsedData>(marketOrderContextParser,
                messageProcessingStatusHolder,
                preProcessedMessageQueue,
                logger) {

    @Autowired
    private lateinit var marketOrderInputValidator: MarketOrderInputValidator

    override fun preProcessParsedData(parsedData: MarketOrderParsedData): Boolean {
        val marketOrderContext = parsedData.messageWrapper.context as MarketOrderContext

        if (messageProcessingStatusHolder.isTradeDisabled(marketOrderContext.assetPair)) {
            writeResponse(parsedData.messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return false
        }

        if (!validateData(marketOrderContext, parsedData.messageWrapper)) {
            return false
        }

        if (isDuplicated()) {
            return false
        }

        return true
    }

    private fun validateData(marketOrderContext: MarketOrderContext, messageWrapper: MessageWrapper): Boolean {
        try {
            marketOrderInputValidator.performValidation(marketOrderContext)
        } catch (e: OrderValidationException) {
            processInvalidData(messageWrapper, marketOrderContext, e.orderStatus, e.message)
            return false
        }

        return true
    }

    private fun isDuplicated(): Boolean {

    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        val marketOrderResponse = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setStatus(status.type)
        if (order.price != null) {
            marketOrderResponse.price = order.price!!.toDouble()
        } else if (reason != null) {
            marketOrderResponse.statusReason = reason
        }
        messageWrapper.writeMarketOrderResponse(marketOrderResponse)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   order: MarketOrder,
                                   statusReason: String? = null) {
        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), statusReason)
    }

    fun processInvalidData(messageWrapper: MessageWrapper,
                           context: MarketOrderContext,
                           status: OrderStatus,
                           message: String) {
        val marketOrder = context.marketOrder
        marketOrder.updateStatus(status, Date())
        //todo: Should we send RMQ notification ?
        writeErrorResponse(messageWrapper, marketOrder, message)
    }
}