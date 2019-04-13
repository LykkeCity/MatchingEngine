package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.MarketOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.MarketOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.daos.context.MarketOrderContext
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.input.MarketOrderInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.BlockingQueue

@Component
class MarketOrderPreprocessor(marketOrderContextParser: MarketOrderContextParser,
                              preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                              private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                              private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
                              private val messageSender: MessageSender,
                              private val processedMessagesCache: ProcessedMessagesCache,
                              private val marketOrderPreprocessorPersistenceManager: PersistenceManager,
                              private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                              @Qualifier("marketOrderPreProcessingLogger")
                              private val logger: ThrottlingLogger) :
        AbstractMessagePreprocessor<MarketOrderParsedData>(marketOrderContextParser,
                messageProcessingStatusHolder,
                preProcessedMessageQueue,
                logger) {

    private companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

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
        logger.info("Input validation failed messageId: ${context.messageId}, details: $message")


        //todo: Should we send RMQ notification ?
        sendErrorNotification(messageWrapper, context.marketOrder, Date())
        saveProcessedMessage(messageWrapper, context)

        writeErrorResponse(messageWrapper, marketOrder, message)
    }

    private fun saveProcessedMessage(messageWrapper: MessageWrapper,
                                     context: MarketOrderContext) {

        val persistSuccess = marketOrderPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))
        if (!persistSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
        } catch (e: Exception) {
            logger.error("Error occurred during processing of invalid market order data, context $context", e)
            METRICS_LOGGER.logError("Error occurred during invalid data processing, ${messageWrapper.type} ${context.messageId}")
        }
    }

    private fun sendErrorNotification(messageWrapper: MessageWrapper,
                                      order: MarketOrder,
                                      now: Date) {
        val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order)
        rabbitSwapQueue.put(marketOrderWithTrades)
        val outgoingMessage = EventFactory.createExecutionEvent(messageSequenceNumberHolder.getNewValue(),
                messageWrapper.messageId!!,
                messageWrapper.id!!,
                now,
                MessageType.MARKET_ORDER,
                marketOrderWithTrades)
        messageSender.sendMessage(outgoingMessage)
    }
}