package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.InvalidFee
import com.lykke.matching.engine.order.OrderStatus.InvalidValue
import com.lykke.matching.engine.order.OrderStatus.InvalidVolume
import com.lykke.matching.engine.order.OrderStatus.InvalidVolumeAccuracy
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.ReservedVolumeGreaterThanBalance
import com.lykke.matching.engine.order.OrderStatus.TooHighMidPriceDeviation
import com.lykke.matching.engine.order.OrderStatus.TooHighPriceDeviation
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.process.context.MarketOrderExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue

@Service
class MarketOrderService @Autowired constructor(
        private val matchingEngine: MatchingEngine,
        private val executionContextFactory: ExecutionContextFactory,
        private val stopOrderBookProcessor: StopOrderBookProcessor,
        private val executionDataApplyService: ExecutionDataApplyService,
        private val matchingResultHandlingHelper: MatchingResultHandlingHelper,
        private val genericLimitOrderService: GenericLimitOrderService,
        private val assetsPairsHolder: AssetsPairsHolder,
        private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
        private val marketOrderValidator: MarketOrderValidator,
        private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
        private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
        private val midPriceHolder: MidPriceHolder,
        private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
        private val messageSender: MessageSender,
        private val performanceStatsHolder: PerformanceStatsHolder) : AbstractService {
    companion object {
        private val CONTROLS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.controls")
        private val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        val parsedMessage = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder

        val assetPair = assetsPairsHolder.getAssetPair(parsedMessage.assetPairId)

        val now = Date()
        val feeInstruction: FeeInstruction?
        val feeInstructions: List<NewFeeInstruction>?

        if (!messageProcessingStatusHolder.isMessageProcessingEnabled(DisabledFunctionalityRule(null, assetPair.assetPairId, MessageType.MARKET_ORDER))) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        feeInstruction = if (parsedMessage.hasFee()) FeeInstruction.create(parsedMessage.fee) else null
        feeInstructions = NewFeeInstruction.create(parsedMessage.feesList)
        LOGGER.debug("Got market order messageId: ${messageWrapper.messageId}, " +
                "id: ${parsedMessage.uid}, client: ${parsedMessage.clientId}, " +
                "asset: ${parsedMessage.assetPairId}, volume: ${NumberUtils.roundForPrint(parsedMessage.volume)}, " +
                "straight: ${parsedMessage.straight}, fee: $feeInstruction, fees: $feeInstructions")

        val order = MarketOrder(UUID.randomUUID().toString(), parsedMessage.uid, parsedMessage.assetPairId, parsedMessage.clientId, BigDecimal.valueOf(parsedMessage.volume), null,
                Processing.name, now, Date(parsedMessage.timestamp), now, null, parsedMessage.straight, BigDecimal.valueOf(parsedMessage.reservedLimitVolume),
                feeInstruction, listOfFee(feeInstruction, feeInstructions))

        try {
            marketOrderValidator.performValidation(order, getOrderBook(order), feeInstruction, feeInstructions)
        } catch (e: OrderValidationException) {
            order.updateStatus(e.orderStatus, now)
            sendErrorNotification(messageWrapper, order, now)
            writeErrorResponse(messageWrapper, order, e.message)
            return
        }


        val executionContext = executionContextFactory.create(messageWrapper.messageId!!,
                messageWrapper.id!!,
                MessageType.MARKET_ORDER,
                messageWrapper.processedMessage,
                mapOf(assetPair.assetPairId to assetPair),
                now,
                LOGGER,
                CONTROLS_LOGGER)

        val midPriceDeviationThreshold = priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPair.assetPairId, executionContext)
        val marketOrderPriceDeviationThreshold = priceDeviationThresholdHolder.getMarketOrderPriceDeviationThreshold(assetPair.assetPairId, executionContext)

        var lowerMidPriceBound: BigDecimal? = null
        var upperMidPriceBound: BigDecimal? = null
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(assetPair, executionContext)

        if (midPriceDeviationThreshold != null && !NumberUtils.equalsIgnoreScale(referenceMidPrice, BigDecimal.ZERO)) {
            lowerMidPriceBound = referenceMidPrice - (referenceMidPrice * midPriceDeviationThreshold)
            upperMidPriceBound = referenceMidPrice + (referenceMidPrice * midPriceDeviationThreshold)
        }


        val marketOrderExecutionContext = MarketOrderExecutionContext(order, executionContext)
        marketOrderExecutionContext.lowerMidPriceBound = lowerMidPriceBound
        marketOrderExecutionContext.upperMidPriceBound = upperMidPriceBound
        val assetOrderBook = genericLimitOrderService.getOrderBook(order.assetPairId)
        if (!OrderValidationUtils.isMidPriceValid(assetOrderBook.getMidPrice(), lowerMidPriceBound, upperMidPriceBound)) {
            val message = "Market order (id=${order.externalId}, assetPairId = ${order.assetPairId}), " +
                    "is rejected because order book mid price: ${NumberUtils.roundForPrint(assetOrderBook.getMidPrice())} " +
                    "already aut of range lowerBound: ${NumberUtils.roundForPrint(lowerMidPriceBound)}, upperBound: ${NumberUtils.roundForPrint(upperMidPriceBound)}"
            executionContext.error(message)
            executionContext.controlsError(message)
            order.updateStatus(TooHighMidPriceDeviation, now)
            sendErrorNotification(messageWrapper, order, now)
            return
        }

        val matchingResult = matchingEngine.match(order,
                getOrderBook(order),
                messageWrapper.messageId!!,
                lowerMidPriceBound = lowerMidPriceBound,
                upperMidPriceBound = upperMidPriceBound,
                moPriceDeviationThreshold = marketOrderPriceDeviationThreshold,
                executionContext = executionContext)
        marketOrderExecutionContext.matchingResult = matchingResult

        when (OrderStatus.valueOf(matchingResult.orderCopy.status)) {
            ReservedVolumeGreaterThanBalance,
            NoLiquidity,
            NotEnoughFunds,
            InvalidFee,
            InvalidVolumeAccuracy,
            InvalidVolume,
            InvalidValue,
            TooHighPriceDeviation,
            TooHighMidPriceDeviation -> {
                processRejectedMatchingResult(marketOrderExecutionContext)
            }
            Matched -> {
                processMatchedStatus(marketOrderExecutionContext, messageWrapper.messageId!!)
            }
            else -> {
                executionContext.error("Not handled order status: ${matchingResult.orderCopy.status}")
            }
        }

        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)
        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)
        if (!persisted) {
            writePersistenceErrorResponse(messageWrapper, order)
            return
        }

        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status))

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    fun processRejectedMatchingResult(marketOrderExecutionContext: MarketOrderExecutionContext) {
        val order = marketOrderExecutionContext.order
        val matchingResult = marketOrderExecutionContext.matchingResult!!

        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            matchingResultHandlingHelper.preProcessCancelledOppositeOrders(marketOrderExecutionContext)
            matchingResultHandlingHelper.preProcessCancelledOrdersWalletOperations(marketOrderExecutionContext)
            matchingResultHandlingHelper.processCancelledOppositeOrders(marketOrderExecutionContext)
            val orderBook = marketOrderExecutionContext.executionContext.orderBooksHolder
                    .getChangedOrderBookCopy(marketOrderExecutionContext.order.assetPairId)
            matchingResult.cancelledLimitOrders.forEach {
                orderBook.removeOrder(it.origin!!)
            }
        }
        marketOrderExecutionContext.executionContext.marketOrderWithTrades = MarketOrderWithTrades(marketOrderExecutionContext.executionContext.messageId, order)
    }

    private fun processMatchedStatus(marketOrderExecutionContext: MarketOrderExecutionContext,
                                     messageId: String) {
        val matchingResult = marketOrderExecutionContext.matchingResult!!
        val executionContext = marketOrderExecutionContext.executionContext
        val order = marketOrderExecutionContext.order

        matchingResultHandlingHelper.formOppositeOrderBookAfterMatching(marketOrderExecutionContext)

        val newMidPrice = getMidPrice(genericLimitOrderService.getOrderBook(order.assetPairId).getBestPrice(order.isBuySide()),
                matchingResult.orderBook.peek()?.price ?: BigDecimal.ZERO, marketOrderExecutionContext.executionContext.assetPairsById[order.assetPairId]!!)

        if (!OrderValidationUtils.isMidPriceValid(newMidPrice, marketOrderExecutionContext.lowerMidPriceBound, marketOrderExecutionContext.upperMidPriceBound)) {
            val message = "Market order (id: ${order.externalId}, assetPairId = ${order.assetPairId}) mid price control failed, " +
                    "m = ${NumberUtils.roundForPrint(newMidPrice)}, l = ${NumberUtils.roundForPrint(marketOrderExecutionContext.lowerMidPriceBound)}, " +
                    "u = ${NumberUtils.roundForPrint(marketOrderExecutionContext.upperMidPriceBound)}"
            executionContext.error(message)
            executionContext.controlsError(message)
            order.updateStatus(TooHighMidPriceDeviation, executionContext.date)
            processRejectedMatchingResult(marketOrderExecutionContext)
            return
        }

        executionContext.controlsInfo("Market order (id: ${order.externalId}, assetPairId = ${order.assetPairId}), mid price control passed, " +
                "m = ${NumberUtils.roundForPrint(newMidPrice)}, l = ${NumberUtils.roundForPrint(marketOrderExecutionContext.lowerMidPriceBound)}, " +
                "u = ${NumberUtils.roundForPrint(marketOrderExecutionContext.upperMidPriceBound)}")

        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            matchingResultHandlingHelper.preProcessCancelledOppositeOrders(marketOrderExecutionContext)
        }
        if (matchingResult.uncompletedLimitOrderCopy != null) {
            matchingResultHandlingHelper.preProcessUncompletedOppositeOrder(marketOrderExecutionContext)
        }
        marketOrderExecutionContext.ownWalletOperations = matchingResult.ownCashMovements
        val preProcessResult = try {
            matchingResultHandlingHelper.processWalletOperations(marketOrderExecutionContext)
            true
        } catch (e: BalanceException) {
            order.updateStatus(OrderStatus.NotEnoughFunds, executionContext.date)
            marketOrderExecutionContext.executionContext.marketOrderWithTrades = MarketOrderWithTrades(messageId, order)
            LOGGER.error("$order: Unable to process wallet operations after matching: ${e.message}")
            false
        }

        if (preProcessResult) {
            executionContext.orderBooksHolder.addCompletedOrders(matchingResult.completedLimitOrders.map { it.origin!! })

            if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                matchingResultHandlingHelper.processCancelledOppositeOrders(marketOrderExecutionContext)
            }
            if (matchingResult.uncompletedLimitOrderCopy != null) {
                matchingResultHandlingHelper.processUncompletedOppositeOrder(marketOrderExecutionContext)
            }

            if (newMidPrice != null) {
                executionContext.currentTransactionMidPriceHolder.addMidPrice(order.assetPairId, newMidPrice, executionContext)
            }

            matchingResult.apply()

            marketOrderExecutionContext.executionContext.orderBooksHolder
                    .getChangedOrderBookCopy(order.assetPairId)
                    .setOrderBook(!order.isBuySide(), matchingResult.orderBook)
            marketOrderExecutionContext.executionContext.lkkTrades.addAll(matchingResult.lkkTrades)

            marketOrderExecutionContext.executionContext.marketOrderWithTrades = MarketOrderWithTrades(messageId, order, matchingResult.marketOrderTrades)
            matchingResult.limitOrdersReport?.orders?.let { marketOrderExecutionContext.executionContext.addClientsLimitOrdersWithTrades(it) }
        }
    }

    private fun getOrderBook(order: MarketOrder) =
            genericLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(!order.isBuySide())


    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun writePersistenceErrorResponse(messageWrapper: MessageWrapper, order: MarketOrder) {
        val message = "Unable to save result data"
        LOGGER.error("$order: $message")
        val start = System.nanoTime()
        writeResponse(messageWrapper, order, MessageStatus.RUNTIME, message)
        val end = System.nanoTime()
        performanceStatsHolder.addWriteResponseTime(MessageType.MULTI_LIMIT_ORDER.type, end - start)
        return
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        val marketOrderResponse = ProtocolMessages.MarketOrderResponse.newBuilder()
                .setStatus(status.type)
        if (order.price != null) {
            marketOrderResponse.price = order.price!!.toDouble()
        } else if (reason != null) {
            marketOrderResponse.statusReason = reason
        }
        val start = System.nanoTime()
        messageWrapper.writeMarketOrderResponse(marketOrderResponse)
        val end = System.nanoTime()
        performanceStatsHolder.addWriteResponseTime(MessageType.MULTI_LIMIT_ORDER.type, end - start)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper,
                                   order: MarketOrder,
                                   statusReason: String? = null) {
        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), statusReason)
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

    private fun getMidPrice(orderSideBestPrice: BigDecimal, oppositeSideBestPrice: BigDecimal, assetPair: AssetPair): BigDecimal? {
        if (NumberUtils.equalsIgnoreScale(orderSideBestPrice, BigDecimal.ZERO) || NumberUtils.equalsIgnoreScale(oppositeSideBestPrice, BigDecimal.ZERO)) {
            return null
        }

        return NumberUtils.setScaleRoundUp(NumberUtils.divideWithMaxScale(orderSideBestPrice + oppositeSideBestPrice, BigDecimal.valueOf(2)), assetPair.accuracy)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid
        messageWrapper.processedMessage = ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val start = System.nanoTime()
        messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder()
                .setStatus(status.type))
        val end = System.nanoTime()
        performanceStatsHolder.addWriteResponseTime(MessageType.MULTI_LIMIT_ORDER.type, end - start)
    }
}