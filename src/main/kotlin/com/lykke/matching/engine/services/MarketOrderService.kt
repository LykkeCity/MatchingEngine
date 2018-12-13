package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
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
import com.lykke.matching.engine.order.OrderStatus.TooHighPriceDeviation
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.process.context.MarketOrderExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date
import java.util.UUID
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
        private val applicationSettingsHolder: ApplicationSettingsHolder,
        private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
        private val messageSender: MessageSender): AbstractService {
    companion object {
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
        val now = Date()
        val feeInstruction: FeeInstruction?
        val feeInstructions: List<NewFeeInstruction>?
        val parsedMessage = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder
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
            val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order)
            rabbitSwapQueue.put(marketOrderWithTrades)
            val outgoingMessage = EventFactory.createExecutionEvent(messageSequenceNumberHolder.getNewValue(),
                    messageWrapper.messageId!!,
                    messageWrapper.id!!,
                    now,
                    MessageType.MARKET_ORDER, marketOrderWithTrades)
            messageSender.sendMessage(outgoingMessage)
            writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(e.orderStatus), e.message)
            return
        }

        val assetPair = getAssetPair(order)

        val executionContext = executionContextFactory.create(messageWrapper.messageId!!,
                messageWrapper.id!!,
                MessageType.MARKET_ORDER,
                messageWrapper.processedMessage,
                mapOf(Pair(assetPair.assetPairId, assetPair)),
                now,
                LOGGER)

        val marketOrderExecutionContext = MarketOrderExecutionContext(order, executionContext)

        val matchingResult = matchingEngine.match(order,
                getOrderBook(order),
                messageWrapper.messageId!!,
                priceDeviationThreshold = applicationSettingsHolder.marketOrderPriceDeviationThreshold(assetPair.assetPairId),
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
            TooHighPriceDeviation -> {
                marketOrderExecutionContext.executionContext.marketOrderWithTrades = MarketOrderWithTrades(executionContext.messageId, order)
            }
            Matched -> {
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
                    order.updateStatus(OrderStatus.NotEnoughFunds, now)
                    marketOrderExecutionContext.executionContext.marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order)
                    LOGGER.error("$order: Unable to process wallet operations after matching: ${e.message}")
                    false
                }

                if (preProcessResult) {
                    matchingResult.apply()
                    executionContext.orderBooksHolder.addCompletedOrders(matchingResult.completedLimitOrders.map { it.origin!! })

                    if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                        matchingResultHandlingHelper.processCancelledOppositeOrders(marketOrderExecutionContext)
                    }
                    if (matchingResult.uncompletedLimitOrderCopy != null) {
                        matchingResultHandlingHelper.processUncompletedOppositeOrder(marketOrderExecutionContext)
                    }

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    marketOrderExecutionContext.executionContext.orderBooksHolder
                            .getChangedOrderBookCopy(order.assetPairId)
                            .setOrderBook(!order.isBuySide(), matchingResult.orderBook)
                    marketOrderExecutionContext.executionContext.lkkTrades.addAll(matchingResult.lkkTrades)

                    marketOrderExecutionContext.executionContext.marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order, matchingResult.marketOrderTrades)
                    matchingResult.limitOrdersReport?.orders?.let { marketOrderExecutionContext.executionContext.addClientsLimitOrdersWithTrades(it) }
                }
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

    private fun getOrderBook(order: MarketOrder) =
            genericLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(!order.isBuySide())

    private fun getAssetPair(order: MarketOrder) = assetsPairsHolder.getAssetPair(order.assetPairId)

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun writePersistenceErrorResponse(messageWrapper: MessageWrapper, order: MarketOrder) {
        val message = "Unable to save result data"
        LOGGER.error("$order: $message")
        writeResponse(messageWrapper, order, MessageStatus.RUNTIME, message)
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
        messageWrapper.writeMarketOrderResponse(marketOrderResponse)
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
        messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder()
                .setStatus(status.type))
    }
}