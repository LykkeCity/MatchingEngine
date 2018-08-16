package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
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
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.messages.*
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

@Service
class MarketOrderService @Autowired constructor(
        private val genericLimitOrderService: GenericLimitOrderService,
        assetsHolder: AssetsHolder,
        private val assetsPairsHolder: AssetsPairsHolder,
        private val balancesHolder: BalancesHolder,
        private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
        private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
        private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
        private val orderBookQueue: BlockingQueue<OrderBook>,
        private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
        private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
        genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory? = null,
        private val marketOrderValidator: MarketOrderValidator,
        private val feeProcessor: FeeProcessor,
        private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
        private val messageSender: MessageSender): AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, feeProcessor)
    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory?.create(LOGGER)
    private val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val now = Date()
        val feeInstruction: FeeInstruction?
        val feeInstructions: List<NewFeeInstruction>?
        val order = if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldMarketOrder
            LOGGER.debug("Got old market order messageId: ${messageWrapper.messageId}, " +
                    "id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, " +
                    "volume: ${NumberUtils.roundForPrint(message.volume)}, straight: ${message.straight}")

            feeInstruction = null
            feeInstructions = null
            MarketOrder(UUID.randomUUID().toString(), message.uid.toString(), message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                    Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume))
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.MarketOrder
            feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
            feeInstructions = NewFeeInstruction.create(message.feesList)
            LOGGER.debug("Got market order messageId: ${messageWrapper.messageId}, " +
                    "id: ${message.uid}, client: ${message.clientId}, " +
                    "asset: ${message.assetPairId}, volume: ${NumberUtils.roundForPrint(message.volume)}, " +
                    "straight: ${message.straight}, fee: $feeInstruction, fees: $feeInstructions")

            MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                    Processing.name, now, Date(message.timestamp), now, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume),
                    feeInstruction, listOfFee(feeInstruction, feeInstructions))
        }

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

        val matchingResult = matchingEngine.initTransaction().match(order, getOrderBook(order), messageWrapper.messageId!!)
        when (OrderStatus.valueOf(matchingResult.order.status)) {
            ReservedVolumeGreaterThanBalance -> {
                writeErrorNotification(messageWrapper, order, now, "Reserved volume is higher than available balance")
            }
            NoLiquidity,
            NotEnoughFunds,
            InvalidFee,
            InvalidVolumeAccuracy,
            InvalidVolume,
            InvalidValue -> {
                writeErrorNotification(messageWrapper, order, now)
            }
            Matched -> {
                val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                    val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.map {
                        val cancelledOrder = it.copy
                        cancelledOrder.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
                        cancelledOrder
                    })
                    cancelledOrdersWalletOperations.addAll(result.walletOperations)
                    cancelledOrdersWithTrades.addAll(result.clientLimitOrderWithTrades)
                    cancelledTrustedOrdersWithTrades.addAll(result.trustedClientLimitOrderWithTrades)
                }
                val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

                val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
                val preProcessResult = try {
                    walletOperationsProcessor.preProcess(matchingResult.ownCashMovements).preProcess(matchingResult.oppositeCashMovements, true)
                    try {
                        walletOperationsProcessor.preProcess(cancelledOrdersWalletOperations)
                    } catch (e: BalanceException) {
                        LOGGER.error("$order: Unable to process cancelled orders wallet operations after matching: ${e.message}")
                    }
                    true
                } catch (e: BalanceException) {
                    order.updateStatus(OrderStatus.NotEnoughFunds, matchingResult.timestamp)
                    val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order)
                    rabbitSwapQueue.put(marketOrderWithTrades)
                    val outgoingMessage = EventFactory.createExecutionEvent(messageSequenceNumberHolder.getNewValue(),
                            messageWrapper.messageId!!,
                            messageWrapper.id!!,
                            now,
                            MessageType.MARKET_ORDER,
                            marketOrderWithTrades)
                    messageSender.sendMessage(outgoingMessage)
                    LOGGER.error("$order: Unable to process wallet operations after matching: ${e.message}")
                    writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), e.message)
                    false
                }

                val clientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
                val trustedClientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
                if (preProcessResult) {
                    trustedClientLimitOrdersReport.orders.addAll(cancelledTrustedOrdersWithTrades)

                    val sequenceNumber = messageSequenceNumberHolder.getNewValue()
                    val trustedClientsSequenceNumber = if (trustedClientLimitOrdersReport.orders.isNotEmpty())
                        messageSequenceNumberHolder.getNewValue() else null
                    val updated = walletOperationsProcessor.persistBalances(messageWrapper.processedMessage(), trustedClientsSequenceNumber ?: sequenceNumber)
                    messageWrapper.triedToPersist = true
                    messageWrapper.persisted = updated
                    if (!updated) {
                        val message = "Unable to save result data"
                        LOGGER.error("$order: $message")
                        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), message)
                        return
                    }
                    walletOperationsProcessor.apply().sendNotification(order.externalId, MessageType.MARKET_ORDER.name, messageWrapper.messageId!!)

                    matchingResult.apply()
                    matchingEngine.apply()
                    genericLimitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders.map { it.origin!! })
                    val ordersToCancel = matchingResult.cancelledLimitOrders.map { it.origin!! }.toMutableList()
                    orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult, ordersToCancel)
                    genericLimitOrderService.cancelLimitOrders(ordersToCancel, matchingResult.timestamp)

                    clientLimitOrdersReport.orders.addAll(cancelledOrdersWithTrades)

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    genericLimitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)
                    genericLimitOrderService.updateOrderBook(order.assetPairId, !order.isBuySide())

                    lkkTradesQueue.put(matchingResult.lkkTrades)

                    val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order, matchingResult.marketOrderTrades.toMutableList())
                    rabbitSwapQueue.put(marketOrderWithTrades)

                    matchingResult.limitOrdersReport?.let {
                        clientLimitOrdersReport.orders.addAll(it.orders)
                    }

                    clientLimitOrdersQueue.put(clientLimitOrdersReport)
                    if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
                        trustedClientsLimitOrdersQueue.put(trustedClientLimitOrdersReport)
                    }

                    val outgoingMessage = EventFactory.createExecutionEvent(sequenceNumber,
                            messageWrapper.messageId!!,
                            messageWrapper.id!!,
                            now,
                            MessageType.MARKET_ORDER,
                            walletOperationsProcessor.getClientBalanceUpdates(),
                            clientLimitOrdersReport.orders,
                            marketOrderWithTrades)
                    messageSender.sendMessage(outgoingMessage)

                    if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
                        val trustedClientsOutgoingMessage = EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                                messageWrapper.messageId!!,
                                messageWrapper.id!!,
                                now,
                                MessageType.MARKET_ORDER,
                                trustedClientLimitOrdersReport.orders)
                        messageSender.sendTrustedClientsMessage(trustedClientsOutgoingMessage)
                    }

                    val orderBook = genericLimitOrderService.getOrderBook(order.assetPairId)
                    val newOrderBook = OrderBook(order.assetPairId, !order.isBuySide(), order.matchedAt!!, orderBook.getCopyOfOrderBook(!order.isBuySide()))
                    genericLimitOrderService.putTradeInfo(TradeInfo(order.assetPairId, !order.isBuySide(), if (order.isBuySide()) orderBook.getAskPrice() else orderBook.getBidPrice(), now))
                    orderBookQueue.put(newOrderBook)
                    rabbitOrderBookQueue.put(newOrderBook)
                    writeResponse(messageWrapper, order, MessageStatus.OK)
                }
            }
            else -> {
            }
        }

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!,
                assetPair.assetPairId,
                now)

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

    private fun parseOld(array: ByteArray): ProtocolMessages.OldMarketOrder {
        return ProtocolMessages.OldMarketOrder.parseFrom(array)
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun writeErrorNotification(messageWrapper: MessageWrapper,
                                       order: MarketOrder,
                                       now: Date,
                                       statusReason: String? = null) {
        val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId!!, order)
        rabbitSwapQueue.put(marketOrderWithTrades)
        val outgoingMessage = EventFactory.createExecutionEvent(messageSequenceNumberHolder.getNewValue(),
                messageWrapper.messageId!!,
                messageWrapper.id!!,
                now,
                MessageType.MARKET_ORDER,
                marketOrderWithTrades)
        messageSender.sendMessage(outgoingMessage)
        writeResponse(messageWrapper, order, MessageStatusUtils.toMessageStatus(order.status), statusReason)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setRecordId(order.id))
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            val newResponseBuilder = ProtocolMessages.NewResponse.newBuilder()
                    .setMatchingEngineId(order.id)
                    .setStatus(status.type)

            if (reason != null) {
                newResponseBuilder.statusReason = reason
            }

            messageWrapper.writeNewResponse(newResponseBuilder)

        } else {
            val marketOrderResponse = ProtocolMessages.MarketOrderResponse.newBuilder()
                    .setStatus(status.type)

            if (order.price != null) {
                marketOrderResponse.price = order.price!!.toDouble()
            } else if (reason != null) {
                marketOrderResponse.statusReason = reason
            }

            messageWrapper.writeMarketOrderResponse(marketOrderResponse)
        }
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message = parseOld(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid.toString()
        } else {
            val message = parse(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(status.type))
        } else {
            messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder()
                    .setStatus(status.type))
        }
    }
}